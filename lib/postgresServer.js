/* eslint-disable */
/**
 * [This Library emulates the PostgreSQL server protocol, giving you the ability to create PostgreSQL-like service]
 */
const crypto = require('crypto');
const { Buffer } = require('buffer');
const utill = require('./utill');
const ConnectionPool = require('./ConnectionPool');
const log = require('./log');

// PostgreSQL message types
const MESSAGES = {
  // Backend messages
  AUTHENTICATION_OK: 'R',
  AUTHENTICATION_CLEAR_TEXT: 'R',
  AUTHENTICATION_MD5: 'R',
  BACKEND_KEY_DATA: 'K',
  BIND_COMPLETE: '2',
  CLOSE_COMPLETE: '3',
  COMMAND_COMPLETE: 'C',
  DATA_ROW: 'D',
  EMPTY_QUERY_RESPONSE: 'I',
  ERROR_RESPONSE: 'E',
  NO_DATA: 'n',
  NOTICE_RESPONSE: 'N',
  NOTIFICATION_RESPONSE: 'A',
  PARAMETER_DESCRIPTION: 't',
  PARAMETER_STATUS: 'S',
  PARSE_COMPLETE: '1',
  PORTAL_SUSPENDED: 's',
  READY_FOR_QUERY: 'Z',
  ROW_DESCRIPTION: 'T',
  
  // Frontend messages
  BIND: 'B',
  CLOSE: 'C',
  DESCRIBE: 'D',
  EXECUTE: 'E',
  FLUSH: 'H',
  PARSE: 'P',
  PASSWORD_MESSAGE: 'p',
  QUERY: 'Q',
  STARTUP_MESSAGE: null, // Special case - no type byte
  SYNC: 'S',
  TERMINATE: 'X'
};

// Transaction status indicators
const TRANSACTION_STATUS = {
  IDLE: 'I',          // Not in a transaction block
  IN_TRANSACTION: 'T', // In a transaction block
  ERROR: 'E'          // In a failed transaction block
};

class PostgreSQLServer {
  constructor(opts) {
    try {
      Object.assign(this, opts);
      
      if (!this.banner) this.banner = "PostgreSQL 14.0";
      if (!this.salt) this.salt = crypto.randomBytes(4);
      
      this.connId = null;
      this.sequence = 0;
      this.phase = 'startup'; // startup, authentication, ready
      this.incoming = Buffer.alloc(0);
      this.authenticated = false;
      this.transactionStatus = TRANSACTION_STATUS.IDLE;
      this.processId = Math.floor(Math.random() * 1000000);
      this.secretKey = Math.floor(Math.random() * 1000000);
      
      this.parameters = new Map([
        ['client_encoding', 'UTF8'],
        ['DateStyle', 'ISO, MDY'],
        ['integer_datetimes', 'on'],
        ['IntervalStyle', 'postgres'],
        ['is_superuser', 'off'],
        ['server_encoding', 'UTF8'],
        ['server_version', '14.0'],
        ['session_authorization', this.username || 'postgres'],
        ['standard_conforming_strings', 'on'],
        ['TimeZone', 'UTC']
      ]);

      this.socket.on('data', this.handleData.bind(this));
      this.socket.on("error", (err) => {
        log.error(`PostgreSQL connection error: ${err.message}`);
        ConnectionPool.releaseConnection(this.connId);
        if (this.operator && this.operator.sessionQueries) {
          this.operator.sessionQueries[this.connId] = undefined;
        }
      });
      this.socket.on("end", () => {
        log.info(`PostgreSQL client disconnected: ${this.connId}`);
        ConnectionPool.releaseConnection(this.connId);
        if (this.operator && this.operator.sessionQueries) {
          this.operator.sessionQueries[this.connId] = undefined;
        }
      });

      // Wait for startup message instead of sending hello immediately
      log.info('PostgreSQL server connection established, waiting for startup message');
    } catch (err) {
      log.error(`PostgreSQL server initialization error: ${err.message}`);
    }
  }

  end = () => {
    try {
      this.socket.end();
    } catch (err) {
      // Silent fail
    }
  };

  handleData = (data) => {
    try {
      this.incoming = Buffer.concat([this.incoming, data]);
      this.processMessages();
    } catch (err) {
      log.error(`Error handling PostgreSQL data: ${err.message}`);
      this.sendErrorResponse('XX000', 'Internal server error');
    }
  };

  processMessages() {
    while (this.incoming.length > 0) {
      if (this.phase === 'startup') {
        if (this.incoming.length < 4) return; // Need at least length

        const messageLength = this.incoming.readUInt32BE(0);
        if (this.incoming.length < messageLength) return; // Need complete message

        this.handleStartupMessage();
        this.incoming = this.incoming.subarray(messageLength);
      } else {
        if (this.incoming.length < 5) return; // Need at least type + length

        const messageType = String.fromCharCode(this.incoming[0]);
        const messageLength = this.incoming.readUInt32BE(1);

        if (this.incoming.length < messageLength + 1) return; // Need complete message

        const messageData = this.incoming.subarray(5, messageLength + 1);
        this.handleMessage(messageType, messageData);
        this.incoming = this.incoming.subarray(messageLength + 1);
      }
    }
  }

  handleStartupMessage() {
    try {
      const messageLength = this.incoming.readUInt32BE(0);
      const protocolVersion = this.incoming.readUInt32BE(4);
      
      // PostgreSQL protocol version 3.0
      if (protocolVersion !== 0x00030000) {
        this.sendErrorResponse('08P01', `Unsupported protocol version: ${protocolVersion.toString(16)}`);
        return;
      }

      // Parse startup parameters
      let offset = 8;
      const params = new Map();
      
      while (offset < messageLength - 1) {
        const keyStart = offset;
        while (offset < messageLength && this.incoming[offset] !== 0) offset++;
        if (offset >= messageLength) break;
        
        const key = this.incoming.subarray(keyStart, offset).toString('utf8');
        offset++; // Skip null terminator
        
        const valueStart = offset;
        while (offset < messageLength && this.incoming[offset] !== 0) offset++;
        if (offset >= messageLength) break;
        
        const value = this.incoming.subarray(valueStart, offset).toString('utf8');
        offset++; // Skip null terminator
        
        params.set(key, value);
      }

      this.username = params.get('user') || 'postgres';
      this.database = params.get('database') || 'postgres';

      log.info(`PostgreSQL startup: user=${this.username}, database=${this.database}`);

      this.phase = 'authentication';
      this.sendAuthenticationOk();
      this.sendParameterStatus();
      this.sendBackendKeyData();
      this.sendReadyForQuery();
    } catch (err) {
      log.error(`Error handling startup message: ${err.message}`);
      this.sendErrorResponse('08P01', 'Invalid startup message');
    }
  }

  handleMessage(messageType, messageData) {
    try {
      switch (messageType) {
        case MESSAGES.QUERY:
          this.handleQuery(messageData);
          break;
        case MESSAGES.PARSE:
          this.handleParse(messageData);
          break;
        case MESSAGES.BIND:
          this.handleBind(messageData);
          break;
        case MESSAGES.DESCRIBE:
          this.handleDescribe(messageData);
          break;
        case MESSAGES.EXECUTE:
          this.handleExecute(messageData);
          break;
        case MESSAGES.SYNC:
          this.handleSync(messageData);
          break;
        case MESSAGES.TERMINATE:
          this.handleTerminate();
          break;
        case MESSAGES.PASSWORD_MESSAGE:
          this.handlePasswordMessage(messageData);
          break;
        default:
          log.warn(`Unhandled PostgreSQL message type: ${messageType}`);
          this.sendErrorResponse('08P01', `Unknown message type: ${messageType}`);
      }
    } catch (err) {
      log.error(`Error handling PostgreSQL message ${messageType}: ${err.message}`);
      this.sendErrorResponse('XX000', 'Internal server error');
    }
  }

  handleQuery(messageData) {
    try {
      const query = messageData.subarray(0, messageData.length - 1).toString('utf8');
      log.info(`PostgreSQL Query: ${query}`);

      if (!query.trim()) {
        this.sendEmptyQueryResponse();
        this.sendReadyForQuery();
        return;
      }

      // Store query for processing
      this.currentQuery = query;

      if (this.onQuery) {
        this.onQuery(query, this.connId, (result) => {
          this.handleQueryResult(result);
        });
      } else {
        this.sendErrorResponse('XX000', 'No query handler defined');
      }
    } catch (err) {
      log.error(`Error handling query: ${err.message}`);
      this.sendErrorResponse('XX000', 'Query processing error');
    }
  }

  handleQueryResult(result) {
    try {
      if (result.error) {
        this.sendErrorResponse(result.code || 'XX000', result.error);
      } else if (Array.isArray(result)) {
        // SELECT result
        if (result.length > 0) {
          this.sendRowDescription(result);
          result.forEach(row => this.sendDataRow(row));
          this.sendCommandComplete(`SELECT ${result.length}`);
        } else {
          this.sendCommandComplete('SELECT 0');
        }
      } else if (result.affectedRows !== undefined) {
        // INSERT/UPDATE/DELETE result
        const command = this.getCommandFromQuery(this.currentQuery);
        this.sendCommandComplete(`${command} ${result.affectedRows || 0}`);
      } else {
        // Other commands
        const command = this.getCommandFromQuery(this.currentQuery);
        this.sendCommandComplete(command);
      }
      this.sendReadyForQuery();
    } catch (err) {
      log.error(`Error handling query result: ${err.message}`);
      this.sendErrorResponse('XX000', 'Result processing error');
      this.sendReadyForQuery();
    }
  }

  getCommandFromQuery(query) {
    const trimmed = query.trim().toUpperCase();
    if (trimmed.startsWith('SELECT')) return 'SELECT';
    if (trimmed.startsWith('INSERT')) return 'INSERT';
    if (trimmed.startsWith('UPDATE')) return 'UPDATE';
    if (trimmed.startsWith('DELETE')) return 'DELETE';
    if (trimmed.startsWith('CREATE')) return 'CREATE';
    if (trimmed.startsWith('DROP')) return 'DROP';
    if (trimmed.startsWith('ALTER')) return 'ALTER';
    return 'UNKNOWN';
  }

  handleParse(messageData) {
    // For now, just send parse complete
    this.sendParseComplete();
  }

  handleBind(messageData) {
    // For now, just send bind complete
    this.sendBindComplete();
  }

  handleDescribe(messageData) {
    // For now, just send no data
    this.sendNoData();
  }

  handleExecute(messageData) {
    // For extended protocol - not implemented for now
    this.sendCommandComplete('SELECT 0');
  }

  handleSync(messageData) {
    this.sendReadyForQuery();
  }

  handleTerminate() {
    this.socket.end();
  }

  handlePasswordMessage(messageData) {
    // Simple authentication - accept any password for now
    this.authenticated = true;
    this.sendAuthenticationOk();
    this.sendParameterStatus();
    this.sendBackendKeyData();
    this.sendReadyForQuery();
  }

  // Message sending methods
  sendMessage(type, data) {
    const typeBuffer = type ? Buffer.from([type.charCodeAt(0)]) : Buffer.alloc(0);
    const lengthBuffer = Buffer.alloc(4);
    const totalLength = 4 + (data ? data.length : 0);
    lengthBuffer.writeUInt32BE(totalLength, 0);
    
    const message = Buffer.concat([typeBuffer, lengthBuffer, data || Buffer.alloc(0)]);
    this.socket.write(message);
  }

  sendAuthenticationOk() {
    const data = Buffer.alloc(4);
    data.writeUInt32BE(0, 0); // Authentication OK
    this.sendMessage(MESSAGES.AUTHENTICATION_OK, data);
  }

  sendParameterStatus() {
    for (const [key, value] of this.parameters) {
      const keyBuffer = Buffer.from(key + '\0', 'utf8');
      const valueBuffer = Buffer.from(value + '\0', 'utf8');
      const data = Buffer.concat([keyBuffer, valueBuffer]);
      this.sendMessage(MESSAGES.PARAMETER_STATUS, data);
    }
  }

  sendBackendKeyData() {
    const data = Buffer.alloc(8);
    data.writeUInt32BE(this.processId, 0);
    data.writeUInt32BE(this.secretKey, 4);
    this.sendMessage(MESSAGES.BACKEND_KEY_DATA, data);
  }

  sendReadyForQuery() {
    const data = Buffer.from([this.transactionStatus.charCodeAt(0)]);
    this.sendMessage(MESSAGES.READY_FOR_QUERY, data);
  }

  sendErrorResponse(code, message) {
    const severity = Buffer.from('ERROR\0', 'utf8');
    const codeField = Buffer.from(`C${code}\0`, 'utf8');
    const messageField = Buffer.from(`M${message}\0`, 'utf8');
    const data = Buffer.concat([
      Buffer.from('S'),
      severity,
      codeField,
      messageField,
      Buffer.from('\0')
    ]);
    this.sendMessage(MESSAGES.ERROR_RESPONSE, data);
  }

  sendCommandComplete(command) {
    const data = Buffer.from(command + '\0', 'utf8');
    this.sendMessage(MESSAGES.COMMAND_COMPLETE, data);
  }

  sendEmptyQueryResponse() {
    this.sendMessage(MESSAGES.EMPTY_QUERY_RESPONSE, null);
  }

  sendParseComplete() {
    this.sendMessage(MESSAGES.PARSE_COMPLETE, null);
  }

  sendBindComplete() {
    this.sendMessage(MESSAGES.BIND_COMPLETE, null);
  }

  sendNoData() {
    this.sendMessage(MESSAGES.NO_DATA, null);
  }

  sendRowDescription(rows) {
    if (!rows || rows.length === 0) return;
    
    const firstRow = rows[0];
    const columns = Object.keys(firstRow);
    const fieldCount = Buffer.alloc(2);
    fieldCount.writeUInt16BE(columns.length, 0);
    
    let fieldsData = fieldCount;
    
    columns.forEach((column, index) => {
      const name = Buffer.from(column + '\0', 'utf8');
      const tableOid = Buffer.alloc(4); // 0 for now
      const columnIndex = Buffer.alloc(2);
      columnIndex.writeUInt16BE(index + 1, 0);
      const typeOid = Buffer.alloc(4);
      typeOid.writeUInt32BE(25, 0); // TEXT type for simplicity
      const typeSize = Buffer.alloc(2);
      typeSize.writeInt16BE(-1, 0); // Variable length
      const typeMod = Buffer.alloc(4);
      typeMod.writeInt32BE(-1, 0);
      const format = Buffer.alloc(2); // 0 = text format
      
      fieldsData = Buffer.concat([
        fieldsData,
        name,
        tableOid,
        columnIndex,
        typeOid,
        typeSize,
        typeMod,
        format
      ]);
    });
    
    this.sendMessage(MESSAGES.ROW_DESCRIPTION, fieldsData);
  }

  sendDataRow(row) {
    const columns = Object.keys(row);
    const fieldCount = Buffer.alloc(2);
    fieldCount.writeUInt16BE(columns.length, 0);
    
    let rowData = fieldCount;
    
    columns.forEach(column => {
      const value = row[column];
      if (value === null || value === undefined) {
        const length = Buffer.alloc(4);
        length.writeInt32BE(-1, 0); // NULL
        rowData = Buffer.concat([rowData, length]);
      } else {
        const stringValue = String(value);
        const valueBuffer = Buffer.from(stringValue, 'utf8');
        const length = Buffer.alloc(4);
        length.writeUInt32BE(valueBuffer.length, 0);
        rowData = Buffer.concat([rowData, length, valueBuffer]);
      }
    });
    
    this.sendMessage(MESSAGES.DATA_ROW, rowData);
  }
}

module.exports = { PostgreSQLServer, MESSAGES, TRANSACTION_STATUS };