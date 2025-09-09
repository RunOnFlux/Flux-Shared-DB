/* eslint-disable */
/**
* [This Library emulates the PostgreSQL server protocol, giving you the ability to create PostgreSQL-like service]
*/
const consts = require('./postgresqlConstants');
const crypto = require('crypto');
const utill = require('./utill');
const ConnectionPool = require('./ConnectionPool');
const log = require('./log');

class Server {
  constructor(opts) {
    try{
      Object.assign(this, opts);
      
      if (!this.banner) this.banner = "PostgreSQL 13.0";
      if (!this.salt) this.salt = crypto.randomBytes(4); 
      this.connId = null;
      this.sequence = 0;
      this.onPacket = this.startupPacketHandler;
      this.incoming = [];
      this.protocolVersion = null;
      this.processId = process.pid;
      this.secretKey = crypto.randomBytes(4).readUInt32BE(0);
      
      this.socket.on('data', this.handleData);
      this.socket.on("error", (err) => {
        ConnectionPool.releaseConnection(this.connId);
        this.operator.sessionQueries[this.connId] = undefined
      });
      this.socket.on("end", () => {
        ConnectionPool.releaseConnection(this.connId);
        this.operator.sessionQueries[this.connId] = undefined
      });
      
      // PostgreSQL doesn't send a hello packet immediately like MySQL
      // It waits for the client to send a startup message
    } catch(err) {
      console.log(err);
    }
  }
 
  end = () => {
    try{
      this.socket.end();
    } catch(err) {
      // console.log(err);
    }
  };

  sendMessage(type, payload) {
    const header = Buffer.alloc(5);
    header.write(type, 0, 1);
    header.writeUInt32BE(payload.length + 4, 1);
    return this.socket.write(Buffer.concat([header, payload]));
  }

  sendMessageWithoutType(payload) {
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length + 4, 0);
    return this.socket.write(Buffer.concat([header, payload]));
  }

  newFieldDefinition(params) {
    return {
      name: params.name || '',
      tableOid: params.tableOid || 0,
      columnIndex: params.columnIndex || 0,
      typeOid: params.typeOid || consts.PG_TYPE_TEXT,
      typeSize: params.typeSize || -1,
      typeModifier: params.typeModifier || -1,
      formatCode: params.formatCode || consts.FORMAT_TEXT
    };
  }

  sendRowDescription(fields) {
    let payload = Buffer.alloc(1024);
    let offset = 0;
    
    // Number of fields
    offset = payload.writeUInt16BE(fields.length, offset);
    
    for (let field of fields) {
      // Field name (null-terminated string)
      offset += payload.write(field.name, offset);
      offset = payload.writeUInt8(0, offset);
      
      // Table OID
      offset = payload.writeUInt32BE(field.tableOid || 0, offset);
      
      // Column index
      offset = payload.writeUInt16BE(field.columnIndex || 0, offset);
      
      // Type OID
      offset = payload.writeUInt32BE(field.typeOid || consts.PG_TYPE_TEXT, offset);
      
      // Type size
      offset = payload.writeUInt16BE(field.typeSize || -1, offset);
      
      // Type modifier
      offset = payload.writeUInt32BE(field.typeModifier || -1, offset);
      
      // Format code
      offset = payload.writeUInt16BE(field.formatCode || consts.FORMAT_TEXT, offset);
    }
    
    this.sendMessage(consts.MSG_ROW_DESCRIPTION, payload.slice(0, offset));
  }

  sendDataRow(row) {
    let payload = Buffer.alloc(1024);
    let offset = 0;
    
    // Number of columns
    offset = payload.writeUInt16BE(row.length, offset);
    
    for (let cell of row) {
      if (cell == null) {
        // NULL value
        offset = payload.writeUInt32BE(-1, offset);
      } else {
        const cellStr = cell.toString();
        const cellBytes = Buffer.from(cellStr, 'utf8');
        offset = payload.writeUInt32BE(cellBytes.length, offset);
        offset += cellBytes.copy(payload, offset);
      }
    }
    
    this.sendMessage(consts.MSG_DATA_ROW, payload.slice(0, offset));
  }

  sendDataRows(rows = []) {
    for (let row of rows) {
      this.sendDataRow(row);
    }
  }

  sendCommandComplete(commandTag) {
    const payload = Buffer.from(commandTag + '\0');
    this.sendMessage(consts.MSG_COMMAND_COMPLETE, payload);
  }

  sendReadyForQuery(transactionStatus = consts.TRANS_IDLE) {
    const payload = Buffer.from(transactionStatus);
    this.sendMessage(consts.MSG_READY_FOR_QUERY, payload);
  }

  sendAuthenticationOk() {
    const payload = Buffer.alloc(4);
    payload.writeUInt32BE(consts.AUTH_OK, 0);
    this.sendMessage(consts.MSG_AUTHENTICATION, payload);
  }

  sendAuthenticationMD5Password(salt) {
    const payload = Buffer.alloc(8);
    payload.writeUInt32BE(consts.AUTH_MD5_PASSWORD, 0);
    salt.copy(payload, 4);
    this.sendMessage(consts.MSG_AUTHENTICATION, payload);
  }

  sendBackendKeyData() {
    const payload = Buffer.alloc(8);
    payload.writeUInt32BE(this.processId, 0);
    payload.writeUInt32BE(this.secretKey, 4);
    this.sendMessage(consts.MSG_BACKEND_KEY_DATA, payload);
  }

  sendParameterStatus(name, value) {
    const nameBytes = Buffer.from(name + '\0');
    const valueBytes = Buffer.from(value + '\0');
    const payload = Buffer.concat([nameBytes, valueBytes]);
    this.sendMessage(consts.MSG_PARAMETER_STATUS, payload);
  }

  handleData = (data) => {
    try{
      if (data && data.length > 0) {
        this.incoming.push(data);
      }
      this.gatherIncoming();
      if (data == null) {
        log.info("Connection closed");
        this.socket.destroy();
      }
    } catch(err) {
      // console.log(err);
    }
  }
 
  gatherIncoming() {
    try{
      let incoming;
      if (this.incoming.length > 1) {
        let len = 0;
        for (let buf of this.incoming) {
          len += buf.length;
        }
        incoming = Buffer.alloc(len);
        len = 0;
        for (let buf of this.incoming) {
          len += buf.copy(incoming, len); 
        }
      } else if (this.incoming.length === 1) {
        incoming = this.incoming[0];
      } else {
        return;
      }
      let remaining = this.readPackets(incoming);
      this.incoming = remaining.length > 0 ? [remaining] : [];
    } catch(err) {
      // console.log(err);
    }
  }
 
  readPackets(buf) {
    let offset = 0;
    while (true) {
      let data = buf.slice(offset);
      
      if (this.protocolVersion === null) {
        // First packet is startup message (no type byte)
        if (data.length < 4) return data;
        
        let packetLength = data.readUInt32BE(0);
        if (data.length < packetLength) return data;
        
        let packet = data.slice(4, packetLength);
        offset += packetLength;
        
        this.onPacket(packet);
      } else {
        // Regular message format (type byte + length + payload)
        if (data.length < 5) return data;
        
        let messageType = data.toString('ascii', 0, 1);
        let packetLength = data.readUInt32BE(1);
        if (data.length < packetLength + 1) return data;
        
        let packet = data.slice(5, packetLength + 1);
        offset += packetLength + 1;
        
        this.onPacket(messageType, packet);
      }
      
      if (offset >= buf.length) break;
    }
    return buf.slice(offset);
  }

  startupPacketHandler = (packet) => {
    try {
      if (packet.length < 4) return this.sendError({ message: "Invalid startup packet" });
      
      let ptr = 0;
      this.protocolVersion = packet.readUInt32BE(ptr);
      ptr += 4;
      
      // Parse parameters
      let parameters = {};
      while (ptr < packet.length - 1) {
        let keyEnd = packet.indexOf(0, ptr);
        if (keyEnd === -1) break;
        
        let key = packet.toString('utf8', ptr, keyEnd);
        ptr = keyEnd + 1;
        
        let valueEnd = packet.indexOf(0, ptr);
        if (valueEnd === -1) break;
        
        let value = packet.toString('utf8', ptr, valueEnd);
        ptr = valueEnd + 1;
        
        parameters[key] = value;
      }
      
      this.database = parameters.database;
      this.user = parameters.user;
      this.applicationName = parameters.application_name;
      
      this.onPacket = this.normalPacketHandler;
      const remoteIP = utill.convertIP(this.socket.remoteAddress);
      
      return Promise.resolve(this.onAuthorize({ 
        username: this.user, 
        database: this.database,
        remoteIP,
        parameters
      }))
      .then(async (authorized) => {
        if (!authorized) throw `${remoteIP} Not Authorized`;
        
        this.connId = await ConnectionPool.getFreeConnection(this.socket);
        
        // Send authentication OK
        this.sendAuthenticationOk();
        
        // Send backend key data
        this.sendBackendKeyData();
        
        // Send parameter status messages
        this.sendParameterStatus('server_version', '13.0');
        this.sendParameterStatus('server_encoding', 'UTF8');
        this.sendParameterStatus('client_encoding', 'UTF8');
        this.sendParameterStatus('DateStyle', 'ISO, MDY');
        this.sendParameterStatus('TimeZone', 'UTC');
        this.sendParameterStatus('integer_datetimes', 'on');
        this.sendParameterStatus('is_superuser', 'off');
        this.sendParameterStatus('session_authorization', this.user || 'postgres');
        this.sendParameterStatus('standard_conforming_strings', 'on');
        
        // Send ready for query
        this.sendReadyForQuery();
        
        this.gatherIncoming();
      })
      .catch((err) => {
        this.sendError({ message: "Authorization Failure" });
        this.socket.destroy();
      });
    } catch(err) {
      this.sendError({ message: "Startup packet error" });
    }
  }

  normalPacketHandler(messageType, packet) {
    const id = this.connId;
    if (packet == null && messageType !== consts.MSG_SYNC) throw "Empty packet";
    
    return this.onCommand({
      messageType: messageType,
      payload: packet,
      id
    });
  }

  sendError({ message = 'Unknown PostgreSQL error', code = '42000', severity = 'ERROR' }) {
    console.log(message);
    let payload = Buffer.alloc(message.length + 64);
    let offset = 0;
    
    // Severity
    offset = payload.writeUInt8(consts.ERROR_SEVERITY.charCodeAt(0), offset);
    offset += payload.write(severity, offset);
    offset = payload.writeUInt8(0, offset);
    
    // SQL State
    offset = payload.writeUInt8(consts.ERROR_CODE.charCodeAt(0), offset);
    offset += payload.write(code, offset);
    offset = payload.writeUInt8(0, offset);
    
    // Message
    offset = payload.writeUInt8(consts.ERROR_MESSAGE.charCodeAt(0), offset);
    offset += payload.write(message, offset);
    offset = payload.writeUInt8(0, offset);
    
    // End of message
    offset = payload.writeUInt8(0, offset);
    
    this.sendMessage(consts.MSG_ERROR_RESPONSE, payload.slice(0, offset));
  }
}

exports.createServer = function (options) {
  return new Server(options);
};