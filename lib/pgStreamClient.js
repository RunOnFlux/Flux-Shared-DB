/* eslint-disable */
/**
 * PostgreSQL Stream Client
 * Provides raw stream access to PostgreSQL server for transparent proxying
 * Similar to MySQL2's stream functionality but for PostgreSQL
 */

const net = require('net');
const { Buffer } = require('buffer');
const crypto = require('crypto');
const log = require('./log');

class PgStreamClient {
  constructor(options = {}) {
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5432,
      user: options.user || 'postgres',
      password: options.password,
      database: options.database || 'postgres',
      onData: options.onData || null,
      onError: options.onError || null,
      onConnect: options.onConnect || null,
      skipAuth: options.skipAuth || false, // For transparent proxying
    };
    
    this.socket = null;
    this.connected = false;
    this.authenticated = false;
    this.processId = null;
    this.secretKey = null;
    this.parameters = new Map();
    this.pendingData = Buffer.alloc(0);
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({
        host: this.options.host,
        port: this.options.port
      });

      this.socket.on('connect', async () => {
        log.info(`Connected to PostgreSQL at ${this.options.host}:${this.options.port}`);
        
        try {
          if (this.options.skipAuth) {
            // Skip authentication for transparent proxying
            this.connected = true;
            this.authenticated = true;
            
            if (this.options.onConnect) {
              this.options.onConnect();
            }
            
            resolve();
          } else {
            await this.performStartup();
            this.connected = true;
            
            if (this.options.onConnect) {
              this.options.onConnect();
            }
            
            resolve();
          }
        } catch (error) {
          reject(error);
        }
      });

      this.socket.on('data', (data) => {
        this.handleServerData(data);
      });

      this.socket.on('error', (error) => {
        log.error(`PostgreSQL stream connection error: ${error.message}`);
        this.connected = false;
        if (this.options.onError) {
          this.options.onError(error);
        }
      });

      this.socket.on('close', () => {
        log.info('PostgreSQL stream connection closed');
        this.connected = false;
      });
    });
  }

  async performStartup() {
    // Send startup message
    const startupMessage = this.buildStartupMessage();
    this.socket.write(startupMessage);
    
    // Wait for authentication response
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('PostgreSQL authentication timeout'));
      }, 10000);

      const handleAuth = (messageType, data) => {
        if (messageType === 'R') { // Authentication message
          const authType = data.readUInt32BE(0);
          
          if (authType === 0) {
            // Authentication OK
            this.authenticated = true;
            clearTimeout(timeout);
            resolve();
          } else if (authType === 3) {
            // Clear text password required
            const passwordMessage = this.buildPasswordMessage(this.options.password);
            this.socket.write(passwordMessage);
          } else if (authType === 5) {
            // MD5 password required
            const salt = data.subarray(4, 8);
            const hashedPassword = this.hashMD5Password(this.options.user, this.options.password, salt);
            const passwordMessage = this.buildPasswordMessage(hashedPassword);
            this.socket.write(passwordMessage);
          } else if (authType === 10) {
            // SASL authentication
            this.handleSASLAuth(data.subarray(4), handleAuth, clearTimeout, timeout, reject);
          } else if (authType === 11) {
            // SASL Continue
            this.handleSASLContinue(data.subarray(4));
          } else if (authType === 12) {
            // SASL Final
            this.handleSASLFinal(data.subarray(4));
          } else {
            clearTimeout(timeout);
            reject(new Error(`Unsupported authentication method: ${authType}`));
          }
        } else if (messageType === 'E') {
          // Error response
          const errorMessage = this.parseErrorMessage(data);
          clearTimeout(timeout);
          reject(new Error(`Authentication failed: ${errorMessage}`));
        }
      };

      // Temporarily handle authentication messages
      this.tempAuthHandler = handleAuth;
    });
  }

  handleServerData(data) {
    // If we're skipping auth (transparent proxy), forward all data immediately
    if (this.options.skipAuth) {
      if (this.options.onData) {
        this.options.onData(data);
      }
      return;
    }

    // Add incoming data to pending buffer
    this.pendingData = Buffer.concat([this.pendingData, data]);
    
    // If we're still authenticating, handle auth messages
    if (!this.authenticated && this.tempAuthHandler) {
      this.processAuthMessages();
      return;
    }
    
    // Forward raw data to client if callback is set
    if (this.options.onData) {
      this.options.onData(data);
    }
  }

  processAuthMessages() {
    while (this.pendingData.length >= 5) {
      const messageType = String.fromCharCode(this.pendingData[0]);
      const messageLength = this.pendingData.readUInt32BE(1);
      
      if (this.pendingData.length < messageLength + 1) {
        break; // Wait for complete message
      }
      
      const messageData = this.pendingData.subarray(5, messageLength + 1);
      
      if (this.tempAuthHandler) {
        this.tempAuthHandler(messageType, messageData);
      }
      
      // Handle parameter status and backend key data
      if (messageType === 'S') {
        // Parameter status
        this.handleParameterStatus(messageData);
      } else if (messageType === 'K') {
        // Backend key data
        this.processId = messageData.readUInt32BE(0);
        this.secretKey = messageData.readUInt32BE(4);
      } else if (messageType === 'Z') {
        // Ready for query - authentication complete
        delete this.tempAuthHandler;
        this.pendingData = Buffer.alloc(0); // Clear auth data
        return;
      }
      
      this.pendingData = this.pendingData.subarray(messageLength + 1);
    }
  }

  buildStartupMessage() {
    const protocolVersion = Buffer.alloc(4);
    protocolVersion.writeUInt32BE(0x00030000, 0); // Protocol version 3.0
    
    const parameters = [
      'user', this.options.user,
      'database', this.options.database,
      'application_name', 'flux-shared-db',
      'client_encoding', 'UTF8'
    ];
    
    let parameterString = '';
    for (let i = 0; i < parameters.length; i += 2) {
      parameterString += parameters[i] + '\0' + parameters[i + 1] + '\0';
    }
    parameterString += '\0'; // Final null terminator
    
    const parameterBuffer = Buffer.from(parameterString, 'utf8');
    const messageLength = 4 + 4 + parameterBuffer.length; // length + version + parameters
    
    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeUInt32BE(messageLength, 0);
    
    return Buffer.concat([lengthBuffer, protocolVersion, parameterBuffer]);
  }

  buildPasswordMessage(password) {
    const passwordBuffer = Buffer.from(password + '\0', 'utf8');
    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeUInt32BE(4 + passwordBuffer.length, 0);
    
    const typeBuffer = Buffer.from('p'); // Password message type
    
    return Buffer.concat([typeBuffer, lengthBuffer, passwordBuffer]);
  }

  hashMD5Password(user, password, salt) {
    // PostgreSQL MD5 password format: 'md5' + md5(md5(password + user) + salt)
    const innerHash = crypto.createHash('md5').update(password + user).digest('hex');
    const outerHash = crypto.createHash('md5').update(innerHash + salt.toString('binary')).digest('hex');
    return 'md5' + outerHash;
  }

  handleSASLAuth(saslData, handleAuth, clearTimeout, timeout, reject) {
    try {
      // Parse SASL mechanisms
      const mechanisms = [];
      let offset = 0;
      
      while (offset < saslData.length - 1) {
        const start = offset;
        while (offset < saslData.length && saslData[offset] !== 0) offset++;
        if (offset > start) {
          mechanisms.push(saslData.subarray(start, offset).toString('utf8'));
        }
        offset++; // Skip null terminator
      }

      log.info('Available SASL mechanisms:', mechanisms);

      // Check if SCRAM-SHA-256 is supported
      if (!mechanisms.includes('SCRAM-SHA-256')) {
        clearTimeout(timeout);
        reject(new Error('Server does not support SCRAM-SHA-256 authentication'));
        return;
      }

      // Start SCRAM-SHA-256 authentication
      this.startSCRAMAuth(handleAuth, clearTimeout, timeout, reject);
      
    } catch (error) {
      clearTimeout(timeout);
      reject(new Error(`SASL authentication setup failed: ${error.message}`));
    }
  }

  startSCRAMAuth(handleAuth, clearTimeout, timeout, reject) {
    try {
      // Generate client nonce
      this.clientNonce = crypto.randomBytes(18).toString('base64');
      
      // Build SCRAM-SHA-256 initial message
      const clientFirstMessage = `n,,n=${this.options.user},r=${this.clientNonce}`;
      this.clientFirstMessageBare = `n=${this.options.user},r=${this.clientNonce}`;
      
      // Send SASL Initial Response
      const mechanism = 'SCRAM-SHA-256\0';
      const mechanismBuffer = Buffer.from(mechanism, 'utf8');
      const messageBuffer = Buffer.from(clientFirstMessage, 'utf8');
      const lengthBuffer = Buffer.alloc(4);
      lengthBuffer.writeUInt32BE(4 + mechanismBuffer.length + 4 + messageBuffer.length, 0);
      const messageLengthBuffer = Buffer.alloc(4);
      messageLengthBuffer.writeUInt32BE(messageBuffer.length, 0);
      
      const saslInitialMessage = Buffer.concat([
        Buffer.from('p'), // SASL Initial Response
        lengthBuffer,
        mechanismBuffer,
        messageLengthBuffer,
        messageBuffer
      ]);

      this.socket.write(saslInitialMessage);

      // Set up continuation handler
      this.saslAuthHandler = handleAuth;
      this.saslClearTimeout = clearTimeout;
      this.saslTimeout = timeout;
      this.saslReject = reject;

    } catch (error) {
      clearTimeout(timeout);
      reject(new Error(`SCRAM authentication start failed: ${error.message}`));
    }
  }

  handleSASLContinue(data) {
    try {
      const serverMessage = data.toString('utf8');
      log.info('SASL Continue:', serverMessage);

      // Parse server first message
      const parts = serverMessage.split(',');
      let serverNonce = '';
      let salt = '';
      let iterations = 4096;

      for (const part of parts) {
        if (part.startsWith('r=')) {
          serverNonce = part.substring(2);
        } else if (part.startsWith('s=')) {
          salt = part.substring(2);
        } else if (part.startsWith('i=')) {
          iterations = parseInt(part.substring(2), 10);
        }
      }

      if (!serverNonce.startsWith(this.clientNonce)) {
        throw new Error('Server nonce does not start with client nonce');
      }

      // Calculate SCRAM values
      const saltBuffer = Buffer.from(salt, 'base64');
      const clientFinalMessageWithoutProof = `c=biws,r=${serverNonce}`;
      const authMessage = `${this.clientFirstMessageBare},${serverMessage},${clientFinalMessageWithoutProof}`;

      // PBKDF2
      const saltedPassword = crypto.pbkdf2Sync(this.options.password, saltBuffer, iterations, 32, 'sha256');
      
      // Client key
      const clientKey = crypto.createHmac('sha256', saltedPassword).update('Client Key').digest();
      
      // Stored key
      const storedKey = crypto.createHash('sha256').update(clientKey).digest();
      
      // Client signature
      const clientSignature = crypto.createHmac('sha256', storedKey).update(authMessage).digest();
      
      // Client proof (XOR of client key and client signature)
      const clientProof = Buffer.alloc(32);
      for (let i = 0; i < 32; i++) {
        clientProof[i] = clientKey[i] ^ clientSignature[i];
      }

      const clientFinalMessage = `${clientFinalMessageWithoutProof},p=${clientProof.toString('base64')}`;

      // Send SASL Response
      const messageBuffer = Buffer.from(clientFinalMessage, 'utf8');
      const lengthBuffer = Buffer.alloc(4);
      lengthBuffer.writeUInt32BE(4 + messageBuffer.length, 0);
      
      const saslResponseMessage = Buffer.concat([
        Buffer.from('p'), // SASL Response
        lengthBuffer,
        messageBuffer
      ]);

      this.socket.write(saslResponseMessage);

      // Calculate server key and signature for verification
      const serverKey = crypto.createHmac('sha256', saltedPassword).update('Server Key').digest();
      this.expectedServerSignature = crypto.createHmac('sha256', serverKey).update(authMessage).digest().toString('base64');

    } catch (error) {
      if (this.saslClearTimeout) this.saslClearTimeout(this.saslTimeout);
      if (this.saslReject) this.saslReject(new Error(`SCRAM continue failed: ${error.message}`));
    }
  }

  handleSASLFinal(data) {
    try {
      const serverMessage = data.toString('utf8');
      log.info('SASL Final:', serverMessage);

      // Parse server final message
      if (serverMessage.startsWith('v=')) {
        const serverSignature = serverMessage.substring(2);
        
        if (serverSignature !== this.expectedServerSignature) {
          throw new Error('Server signature verification failed');
        }

        log.info('SCRAM-SHA-256 authentication successful');
        this.authenticated = true;
        
        // Clean up SASL state
        delete this.saslAuthHandler;
        delete this.saslClearTimeout;
        delete this.saslTimeout;
        delete this.saslReject;
        delete this.clientNonce;
        delete this.clientFirstMessageBare;
        delete this.expectedServerSignature;

      } else {
        throw new Error(`Server authentication failed: ${serverMessage}`);
      }

    } catch (error) {
      if (this.saslClearTimeout) this.saslClearTimeout(this.saslTimeout);
      if (this.saslReject) this.saslReject(new Error(`SCRAM final failed: ${error.message}`));
    }
  }

  handleParameterStatus(data) {
    let offset = 0;
    while (offset < data.length - 1) {
      const keyStart = offset;
      while (offset < data.length && data[offset] !== 0) offset++;
      const key = data.subarray(keyStart, offset).toString('utf8');
      offset++; // Skip null
      
      const valueStart = offset;
      while (offset < data.length && data[offset] !== 0) offset++;
      const value = data.subarray(valueStart, offset).toString('utf8');
      offset++; // Skip null
      
      this.parameters.set(key, value);
    }
  }

  parseErrorMessage(data) {
    // Simple error message parsing
    let message = '';
    let offset = 0;
    
    while (offset < data.length - 1) {
      const field = String.fromCharCode(data[offset]);
      offset++;
      
      const valueStart = offset;
      while (offset < data.length && data[offset] !== 0) offset++;
      const value = data.subarray(valueStart, offset).toString('utf8');
      offset++;
      
      if (field === 'M') { // Message field
        message = value;
        break;
      }
    }
    
    return message || 'Unknown error';
  }

  // Write data directly to PostgreSQL server
  writeToServer(data) {
    if (this.socket && this.connected) {
      this.socket.write(data);
    }
  }

  // Close connection
  async close() {
    if (this.socket) {
      this.socket.end();
      this.connected = false;
    }
  }

  // Send query and get raw response
  async query(sql) {
    if (!this.connected) {
      throw new Error('Not connected to PostgreSQL server');
    }

    // Build simple query message
    const sqlBuffer = Buffer.from(sql + '\0', 'utf8');
    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeUInt32BE(4 + sqlBuffer.length, 0);
    const typeBuffer = Buffer.from('Q'); // Simple query
    
    const queryMessage = Buffer.concat([typeBuffer, lengthBuffer, sqlBuffer]);
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Query timeout'));
      }, 30000);

      let responseData = Buffer.alloc(0);
      let isComplete = false;

      const originalOnData = this.options.onData;
      
      this.options.onData = (data) => {
        responseData = Buffer.concat([responseData, data]);
        
        // Check if we have ReadyForQuery message (Z)
        if (data.includes(Buffer.from('Z'))) {
          isComplete = true;
          clearTimeout(timeout);
          this.options.onData = originalOnData; // Restore original handler
          resolve(responseData);
        }
        
        // Also call original handler if it exists
        if (originalOnData) {
          originalOnData(data);
        }
      };

      this.socket.write(queryMessage);
    });
  }
}

module.exports = PgStreamClient;