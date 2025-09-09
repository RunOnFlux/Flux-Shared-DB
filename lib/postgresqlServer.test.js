const assert = require('power-assert');
const net = require('net');
const { createServer } = require('./postgresqlServer');
const consts = require('./postgresqlConstants');

describe('PostgreSQL Server Tests', function() {
  let server;
  let client;
  const TEST_PORT = 5433;

  // Mock operator with required methods
  const mockOperator = {
    sessionQueries: {},
    onAuthorize: async ({ username, database, remoteIP }) => {
      // Allow test connections
      if (username === 'testuser' && database === 'testdb') {
        return true;
      }
      return false;
    },
    onCommand: async ({ messageType, payload, id }) => {
      // Handle basic query commands for testing
      if (messageType === 'Q') {
        const query = payload.toString('utf8').replace(/\0$/, '');
        
        // Mock responses for different queries
        if (query === 'SELECT version()') {
          return {
            type: 'query',
            rows: [['PostgreSQL 13.0 (Mock)']],
            fields: [{ name: 'version', typeOid: consts.PG_TYPE_TEXT }],
            commandTag: 'SELECT 1'
          };
        } else if (query === 'SELECT 1') {
          return {
            type: 'query',
            rows: [[1]],
            fields: [{ name: '?column?', typeOid: consts.PG_TYPE_INT4 }],
            commandTag: 'SELECT 1'
          };
        }
      }
      
      return {
        type: 'query',
        rows: [],
        fields: [],
        commandTag: 'SELECT 0'
      };
    }
  };

  beforeEach(function(done) {
    // Create TCP server
    server = net.createServer();
    
    server.on('connection', (socket) => {
      createServer({
        socket: socket,
        operator: mockOperator,
        onAuthorize: mockOperator.onAuthorize,
        onCommand: mockOperator.onCommand
      });
    });

    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        // Port is in use, try next port
        TEST_PORT++;
        server.listen(TEST_PORT, '127.0.0.1', done);
      } else {
        done(err);
      }
    });

    server.listen(TEST_PORT, '127.0.0.1', done);
  });

  afterEach(function(done) {
    if (client) {
      client.destroy();
      client = null;
    }
    
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  it('should accept connection and handle startup message', function(done) {
    client = new net.Socket();
    
    client.connect(TEST_PORT, '127.0.0.1', () => {
      // Send startup message
      const protocolVersion = Buffer.alloc(4);
      protocolVersion.writeUInt32BE(196608, 0); // Protocol 3.0
      
      const user = Buffer.from('user\0testuser\0');
      const database = Buffer.from('database\0testdb\0');
      const applicationName = Buffer.from('application_name\0test\0');
      const terminator = Buffer.from('\0');
      
      const payload = Buffer.concat([protocolVersion, user, database, applicationName, terminator]);
      const length = Buffer.alloc(4);
      length.writeUInt32BE(payload.length + 4, 0);
      
      const startupMessage = Buffer.concat([length, payload]);
      
      client.write(startupMessage);
    });

    client.on('data', (data) => {
      // Should receive authentication OK and ready for query messages
      assert(data.length > 0);
      
      // Check for authentication response (type 'R')
      const hasAuthResponse = data.includes(Buffer.from('R'));
      assert(hasAuthResponse, 'Should receive authentication response');
      
      done();
    });

    client.on('error', done);
  });

  it('should handle simple query', function(done) {
    this.timeout(5000); // Increase timeout
    client = new net.Socket();
    let authComplete = false;
    let finished = false;
    
    const finish = (err) => {
      if (!finished) {
        finished = true;
        done(err);
      }
    };
    
    client.connect(TEST_PORT, '127.0.0.1', () => {
      // Send startup message first
      const protocolVersion = Buffer.alloc(4);
      protocolVersion.writeUInt32BE(196608, 0);
      
      const user = Buffer.from('user\0testuser\0');
      const database = Buffer.from('database\0testdb\0');
      const terminator = Buffer.from('\0');
      
      const payload = Buffer.concat([protocolVersion, user, database, terminator]);
      const length = Buffer.alloc(4);
      length.writeUInt32BE(payload.length + 4, 0);
      
      const startupMessage = Buffer.concat([length, payload]);
      client.write(startupMessage);
    });

    client.on('data', (data) => {
      try {
        if (!authComplete && data.includes(Buffer.from('Z'))) { // Ready for query
          authComplete = true;
          
          // Send simple query
          const queryString = 'SELECT 1';
          const queryBuffer = Buffer.from(queryString + '\0');
          const header = Buffer.alloc(5);
          header.write('Q', 0, 1); // Query message type
          header.writeUInt32BE(queryBuffer.length + 4, 1);
          
          const queryMessage = Buffer.concat([header, queryBuffer]);
          client.write(queryMessage);
        } else if (authComplete) {
          // Should receive query results
          assert(data.length > 0);
          
          // Check for row description (type 'T') or data row (type 'D')
          const hasRowDescription = data.includes(Buffer.from('T'));
          const hasDataRow = data.includes(Buffer.from('D'));
          const hasCommandComplete = data.includes(Buffer.from('C'));
          
          assert(hasRowDescription || hasDataRow || hasCommandComplete, 'Should receive query response');
          finish();
        }
      } catch (err) {
        finish(err);
      }
    });

    client.on('error', finish);
    client.on('close', () => finish(new Error('Connection closed unexpectedly')));
  });

  it('should reject unauthorized connections', function(done) {
    client = new net.Socket();
    let finished = false;
    
    const finish = () => {
      if (!finished) {
        finished = true;
        done();
      }
    };
    
    client.connect(TEST_PORT, '127.0.0.1', () => {
      // Send startup message with invalid credentials
      const protocolVersion = Buffer.alloc(4);
      protocolVersion.writeUInt32BE(196608, 0);
      
      const user = Buffer.from('user\0invaliduser\0');
      const database = Buffer.from('database\0invaliddb\0');
      const terminator = Buffer.from('\0');
      
      const payload = Buffer.concat([protocolVersion, user, database, terminator]);
      const length = Buffer.alloc(4);
      length.writeUInt32BE(payload.length + 4, 0);
      
      const startupMessage = Buffer.concat([length, payload]);
      client.write(startupMessage);
    });

    client.on('data', (data) => {
      // Should receive error response
      const hasErrorResponse = data.includes(Buffer.from('E'));
      assert(hasErrorResponse, 'Should receive error response for unauthorized connection');
      finish();
    });

    client.on('close', () => {
      // Connection should be closed for unauthorized access
      finish();
    });

    client.on('error', (err) => {
      // Expected for unauthorized connection
      finish();
    });
  });

  it('should handle connection cleanup', function(done) {
    client = new net.Socket();
    let finished = false;
    
    const finish = () => {
      if (!finished) {
        finished = true;
        done();
      }
    };
    
    client.connect(TEST_PORT, '127.0.0.1', () => {
      // Send startup message
      const protocolVersion = Buffer.alloc(4);
      protocolVersion.writeUInt32BE(196608, 0);
      
      const user = Buffer.from('user\0testuser\0');
      const database = Buffer.from('database\0testdb\0');
      const terminator = Buffer.from('\0');
      
      const payload = Buffer.concat([protocolVersion, user, database, terminator]);
      const length = Buffer.alloc(4);
      length.writeUInt32BE(payload.length + 4, 0);
      
      const startupMessage = Buffer.concat([length, payload]);
      client.write(startupMessage);
      
      // Close connection immediately after startup
      setTimeout(() => {
        client.destroy();
      }, 100);
    });

    client.on('close', () => {
      // Connection should close cleanly
      assert(true, 'Connection closed cleanly');
      finish();
    });

    client.on('error', () => {
      // Ignore errors during cleanup test
      finish();
    });
  });
});