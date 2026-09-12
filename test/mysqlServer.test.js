/* eslint-disable no-underscore-dangle */
/* eslint-disable no-restricted-syntax */
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');
const { Duplex } = require('node:stream');
const mysql = require('mysql2/promise');
const { identify } = require('sql-query-identifier');
const constants = require('../lib/mysqlConstants');

// Isolate protocol handling from the real database, operator services and log files.
function loadModule(relativePath, dependencies) {
  const filename = path.join(__dirname, '..', relativePath);
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    exports: module.exports,
    require: (name) => dependencies[name] || {},
    Buffer,
    process,
    console,
  }, { filename });
  return module.exports;
}

function fixture(options = {}) {
  const logs = [];
  const log = Object.fromEntries(['error', 'warn', 'info', 'debug'].map((level) => [
    level, (message) => logs.push({ level, message }),
  ]));
  const socket = new EventEmitter();
  socket.remoteAddress = '192.0.2.10';
  socket.remotePort = 12345;
  socket.writes = [];
  socket.write = (data) => { socket.writes.push(Buffer.from(data)); return true; };
  socket.destroy = () => { socket.destroyed = true; };
  socket.end = () => { socket.ended = true; };
  const pool = { getFreeConnection: async () => 7, releaseConnection: () => {} };
  const emulator = loadModule('lib/mysqlServer.js', {
    './mysqlConstants': constants,
    './utill': { convertIP: (ip) => ip },
    './ConnectionPool': pool,
    './log': log,
    crypto,
  });
  const operator = loadModule('ClusterOperator/Operator.js', {
    '../lib/mysqlConstants': constants,
    '../lib/ConnectionPool': pool,
    '../lib/log': log,
    '../lib/sqlAnalyzer': loadModule('lib/sqlAnalyzer.js', {
      'sql-query-identifier': { identify },
      './log': log,
      '../ClusterOperator/config': { clientType: 'mariadb', containerDataPath: '' },
    }),
  });
  const server = emulator.createServer({
    socket,
    onAuthorize: async () => true,
    onCommand: operator.handleCommand,
    onStmtResult: operator.handleStmtResult,
    operator: { status: 'OK', sessionQueries: {} },
    status: 'OK',
    isNotBacklogQuery: () => true,
    ...options,
  });
  server.connId = 7;
  server.onPacket = server.normalPacketHandler;
  socket.writes.length = 0;
  return {
    server, socket, logs, pool,
  };
}

async function receive(server, payload, sequence = 0) {
  const header = Buffer.alloc(4);
  header.writeUIntLE(payload.length, 0, 3);
  header[3] = sequence;
  server.handleData(Buffer.concat([header, payload]));
  await server._packetQueue;
}

function hello() {
  return Buffer.concat([Buffer.alloc(32), Buffer.from('client\0\0')]);
}

test('unknown commands produce a contextual error log and ERR response', async () => {
  const { server, logs, socket } = fixture();
  await receive(server, Buffer.from([0x7f]));
  const failure = logs.find(({ level }) => level === 'error').message;
  assert.match(failure, /conn=7 peer=192\.0\.2\.10:12345 command=UNKNOWN\(0x7f\)/);
  assert.match(failure, /packet=1 seq=1 inFlight=true/);
  assert.match(failure, /errno=1047 sqlState=08S01.*Unknown Command: 127/);
  assert.equal(socket.writes[0][4], 0xff);
  assert.equal(socket.writes[0].readUInt16LE(5), 1047);
});

test('socket error, timeout, peer end and close retain the last command', async () => {
  const { server, logs, socket } = fixture();
  await receive(server, Buffer.from([constants.COM_PING]));
  socket.emit('error', Object.assign(new Error('connection reset'), { code: 'ECONNRESET' }));
  socket.emit('timeout');
  socket.emit('end');
  socket.emit('close', true);
  assert.ok(logs.some(({ level, message }) => level === 'error'
    && /COM_PING.*socket error: code=ECONNRESET.*Error: connection reset/.test(message)));
  assert.ok(logs.some(({ message }) => /socket timeout/.test(message)));
  assert.ok(logs.some(({ message }) => /client ended connection/.test(message)));
  assert.ok(logs.some(({ level, message }) => level === 'warn' && /socket closed hadError=true/.test(message)));
});

test('partial packets are recorded on close without being logged as parsing errors', () => {
  const { server, logs, socket } = fixture();
  server.handleData(Buffer.from([20, 0, 0]));
  socket.emit('close', false);
  assert.equal(logs.length, 1);
  assert.match(logs[0].message, /bufferedBytes=3/);
  assert.equal(logs[0].level, 'warn');
});

test('malformed prepared packets and rejected handlers are logged; the queue continues', async () => {
  const { server, logs, socket } = fixture();
  await receive(server, Buffer.from([constants.COM_STMT_EXECUTE, 1]));
  assert.ok(logs.some(({ message }) => /COM_STMT_EXECUTE.*packet handler failed.*RangeError/.test(message)));
  server.onCommand = async () => { throw new Error('async handler failed'); };
  await receive(server, Buffer.from([constants.COM_PING]));
  assert.ok(logs.some(({ message }) => /COM_PING.*async handler failed/.test(message)));
  server.onCommand = () => server.sendOK({ message: 'OK' });
  await receive(server, Buffer.from([constants.COM_PING]));
  assert.equal(socket.writes.at(-1)[4], 0);
  assert.equal(server.packetInFlight, false);
});

test('missing packet handlers and framing failures are no longer silent', async () => {
  const { server, logs } = fixture();
  server.onPacket = null;
  await receive(server, Buffer.from([constants.COM_QUERY]));
  assert.ok(logs.some(({ message }) => /packet dropped.*No packet handler/.test(message)));
  server.readPackets = () => { throw new Error('bad framing'); };
  server.handleData(Buffer.alloc(4));
  assert.ok(logs.some(({ message }) => /packet framing failed.*bad framing/.test(message)));
  server.gatherIncoming = () => { throw new Error('bad data'); };
  server.handleData(Buffer.alloc(4));
  assert.ok(logs.some(({ message }) => /receiving data failed.*bad data/.test(message)));
});

test('authorization rejection and allocation failure are logged before destruction', async () => {
  for (const allocationFailure of [false, true]) {
    const {
      server, logs, pool, socket,
    } = fixture({ onAuthorize: async () => allocationFailure });
    server.connId = null;
    server.onPacket = server.helloPacketHandler;
    pool.getFreeConnection = async () => { throw new Error('pool exhausted'); };
    // eslint-disable-next-line no-await-in-loop
    await receive(server, hello(), 1);
    assert.equal(socket.destroyed, true);
    assert.ok(logs.some(({ message }) => /command=HANDSHAKE.*authorization or connection allocation failed/.test(message)));
    assert.ok(logs.some(({ message }) => message.includes(allocationFailure ? 'pool exhausted' : 'Not Authorized')));
  }
});

test('successful handshake and ping do not emit failure logs', async () => {
  const { server, logs, socket } = fixture();
  server.onPacket = server.helloPacketHandler;
  await receive(server, hello(), 1);
  await receive(server, Buffer.from([constants.COM_PING]));
  assert.equal(logs.length, 0);
  assert.equal(socket.writes.length, 2);
  assert.equal(socket.writes[0][3], 2);
  assert.equal(socket.writes[1][3], 1);
});

test('prepared read failures reach the client as ERR with original DB error details', async () => {
  const {
    server, logs, pool, socket,
  } = fixture();
  const error = Object.assign(new Error('table missing'), {
    code: 'ER_NO_SUCH_TABLE', errno: 1146, sqlState: '42S02',
  });
  let restored = false;
  pool.getConnectionById = () => ({
    disableSocketWrite() {},
    query: async () => [null, null, error],
    setSocket() { restored = true; },
  });
  pool.getSocketById = () => socket;
  server.preparedStatements.set(1, { sql: 'SELECT * FROM missing', numParams: 0 });
  await receive(server, Buffer.from([constants.COM_STMT_EXECUTE, 1, 0, 0, 0, 0, 1, 0, 0, 0]));
  assert.equal(restored, true);
  assert.equal(socket.writes.length, 1);
  assert.equal(socket.writes[0][4], 0xff);
  assert.equal(socket.writes[0].readUInt16LE(5), 1146);
  assert.equal(socket.writes[0].toString('ascii', 8, 13), '42S02');
  assert.ok(logs.some(({ message }) => /COM_STMT_EXECUTE.*code=ER_NO_SUCH_TABLE errno=1146 sqlState=42S02/.test(message)));
});

test('unavailable prepared query paths return errors instead of success', async () => {
  for (const reason of ['status', 'pool', 'write']) {
    const {
      server, socket, pool, logs,
    } = fixture();
    if (reason === 'status') server.operator.status = 'SYNC';
    pool.getConnectionById = () => (reason === 'pool' ? null : {
      disableSocketWrite() {}, setSocket() {},
    });
    pool.getSocketById = () => socket;
    server.operator.sendWriteQuery = async () => null;
    server.preparedStatements.set(1, { sql: 'INSERT INTO t VALUES (1)', numParams: 0 });
    // eslint-disable-next-line no-await-in-loop
    await receive(server, Buffer.from([constants.COM_STMT_EXECUTE, 1, 0, 0, 0, 0, 1, 0, 0, 0]));
    assert.equal(socket.writes[0][4], 0xff, reason);
    assert.ok(logs.some(({ level }) => level === 'error'), reason);
  }
});

test('command exceptions and skipped queries include connection context', async () => {
  const { server, logs } = fixture();
  await receive(server, Buffer.from([constants.COM_QUERY]));
  assert.ok(logs.some(({ message }) => /COM_QUERY.*command failed.*TypeError/.test(message)));
  server.status = 'SYNC';
  await receive(server, Buffer.from([constants.COM_QUERY]));
  assert.ok(logs.some(({ level, message }) => level === 'warn' && /query skipped: operator status=SYNC/.test(message)));
});

test('prepared write failures from master and replica retain the database error', async () => {
  const error = Object.assign(new Error('duplicate entry'), { errno: 1062, sqlState: '23000' });
  for (const result of [[[null, null, error], 123, 456], [null, null, error]]) {
    const {
      server, pool, socket, logs,
    } = fixture();
    pool.getConnectionById = () => ({ disableSocketWrite() {}, setSocket() {} });
    pool.getSocketById = () => socket;
    server.operator.sendWriteQuery = async () => result;
    server.preparedStatements.set(1, { sql: 'INSERT INTO t VALUES (1)', numParams: 0 });
    // eslint-disable-next-line no-await-in-loop
    await receive(server, Buffer.from([constants.COM_STMT_EXECUTE, 1, 0, 0, 0, 0, 1, 0, 0, 0]));
    assert.equal(socket.writes[0][4], 0xff);
    assert.equal(socket.writes[0].readUInt16LE(5), 1062);
    assert.ok(logs.some(({ message }) => /errno=1062 sqlState=23000.*duplicate entry/.test(message)));
  }
});

test('synchronous write and end failures use the operator error logger', () => {
  const { server, socket, logs } = fixture();
  socket.write = () => { throw new Error('write failed'); };
  assert.throws(() => server.sendOK({ message: 'OK' }), /write failed/);
  socket.end = () => { throw new Error('end failed'); };
  server.end();
  assert.ok(logs.some(({ message }) => /socket write failed.*write failed/.test(message)));
  assert.ok(logs.some(({ message }) => /ending connection failed.*end failed/.test(message)));
});

async function connectedClient(t) {
  const context = fixture();
  const { server, socket } = context;
  const stream = new Duplex({
    read() {},
    write(chunk, encoding, callback) {
      socket.emit('data', Buffer.from(chunk));
      callback();
    },
  });
  socket.write = (data) => { socket.writes.push(Buffer.from(data)); return stream.push(Buffer.from(data)); };
  socket.end = () => stream.push(null);
  const connecting = mysql.createConnection({ stream, user: 'client', multipleStatements: true });
  server.onPacket = server.helloPacketHandler;
  server.sequence = 0;
  server.sendServerHello();
  const client = await connecting;
  const clientErrors = [];
  client.on('error', (err) => clientErrors.push(err));
  t.after(() => { client.destroy(); stream.destroy(); });
  return { ...context, client, clientErrors };
}

function batchBackend(context) {
  const { server, socket, pool } = context;
  const executed = [];
  const replicated = [];
  let forwarding = true;
  const backend = {
    disableSocketWrite() { forwarding = false; },
    setSocket() { forwarding = true; },
    async query(sql) {
      executed.push(typeof sql === 'string' ? sql : sql.sql);
      const result = {
        affectedRows: 1, insertId: 0, serverStatus: constants.SERVER_STATUS_AUTOCOMMIT, warningStatus: 0,
      };
      // Each independent backend command starts at sequence 1 with MORE_RESULTS clear.
      if (forwarding) socket.write(Buffer.from([7, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0]));
      return [result, undefined];
    },
  };
  pool.getConnectionById = () => backend;
  pool.getSocketById = () => socket;
  const write = async (sql) => {
    replicated.push(sql);
    const [result] = await backend.query(sql);
    return [result, replicated.length, Date.now()];
  };
  server.sendWriteQuery = write;
  server.operator.sendWriteQuery = write;
  return { backend, executed, replicated };
}

test('mysql2 receives nine chained write results and can run the next query', { timeout: 5000 }, async (t) => {
  const context = await connectedClient(t);
  const { client, clientErrors } = context;
  const { executed, replicated } = batchBackend(context);
  const statements = Array.from({ length: 8 }, (_, i) => `CREATE TABLE t${i} (id INT)`);
  statements.push('INSERT INTO t0 VALUES (1)');
  const [results] = await client.query(statements.join('; '));
  await context.server._packetQueue;
  await client.ping();
  assert.equal(results.length, 9);
  assert.equal(executed.length, 9);
  assert.equal(replicated.length, 9);
  await client.query('INSERT INTO t0 VALUES (2)');
  assert.equal(clientErrors.length, 0);
});

test('mixed batches preserve SELECT rows, binary data and large UTF-8 values', { timeout: 5000 }, async (t) => {
  const context = await connectedClient(t);
  const { client, clientErrors } = context;
  const { backend, replicated } = batchBackend(context);
  const query = backend.query.bind(backend);
  const text = '\u20ac'.repeat(1500);
  const binary = Buffer.from([0, 0xff, 0xfe, 0x80]);
  const columns = ['payload', 'duplicate', 'duplicate', 'raw_data'].map((name, i) => ({
    name, columnType: i === 3 ? 252 : 253, characterSet: i === 3 ? 63 : 8,
  }));
  backend.query = async (sql) => {
    const result = await query(sql);
    if (sql.sql && sql.sql.startsWith('SELECT')) {
      assert.equal(sql.rowsAsArray, true);
      return [[[text, 'first', 'second', binary], [null, '', 'third', Buffer.alloc(0)]], columns];
    }
    return result;
  };
  const [results] = await client.query({
    sql: 'SELECT payload, duplicate, duplicate, raw_data FROM t; INSERT INTO t VALUES (1); SELECT payload, duplicate, duplicate, raw_data FROM t',
    rowsAsArray: true,
  });
  assert.equal(results.length, 3);
  assert.deepEqual(results[0], [[text, 'first', 'second', binary], [null, '', 'third', Buffer.alloc(0)]]);
  assert.equal(results[1].affectedRows, 1);
  assert.deepEqual(results[2], results[0]);
  assert.equal(replicated.length, 1);
  await client.ping();
  assert.equal(clientErrors.length, 0);
});

test('a failed batch stops at the error and mysql2 can reuse the connection', { timeout: 5000 }, async (t) => {
  const context = await connectedClient(t);
  const { client, server, logs } = context;
  batchBackend(context);
  const write = server.operator.sendWriteQuery;
  const attempts = [];
  const error = Object.assign(new Error('duplicate entry'), { errno: 1062, sqlState: '23000' });
  server.operator.sendWriteQuery = async (sql) => {
    attempts.push(sql);
    if (attempts.length === 2) return [[null, null, error], 2, Date.now()];
    return write(sql);
  };
  await assert.rejects(client.query('INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (3)'), {
    errno: 1062, sqlState: '23000',
  });
  assert.equal(attempts.length, 2);
  assert.ok(logs.some(({ message }) => /multi-statement query failed at statement=2\/3/.test(message)));
  await client.ping();
  await client.query('INSERT INTO t VALUES (4)');
});

test('batch OK packets preserve status, warnings and large insert IDs across sequence wrap', { timeout: 5000 }, async (t) => {
  const context = await connectedClient(t);
  const { client, server, clientErrors } = context;
  batchBackend(context);
  const insertId = 2 ** 32;
  server.operator.sendWriteQuery = async () => [{
    affectedRows: 2 ** 25,
    insertId,
    serverStatus: constants.SERVER_STATUS_IN_TRANS,
    warningStatus: 3,
  }, 1, Date.now()];
  const [results] = await client.query(Array(260).fill('INSERT INTO t VALUES (1)').join('; '));
  assert.equal(results.length, 260);
  // eslint-disable-next-line no-bitwise
  assert.equal(results[0].serverStatus, constants.SERVER_STATUS_IN_TRANS | constants.SERVER_MORE_RESULTS_EXISTS);
  assert.equal(results.at(-1).serverStatus, constants.SERVER_STATUS_IN_TRANS);
  assert.equal(results[0].warningStatus, 3);
  assert.equal(results[0].insertId, insertId);
  assert.equal(results[0].affectedRows, 2 ** 25);
  await client.ping();
  assert.equal(clientErrors.length, 0);
});
