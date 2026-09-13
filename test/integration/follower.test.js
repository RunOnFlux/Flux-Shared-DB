/* eslint-disable no-restricted-syntax, no-await-in-loop, no-underscore-dangle */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const net = require('node:net');
const crypto = require('node:crypto');
const { performance } = require('node:perf_hooks');
const { once } = require('node:events');
const timer = require('node:timers/promises');
const mysql = require('mysql2/promise');
const { Server: SocketServer } = require('socket.io');
const socketClient = require('socket.io-client');
const { identify } = require('sql-query-identifier');
const constants = require('../../lib/mysqlConstants');

function load(relativePath, dependencies) {
  const filename = path.join(__dirname, '../..', relativePath);
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    exports: module.exports,
    require: (name) => dependencies[name] || {},
    Buffer,
    process,
    console,
    performance,
  }, { filename });
  return module.exports;
}

async function nodeInstance(port, t) {
  const config = {
    dbHost: '127.0.0.1',
    dbPort: port,
    dbType: 'mysql',
    clientType: 'mariadb',
    dbUser: 'root',
    dbBacklog: 'flux_backlog',
    dbBacklogCollection: 'backlog',
    containerDataPath: '',
  };
  const messages = [];
  const log = Object.fromEntries(['info', 'debug', 'warn', 'error'].map((level) => [level, (message) => messages.push(String(message))]));
  const factory = load('ClusterOperator/DBClient.js', {
    'mysql2/promise': mysql,
    net,
    './config': config,
    './Security': { getKey: () => 'integration-secret' },
    '../lib/log': log,
  });
  const connections = [];
  async function connection() {
    const conn = await factory.createClient();
    assert.ok(conn, messages.join('\n'));
    connections.push(conn);
    return conn;
  }
  t.after(async () => {
    for (const conn of connections) await conn.close();
  });
  const user = await connection();
  await user.connection.query('CREATE DATABASE IF NOT EXISTS flux_backlog');
  await user.connection.query('CREATE TABLE IF NOT EXISTS flux_backlog.backlog (seq BIGINT PRIMARY KEY, query LONGTEXT, timestamp BIGINT)');
  await user.connection.query('TRUNCATE TABLE flux_backlog.backlog');
  const backlogConn = await connection();
  await backlogConn.connection.query('USE flux_backlog');
  const poolConnections = [];
  const pool = {
    async getFreeConnection(socket) {
      const conn = await connection();
      const id = poolConnections.length;
      conn.setSocket(socket, id);
      poolConnections.push(conn);
      // Delay the real database request, including the raw bytes it will return.
      const query = conn.query.bind(conn);
      conn.query = async (...args) => { await timer.setTimeout(25); return query(...args); };
      return id;
    },
    getConnectionById: (id) => poolConnections[id],
    getSocketById: (id) => poolConnections[id].socketCallBack,
    releaseConnection(id) { if (id !== null) poolConnections[id].disableSocketWrite(); },
  };
  const backlog = load('ClusterOperator/Backlog.js', {
    './config': config, './DBClient': factory, '../lib/ConnectionPool': pool, '../lib/log': log,
  });
  backlog.BLClient = backlogConn;
  backlog.UserDBClient = user;
  const operator = load('ClusterOperator/Operator.js', {
    'socket.io-client': socketClient,
    './Backlog': backlog,
    './config': config,
    '../lib/log': log,
    '../lib/ConnectionPool': pool,
    '../lib/mysqlConstants': constants,
    '../lib/sqlAnalyzer': load('lib/sqlAnalyzer.js', {
      'sql-query-identifier': { identify }, './log': log, '../ClusterOperator/config': config,
    }),
  });
  operator.status = 'OK';
  // Discovery and initial snapshot sync are outside this protocol regression.
  operator.syncLocalDB = async () => {};
  operator.findMaster = async () => {};
  const emulator = load('lib/mysqlServer.js', {
    crypto,
    './mysqlConstants': constants,
    './log': log,
    './ConnectionPool': pool,
    './utill': { convertIP: (ip) => ip },
  });
  const listener = net.createServer((socket) => {
    emulator.createServer({
      socket,
      onAuthorize: () => true,
      onCommand: operator.handleCommand,
      onStmtResult: operator.handleStmtResult,
      operator,
      status: 'OK',
      isNotBacklogQuery: operator.isNotBacklogQuery,
      BACKLOG_DB: 'flux_backlog',
      sendWriteQuery: (...args) => operator.sendWriteQuery(...args),
    });
  });
  listener.listen(0, '127.0.0.1');
  await once(listener, 'listening');
  t.after(() => new Promise((resolve) => { listener.close(resolve); }));
  return {
    operator, backlog, user, config, messages, port: listener.address().port,
  };
}

test('three MariaDB nodes: follower batches, prepared writes and connection reuse', {
  skip: process.env.MARIADB_INTEGRATION !== '1', timeout: 60000,
}, async (t) => {
  const nodes = [];
  const clients = [];
  const applications = [];
  t.after(async () => {
    for (const client of clients) client.destroy();
    for (const node of nodes) {
      node.operator.status = 'COMPRESSING';
      node.operator.closeMasterConnection();
    }
    await Promise.all(applications);
  });
  for (const port of [17306, 17307, 17308]) nodes.push(await nodeInstance(port, t));
  const [master, ...followers] = nodes;
  const transport = new SocketServer(0, { transports: ['websocket'] });
  await once(transport.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { transport.close(resolve); }));
  let sequence = 0;
  transport.on('connection', (socket) => {
    // Same ordering as server.js: master execution, echo, then acknowledgement.
    socket.on('writeQuery', async (sql, connId, callback) => {
      const result = await master.backlog.pushQuery(sql);
      [, sequence] = result;
      socket.broadcast.emit('query', sql, result[1], result[2], false);
      socket.emit('query', sql, result[1], result[2], connId);
      callback({ status: 'OK', result: result[0] });
    });
  });
  for (const follower of followers) {
    follower.operator.masterNode = '127.0.0.1';
    follower.config.containerApiPort = transport.httpServer.address().port;
    follower.operator.initMasterConnection();
    const socket = follower.operator.masterWSConn;
    const apply = socket.listeners('query')[0];
    socket.off('query', apply);
    socket.on('query', (...args) => { applications.push(apply(...args)); });
    await once(follower.operator.masterWSConn, 'connect');
    t.after(() => follower.operator.closeMasterConnection());
  }
  for (const [index, follower] of followers.entries()) {
    const client = await mysql.createConnection({
      host: '127.0.0.1', port: follower.port, user: 'root', multipleStatements: true,
    });
    clients.push(client);
    const errors = [];
    client.on('error', (error) => errors.push(error));
    const database = `follower_case_${index}`;
    await client.query(`DROP DATABASE IF EXISTS ${database}; CREATE DATABASE ${database}; USE ${database}`);
    const statements = Array.from({ length: 8 }, (_, i) => `CREATE TABLE t${i} (id INT PRIMARY KEY)`);
    statements.push('INSERT INTO t0 VALUES (1)');
    const [results] = await client.query(statements.join('; '));
    assert.equal(results.length, 9);
    await timer.setTimeout(100);
    await client.ping();
    const [rows] = await client.query('SELECT COUNT(*) AS count FROM t0');
    assert.equal(rows[0].count, 1);
    await client.execute('INSERT INTO t0 VALUES (2)');
    await client.ping();
    await assert.rejects(client.query('INSERT INTO t0 VALUES (2); INSERT INTO t0 VALUES (3)'), { errno: 1062 });
    await client.query('INSERT INTO t0 VALUES (4)');
    await client.ping();
    assert.equal(errors.length, 0);
    assert.equal(follower.operator.pendingLocalWrites.size, 0);
    for (const node of nodes) {
      // Non-originating replicas are asynchronous; poll the observable data.
      let replicated = false;
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const [records] = await node.user.connection.query(`SELECT id FROM ${database}.t0 ORDER BY id`);
        if (records.map((row) => row.id).join(',') === '1,2,4') { replicated = true; break; }
        await timer.setTimeout(20);
      }
      assert.equal(replicated, true);
    }
  }
  assert.ok(sequence >= 30);
});
