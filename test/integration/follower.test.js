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
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const mysql = require('mysql2/promise');
// The legacy ESLint resolver does not understand this package's CJS exports.
// eslint-disable-next-line import/no-unresolved
const mariadb = require('mariadb');
const { Server: SocketServer } = require('socket.io');
const socketClient = require('socket.io-client');
const { identify } = require('sql-query-identifier');
const constants = require('../../lib/mysqlConstants');
const operatorAuth = require('../../lib/operatorAuth');
const backendStream = require('../../lib/mysqlBackendStream');
const Importer = require('../../lib/mysqlimport');
const mysqldump = require('../../lib/mysqldump');

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
    dbPass: 'secret',
    DBAppName: 'integration-test',
    dbPort: port,
    dbType: 'mysql',
    clientType: 'mariadb',
    dbUser: 'root',
    whiteListedIps: '127.0.0.1',
    dbBacklog: 'flux_backlog',
    dbBacklogCollection: 'backlog',
    dbBacklogBuffer: 'backlog_buffer',
    dbOptions: 'options',
    dbInitDB: 'restore_case',
    containerDataPath: '',
  };
  const messages = [];
  const log = Object.fromEntries(['info', 'debug', 'warn', 'error'].map((level) => [level, (message) => messages.push(String(message))]));
  const factory = load('ClusterOperator/DBClient.js', {
    'mysql2/promise': mysql,
    '../lib/mysqlBackendStream': backendStream,
    net,
    './config': config,
    './Security': { getKey: () => 'secret' },
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
  await user.connection.query('CREATE TABLE IF NOT EXISTS flux_backlog.options (k VARCHAR(64) PRIMARY KEY, value TEXT)');
  await user.connection.query('TRUNCATE TABLE flux_backlog.options');
  await user.connection.query('CREATE TABLE IF NOT EXISTS flux_backlog.backlog_buffer (seq BIGINT PRIMARY KEY, query LONGTEXT, timestamp BIGINT)');
  await user.connection.query('CREATE DATABASE IF NOT EXISTS restore_case');
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
    net,
    'socket.io-client': socketClient,
    '../lib/operatorAuth': {
      ...operatorAuth,
      peerURL: (ip, apiPort) => operatorAuth.peerURL(ip, apiPort, config),
      clientOptions: (getSessionId, _settings, role) => operatorAuth.clientOptions(getSessionId, config, role),
    },
    './Backlog': backlog,
    fs,
    'timers/promises': timer,
    '../lib/mysqlimport': Importer,
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
      getPassword: () => 'secret',
      onAuthorize: operator.handleAuthorize,
      appIPList: [],
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
    operator,
    backlog,
    user,
    config,
    messages,
    port: listener.address().port,
    allocatedConnections: () => poolConnections.length,
  };
}

const databaseEngine = process.env.DB_INTEGRATION || (process.env.MARIADB_INTEGRATION === '1' ? 'mariadb' : '');
if (databaseEngine && !['mysql', 'mariadb'].includes(databaseEngine)) throw new Error('DB_INTEGRATION must be mysql or mariadb');

test(`three ${databaseEngine || 'database'} nodes: raw values, follower replication and snapshot recovery`, {
  skip: !databaseEngine, timeout: 60000,
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
  const authSettings = { ...master.config, operatorAuthEnforceAt: operatorAuth.DEFAULT_ENFORCE_AT };
  const cutoff = operatorAuth.enforcementTime(authSettings);
  let authNow = cutoff - 60 * 60 * 1000;
  const authPolicy = operatorAuth.install(transport, () => true, authSettings, { now: () => authNow });
  assert.equal(operatorAuth.hasReplicationPeer(transport, authSettings), false);
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
  // Exercise a legacy writer before cutoff, then keep authenticated replication
  // sockets alive while enforcing the deadline on that legacy connection.
  const legacy = socketClient.io(`http://127.0.0.1:${transport.httpServer.address().port}`, {
    autoConnect: false, reconnection: false, transports: ['websocket'],
  });
  t.after(() => legacy.disconnect());
  const legacyConnected = once(legacy, 'connect');
  legacy.connect();
  await legacyConnected;
  const legacyResult = await legacy.timeout(2000).emitWithAck('writeQuery', 'CREATE DATABASE IF NOT EXISTS auth_grace_test', false);
  assert.equal(legacyResult.status, 'OK');
  const legacyDisconnected = once(legacy, 'disconnect');
  authNow = cutoff;
  authPolicy.enforce();
  await legacyDisconnected;
  for (const follower of followers) assert.equal(follower.operator.masterWSConn.connected, true);
  const legacyRejected = once(legacy, 'connect_error');
  legacy.connect();
  assert.match((await legacyRejected)[0].message, /authentication failed/);
  legacy.disconnect();
  assert.equal(operatorAuth.hasReplicationPeer(transport, authSettings), true);
  for (const [index, follower] of followers.entries()) {
    if (index === 0 && process.env.MYSQL_CLI_IMAGE) {
      const run = promisify(execFile);
      const args = ['run', '--rm', '--network', 'host', '--entrypoint', 'mysql', process.env.MYSQL_CLI_IMAGE,
        '--protocol=TCP', '--host=127.0.0.1', `--port=${follower.port}`, '--user=root',
        '--ssl-mode=DISABLED', '--default-auth=caching_sha2_password', '--skip-column-names', '--execute=SELECT 1'];
      const { stdout } = await run('docker', [...args, '--password=secret'], { timeout: 20000 });
      assert.equal(stdout.trim(), '1');
      const beforeBadCLI = follower.allocatedConnections();
      await assert.rejects(
        run('docker', [...args, '--password=wrong-password'], { timeout: 20000 }),
        (error) => error.stderr.includes('1045'),
      );
      assert.equal(follower.allocatedConnections(), beforeBadCLI);
    }
    const beforeAuthFailures = follower.allocatedConnections();
    for (const createConnection of [mysql.createConnection.bind(mysql), mariadb.createConnection.bind(mariadb)]) {
      for (const credentials of [
        { user: 'root', password: 'wrong-password' },
        { user: 'root' },
        { user: 'wrong-user', password: 'secret' },
      ]) {
        await assert.rejects(
          createConnection({ host: '127.0.0.1', port: follower.port, ...credentials }),
          (error) => error.errno === 1045 && error.sqlState === '28000',
        );
      }
    }
    assert.equal(follower.allocatedConnections(), beforeAuthFailures);
    const mariaClient = await mariadb.createConnection({
      host: '127.0.0.1', port: follower.port, user: 'root', password: 'secret',
    }).catch((error) => {
      throw new Error(`${error.message}\nOperator logs:\n${follower.messages.slice(-12).join('\n')}`);
    });
    await mariaClient.ping();
    const mariaRows = await mariaClient.query('SELECT CAST(9007199254740993 AS UNSIGNED) AS n');
    assert.equal(mariaRows[0].n, 9007199254740993n);
    await mariaClient.end();
    const client = await mysql.createConnection({
      host: '127.0.0.1', port: follower.port, user: 'root', password: 'secret', multipleStatements: true, supportBigNumbers: true, bigNumberStrings: true,
    });
    clients.push(client);
    // Raw reads must preserve values with standard backend metadata on either engine.
    const [values] = await client.query(
      "SELECT 'snow ☃' AS text_value, CAST('bytes' AS BINARY) AS binary_value, NULL AS null_value",
    );
    assert.equal(values[0].text_value, 'snow ☃');
    assert.deepEqual(values[0].binary_value, Buffer.from('bytes'));
    assert.equal(values[0].null_value, null);
    const [precise] = await client.query("SELECT CAST(9007199254740993 AS UNSIGNED) AS n, JSON_OBJECT('answer', 42) AS j");
    assert.equal(precise[0].n, '9007199254740993');
    assert.deepEqual(typeof precise[0].j === 'string' ? JSON.parse(precise[0].j) : precise[0].j, { answer: 42 });
    const [preparedValues] = await client.execute("SELECT CAST(9007199254740993 AS UNSIGNED) AS n, JSON_OBJECT('answer', 42) AS j");
    assert.equal(preparedValues[0].n, '9007199254740993');
    assert.deepEqual(typeof preparedValues[0].j === 'string' ? JSON.parse(preparedValues[0].j) : preparedValues[0].j, { answer: 42 });
    const [batchValues] = await client.query("SELECT CAST(9007199254740993 AS UNSIGNED) AS n; SELECT JSON_OBJECT('answer',42) AS j");
    assert.equal(batchValues[0][0].n, '9007199254740993');
    assert.deepEqual(typeof batchValues[1][0].j === 'string' ? JSON.parse(batchValues[1][0].j) : batchValues[1][0].j, { answer: 42 });
    const [cte] = await client.query('WITH x AS (SELECT 42 AS n) SELECT n FROM x');
    assert.equal(cte[0].n, 42);
    const [plan] = await client.query('EXPLAIN SELECT 1');
    assert.ok(plan.length);
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
    assert.equal(rows[0].count, '1');
    await client.execute('INSERT INTO t0 VALUES (2)');
    await client.ping();
    await assert.rejects(client.query('INSERT INTO t0 VALUES (2); INSERT INTO t0 VALUES (3)'), { errno: 1062 });
    await client.query('INSERT INTO t0 VALUES (4)');
    await client.ping();
    assert.equal(errors.length, 0);
    assert.equal(follower.operator.pendingLocalWrites.size, 0);
    for (const node of nodes) {
      await node.user.connection.query(`DROP PROCEDURE IF EXISTS ${database}.metadata_p`);
      await node.user.connection.query(`CREATE PROCEDURE ${database}.metadata_p() BEGIN SELECT 7 AS n; SELECT 8 AS n; END`);
    }
    const [procedure] = await client.query(`CALL ${database}.metadata_p()`);
    assert.equal(String(procedure[0][0].n), '7');
    assert.equal(String(procedure[1][0].n), '8');
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
  // Round-trip the production dumper's trigger syntax, then fail after a real
  // replicated prefix and prove that both existing and new clients are fenced.
  await master.user.connection.query('USE restore_case');
  await master.user.connection.query('DROP TABLE IF EXISTS trigger_items');
  await master.user.connection.query('CREATE TABLE trigger_items (id INT)');
  await master.user.connection.query('CREATE TRIGGER restore_trigger BEFORE INSERT ON trigger_items FOR EACH ROW BEGIN SET NEW.id=NEW.id+1; END');
  await master.user.connection.query('INSERT INTO trigger_items VALUES (4)');
  const snapshot = `integration_restore_${process.pid}_${crypto.randomBytes(4).toString('hex')}`;
  const snapshotFile = path.join(__dirname, '../../dumps', `${snapshot}.sql`);
  t.after(() => fs.rmSync(snapshotFile, { force: true }));
  await mysqldump({
    connection: {
      host: '127.0.0.1', port: 17306, user: 'root', password: 'secret', database: 'restore_case',
    },
    dump: { schema: { table: { dropIfExist: true } } },
    dumpToFile: snapshotFile,
  });
  const validDump = fs.readFileSync(snapshotFile, 'utf8');
  assert.match(validDump, /DELIMITER ;;/);
  master.operator.IamMaster = true;
  master.operator.masterNode = '127.0.0.1';
  master.operator.serverSocket = transport;
  fs.appendFileSync(snapshotFile, '\nINSERT INTO missing_restore_table VALUES(1);\n');
  await assert.rejects(master.operator.restoreSnapshot(snapshot, { replicate: true }), { code: 'ER_NO_SUCH_TABLE' });
  for (const node of nodes) {
    assert.equal(node.operator.status, 'IMPORT_FAILED', node.messages.slice(-10).join('\n'));
    assert.ok(await node.backlog.getKey('snapshotImportFailure', false, true));
  }
  for (const client of clients) {
    await assert.rejects(client.query('SELECT 1'), { errno: 1047 });
    await assert.rejects(client.execute('SELECT 1'));
  }
  for (const follower of followers) {
    await assert.rejects(mysql.createConnection({
      host: '127.0.0.1', port: follower.port, user: 'root', password: 'secret',
    }), { errno: 1045 });
  }
  fs.writeFileSync(snapshotFile, validDump);
  await master.operator.restoreSnapshot(snapshot, { replicate: true });
  for (const node of nodes) {
    assert.equal(node.operator.status, 'OK');
    assert.equal(await node.backlog.getKey('snapshotImportFailure', false, true), null);
    const [triggers] = await node.user.connection.query('SHOW TRIGGERS FROM restore_case');
    assert.equal(triggers.length, 1);
    const [records] = await node.user.connection.query('SELECT id FROM restore_case.trigger_items');
    assert.equal(records[0].id, 5);
    await node.user.connection.query('INSERT INTO restore_case.trigger_items VALUES (9)');
    const [inserted] = await node.user.connection.query('SELECT id FROM restore_case.trigger_items WHERE id=10');
    assert.equal(inserted.length, 1);
  }
});
