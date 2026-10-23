/* eslint-disable no-restricted-syntax, no-await-in-loop, no-loop-func */
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { Server } = require('socket.io');
const { io } = require('socket.io-client');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const http = require('node:http');
const https = require('node:https');
const os = require('node:os');
const { execFileSync } = require('node:child_process');
const auth = require('../lib/operatorAuth');

const settings = { dbPass: 'a-random-test-password-of-at-least-32-bytes', DBAppName: 'operator-test' };

function load(relativePath, dependencies, transform = (source) => source) {
  const filename = path.join(__dirname, '..', relativePath);
  const module = { exports: {} };
  vm.runInNewContext(transform(fs.readFileSync(filename, 'utf8')), {
    module,
    Buffer,
    setTimeout,
    clearTimeout,
    setInterval: () => {},
    require: (name) => dependencies[name] || {},
    process: { exit: () => { throw new Error('Unexpected server initialization failure'); } },
  }, { filename });
  return module.exports;
}

function configuredAuth(config) {
  return {
    ...auth,
    deriveKey: () => auth.deriveKey(config),
    middleware: (allowed) => auth.middleware(allowed, config),
    clientOptions: (getId) => auth.clientOptions(getId, config),
    peerURL: (ip, port) => auth.peerURL(ip, port, config),
    serverTLSOptions: () => auth.serverTLSOptions(config),
  };
}

test('key derivation is deterministic, separated by cluster and secret, and fails closed', () => {
  assert.deepEqual(auth.deriveKey(settings), auth.deriveKey({ ...settings }));
  assert.notDeepEqual(auth.deriveKey(settings), auth.deriveKey({ ...settings, DBAppName: 'other' }));
  assert.notDeepEqual(auth.deriveKey(settings), auth.deriveKey({ ...settings, dbPass: `${settings.dbPass}!` }));
  const dedicated = { ...settings, operatorApiSecret: 'an-independent-random-secret-at-least-32-bytes' };
  assert.deepEqual(auth.deriveKey(dedicated), auth.deriveKey({ ...dedicated, dbPass: 'changed' }));
  for (const dbPass of [undefined, '', 'secret', 'short']) assert.throws(() => auth.deriveKey({ ...settings, dbPass }));
  assert.throws(() => auth.deriveKey({ ...settings, operatorApiSecret: '' }));
  assert.throws(() => auth.deriveKey({ ...settings, DBAppName: '' }));
});

test('TLS configuration fails closed and never disables certificate verification', () => {
  assert.equal(auth.peerURL('127.0.0.1', 7071, settings), 'http://127.0.0.1:7071');
  assert.equal(auth.serverTLSOptions(settings), null);
  assert.equal(auth.clientOptions(() => 'session', settings).rejectUnauthorized, true);
  assert.throws(() => auth.peerURL('host', 7071, { ...settings, operatorTLSCert: 'cert' }));
  assert.throws(() => auth.clientOptions(() => 'session', { ...settings, operatorTLSCA: 'ca' }));
  assert.equal(auth.peerURL('host', 7071, { ...settings, operatorTLSCert: 'cert', operatorTLSKey: 'key' }), 'https://host:7071');
});

test('operator TLS accepts a trusted identity and rejects untrusted or mismatched certificates', { timeout: 15000 }, async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'operator-tls-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const cert = path.join(directory, 'cert.pem');
  const key = path.join(directory, 'key.pem');
  execFileSync('openssl', [
    'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', '/CN=operator.test', '-addext', 'subjectAltName=DNS:operator.test',
    '-keyout', key, '-out', cert,
  ], { stdio: 'ignore' });
  const tlsSettings = {
    ...settings,
    operatorTLSCert: cert,
    operatorTLSKey: key,
    operatorTLSCA: cert,
    operatorTLSServername: 'operator.test',
  };
  const listener = https.createServer(auth.serverTLSOptions(tlsSettings));
  const server = new Server(listener);
  server.use(auth.middleware(() => true, tlsSettings));
  listener.listen(0, '127.0.0.1');
  await once(listener, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  const url = auth.peerURL('127.0.0.1', listener.address().port, tlsSettings);
  for (const transports of [['websocket'], ['polling']]) {
    for (const [clientSettings, expectedEvent] of [
      [tlsSettings, 'connect'],
      [{ ...tlsSettings, operatorTLSCA: undefined }, 'connect_error'],
      [{ ...tlsSettings, operatorTLSServername: 'wrong.test' }, 'connect_error'],
    ]) {
      const socket = io(url, {
        autoConnect: false, reconnection: false, transports, timeout: 1000,
      });
      Object.assign(socket.io.opts, auth.clientOptions(() => socket.io.engine.id, clientSettings));
      socket.auth = socket.io.opts.auth;
      t.after(() => socket.disconnect());
      const result = new Promise((resolve, reject) => {
        socket.once(expectedEvent, resolve);
        if (expectedEvent === 'connect') socket.once('connect_error', reject);
      });
      socket.connect();
      await result;
      assert.equal(socket.connected, expectedEvent === 'connect');
      socket.disconnect();
    }
  }
});

test('production server gates writes, sessions and secrets; discovery clients authenticate', { timeout: 15000 }, async (t) => {
  const config = { ...settings, apiPort: 0, whiteListedIps: '127.0.0.1' };
  const log = Object.fromEntries(['info', 'debug', 'warn', 'error'].map((level) => [level, () => {}]));
  let server;
  let writes = 0;
  let sessions = 0;
  const operator = {
    OpNodes: [],
    AppNodes: [],
    status: 'OK',
    IamMaster: true,
    myIP: '127.0.0.1',
    keys: {},
    resolveAppName: async () => {},
    init: async () => {},
    findMaster: async () => {},
    getMaster: () => '127.0.0.1',
    setServerSocket: (ioServer) => { server = ioServer; },
  };
  const init = load('ClusterOperator/server.js', {
    'socket.io': { Server },
    http,
    'memory-cache': { put: () => {} },
    './config': config,
    './Operator': operator,
    '../lib/operatorAuth': configuredAuth(config),
    './Security': {
      init: () => {},
      publicEncrypt: () => 'encrypted',
      getCommAESKey: () => 'private',
      getCommAESIv: () => 'iv',
      getKey: () => 'password',
      getIV: () => 'iv',
      encryptComm: () => 'encrypted',
    },
    './IdService': { init: () => {}, addNewSession: () => { sessions += 1; } },
    './Backlog': {
      pushQuery: async () => { writes += 1; return [{ affectedRows: 1 }, 1, 1]; },
      getFirstSequenceNumber: async () => 1,
      getAllKeys: async () => ({}),
    },
    '../lib/utill': { convertIP: () => '127.0.0.1' },
    '../lib/log': log,
  }, (source) => source.replace(/initServer\(\);\s*$/, 'startUI = async () => {}; module.exports = initServer;'));
  await init();
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  if (!server.httpServer.listening) await once(server.httpServer, 'listening');
  const { port } = server.httpServer.address();
  const unauthorized = io(`http://127.0.0.1:${port}`, { autoConnect: false, reconnection: false });
  t.after(() => unauthorized.disconnect());
  let responses = 0;
  for (const [event, args] of [
    ['writeQuery', ['DROP DATABASE example', 1]], ['shareKeys', ['attacker-public-key']],
    ['getKeys', []], ['userSession', ['add', 'forged-session', 'value']],
  ]) unauthorized.emit(event, ...args, () => { responses += 1; });
  const denied = once(unauthorized, 'connect_error');
  unauthorized.connect();
  assert.match((await denied)[0].message, /authentication failed/);
  unauthorized.disconnect();
  assert.equal(writes, 0);
  assert.equal(sessions, 0);
  assert.equal(responses, 0);
  assert.equal(server.sockets.sockets.size, 0);

  const discovery = load('lib/fluxAPI.js', {
    https, 'socket.io-client': { io }, './log': log, './operatorAuth': configuredAuth(config),
  });
  assert.equal(await discovery.getMaster('127.0.0.1', port), '127.0.0.1');
  assert.equal(await discovery.getMyIp('127.0.0.1', port), '127.0.0.1');
  assert.equal((await discovery.getStatus('127.0.0.1', port)).status, 'OK');
  assert.equal((await discovery.resetMaster('127.0.0.1', port)).status, 'OK');

  const socket = io(`http://127.0.0.1:${port}`, { autoConnect: false, reconnection: false });
  socket.auth = auth.clientOptions(() => socket.io.engine.id, config).auth;
  t.after(() => socket.disconnect());
  const connected = once(socket, 'connect');
  socket.connect();
  await connected;
  assert.equal((await socket.timeout(1000).emitWithAck('writeQuery', 'INSERT INTO example VALUES (1)', 1)).status, 'OK');
  assert.equal((await socket.timeout(1000).emitWithAck('userSession', 'add', 'session', 'value')).status, 'OK');
  assert.equal(writes, 1);
  assert.equal(sessions, 1);
});

test('real Socket.IO rejects same-IP unauthenticated clients and replays before handlers or broadcasts', { timeout: 15000 }, async (t) => {
  const server = new Server(0);
  await once(server.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  let allowed = true;
  let connections = 0;
  let writes = 0;
  server.use(auth.middleware(() => allowed, settings));
  server.on('connection', (socket) => {
    connections += 1;
    socket.on('writeQuery', (query, callback) => { writes += 1; callback(query); });
    socket.on('getStatus', (callback) => callback({ status: 'OK' }));
  });
  const url = `http://127.0.0.1:${server.httpServer.address().port}`;
  function client(options = {}) {
    const socket = io(url, {
      autoConnect: false, reconnection: false, timeout: 1000, ...options,
    });
    t.after(() => socket.disconnect());
    return socket;
  }
  async function reject(options) {
    const socket = client(options);
    let broadcasts = 0;
    socket.on('query', () => { broadcasts += 1; });
    socket.emit('writeQuery', 'unauthorized', () => assert.fail('Unauthorized write accepted'));
    const failure = once(socket, 'connect_error');
    socket.connect();
    assert.match((await failure)[0].message, /authentication failed/);
    server.emit('query', 'private');
    assert.equal(socket.connected, false);
    assert.equal(broadcasts, 0);
    socket.disconnect();
  }
  for (const operatorProof of [undefined, null, {}, '', 'a'.repeat(63), 'g'.repeat(64), '0'.repeat(64)]) {
    await reject({ auth: { operatorProof } });
  }
  await reject({ auth: (callback) => callback({ operatorProof: auth.proof(auth.deriveKey(settings), 'old-session') }) });
  assert.equal(connections, 0);
  assert.equal(writes, 0);

  for (const transports of [['websocket'], ['polling']]) {
    let captured;
    const socket = client({ transports });
    const options = auth.clientOptions(() => socket.io.engine.id, settings);
    socket.auth = (callback) => options.auth((credentials) => { captured = credentials; callback(credentials); });
    const connected = once(socket, 'connect');
    socket.connect();
    await connected;
    assert.deepEqual(await socket.timeout(1000).emitWithAck('getStatus'), { status: 'OK' });
    assert.equal(await socket.timeout(1000).emitWithAck('writeQuery', 'authorized'), 'authorized');
    const firstProof = captured.operatorProof;
    socket.disconnect();
    await reject({ auth: captured, transports });
    const reconnected = once(socket, 'connect');
    socket.connect();
    await reconnected;
    assert.notEqual(captured.operatorProof, firstProof);
    socket.disconnect();
  }
  assert.equal(writes, 2);
  const denied = client();
  denied.auth = auth.clientOptions(() => denied.io.engine.id, settings).auth;
  allowed = false;
  const failure = once(denied, 'connect_error');
  denied.connect();
  assert.match((await failure)[0].message, /authentication failed/);
  const wrongCluster = client();
  allowed = true;
  wrongCluster.auth = auth.clientOptions(() => wrongCluster.io.engine.id, { ...settings, DBAppName: 'other' }).auth;
  const wrongFailure = once(wrongCluster, 'connect_error');
  wrongCluster.connect();
  assert.match((await wrongFailure)[0].message, /authentication failed/);
});
