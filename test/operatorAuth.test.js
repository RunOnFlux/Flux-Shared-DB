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
const { WebSocket } = require('ws');
const auth = require('../lib/operatorAuth');

const settings = {
  dbPass: 'a-random-test-password-of-at-least-32-bytes', DBAppName: 'operator-test', operatorAuthEnforceAt: '2000-01-01T00:00:00Z',
};

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
  for (const dbPass of [undefined, '']) assert.throws(() => auth.deriveKey({ ...settings, dbPass }));
  for (const dbPass of ['secret', 'short']) assert.equal(auth.deriveKey({ ...settings, dbPass }).length, 32);
  for (const operatorApiSecret of ['', 'secret', 'short']) {
    assert.throws(() => auth.deriveKey({ ...settings, operatorApiSecret }));
  }
  assert.throws(() => auth.deriveKey({ ...settings, DBAppName: '' }));
});

test('key preparation is lazy, validates secondary keys and invalidates on identity changes', () => {
  assert.throws(() => auth.initialize({ ...settings, DBAppName: '' }));
  assert.throws(() => auth.initialize({ ...settings, operatorApiSecretSecondary: '' }));
  const config = { ...settings };
  let first;
  auth.clientOptions(() => 'session', config).auth((data) => { first = data.operatorProof; });
  config.DBAppName = 'changed-identity';
  auth.clientOptions(() => 'session', config).auth((data) => assert.notEqual(data.operatorProof, first));
});

test('two-phase secondary-key rotation works bidirectionally without accepting unrelated or retired keys', { timeout: 15000 }, async (t) => {
  const oldKey = 'A'.repeat(32);
  const nextKey = 'B'.repeat(32);
  const configs = {
    original: { ...settings, operatorApiSecret: oldKey },
    prepared: { ...settings, operatorApiSecret: oldKey, operatorApiSecretSecondary: nextKey },
    switched: { ...settings, operatorApiSecret: nextKey, operatorApiSecretSecondary: oldKey },
    retired: { ...settings, operatorApiSecret: nextKey },
  };
  const servers = {};
  for (const [name, config] of Object.entries(configs)) {
    const server = new Server(0);
    await once(server.httpServer, 'listening');
    t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
    auth.install(server, () => true, config);
    server.on('connection', (socket) => socket.on('getStatus', (callback) => callback({ status: 'OK' })));
    servers[name] = server;
  }
  async function connect(clientConfig, serverName, succeeds) {
    const server = servers[serverName];
    const socket = io(`http://127.0.0.1:${server.httpServer.address().port}`, { autoConnect: false, reconnection: false });
    socket.auth = auth.clientOptions(() => socket.io.engine.id, clientConfig, 'replica').auth;
    t.after(() => socket.disconnect());
    const result = new Promise((resolve, reject) => {
      socket.once(succeeds ? 'connect' : 'connect_error', resolve);
      if (succeeds) socket.once('connect_error', reject);
    });
    socket.connect();
    await result;
    assert.equal(socket.connected, succeeds);
    if (succeeds) assert.equal((await socket.timeout(1000).emitWithAck('getStatus')).status, 'OK');
    socket.disconnect();
  }
  for (const [a, b] of [['original', 'prepared'], ['prepared', 'switched'], ['switched', 'retired']]) {
    await connect(configs[a], b, true);
    await connect(configs[b], a, true);
  }
  await connect(configs.original, 'retired', false);
  await connect(configs.switched, 'original', false); // Skipping preparation is deliberately unsupported.
  await connect({ ...settings, operatorApiSecret: 'C'.repeat(32) }, 'prepared', false);
  const shortOld = { ...settings, dbPass: 'secret', operatorApiSecretSecondary: nextKey };
  const migration = { ...settings, operatorApiSecret: nextKey, operatorApiSecretSecondary: 'secret' };
  const server = new Server(0);
  await once(server.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  auth.install(server, () => true, migration);
  servers.migration = server;
  server.on('connection', (socket) => socket.on('getStatus', (callback) => callback({ status: 'OK' })));
  await connect(shortOld, 'migration', true);
});

test('raw and rejected Engine.IO sessions and discovery sockets do not satisfy replica readiness', { timeout: 10000 }, async (t) => {
  const server = new Server(0);
  await once(server.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  auth.install(server, () => true, settings);
  const { port } = server.httpServer.address();
  const raw = new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`);
  t.after(() => raw.terminate());
  await once(raw, 'open');
  assert.equal(server.engine.clientsCount, 1);
  assert.equal(auth.hasReplicationPeer(server, settings), false);
  const rejected = new Promise((resolve) => { raw.on('message', (data) => { if (data.toString().startsWith('44')) resolve(); }); });
  raw.send(`40${JSON.stringify({ operatorProof: '0'.repeat(64) })}`);
  await rejected;
  assert.equal(server.engine.clientsCount, 1);
  assert.equal(auth.hasReplicationPeer(server, settings), false);
  raw.terminate();
  for (const role of ['discovery', 'replica']) {
    const socket = io(`http://127.0.0.1:${port}`, { autoConnect: false, reconnection: false });
    socket.auth = auth.clientOptions(() => socket.io.engine.id, settings, role).auth;
    t.after(() => socket.disconnect());
    const connected = once(socket, 'connect');
    socket.connect();
    await connected;
    assert.equal(auth.hasReplicationPeer(server, settings), role === 'replica');
    socket.disconnect();
  }
  const legacySocket = { connected: true, data: { operatorReplica: true, operatorAuthenticated: false } };
  const legacyServer = { sockets: { sockets: new Map([['legacy', legacySocket]]) } };
  assert.equal(auth.hasReplicationPeer(legacyServer, { ...settings, operatorAuthEnforceAt: '2100-01-01T00:00:00Z' }), true);
  assert.equal(auth.hasReplicationPeer(legacyServer, settings), false);
});

test('admitted older peers without roles count during snapshot import without weakening the cutoff', { timeout: 10000 }, async (t) => {
  for (const [authenticated, grace, succeeds] of [[false, true, true], [true, true, true], [true, false, true], [false, false, false]]) {
    const config = { ...settings, operatorAuthEnforceAt: grace ? '2100-01-01T00:00:00Z' : settings.operatorAuthEnforceAt };
    const server = new Server(0);
    await once(server.httpServer, 'listening');
    t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
    auth.install(server, () => true, config);
    const socket = io(`http://127.0.0.1:${server.httpServer.address().port}`, { autoConnect: false, reconnection: false });
    if (authenticated) {
      // The previously released client sends a proof but no replica-role field.
      socket.auth = (callback) => callback({ operatorProof: auth.proof(auth.deriveKey(config), socket.io.engine.id) });
    }
    t.after(() => socket.disconnect());
    const result = new Promise((resolve, reject) => {
      socket.once(succeeds ? 'connect' : 'connect_error', resolve);
      if (succeeds) socket.once('connect_error', reject);
    });
    socket.connect();
    await result;
    // No getBackLog or other application packet has been sent yet.
    assert.equal(auth.hasReplicationPeer(server, config), succeeds);
    socket.disconnect();
  }
});

test('TLS configuration and keys are cached lazily and the CA is read once', () => {
  const reads = [];
  let derivations = 0;
  const crypto = require('node:crypto'); // eslint-disable-line global-require
  const cached = load('lib/operatorAuth.js', {
    crypto: { ...crypto, hkdfSync: (...args) => { derivations += 1; return crypto.hkdfSync(...args); } },
    fs: { readFileSync: (file) => { reads.push(file); return Buffer.from(file); } },
    https,
  });
  assert.deepEqual(reads, []);
  const config = {
    ...settings, operatorTLSCert: 'cert', operatorTLSKey: 'key', operatorTLSCA: 'ca',
  };
  const first = cached.clientOptions(() => 'session', config);
  const second = cached.clientOptions(() => 'session', config);
  cached.serverTLSOptions(config);
  assert.equal(first.agent, second.agent);
  assert.equal(first.ca, second.ca);
  assert.deepEqual(reads.sort(), ['ca', 'cert', 'key']);
  assert.equal(derivations, 1);
  first.agent.destroy();
});

test('cutoff is a fixed UTC instant and invalid overrides fail closed', () => {
  assert.equal(auth.DEFAULT_ENFORCE_AT, '2026-10-25T17:48:17Z');
  assert.equal(auth.enforcementTime({}), Date.parse('2026-10-25T17:48:17Z'));
  for (const operatorAuthEnforceAt of ['', null, 'tomorrow', '2026-10-25', '2026-10-25T17:48:17',
    '2026-10-25T19:48:17+02:00', '2026-02-30T00:00:00Z', '2026-10-25T25:00:00Z']) {
    assert.throws(() => auth.enforcementTime({ operatorAuthEnforceAt }));
  }
  assert.equal(auth.enforcementTime({ operatorAuthEnforceAt: '2026-10-25T17:48:17.123Z' }), Date.parse('2026-10-25T17:48:17.123Z'));
});

for (const transports of [['websocket'], ['polling']]) {
  test(`legacy grace and live cutoff (${transports[0]}) preserve authenticated peers and block stale sockets`, { timeout: 15000 }, async (t) => {
    const server = new Server(0);
    await once(server.httpServer, 'listening');
    t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
    const config = { ...settings, operatorAuthEnforceAt: auth.DEFAULT_ENFORCE_AT };
    const cutoff = auth.enforcementTime(config);
    let now = cutoff - 1000;
    let wake;
    let allowed = true;
    let writes = 0;
    const policy = auth.install(server, () => allowed, config, {
      now: () => now,
      setTimer: (callback, delay) => { wake = callback; assert.equal(delay, 1000); return { unref() {} }; },
      clearTimer: () => {},
    });
    server.on('connection', (socket) => {
      socket.on('writeQuery', (query, callback) => { writes += 1; callback(query); });
      socket.on('getStatus', (callback) => callback({ status: 'OK' }));
    });
    const url = `http://127.0.0.1:${server.httpServer.address().port}`;
    async function client(authenticated, expected = 'connect', credentials) {
      const socket = io(url, {
        autoConnect: false, reconnection: false, timeout: 1000, transports,
      });
      if (authenticated) socket.auth = auth.clientOptions(() => socket.io.engine.id, config).auth;
      if (credentials !== undefined) socket.auth = credentials;
      t.after(() => socket.disconnect());
      const result = new Promise((resolve, reject) => {
        socket.once(expected, resolve);
        if (expected === 'connect') socket.once('connect_error', reject);
      });
      socket.connect();
      await result;
      assert.equal(socket.connected, expected === 'connect');
      return socket;
    }
    for (const operatorProof of [null, '', '0'.repeat(64)]) {
      await client(false, 'connect_error', { operatorProof });
    }
    allowed = false;
    await client(false, 'connect_error');
    await client(true, 'connect_error');
    allowed = true;
    const legacyActive = await client(false);
    const legacyPassive = await client(false);
    const authenticated = await client(true);
    assert.equal(await legacyActive.timeout(1000).emitWithAck('writeQuery', 'legacy write'), 'legacy write');
    assert.equal(await authenticated.timeout(1000).emitWithAck('writeQuery', 'authenticated write'), 'authenticated write');
    now = cutoff - 1;
    assert.equal((await legacyActive.timeout(1000).emitWithAck('getStatus')).status, 'OK');
    const beforeCutoffBroadcast = once(legacyPassive, 'query');
    server.emit('query', 'legacy replication');
    assert.equal((await beforeCutoffBroadcast)[0], 'legacy replication');
    now = cutoff;
    // Packet guard applies before the scheduled callback is allowed to run.
    const activeDisconnected = once(legacyActive, 'disconnect');
    legacyActive.emit('writeQuery', 'must not run', () => assert.fail('Legacy write passed cutoff'));
    await activeDisconnected;
    assert.equal(writes, 2);
    const passiveDisconnected = once(legacyPassive, 'disconnect');
    wake();
    await passiveDisconnected;
    assert.equal(server.sockets.sockets.size, 1);
    assert.equal(authenticated.connected, true);
    const authenticatedBroadcast = once(authenticated, 'query');
    server.emit('query', 'private replication');
    assert.equal((await authenticatedBroadcast)[0], 'private replication');
    assert.equal(await authenticated.timeout(1000).emitWithAck('writeQuery', 'after cutoff'), 'after cutoff');
    await client(false, 'connect_error');
    await client(true);
    // Once enforced, a backwards clock adjustment must not reopen legacy access.
    now = cutoff - 10000;
    assert.equal(policy.isEnforced(), true);
    await client(false, 'connect_error');
  });
}

test('old-style servers accept new authenticated clients without a protocol downgrade', { timeout: 10000 }, async (t) => {
  const server = new Server(0);
  await once(server.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  server.on('connection', (socket) => {
    socket.on('getStatus', (callback) => callback({ status: 'OK' }));
  });
  const socket = io(`http://127.0.0.1:${server.httpServer.address().port}`, { autoConnect: false, reconnection: false });
  socket.auth = auth.clientOptions(() => socket.io.engine.id, settings).auth;
  t.after(() => socket.disconnect());
  const connected = once(socket, 'connect');
  socket.connect();
  await connected;
  assert.deepEqual(await socket.timeout(1000).emitWithAck('getStatus'), { status: 'OK' });
});

test('scheduled enforcement drops an idle legacy socket without receiving another packet', { timeout: 10000 }, async (t) => {
  const server = new Server(0);
  await once(server.httpServer, 'listening');
  t.after(() => new Promise((resolve) => { server.disconnectSockets(true); server.close(resolve); }));
  const config = { ...settings, operatorAuthEnforceAt: auth.DEFAULT_ENFORCE_AT };
  const cutoff = auth.enforcementTime(config);
  let now = cutoff - 50;
  auth.install(server, () => true, config, { now: () => now });
  const socket = io(`http://127.0.0.1:${server.httpServer.address().port}`, { autoConnect: false, reconnection: false });
  t.after(() => socket.disconnect());
  const connected = once(socket, 'connect');
  socket.connect();
  await connected;
  const disconnected = once(socket, 'disconnect');
  now = cutoff;
  await disconnected;
  assert.equal(socket.connected, false);
  assert.equal(server.sockets.sockets.size, 0);
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

for (const [label, dbPass] of [['configured', settings.dbPass], ['legacy defaults', 'secret']]) {
  test(`production server (${label}) gates writes, sessions and secrets; discovery clients authenticate`, { timeout: 15000 }, async (t) => {
    const config = {
      ...settings, dbPass, apiPort: 0, whiteListedIps: '127.0.0.1',
    };
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
        getLogs: async () => [],
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
    assert.equal(auth.hasReplicationPeer(server, config), false);
    assert.equal((await socket.timeout(1000).emitWithAck('getBackLog', 1)).status, 'OK');
    assert.equal(auth.hasReplicationPeer(server, config), true);
    assert.equal((await socket.timeout(1000).emitWithAck('writeQuery', 'INSERT INTO example VALUES (1)', 1)).status, 'OK');
    assert.equal((await socket.timeout(1000).emitWithAck('userSession', 'add', 'session', 'value')).status, 'OK');
    assert.equal(writes, 1);
    assert.equal(sessions, 1);
  });
}

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
