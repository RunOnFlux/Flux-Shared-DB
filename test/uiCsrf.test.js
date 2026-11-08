/* eslint-disable no-restricted-syntax, no-await-in-loop, class-methods-use-this */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { once, EventEmitter } = require('node:events');
const { Readable, Writable } = require('node:stream');
const express = require('express');
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const cors = require('cors');
const crypto = require('node:crypto');
const { secp256k1 } = require('@noble/curves/secp256k1');
const bitcoinMessage = require('../lib/bitcoinMessage');
const csrf = require('../lib/uiCsrf');
const installCsrfInterceptor = require('../ui/assets/csrf');
const utill = require('../lib/utill');

const SESSION = `1790000000000${'session'.repeat(6)}`;
const OTHER_SESSION = `1790000000001${'other'.repeat(6)}`;

async function fixture(t, protection = true, identity = null, peerIP = null) {
  const actions = [];
  let listener;
  function expressFactory() {
    const app = express();
    if (peerIP) app.use((req, _res, next) => { Object.defineProperty(req, 'ip', { value: peerIP }); next(); });
    const listen = app.listen.bind(app);
    app.listen = (...args) => { listener = listen(...args); return listener; };
    return app;
  }
  expressFactory.Router = express.Router;
  const mockFS = {
    readFile(file, _encoding, callback) {
      actions.push({ readLog: file });
      callback(null, 'first<br>second<br>third<br>');
    },
    writeFileSync() {},
    appendFileSync() {},
    constants: fs.constants,
    promises: { mkdir: async () => {}, access: async () => {} },
    createWriteStream: () => new Writable({ write: (_chunk, _encoding, done) => done() }),
  };
  const download = {
    get: (url, _options, callback) => {
      actions.push({ download: url });
      const downloadRequest = new EventEmitter();
      process.nextTick(() => {
        const response = Readable.from(['-- harmless fixture SQL']);
        response.statusCode = 200;
        response.headers = {};
        callback(response);
      });
      return downloadRequest;
    },
  };
  class Importer {
    onProgress() {}

    setEncoding() {}

    async import(file) { actions.push({ import: file }); }

    getImported() { return ['fixture']; }
  }
  const log = Object.fromEntries(['info', 'warn', 'error', 'debug'].map((level) => [level, () => {}]));
  const dependencies = {
    express: expressFactory,
    'express-rate-limit': () => (_req, _res, next) => next(),
    'express-fileupload': () => (_req, _res, next) => next(),
    'body-parser': bodyParser,
    'cookie-parser': cookieParser,
    cors,
    fs: mockFS,
    path,
    http: download,
    https: download,
    'timers/promises': { setTimeout: async () => {} },
    'sanitize-filename': require('sanitize-filename'), // eslint-disable-line global-require
    '../lib/utill': utill,
    '../lib/log': log,
    '../lib/mysqlimport': Importer,
    '../lib/uiCsrf': protection ? csrf : { ...csrf, cookieCsrfProtection: (_req, _res, next) => next() },
    './config': { debugUIPort: 0, ssl: false, whiteListedIps: '127.0.0.1' },
    './Operator': { IamMaster: true, emitUserSession() {}, rollBack: async (seqNo) => actions.push({ rollback: seqNo }) },
    './Backlog': {},
    './IdService': identity || { verifySession: (session, ip) => session === SESSION && ip === '127.0.0.1' },
  };
  const filename = path.join(__dirname, '../ClusterOperator/server.js');
  const module = { exports: {} };
  const source = fs.readFileSync(filename, 'utf8').replace(/initServer\(\);\s*$/, 'module.exports = startUI;');
  vm.runInNewContext(source, {
    module,
    __dirname: path.dirname(filename),
    Buffer,
    URL,
    process,
    require: (name) => dependencies[name] || {},
  }, { filename });
  await module.exports();
  t.after(() => new Promise((resolve) => { listener.closeAllConnections(); listener.close(resolve); }));
  if (!listener.listening) await once(listener, 'listening');
  const base = `http://127.0.0.1:${listener.address().port}`;
  async function request(route, options = {}) {
    const response = await fetch(`${base}${route}`, {
      ...options, headers: { Cookie: `loginphrase=${SESSION}`, ...options.headers },
    });
    const text = await response.text();
    return { status: response.status, headers: response.headers, text };
  }
  return { request, actions };
}

const attacks = [
  ['/rollback', { seqNo: '0' }],
  ['/import-from-url', { url: 'https://attacker.example/payload.sql' }],
  ['/executebackup', { filename: 'payload' }],
];

test('reproduces the report against production handlers without the new CSRF guard', async (t) => {
  const { request, actions } = await fixture(t, false);
  for (const [route, body] of attacks) {
    const response = await request(route, {
      method: 'POST',
      headers: { Origin: 'https://evil.runonflux.io', 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams(body),
    });
    assert.equal(response.status, 200, response.text);
  }
  assert.equal(actions.filter((action) => 'rollback' in action).length, 2);
  assert.equal(actions.filter((action) => 'download' in action).length, 1);
  assert.equal(actions.filter((action) => 'import' in action).length, 1);
});

test('forged same-site forms cannot start downloads, rollback or backup execution', async (t) => {
  const { request, actions } = await fixture(t);
  for (const [route, body] of attacks) {
    for (const site of [undefined, 'same-site', 'cross-site']) {
      const response = await request(route, {
        method: 'POST',
        headers: {
          Origin: 'https://evil.runonflux.io',
          'Content-Type': 'application/x-www-form-urlencoded',
          ...(site ? { 'Sec-Fetch-Site': site } : {}),
        },
        body: new URLSearchParams(body),
      });
      assert.equal(response.status, 403, response.text);
    }
  }
  assert.deepEqual(actions, []);
});

test('valid same-origin cookie requests obtain a session-bound token and preserve all three actions', async (t) => {
  const { request, actions } = await fixture(t);
  const response = await request('/csrf-token');
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  assert.match(response.headers.get('vary'), /Cookie/);
  assert.equal(response.headers.get('access-control-allow-credentials'), null);
  const token = JSON.parse(response.text).csrfToken;
  assert.equal(token, csrf.tokenForSession(SESSION));
  for (const [route, body] of attacks) {
    const result = await request(route, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': token, 'Sec-Fetch-Site': 'same-origin' },
      body: JSON.stringify(body),
    });
    assert.equal(result.status, 200, result.text);
  }
  assert.equal(actions.length, 4);
});

test('missing, malformed and other-session tokens fail; explicit session-header API authentication is preserved', async (t) => {
  const { request, actions } = await fixture(t);
  for (const token of ['', 'bad', '0'.repeat(64), csrf.tokenForSession(OTHER_SESSION)]) {
    const result = await request('/rollback', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': token }, body: '{"seqNo":0}',
    });
    assert.equal(result.status, 403);
  }
  assert.deepEqual(actions, []);
  const tokenResponse = await request('/csrf-token', { headers: { Cookie: '' } });
  assert.equal(tokenResponse.status, 401);
  const badHeader = await request('/rollback', {
    method: 'POST', headers: { loginphrase: 'invalid', 'Content-Type': 'application/json' }, body: '{"seqNo":0}',
  });
  assert.equal(badHeader.status, 401);
  assert.deepEqual(actions, []);
  const api = await request('/rollback', {
    method: 'POST', headers: { loginphrase: SESSION, 'Content-Type': 'application/json' }, body: '{"seqNo":"0"}',
  });
  assert.equal(api.status, 200);
  assert.deepEqual(actions, [{ rollback: '0' }]);
});

test('cookie-authenticated DB Manager writes and mutating GET routes use the same guard', async (t) => {
  const { request } = await fixture(t);
  for (const [method, route] of [['PUT', '/api/db-manager/db/tables/t/rows'], ['DELETE', '/api/db-manager/db/tables/t'],
    ['POST', '/upload-sql'], ['GET', '/logout/'], ['GET', '/isloggedin/'], ['GET', '/LoGoUt']]) {
    assert.equal((await request(route, { method })).status, 403);
  }
});

test('signed wallet callbacks are not made dependent on an existing session CSRF token', () => {
  let allowed = false;
  csrf.cookieCsrfProtection({
    method: 'POST', path: '/verifylogin/', headers: { 'sec-fetch-site': 'cross-site' }, cookies: { loginphrase: SESSION },
  }, {}, () => { allowed = true; });
  assert.equal(allowed, true); // The route still verifies the payload's owner signature.
});

test('UI interceptor adds tokens to same-origin writes and logout, and never sends tokens to another origin', async () => {
  let intercept;
  let tokenFetches = 0;
  const token = csrf.tokenForSession(SESSION);
  const axios = {
    interceptors: { request: { use: (handler) => { intercept = handler; } } },
    get: async (url) => { assert.equal(url, 'https://db.runonflux.io/csrf-token'); tokenFetches += 1; return { data: { csrfToken: token } }; },
  };
  installCsrfInterceptor(axios, new URL('https://db.runonflux.io/'));
  for (const [method, url] of [['post', '/rollback'], ['post', '/executebackup'], ['post', '/upload-sql'],
    ['put', '/api/db-manager/db/tables/t/rows'], ['delete', '/api/db-manager/db/tables/t'], ['get', '/logout']]) {
    const request = await intercept({ method, url, headers: { 'Content-Type': 'application/json' } });
    assert.equal(request.headers['X-CSRF-Token'], token);
    assert.equal(request.headers['Content-Type'], 'application/json');
  }
  assert.equal(tokenFetches, 6);
  for (const url of ['/csrf-token', '/status', 'https://evil.runonflux.io/rollback']) {
    const request = await intercept({ method: url.includes('evil') ? 'post' : 'get', url });
    assert.equal(request.headers, undefined);
  }
  assert.equal(tokenFetches, 6);
  axios.get = async () => { throw new Error('Unauthorized'); };
  await assert.rejects(intercept({ method: 'post', url: '/rollback' }), /Unauthorized/);
});

test('HTTP wallet login rejects unrelated signed messages and establishes a scoped admin session', async (t) => {
  const cache = new Map();
  const dependencies = {
    crypto,
    'memory-cache': { get: (key) => cache.get(key), put: (key, value) => cache.set(key, value), del: (key) => cache.delete(key) },
    '../lib/bitcoinMessage': bitcoinMessage,
    '../lib/fluxAPI': {},
    './config': { DBAppName: 'http-login-test' },
    '../lib/log': { info() {} },
  };
  const identityModule = { exports: {} };
  const filename = path.join(__dirname, '../ClusterOperator/IdService.js');
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module: identityModule,
    require: (name) => { assert.ok(Object.hasOwn(dependencies, name)); return dependencies[name]; },
  }, { filename });
  const identity = identityModule.exports;
  const privateKey = Buffer.alloc(32);
  privateKey[31] = 1;
  identity.ownerZelID = bitcoinMessage.publicKeyToAddress(secp256k1.getPublicKey(privateKey, true));
  const { request } = await fixture(t, true, identity);
  const challenge = await request('/loginphrase');
  assert.equal(challenge.status, 200);
  assert.equal(challenge.headers.get('cache-control'), 'no-store');
  const generic = `${Date.now()}${'a'.repeat(27)}`;
  const foreignScope = challenge.text.split(':');
  foreignScope[2] = 'Z'.repeat(22);
  for (const message of [generic, foreignScope.join(':'), challenge.text]) {
    const proof = secp256k1.sign(bitcoinMessage.magicHash(message), privateKey);
    const signature = Buffer.concat([Buffer.from([31 + proof.recovery]), Buffer.from(proof.toCompactRawBytes())]).toString('base64');
    const response = await request('/verifylogin', {
      method: 'POST', headers: { Cookie: '', 'Content-Type': 'application/json' }, body: JSON.stringify({ message, signature }),
    });
    if (message !== challenge.text) {
      assert.equal(response.status, 401, response.text);
      assert.equal(response.headers.has('set-cookie'), false);
      assert.equal(cache.has(message), false);
    } else {
      assert.equal(response.status, 200, response.text);
      assert.match(response.headers.get('set-cookie'), /loginphrase=.*HttpOnly/);
      assert.equal(cache.get(message), '127.0.0.1');
      const session = await request('/isloggedin', { headers: { Cookie: '', loginphrase: message } });
      assert.equal(session.status, 200);
    }
  }
});

test('legacy log routes are gone, including for the former debugging IPs and configured whitelist', async (t) => {
  for (const peerIP of ['167.235.234.45', '45.89.52.198', '127.0.0.1']) {
    const { request, actions } = await fixture(t, true, null, peerIP);
    for (const route of ['/logs', '/logs?file=debug', '/logs/errors', '/logs/debug', '/logs/query']) {
      const response = await request(route, { headers: { Cookie: '' } });
      assert.equal(response.status, 404, `${peerIP} ${route}: ${response.text}`);
    }
    const adminLog = await request('/getlogfile?file=errors', { headers: { Cookie: '' } });
    assert.equal(adminLog.status, 401);
    assert.equal(actions.length, 0);
  }
});

test('admin log reader requires a session and preserves log selection and pagination', async (t) => {
  const { request, actions } = await fixture(t);
  for (const headers of [{ Cookie: '' }, { Cookie: `loginphrase=${OTHER_SESSION}` }]) {
    const response = await request('/getlogfile?file=errors', { headers });
    assert.equal(response.status, 401);
  }
  assert.equal(actions.length, 0);
  for (const file of ['debug', 'info', 'warnings', 'errors', 'query', 'compress']) {
    const latest = await request(`/getlogfile?file=${file}&pageSize=2`);
    assert.equal(latest.status, 200, latest.text);
    assert.deepEqual(JSON.parse(latest.text), {
      lines: ['second', 'third'], totalLines: 3, page: 0, pageSize: 2,
    });
    assert.equal(actions[actions.length - 1].readLog, `${file}.txt`);
  }
  const older = await request('/getlogfile?file=errors&pageSize=2&page=1', { headers: { Cookie: '', loginphrase: SESSION } });
  assert.equal(older.status, 200);
  assert.deepEqual(JSON.parse(older.text), {
    lines: ['first'], totalLines: 3, page: 1, pageSize: 2,
  });
  const reads = actions.length;
  const invalid = await request('/getlogfile?file=../../secret');
  assert.equal(invalid.status, 400);
  assert.equal(actions.length, reads);
});
