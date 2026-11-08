/* eslint-disable no-restricted-syntax, no-await-in-loop */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const vm = require('node:vm');
const https = require('node:https');
const http = require('node:http');
const { once } = require('node:events');
const { execFileSync } = require('node:child_process');
const axios = require('axios');
const crypto = require('node:crypto');

const OWNER = '1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH';
const SPECS = { owner: OWNER, instances: 3 };
const LOCATIONS = [{ ip: '203.0.113.40' }];

function loadAPI(client, insecure = false) {
  const filename = path.join(__dirname, '../lib/fluxAPI.js');
  const module = { exports: {} };
  const dependencies = {
    axios: client,
    https,
    'socket.io-client': {},
    './log': { error() {} },
    './operatorAuth': {},
  };
  let source = fs.readFileSync(filename, 'utf8');
  // Reproduce the vulnerable TLS behavior without weakening production code.
  if (insecure) source = source.replace('rejectUnauthorized: true', 'rejectUnauthorized: false');
  vm.runInNewContext(source, {
    module,
    require: (name) => { assert.ok(Object.hasOwn(dependencies, name)); return dependencies[name]; },
  }, { filename });
  return module.exports;
}

async function fixture(t, {
  hostname = 'api.runonflux.io', trusted = false, insecure = false, redirect = null,
} = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'flux-api-tls-'));
  const certPath = path.join(directory, 'cert.pem');
  const keyPath = path.join(directory, 'key.pem');
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  execFileSync('openssl', [
    'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', `/CN=${hostname}`, '-addext', `subjectAltName=DNS:${hostname}`,
    '-keyout', keyPath, '-out', certPath,
  ], { stdio: 'ignore' });
  const cert = fs.readFileSync(certPath);
  let requests = 0;
  const server = https.createServer({ cert, key: fs.readFileSync(keyPath) }, (req, res) => {
    requests += 1;
    if (redirect) {
      res.writeHead(302, { Location: redirect });
      res.end();
      return;
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ status: 'success', data: req.url.includes('/location/') ? LOCATIONS : SPECS }));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }));
  const errors = [];
  const agents = new Set();
  t.after(() => { for (const agent of agents) agent.destroy(); });
  const client = {
    async get(url, options) {
      const target = new URL(url);
      assert.equal(target.origin, 'https://api.runonflux.io');
      agents.add(options.httpsAgent);
      // Route the real Axios/TLS connection to localhost, retaining the API hostname
      // for SNI and certificate identity checks. No public network access is used.
      Object.assign(options.httpsAgent.options, {
        lookup: (_name, opts, callback) => callback(null, ...(opts.all ? [[{ address: '127.0.0.1', family: 4 }]] : ['127.0.0.1', 4])),
        ...(trusted ? { ca: cert } : {}),
      });
      target.port = server.address().port;
      try {
        return await axios.get(target.toString(), { ...options, proxy: false });
      } catch (error) {
        errors.push(error.code);
        throw error;
      }
    },
  };
  return { api: loadAPI(client, insecure), errors, requests: () => requests };
}

function assertEmpty(value) {
  // Arrays originate in the VM module; compare their data, not realm prototypes.
  assert.ok(Array.isArray(value));
  assert.equal(value.length, 0);
}

async function assertNoTrust(api) {
  assert.equal(await api.getApplicationOwner('test-app'), null);
  assertEmpty(await api.getApplicationSpecs('test-app'));
  assertEmpty(await api.getApplicationIP('test-app'));
}

test('insecure agent reproduces an on-path owner and membership replacement', async (t) => {
  const { api, requests } = await fixture(t, { insecure: true });
  assert.equal(await api.getApplicationOwner('test-app'), OWNER);
  assert.deepEqual(await api.getApplicationSpecs('test-app'), SPECS);
  assert.deepEqual(await api.getApplicationIP('test-app'), LOCATIONS);
  assert.equal(requests(), 3);
});

test('all three Flux API lookups reject an untrusted certificate before accepting data', async (t) => {
  const { api, errors, requests } = await fixture(t);
  await assertNoTrust(api);
  assert.equal(requests(), 0);
  assert.equal(errors.length, 3);
  assert.ok(errors.every((code) => /SELF_SIGNED|UNABLE_TO_VERIFY/.test(code)), errors.join(', '));
});

test('verified API identity still supplies owner, specs and deployment locations', async (t) => {
  const { api, errors, requests } = await fixture(t, { trusted: true });
  assert.equal(await api.getApplicationOwner('test-app'), OWNER);
  assert.deepEqual(await api.getApplicationSpecs('test-app'), SPECS);
  assert.deepEqual(await api.getApplicationIP('test-app'), LOCATIONS);
  assert.equal(requests(), 3);
  assert.equal(errors.length, 0);
});

test('a trusted certificate for the wrong hostname cannot supply identity or membership', async (t) => {
  const { api, errors, requests } = await fixture(t, { hostname: 'attacker.test', trusted: true });
  await assertNoTrust(api);
  assert.equal(requests(), 0);
  assert.deepEqual(errors, Array(3).fill('ERR_TLS_CERT_ALTNAME_INVALID'));
});

test('API redirects cannot downgrade identity and membership requests to HTTP', async (t) => {
  let downgradedRequests = 0;
  const server = http.createServer((_req, res) => { downgradedRequests += 1; res.end('{}'); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }));
  const { api, requests } = await fixture(t, { trusted: true, redirect: `http://127.0.0.1:${server.address().port}/forged` });
  await assertNoTrust(api);
  assert.equal(requests(), 3);
  assert.equal(downgradedRequests, 0);
});

test('failed or malformed owner responses yield no owner instead of a truthy array', async () => {
  for (const data of [
    { status: 'error' }, { status: 'success', data: {} }, { status: 'success' },
    { status: 'success', data: { owner: [] } }, { status: 'success', data: { owner: ' ' } },
  ]) {
    const api = loadAPI({ get: async () => ({ data }) });
    assert.equal(await api.getApplicationOwner('test-app'), null);
  }
  const api = loadAPI({ get: async () => { throw new Error('Network unavailable'); } });
  await assertNoTrust(api);
});

test('an unavailable Flux API does not disable maintenance administrator login', async () => {
  const api = loadAPI({ get: async () => { throw new Error('TLS verification failed'); } });
  const dependencies = {
    crypto,
    'memory-cache': {},
    '../lib/bitcoinMessage': { verify: (_message, address, signature) => address === '16iJqiVbHptCx87q6XQwNpKdgEZnFtKcyP' && signature === 'maintenance-proof' },
    '../lib/fluxAPI': api,
    './config': { AppName: 'client-test', DBAppName: 'shared-db-test' },
    '../lib/log': { info() {} },
  };
  const module = { exports: {} };
  const filename = path.join(__dirname, '../ClusterOperator/IdService.js');
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    require: (name) => { assert.ok(Object.hasOwn(dependencies, name)); return dependencies[name]; },
  }, { filename });
  const identity = module.exports;
  await identity.init();
  assert.equal(identity.ownerZelID, null);
  assert.equal(identity.verifyLogin(identity.generateLoginPhrase(), 'maintenance-proof'), true);
});
