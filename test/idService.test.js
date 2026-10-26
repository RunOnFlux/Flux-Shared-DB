/* eslint-disable no-restricted-syntax */
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { secp256k1 } = require('@noble/curves/secp256k1');
const bitcoinMessage = require('../lib/bitcoinMessage');

const privateKey = Buffer.alloc(32);
privateKey[31] = 1;
const owner = bitcoinMessage.publicKeyToAddress(secp256k1.getPublicKey(privateKey, true));
const windowMs = 30 * 60 * 1000;

function sign(message) {
  const signature = secp256k1.sign(bitcoinMessage.magicHash(message), privateKey);
  return Buffer.concat([Buffer.from([31 + signature.recovery]), Buffer.from(signature.toCompactRawBytes())]).toString('base64');
}

function fixture(appName = 'shared-db-test', verifier = bitcoinMessage) {
  let now = 1790000000000;
  class TestDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }

    static now() { return now; }
  }
  const cache = new Map();
  const dependencies = {
    'memory-cache': { get: (key) => cache.get(key), put: (key, value) => cache.set(key, value), del: (key) => cache.delete(key) },
    crypto,
    '../lib/bitcoinMessage': verifier,
    '../lib/fluxAPI': {},
    './config': { DBAppName: appName },
    '../lib/log': { info() {} },
  };
  const filename = path.join(__dirname, '../ClusterOperator/IdService.js');
  const module = { exports: {} };
  // Isolate the real authentication code from production configuration and APIs.
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    Date: TestDate,
    require: (name) => {
      assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency: ${name}`);
      return dependencies[name];
    },
  }, { filename });
  const IdService = module.exports;
  IdService.ownerZelID = owner;
  return { IdService, now, advanceTime: (milliseconds) => { now += milliseconds; } };
}

const phrase = (timestamp, length = 40) => `${timestamp}${'a'.repeat(length - 13)}`;

test('rejects valid owner signatures over nonnumeric and malformed login phrases', () => {
  const { IdService, now } = fixture();
  const messages = [
    'offline signed owner message'.padEnd(40, '.'),
    `fluxappregister1${JSON.stringify({ name: 'offline-test-app', owner })}${now}`,
    phrase(now, 39),
    phrase(now, 71),
    ` ${now.toString().slice(1)}${'a'.repeat(27)}`,
    `1e12${'a'.repeat(36)}`,
    `123456789012x${'a'.repeat(27)}`,
    `１２３４５６７８９０１２３${'a'.repeat(27)}`,
  ];
  for (const message of messages) {
    const signature = sign(message);
    assert.equal(bitcoinMessage.verify(message, owner, signature), true);
    assert.equal(IdService.verifyLogin(message, signature), false, `Accepted invalid phrase: ${message}`);
  }
  for (const message of [undefined, null, 123, {}, [], '']) {
    assert.equal(IdService.verifyLogin(message, 'invalid'), false);
  }
});

test('accepts scoped login challenges at timestamp boundaries and across replicas', () => {
  const { IdService, now, advanceTime } = fixture();
  const replica = fixture().IdService;
  const message = IdService.generateLoginPhrase();
  assert.equal(message.length, 69);
  assert.match(message, /^\d{13}:SDB-login:[A-Za-z0-9_-]{22}:[A-Za-z0-9_-]{22}$/);
  assert.notEqual(message, IdService.generateLoginPhrase());
  assert.equal(IdService.verifyLogin(message, sign(message)), true);
  assert.equal(replica.verifyLogin(message, sign(message)), true);
  const future = message.replace(String(now), String(now + 1));
  assert.equal(IdService.verifyLogin(future, sign(future)), false);
  advanceTime(windowMs);
  assert.equal(IdService.verifyLogin(message, sign(message)), true);
  advanceTime(1);
  assert.equal(IdService.verifyLogin(message, sign(message)), false);
});

test('signed nonnumeric messages remain rejected after time advances', () => {
  const { IdService, advanceTime } = fixture();
  const message = 'offline signed owner message'.padEnd(40, '.');
  const signature = sign(message);
  assert.equal(IdService.verifyLogin(message, signature), false);
  advanceTime(365 * 24 * 60 * 60 * 1000);
  assert.equal(IdService.verifyLogin(message, signature), false);
});

test('valid login signatures stop working after the 30-minute window', () => {
  const { IdService, advanceTime } = fixture();
  const message = IdService.generateLoginPhrase();
  const signature = sign(message);
  assert.equal(IdService.verifyLogin(message, signature), true);
  advanceTime(windowMs + 1);
  assert.equal(IdService.verifyLogin(message, signature), false);
});

test('rejects incorrect and malformed signatures', () => {
  const { IdService } = fixture();
  const message = IdService.generateLoginPhrase();
  const wrongKey = Buffer.alloc(32);
  wrongKey[31] = 2;
  IdService.ownerZelID = bitcoinMessage.publicKeyToAddress(secp256k1.getPublicKey(wrongKey, true));
  assert.equal(IdService.verifyLogin(message, sign(message)), false);
  for (const signature of [undefined, null, '', 'invalid', {}, [], 123]) {
    assert.equal(IdService.verifyLogin(message, signature), false);
  }
});

test('rejects previously cached malformed sessions while preserving valid IP-bound sessions', () => {
  const { IdService, now, advanceTime } = fixture();
  const malformed = 'offline signed owner message'.padEnd(40, '.');
  IdService.addNewSession(malformed, '192.0.2.4');
  assert.equal(IdService.verifySession(malformed, '192.0.2.4'), false);
  const message = phrase(now);
  IdService.addNewSession(message, '192.0.2.4');
  assert.equal(IdService.verifySession(message, '192.0.2.4'), true);
  assert.equal(IdService.verifySession(message, '192.0.2.5'), false);
  advanceTime(windowMs + 1);
  assert.equal(IdService.verifySession(message, '192.0.2.4'), true);
  IdService.removeSession(message);
  assert.equal(IdService.verifySession(message, '192.0.2.4'), false);
});

test('valid signatures over generic timestamp messages and other app logins grant no access', () => {
  const { IdService, now } = fixture();
  const otherApp = fixture('different-shared-db').IdService;
  const foreignMessage = otherApp.generateLoginPhrase();
  assert.equal(otherApp.verifyLogin(foreignMessage, sign(foreignMessage)), true);
  for (const message of [phrase(now), phrase(now, 70), foreignMessage]) {
    assert.equal(bitcoinMessage.verify(message, owner, sign(message)), true);
    assert.equal(IdService.verifyLogin(message, sign(message)), false);
  }
  const scoped = IdService.generateLoginPhrase();
  const wrongPurpose = scoped.replace('SDB-login', 'SDB-other');
  assert.equal(IdService.verifyLogin(wrongPurpose, sign(wrongPurpose)), false);
});

test('maintenance principal remains authorized on every app, with a separately scoped signature', () => {
  const maintenance = '16iJqiVbHptCx87q6XQwNpKdgEZnFtKcyP';
  const verified = [];
  // The real maintenance private key is intentionally unavailable to tests.
  // Verify routing to its unchanged address; owner tests exercise real signatures.
  const verifier = {
    verify: (message, address, signature) => {
      verified.push(address);
      return address === maintenance && signature === `maintenance:${message}`;
    },
  };
  const first = fixture('public-shared-db', verifier).IdService;
  const second = fixture('enterprise-shared-db', verifier).IdService;
  for (const service of [first, second]) {
    const message = service.generateLoginPhrase();
    assert.equal(service.verifyLogin(message, `maintenance:${message}`), true);
    service.ownerZelID = null;
    assert.equal(service.verifyLogin(message, `maintenance:${message}`), true);
  }
  const message = first.generateLoginPhrase();
  assert.equal(second.verifyLogin(message, `maintenance:${message}`), false);
  const generic = phrase(1790000000000);
  assert.equal(first.verifyLogin(generic, `maintenance:${generic}`), false);
  assert.ok(verified.includes(maintenance));
});

test('unknown app identity cannot issue or accept an admin login', () => {
  const { IdService } = fixture('');
  assert.throws(() => IdService.generateLoginPhrase(), /resolved Shared-DB application name/);
  const message = fixture().IdService.generateLoginPhrase();
  assert.equal(IdService.verifyLogin(message, sign(message)), false);
});
