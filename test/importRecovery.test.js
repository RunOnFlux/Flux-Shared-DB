/* eslint-disable no-underscore-dangle, no-restricted-syntax, no-await-in-loop, class-methods-use-this */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const Importer = require('../lib/mysqlimport');

function load(file, overrides = {}) {
  const filename = path.join(__dirname, '..', file);
  const module = { exports: {} };
  const localRequire = createRequire(filename);
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    exports: module.exports,
    Buffer,
    process,
    console,
    performance,
    require: (name) => (Object.hasOwn(overrides, name) ? overrides[name] : localRequire(name)),
  }, { filename });
  return module.exports;
}

async function input(t, sql, settings) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'shared-db-import-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filename = path.join(directory, 'fixture.sql');
  fs.writeFileSync(filename, sql);
  const importer = new Importer(settings);
  return { importer, run: () => importer.import(filename) };
}

for (const nested of [false, true]) {
  test(`failed SQL callback stops import and preserves its error (${nested ? 'backlog' : 'adapter'} result)`, async (t) => {
    const statements = [];
    const sqlError = Object.assign(new Error('Duplicate values'), { code: 'ER_DUP_ENTRY', errno: 1062 });
    const errorResult = [null, null, sqlError];
    const fixture = await input(t, 'CREATE TABLE x(id INT); ALTER TABLE x ADD UNIQUE KEY id(id); INSERT INTO x VALUES(1);', {
      requireResult: true,
      callback: async (sql) => {
        statements.push(sql);
        if (sql.startsWith('ALTER')) return nested ? [errorResult, 2, 0] : errorResult;
        return [{ affectedRows: 0 }, 1, 0];
      },
    });
    let completion;
    fixture.importer.onDumpCompleted((result) => { completion = result; });
    await assert.rejects(fixture.run(), (error) => error.code === 'ER_DUP_ENTRY' && error.errno === 1062 && error.statementNumber === 2 && error.cause === sqlError);
    assert.equal(statements.length, 2);
    assert.equal(fixture.importer.getImported().length, 0);
    assert.equal(completion.error.code, 'ER_DUP_ENTRY');
  });
}

for (const result of [null, undefined, []]) {
  test(`internal import rejects unconfirmed execution: ${JSON.stringify(result)}`, async (t) => {
    const fixture = await input(t, 'CREATE TABLE x(id INT);', { requireResult: true, callback: async () => result });
    await assert.rejects(fixture.run(), { code: 'IMPORT_UNCONFIRMED_STATEMENT' });
    assert.equal(fixture.importer.getImported().length, 0);
  });
}

test('generic callback may still return no result when strict confirmation is not requested', async (t) => {
  const fixture = await input(t, 'SELECT 1;', { callback: async () => {} });
  await fixture.run();
  assert.equal(fixture.importer.getImported().length, 1);
});

test('SQL at EOF is executed instead of silently discarded', async (t) => {
  const statements = [];
  const fixture = await input(t, 'CREATE TABLE x(id INT); INSERT INTO x VALUES(1)', { callback: async (sql) => statements.push(sql) });
  await fixture.run();
  assert.equal(statements.length, 2);
  assert.equal(statements[1], 'INSERT INTO x VALUES(1)');
});

test('truncated quoted SQL is rejected at EOF', async (t) => {
  const fixture = await input(t, "INSERT INTO x VALUES('unfinished", { callback: async () => {} });
  await assert.rejects(fixture.run(), { code: 'IMPORT_INCOMPLETE_STATEMENT' });
  assert.equal(fixture.importer.getImported().length, 0);
});

test('SQL errors thrown by a callback stop the following statements', async (t) => {
  let calls = 0;
  const fixture = await input(t, 'ALTER TABLE x ADD KEY s(status); SELECT 1;', {
    callback: async () => { calls += 1; throw Object.assign(new Error('Key length required'), { code: 'ER_BLOB_KEY_WITHOUT_LENGTH' }); },
  });
  await assert.rejects(fixture.run(), { code: 'ER_BLOB_KEY_WITHOUT_LENGTH', statementNumber: 1 });
  assert.equal(calls, 1);
});

function backlogFixture(results) {
  let executions = 0;
  let records = 0;
  const delays = [];
  const log = Object.fromEntries(['info', 'warn', 'error'].map((level) => [level, () => {}]));
  const backlog = load('ClusterOperator/Backlog.js', {
    './config': { dbType: 'mysql', dbBacklogCollection: 'backlog' },
    './DBClient': {},
    './Security': {},
    '../lib/ConnectionPool': {},
    '../lib/log': log,
    'timers/promises': { setTimeout: async (delay) => { delays.push(delay); } },
  });
  backlog.BLClient = { query: async () => {}, execute: async () => { records += 1; return {}; } };
  backlog.UserDBClient = { query: async () => { const result = results[Math.min(executions, results.length - 1)]; executions += 1; return result; } };
  return {
    backlog, delays, executions: () => executions, records: () => records,
  };
}

for (const code of ['ER_LOCK_WAIT_TIMEOUT', 'ER_LOCK_DEADLOCK']) {
  test(`confirmed internal DDL ${code} is retried without duplicating the backlog`, async () => {
    const fixture = backlogFixture([[null, null, { code }], { affectedRows: 0 }]);
    const result = await fixture.backlog.pushQuery('ALTER TABLE x ADD KEY status(status)', 0, 0);
    assert.equal(fixture.executions(), 2);
    assert.equal(fixture.records(), 1);
    assert.equal(result[1], 1);
    assert.deepEqual(fixture.delays, [250]);
  });
}

test('transient DDL retries have a bounded attempt count and backoff', async () => {
  const error = { code: 'ER_LOCK_WAIT_TIMEOUT' };
  const fixture = backlogFixture([[null, null, error]]);
  const result = await fixture.backlog.pushQuery('CREATE TABLE x(id INT)', 0, 0);
  assert.equal(fixture.executions(), 3);
  assert.equal(fixture.records(), 1);
  assert.deepEqual(fixture.delays, [250, 500]);
  assert.equal(result[0][2], error);
});

for (const code of ['ER_DUP_ENTRY', 'ER_BLOB_KEY_WITHOUT_LENGTH', 'ER_PARSE_ERROR', 'ECONNRESET', 'PROTOCOL_CONNECTION_LOST', 'ETIMEDOUT']) {
  test(`internal DDL does not retry permanent or ambiguous failure ${code}`, async () => {
    const fixture = backlogFixture([[null, null, { code }]]);
    await fixture.backlog.pushQuery('ALTER TABLE x ADD KEY status(status)', 0, 0);
    assert.equal(fixture.executions(), 1);
    assert.deepEqual(fixture.delays, []);
  });
}

for (const sql of ['INSERT INTO x VALUES(1)', 'COMMIT', 'ALTER TABLE x ADD KEY status(status); INSERT INTO x VALUES(1)']) {
  test(`statement is not blindly retried: ${sql}`, async () => {
    const fixture = backlogFixture([[null, null, { code: 'ER_LOCK_DEADLOCK' }]]);
    await fixture.backlog.pushQuery(sql, 0, 0);
    assert.equal(fixture.executions(), 1);
  });
}

function operatorFixture() {
  const backlog = { pushKey: async () => {}, compressionTask: -1, readBeaconFile: async () => ({ backupFilename: 'bad' }) };
  const operator = load('ClusterOperator/Operator.js', {
    './Backlog': backlog, './config': {}, '../lib/log': { error() {} },
  });
  return { operator, backlog };
}

test('failed snapshot cannot serve clients, elect a primary or be replayed indefinitely', async () => {
  const { operator, backlog } = operatorFixture();
  await operator.failImport('bad', { code: 'ER_DUP_ENTRY' });
  assert.equal(operator.status, 'IMPORT_FAILED');
  assert.equal(backlog.compressionTask, -2);
  assert.equal(operator.snapshotRecoveryRequired, true);
  assert.equal(await operator.findMaster(), null);
  await operator.syncLocalDB();
  await operator.doHealthCheck();
  assert.equal(operator.status, 'IMPORT_FAILED');
});

test('a newer snapshot lets a connected follower attempt a full restore', async () => {
  const { operator, backlog } = operatorFixture();
  await operator.failImport('bad', { code: 'ER_DUP_ENTRY' });
  backlog.readBeaconFile = async () => ({ backupFilename: 'fixed' });
  operator.masterWSConn = { connected: true };
  operator.IamMaster = false;
  let attempts = 0;
  operator.syncLocalDB = async () => { attempts += 1; assert.equal(operator.snapshotRecoveryRequired, true); };
  await operator.doHealthCheck();
  assert.equal(attempts, 1);
  assert.equal(operator.importFailure, null);
});

function snapshotFixture(failure = null) {
  const calls = {
    imports: 0, clears: 0, shifts: [], resumed: 0,
  };
  const backlog = {
    pushKey: async () => {},
    sequenceNumber: 500,
    readBeaconFile: async () => ({ backupFilename: 'snapshot', seqNo: 100, BackupFilesize: 2000 }),
    clearBacklog: async () => { calls.clears += 1; },
    clearBuffer: async () => {},
    getLastSequenceNumber: async () => 10,
    shiftBacklogSeqNo: async (delta) => calls.shifts.push(delta),
  };
  let operator;
  class SnapshotImporter {
    onProgress() {}

    setEncoding() {}

    getImported() { return ['snapshot']; }

    async import() {
      calls.imports += 1;
      if (failure) throw failure;
      operator.syncLocalDB = async () => { calls.resumed += 1; };
    }
  }
  operator = load('ClusterOperator/Operator.js', {
    './Backlog': backlog,
    './config': {},
    '../lib/log': { error() {}, info() {} },
    fs: { existsSync: () => true, statSync: () => ({ size: 2000 }) },
    '../lib/fluxAPI': { getStatus: async () => ({ firstSequenceNumber: 1 }) },
    '../lib/mysqlimport': SnapshotImporter,
    'timers/promises': { setTimeout: async () => {} },
  });
  operator.masterWSConn = { connected: true };
  operator.snapshotRecoveryRequired = true;
  return { operator, backlog, calls };
}

test('failed snapshot restore does not advance to the snapshot sequence or retry the bad file', async () => {
  const { operator, calls } = snapshotFixture({ code: 'ER_BLOB_KEY_WITHOUT_LENGTH' });
  await operator.syncLocalDB();
  assert.equal(calls.imports, 1);
  assert.equal(calls.shifts.length, 0);
  assert.equal(operator.status, 'IMPORT_FAILED');
  await operator.syncLocalDB();
  assert.equal(calls.imports, 1);
});

test('recovery forces a full snapshot import even if partial progress appears ahead of the snapshot', async () => {
  const { operator, backlog, calls } = snapshotFixture();
  await operator.syncLocalDB();
  assert.equal(calls.imports, 1);
  assert.equal(calls.clears, 1);
  assert.deepEqual(calls.shifts, [90]);
  assert.equal(calls.resumed, 1);
  assert.equal(operator.snapshotRecoveryRequired, false);
  assert.equal(backlog.exitOnError, false);
});

test('a saved restore failure blocks primary election after process restart', async () => {
  const { operator, backlog } = operatorFixture();
  backlog.getKey = async () => JSON.stringify({ snapshot: 'bad', code: 'ER_DUP_ENTRY' });
  await operator.checkIfBeaconExists();
  assert.equal(operator.status, 'IMPORT_FAILED');
  assert.equal(await operator.findMaster(), null);
});

test('successful restore clears the durable failure marker', async () => {
  const { operator, backlog } = operatorFixture();
  let saved;
  backlog.pushKey = async (key, value, encrypt) => { saved = { key, value, encrypt }; };
  await operator.clearImportFailure();
  assert.deepEqual(saved, { key: 'snapshotImportFailure', value: null, encrypt: false });
  assert.equal(operator.importFailure, null);
  assert.equal(operator.snapshotRecoveryRequired, false);
});
