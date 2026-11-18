/* eslint-disable no-underscore-dangle, no-restricted-syntax, no-await-in-loop, class-methods-use-this, max-classes-per-file */
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

for (const code of ['ER_LOCK_WAIT_TIMEOUT', 'ER_LOCK_DEADLOCK', 'ER_DUP_ENTRY', 'ER_BLOB_KEY_WITHOUT_LENGTH', 'ER_PARSE_ERROR', 'ECONNRESET', 'PROTOCOL_CONNECTION_LOST', 'ETIMEDOUT']) {
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
  const backlog = {
    pushKey: async () => {}, getKey: async () => null, compressionTask: -1, readBeaconFile: async () => ({ backupFilename: 'bad' }),
  };
  const operator = load('ClusterOperator/Operator.js', {
    './Backlog': backlog, './config': {}, '../lib/log': { error() {}, info() {} },
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

function snapshotFixture(failure = null) {
  const calls = {
    imports: 0, clears: 0, shifts: [], resumed: 0, available: true,
  };
  const backlog = {
    pushKey: async () => {},
    sequenceNumber: 500,
    readBeaconFile: async () => ({ backupFilename: 'snapshot', seqNo: 100, BackupFilesize: 2000 }),
    resetForSnapshot: async () => { calls.clears += 1; },
    getKey: async () => null,
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
    fs: { existsSync: () => calls.available, statSync: () => ({ size: 2000 }) },
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

for (const prefix of ['', '\n', '\r\n  ', '# dump header\n-- trigger section\n', ' '.repeat(65532)]) {
  test(`trigger delimiters survive leading whitespace, comments and stream boundaries (${prefix.length} bytes)`, async (t) => {
    const trigger = "CREATE TRIGGER tr BEFORE INSERT ON x FOR EACH ROW BEGIN SET NEW.id=1; SET @text='semi;colon'; END";
    const statements = [];
    const fixture = await input(t, `${prefix}DELIMITER ;;\r\n${trigger};;\r\nDELIMITER ;\r\nINSERT INTO x VALUES(2);`, {
      requireResult: true, callback: async (sql) => { statements.push(sql); return { affectedRows: 0 }; },
    });
    await fixture.run();
    assert.deepEqual(statements, [trigger, 'INSERT INTO x VALUES(2)']);
  });
}

test('DELIMITER reset at EOF is accepted without sending it to SQL', async (t) => {
  const statements = [];
  const fixture = await input(t, 'DELIMITER ;;\nSELECT 1;;\nDELIMITER ;', { callback: async (sql) => { statements.push(sql); } });
  await fixture.run();
  assert.deepEqual(statements, ['SELECT 1']);
});

test('a confirmed DDL error aborts the import without a retry gap or later statements', async (t) => {
  const error = { code: 'ER_LOCK_DEADLOCK' };
  const database = backlogFixture([[null, null, error], { affectedRows: 1 }]);
  const fixture = await input(t, 'ALTER TABLE x ADD COLUMN y INT; INSERT INTO x(y) VALUES(1);', {
    requireResult: true, callback: (sql) => database.backlog.pushQuery(sql),
  });
  await assert.rejects(fixture.run(), { code: 'ER_LOCK_DEADLOCK' });
  assert.equal(database.executions(), 1);
  assert.equal(database.records(), 1);
  assert.deepEqual(database.delays, []);
});

for (const result of [[null, null, { code: 'ER_DISK_FULL' }], undefined, {}]) {
  test(`durable snapshot markers reject unconfirmed writes (${JSON.stringify(result)})`, async () => {
    const { backlog } = backlogFixture([]);
    backlog.BLClient.execute = async () => result;
    await assert.rejects(backlog.pushKey('snapshotImportFailure', 'failure', false, true));
  });
}

test('durable snapshot writes use one upsert and propagate thrown database errors', async () => {
  const { backlog } = backlogFixture([]);
  const statements = [];
  backlog.BLClient.execute = async (sql, values) => { statements.push({ sql, values }); return { affectedRows: 1 }; };
  await backlog.pushKey('snapshotImportFailure', 'failure', false, true);
  assert.equal(statements.length, 1);
  assert.match(statements[0].sql, /ON DUPLICATE KEY UPDATE/);
  backlog.BLClient.execute = async () => { throw new Error('disk full'); };
  await assert.rejects(backlog.pushKey('snapshotImportFailure', 'failure', false, true), /disk full/);
});

test('marker read failure keeps startup fenced and does not recurse in master discovery', async () => {
  const { operator, backlog } = operatorFixture();
  let reads = 0;
  backlog.getKey = async () => { reads += 1; throw new Error('read failed'); };
  assert.equal(await operator.findMaster(), null);
  assert.equal(reads, 1);
  assert.equal(operator.status, 'IMPORT_FAILED');
  assert.equal(operator.snapshotRecoveryRequired, true);
});

test('a newer local snapshot recovers a restarted node without a master connection', async () => {
  const { operator, backlog, calls } = snapshotFixture();
  let marker = JSON.stringify({ snapshot: 'bad', code: 'ER_DUP_ENTRY' });
  backlog.getKey = async () => marker;
  backlog.pushKey = async (_key, value) => { marker = value; };
  let beacon = { backupFilename: 'bad', seqNo: 100, BackupFilesize: 2000 };
  backlog.readBeaconFile = async () => beacon;
  operator.masterWSConn = null;
  operator.snapshotRecoveryRequired = false;
  await operator.findMaster();
  assert.equal(operator.status, 'IMPORT_FAILED');
  assert.equal(calls.imports, 0);
  beacon = { ...beacon, backupFilename: 'fixed' };
  let discoveries = 0;
  operator.findMaster = async () => { assert.equal(operator.snapshotRecoveryRequired, false); discoveries += 1; };
  operator.initMasterConnection = () => {};
  await operator.doHealthCheck();
  assert.equal(calls.imports, 1);
  assert.equal(marker, null);
  assert.equal(operator.importFailure, null);
  assert.equal(discoveries, 1);
});

async function restoreFixture(t, sql, persist = async () => {}) {
  // Use the real importer while keeping the SQL file outside the repository.
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'shared-db-restore-'));
  t.after(() => fs.rmSync(temp, { recursive: true, force: true }));
  const filename = path.join(temp, 'fixture.sql');
  fs.writeFileSync(filename, sql);
  class FixtureImporter extends Importer { import() { return super.import(filename); } }
  const events = [];
  function instance(name) {
    const state = { marker: null, applied: [], resets: 0 };
    const backlog = {
      sequenceNumber: 0,
      pushKey: async (_key, value, _encrypt, required) => {
        assert.equal(required, true);
        await persist(name, value);
        state.marker = value;
      },
      resetForSnapshot: async () => { state.resets += 1; state.applied = []; backlog.sequenceNumber = 0; },
      pushQuery: async (query, sequence = 0) => {
        backlog.sequenceNumber = sequence || backlog.sequenceNumber + 1;
        if (query.includes('missing')) return [[null, null, { code: 'ER_NO_SUCH_TABLE' }], backlog.sequenceNumber, 0];
        state.applied.push(query);
        return [{ affectedRows: 1 }, backlog.sequenceNumber, 0];
      },
    };
    const operator = load('ClusterOperator/Operator.js', {
      './Backlog': backlog, './config': {}, '../lib/log': { info() {}, error() {} }, '../lib/mysqlimport': FixtureImporter,
    });
    operator.status = 'OK';
    return { operator, backlog, state };
  }
  const master = instance('master');
  const follower = instance('follower');
  master.operator.IamMaster = true;
  master.operator.masterNode = '127.0.0.1';
  const peer = {
    connected: true,
    data: { operatorReplica: true },
    timeout() { return this; },
    async emitWithAck(_event, state) {
      events.push(state.phase);
      await follower.operator.handleRestoreState(state);
      return { status: 'OK' };
    },
  };
  master.operator.serverSocket = {
    sockets: { sockets: new Map([['follower', peer]]) },
    emit(_event, ...args) {
      assert.equal(follower.operator.status, 'IMPORTING');
      follower.operator.queueRestoreQuery(...args);
    },
  };
  return {
    master, follower, peer, events,
  };
}

test('a failed replicated restore fences every replica and preserves the failure across restart', async (t) => {
  const { master, follower, events } = await restoreFixture(t, 'CREATE TABLE x(id INT); INSERT INTO x VALUES(1); INSERT INTO missing VALUES(2);');
  await assert.rejects(master.operator.restoreSnapshot('fixture', { replicate: true }), { code: 'ER_NO_SUCH_TABLE' });
  assert.equal(master.operator.status, 'IMPORT_FAILED');
  assert.equal(follower.operator.status, 'IMPORT_FAILED');
  assert.equal(follower.state.applied.length, 2);
  assert.deepEqual(events, ['begin', 'reset', 'failed']);
  assert.equal(JSON.parse(follower.state.marker).snapshot, 'fixture');
  assert.equal(master.operator.restoreInProgress, false);
});

test('a successful replicated restore drains follower queries before clearing durable fences', async (t) => {
  const { master, follower, events } = await restoreFixture(t, 'CREATE TABLE x(id INT); INSERT INTO x VALUES(1);');
  await master.operator.restoreSnapshot('fixture', { replicate: true });
  assert.deepEqual(events, ['begin', 'reset', 'complete']);
  assert.equal(master.operator.status, 'OK');
  assert.equal(follower.operator.status, 'OK');
  assert.equal(master.state.marker, null);
  assert.equal(follower.state.marker, null);
  assert.equal(follower.state.applied.length, 2);
});

test('failed fencing acknowledgements abort before resetting or importing any database', async (t) => {
  const { master, follower } = await restoreFixture(t, 'CREATE TABLE x(id INT);');
  master.operator.notifyRestore = async () => { throw new Error('legacy replica did not acknowledge'); };
  await assert.rejects(master.operator.restoreSnapshot('fixture', { replicate: true }), /did not acknowledge/);
  assert.equal(master.state.resets, 0);
  assert.equal(follower.state.resets, 0);
  assert.equal(master.state.applied.length, 0);
});

test('a failed initial durable marker write aborts before any destructive operation', async (t) => {
  const { master } = await restoreFixture(t, 'CREATE TABLE x(id INT);', async () => { throw new Error('disk full'); });
  await assert.rejects(master.operator.restoreSnapshot('fixture', { replicate: true }), /disk full/);
  assert.equal(master.state.resets, 0);
  assert.equal(master.operator.status, 'IMPORT_FAILED');
});

test('failed marker clearing leaves the completed node fenced with its previous durable marker', async (t) => {
  const { master } = await restoreFixture(t, 'CREATE TABLE x(id INT);', async (_node, value) => { if (value === null) throw new Error('clear failed'); });
  await assert.rejects(master.operator.restoreSnapshot('fixture', { replicate: true }), /clear failed/);
  assert.equal(master.operator.status, 'IMPORT_FAILED');
  assert.ok(master.state.marker);
});

test('an incomplete replacement snapshot never permits election or clears the failure fence', async () => {
  const { operator, calls } = snapshotFixture();
  calls.available = false;
  assert.equal(await operator.findMaster(), null);
  assert.equal(calls.imports, 0);
  assert.equal(operator.snapshotRecoveryRequired, true);
  calls.available = true;
  operator.findMaster = async () => null;
  operator.initMasterConnection = () => {};
  await operator.doHealthCheck();
  assert.equal(calls.imports, 1);
  assert.equal(operator.snapshotRecoveryRequired, false);
});

test('snapshot reset stops immediately when the database returns an error tuple', async () => {
  const { backlog } = backlogFixture([]);
  const statements = [];
  backlog.BLClient.query = async (sql) => { statements.push(sql); return [null, null, { code: 'ER_DBACCESS_DENIED_ERROR' }]; };
  await assert.rejects(backlog.resetForSnapshot(), { code: 'ER_DBACCESS_DENIED_ERROR' });
  assert.equal(statements.length, 1);
});

test('a follower with a sequence gap cannot acknowledge a completed restore', async () => {
  const { operator, backlog } = operatorFixture();
  backlog.sequenceNumber = 0;
  await operator.handleRestoreState({ phase: 'begin', snapshot: 'fixture' });
  await operator.queueRestoreQuery('INSERT INTO x VALUES(1)', 2, 0);
  assert.equal(operator.status, 'IMPORT_FAILED');
  await assert.rejects(operator.handleRestoreState({ phase: 'complete', snapshot: 'fixture' }), /sequence gap/);
  assert.equal(operator.snapshotRecoveryRequired, true);
});
