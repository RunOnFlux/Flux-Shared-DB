const assert = require('node:assert/strict');
const test = require('node:test');
const BackLog = require('../ClusterOperator/Backlog');
const { buildDeleteQuery, isPrimaryKeyValue, unpackBacklogWriteResult } = require('../lib/rawQuery');

test('builds a self-contained delete query with escaped identifiers and value', () => {
  assert.equal(
    buildDeleteQuery('customer_db', 'users', 'external_id', "x' OR 1=1 -- "),
    "DELETE FROM `customer_db`.`users` WHERE `external_id` = 'x\\' OR 1=1 -- '",
  );
  assert.equal(
    buildDeleteQuery('customer_db', 'users', 'id', 42),
    'DELETE FROM `customer_db`.`users` WHERE `id` = 42',
  );
});

test('rejects values that cannot be primary keys from a JSON request', () => {
  assert.equal(isPrimaryKeyValue('id'), true);
  assert.equal(isPrimaryKeyValue(1), true);
  assert.equal(isPrimaryKeyValue(true), true);
  assert.equal(isPrimaryKeyValue(Number.NaN), false);
  assert.equal(isPrimaryKeyValue(Number.POSITIVE_INFINITY), false);
  assert.equal(isPrimaryKeyValue(null), false);
  assert.equal(isPrimaryKeyValue(['1 OR 1=1']), false);
  assert.equal(isPrimaryKeyValue({ value: 1 }), false);
  assert.throws(() => buildDeleteQuery('db', 'table', 'id', ['1 OR 1=1']), TypeError);
});

test('unwraps successful and failed backlog writes', () => {
  const databaseResult = { affectedRows: 1 };
  assert.deepEqual(unpackBacklogWriteResult([databaseResult, 7, 1234]), {
    error: undefined,
    result: databaseResult,
  });

  const databaseError = Object.assign(new Error('missing table'), { code: 'ER_NO_SUCH_TABLE' });
  assert.deepEqual(unpackBacklogWriteResult([[null, null, databaseError], 8, 1235]), {
    error: databaseError,
    result: [null, null, databaseError],
  });
  assert.deepEqual(unpackBacklogWriteResult(null), { error: undefined, result: null });
});

test('stores and executes the same raw delete query through the backlog', async () => {
  const query = buildDeleteQuery('customer_db', 'users', 'id', "x' OR 1=1 -- ");
  let storedQuery;
  let executedQuery;
  const previousBLClient = BackLog.BLClient;
  const previousUserDBClient = BackLog.UserDBClient;
  const previousSequenceNumber = BackLog.sequenceNumber;

  BackLog.BLClient = {
    query: async () => {},
    execute: async (sql, params) => {
      if (sql.startsWith('INSERT INTO')) {
        const [, queryValue] = params;
        storedQuery = queryValue;
      }
      return { affectedRows: 1 };
    },
  };
  BackLog.UserDBClient = {
    query: async (sql) => {
      executedQuery = sql;
      return { affectedRows: 1 };
    },
  };
  BackLog.sequenceNumber = 0;

  try {
    const [result] = await BackLog.pushQuery(query, 0, 1234);
    assert.equal(storedQuery, query);
    assert.equal(executedQuery, query);
    assert.equal(result.affectedRows, 1);
  } finally {
    BackLog.BLClient = previousBLClient;
    BackLog.UserDBClient = previousUserDBClient;
    BackLog.sequenceNumber = previousSequenceNumber;
  }
});
