const assert = require('node:assert/strict');
const test = require('node:test');
const sqlAnalyzer = require('../lib/sqlAnalyzer');

test('classifies transaction isolation configuration as local-only state', () => {
  assert.deepEqual(
    sqlAnalyzer('SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'mysql'),
    [['SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'r']],
  );
  assert.deepEqual(
    sqlAnalyzer('set transaction isolation level serializable', 'mysql'),
    [['set transaction isolation level serializable', 'r']],
  );
});

test('continues to classify data modifications as replicated writes', () => {
  assert.deepEqual(
    sqlAnalyzer('UPDATE players SET cash = 10 WHERE id = 1', 'mysql'),
    [['UPDATE players SET cash = 10 WHERE id = 1', 'w']],
  );
});
