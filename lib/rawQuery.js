const mysql = require('mysql2');

function isPrimaryKeyValue(value) {
  return value !== null
    && ['string', 'number', 'boolean'].includes(typeof value)
    && (typeof value !== 'number' || Number.isFinite(value));
}

function buildDeleteQuery(database, table, primaryKeyColumn, primaryKeyValue) {
  if (!isPrimaryKeyValue(primaryKeyValue)) {
    throw new TypeError('Primary key value must be a string, number, or boolean.');
  }

  return mysql.format(
    'DELETE FROM ??.?? WHERE ?? = ?',
    [database, table, primaryKeyColumn, primaryKeyValue],
  );
}

function unpackBacklogWriteResult(backlogResult) {
  const isBacklogEnvelope = Array.isArray(backlogResult)
    && Number.isInteger(backlogResult[1])
    && typeof backlogResult[2] === 'number';
  const result = isBacklogEnvelope ? backlogResult[0] : backlogResult;
  const error = Array.isArray(result) ? result[2] : result?.error;
  return { error, result };
}

module.exports = { buildDeleteQuery, isPrimaryKeyValue, unpackBacklogWriteResult };
