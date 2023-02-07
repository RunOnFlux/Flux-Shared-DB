/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/**
* [This Library helps analyze sql queries and seperate read/write queries for mysql, mssql and postgre]
*/
const querySplitter = require('dbgate-query-splitter');
const log = require('./log');

let prevQuery = '';
/**
 * [getQueryType analyzes a single query and returns r or w for query type]
 * @param {string} sql sql query
 * @return {string}        [r/w]
 */
function getQueryType(sql) {
  const readFlags = ['select', 'show', 'describe', 'set names', 'kill', 'set profiling'];
  // eslint-disable-next-line no-restricted-syntax
  for (const flag of readFlags) { if (sql.toLowerCase().startsWith(flag)) return 'r'; }

  // these codes are to fix ridiculus duplicate wordpress queries
  /*
  if (sql.includes('`wp_options`') && sql.includes("'_transient_")) {
    log.warn('wp transient query');
    return 'r';
  }
  if (sql.includes('`wp_options`') && sql.includes("`option_name` = 'cron'")) {
    log.warn('wp cron query');
    return 'r';
  }
  */
  if (sql.includes('SET SESSION')) {
    return 's';
  }
  if (sql === prevQuery && (sql.includes('`wp_options`'))) {
    log.warn(`duplicate write query ${sql}`);
    prevQuery = sql;
    return 'r';
  }
  prevQuery = sql;
  // end fix for wordpress fuckery

  // it's a write query
  return 'w';
}
function removeFirstLine(str) {
  const lines = str.split('\n');
  lines.splice(0, 1);
  return lines.join('\n');
}
/**
 * [clean up linebraks]
 * @param {string} sql sql query
 */
function cleanUP(sql) {
  if (sql.startsWith('/*') && sql.endsWith('*/')) return false;
  if (sql.startsWith('--')) {
    // eslint-disable-next-line no-param-reassign
    while (sql.startsWith('--') || sql.startsWith('\r\n') || sql.startsWith('\n')) sql = removeFirstLine(sql);
  }
  return sql;
}
/**
 * [analyzeSql analyzes the sql and marks them as read/write]
 * @param {string} sql sql query
 * @param {string} options sql engine:mysql,mssql,postgre
 * @return {array}        [array of queries]
 */
function analyzeSql(sql, options) {
  let sqlOptions = null;
  if (options === 'mysql') {
    sqlOptions = querySplitter.mysqlSplitterOptions;
    sqlOptions.multilineComments = true;
  } else if (options === 'mssql') {
    sqlOptions = querySplitter.mssqlSplitterOptions;
  } else if (options === 'postgre') {
    sqlOptions = querySplitter.postgreSplitterOptions;
  } else return [];
  const output = querySplitter.splitQuery(sql, sqlOptions);
  const analyzedArray = [];
  for (let i = 0; i < output.length; i += 1) {
    const tempSql = cleanUP(output[i]);
    if (tempSql) analyzedArray.push([tempSql, getQueryType(tempSql)]);
  }
  return analyzedArray;
}

module.exports = analyzeSql;
