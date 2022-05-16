/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/**
* [This Library helps analyze sql queries and seperate read/write queries for mysql, mssql and postgre]
*/
const querySplitter = require('dbgate-query-splitter');

/**
 * [getQueryType analyzes a single query and returns r or w for query type]
 * @param {string} sql sql query
 * @return {string}        [r/w]
 */
function getQueryType(sql) {
  const readFlags = ['select', 'show'];
  if (sql.toLowerCase().startsWith(readFlags[0]) || sql.toLowerCase().startsWith(readFlags[1])) {
    // it's a read query
    return 'r';
  }
  // it's a write query
  return 'w';
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
      var sql = cleanUP(output[i]);
      if(sql) analyzedArray.push([sql, getQueryType(sql)]);
  }
  return analyzedArray;
}
/**
 * [clean up linebraks]
 * @param {string} sql sql query
 */
function cleanUP(sql){
  if(sql.startsWith('/*') && sql.endsWith('*/')) return false;
  if(sql.startsWith('--')){
    while(sql.startsWith('--') || sql.startsWith('\r\n') || sql.startsWith('\n')) sql = removeFirstLine(sql);
  }
  return sql;
}

function removeFirstLine(str){
  var lines = str.split('\n');
  lines.splice(0,1);
  return lines.join('\n');
}

module.exports = analyzeSql;
