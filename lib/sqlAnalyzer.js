/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/**
* [This Library helps analyze sql queries and seperate read/write queries for mysql, mssql and postgre]
*/
import {
  splitQuery,
  mysqlSplitterOptions,
  mssqlSplitterOptions,
  postgreSplitterOptions,
} from 'dbgate-query-splitter';

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
  if (options === 'mysql') {
    const sqlOptions = mysqlSplitterOptions;
  } else if (options === 'mssql') {
    const sqlOptions = mssqlSplitterOptions;
  } else if (options === 'postgre') {
    const sqlOptions = postgreSplitterOptions;
  } else return [];
  const output = splitQuery(sql, sqlOptions);
  const analyzedArray = [];
  for (let i = 0; i < output.length; i += 1) {
    analyzedArray.push(output[i], getQueryType(output[i]));
  }
  return analyzedArray;
}
