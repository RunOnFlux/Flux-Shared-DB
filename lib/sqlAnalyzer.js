/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/**
* [This Library helps analyze sql queries and seperate read/write queries for mysql, mssql and postgre]
*/
const log = require('./log');
const config = require('../ClusterOperator/config');

/**
 * [getQueryType analyzes a single query and returns r or w for query type]
 * @param {string} sql sql query
 * @param {string} dbType database type (mysql/postgresql)
 * @return {string}        [r/w/s]
 */
function getQueryType(sql, dbType = 'mysql') {
  const lowerSql = sql.toLowerCase().trim();

  // Common read operations for both MySQL and PostgreSQL
  const commonReadFlags = ['select', 'show', 'describe', 'explain'];

  // MySQL-specific read operations
  const mysqlReadFlags = ['set names', 'kill', 'set profiling'];

  // PostgreSQL-specific read operations
  const postgresReadFlags = [
    '\\d', // psql describe commands
    '\\dt', // list tables
    '\\l', // list databases
    '\\dn', // list schemas
    '\\df', // list functions
  ];

  // Check common read flags
  // eslint-disable-next-line no-restricted-syntax
  for (const flag of commonReadFlags) {
    if (lowerSql.startsWith(flag)) return 'r';
  }

  // Database-specific checks
  if (dbType === 'mysql') {
    // eslint-disable-next-line no-restricted-syntax
    for (const flag of mysqlReadFlags) {
      if (lowerSql.startsWith(flag)) return 'r';
    }

    // disable transient filtering for test apps (remove the whole block after tests are done)
    if (config.containerDataPath !== 's:/app/dumps') {
      if (sql.includes('`wp_options`') && sql.includes('_transient_')) {
      //  log.warn('wp transient query');
        return 'r';
      }
    }

    if (lowerSql.startsWith('set session')) {
      return 's';
    }

    // MySQL-specific session queries
    if (lowerSql.startsWith('set @@') || lowerSql.startsWith('set autocommit') || lowerSql.startsWith('set sql_mode')) {
      return 's';
    }
  } else if (dbType === 'postgresql') {
    // eslint-disable-next-line no-restricted-syntax
    for (const flag of postgresReadFlags) {
      if (lowerSql.startsWith(flag)) return 'r';
    }

    // PostgreSQL-specific session/configuration queries
    if (lowerSql.startsWith('set ') || lowerSql.startsWith('reset ')) {
      return 's';
    }

    // PostgreSQL system information queries
    if (lowerSql.includes('pg_catalog')
        || lowerSql.includes('information_schema')
        || lowerSql.includes('pg_stat_')
        || lowerSql.includes('pg_class')
        || lowerSql.includes('pg_namespace')
        || lowerSql.includes('pg_type')
        || lowerSql.includes('pg_database')) {
      return 'r';
    }

    // PostgreSQL transaction commands that don't modify data
    if (lowerSql.startsWith('begin')
        || lowerSql.startsWith('commit')
        || lowerSql.startsWith('rollback')
        || lowerSql.startsWith('start transaction')) {
      return 's';
    }
  }

  // it's a write query
  return 'w';
}
function removeFirstLine(str) {
  const lines = str.split('\n');
  lines.splice(0, 1);
  return lines.join('\n');
}
/**
 * [clean up linebreaks and handle database-specific quirks]
 * @param {string} sql sql query
 * @param {string} dbType database type (mysql/postgresql)
 */
function cleanUP(sql, dbType = 'mysql') {
  // log.query(sql);
  let cleanedSql = sql;

  if (dbType === 'mysql') {
    // fixes mysql2 client utf8 support issue
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('/*!40101 set character_set_client = utf8 */')) cleanedSql = '/*!40101 SET character_set_client = utf8mb4 */';
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('set character_set_client = utf8')) cleanedSql = 'SET character_set_client = utf8mb4';
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('/*!40101 set names utf8 */')) cleanedSql = '/*!40101 SET NAMES utf8mb4 */';
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('set names utf8')) cleanedSql = 'SET NAMES utf8mb4';
    // fixes navicat client sending multiple queries after connection
    if (cleanedSql.startsWith("SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'; ")) {
      // eslint-disable-next-line no-param-reassign
      cleanedSql = cleanedSql.replace("SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'; ", '');
      // log.debug(cleanedSql);
    }
  } else if (dbType === 'postgresql') {
    // PostgreSQL-specific cleanup
    // Handle client encoding issues
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('set client_encoding to utf8')) cleanedSql = 'SET client_encoding TO UTF8';
    // eslint-disable-next-line no-param-reassign
    if (cleanedSql.toLowerCase().startsWith('set client_encoding = utf8')) cleanedSql = 'SET client_encoding = UTF8';

    // Handle common PostgreSQL client tools queries
    if (cleanedSql.includes('pg_stat_activity') && cleanedSql.includes('current_query')) {
      // Old psql versions might use current_query instead of query
      // eslint-disable-next-line no-param-reassign
      cleanedSql = cleanedSql.replace(/current_query/g, 'query');
    }
  }

  // Common cleanup for both databases
  if (cleanedSql.startsWith('--')) {
    // eslint-disable-next-line no-param-reassign
    while (cleanedSql.startsWith('--') || cleanedSql.startsWith('\r\n') || cleanedSql.startsWith('\n')) cleanedSql = removeFirstLine(cleanedSql);
  }

  return cleanedSql;
}
/**
 * [analyzeSql analyzes the sql and marks them as read/write]
 * @param {string} sql sql query
 * @param {string} options sql engine:mysql,mssql,postgresql
 * @return {array}        [array of queries]
 */
function analyzeSql(sql, options) {
  // Normalize the options parameter
  let dbType = 'mysql';
  if (options === 'mysql') {
    dbType = 'mysql';
  } else if (options === 'mssql') {
    dbType = 'mssql';
    // TODO: Add MSSQL support if needed
  } else if (options === 'postgre' || options === 'postgresql') {
    dbType = 'postgresql';
  } else {
    // Default to mysql if unknown option
    log.warn(`Unknown database type: ${options}, defaulting to mysql`);
    dbType = 'mysql';
  }

  const analyzedArray = [];
  const tempSql = cleanUP(sql.trim(), dbType);

  if (tempSql) {
    const queryType = getQueryType(tempSql, dbType);
    analyzedArray.push([tempSql, queryType]);
    // log.query(`${dbType}: ${tempSql} -> ${queryType}`, 'yellow');
  }

  return analyzedArray;
}

module.exports = analyzeSql;
