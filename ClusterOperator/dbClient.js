/* eslint-disable no-unused-vars */
const mySql = require('mysql2/promise');
const config = require('./config');
const log = require('../lib/log');

class DBClient {
  constructor() {
    this.connection = {};
  }

  /**
  * [init]
  */
  async init() {
    if (config.dbType === 'mysql') {
      this.connection = await mySql.createConnection({
        host: 'localhost',
        user: config.dbUser,
        password: config.dbPass,
        port: config.dbPort,
      });
    }
  }

  /**
  * [query]
  * @param {string} query [description]
  */
  async query(query, rawResult = false) {
    if (config.dbType === 'mysql') {
      try {
        const [rows, fields, err] = await this.connection.execute(query);
        if (rawResult) return [rows, fields, err];
        return rows;
      } catch (err) {
        log.info(err);
      }
    }
    return null;
  }

  /**
  * [createDB]
  * @param {string} dbName [description]
  */
  async createDB(dbName) {
    if (config.dbType === 'mysql') {
      try {
        await this.query(`CREATE DATABASE IF NOT EXISTS ${dbName}`);
      } catch (err) {
        log.info(err);
      }
    }
    return null;
  }

  /**
  * [setDB]
  * @param {string} dbName [description]
  */
  async setDB(dbName) {
    if (config.dbType === 'mysql') {
      this.connection = await mySql.createConnection({
        host: 'localhost',
        user: config.dbUser,
        password: config.dbPass,
        port: config.dbPort,
        database: dbName,
      });
    }
  }
}

// eslint-disable-next-line func-names
exports.createClient = async function () {
  try {
    const cl = new DBClient();
    await cl.init();
    return cl;
  } catch (e) {
    return null;
  }
};
