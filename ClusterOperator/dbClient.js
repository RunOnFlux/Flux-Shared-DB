/* eslint-disable no-unused-vars */
const mySql = require('mysql2/promise');
const config = require('./config');
const log = require('../lib/log');

class DBClient {
  constructor() {
    this.connection = {};
  }

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

  async query(query) {
    if (config.dbType === 'mysql') {
      try {
        const [rows, fields, err] = await this.connection.execute(query);
        return rows;
      } catch (err) {
        log.info(err);
      }
    }
    return null;
  }

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
  const cl = new DBClient();
  await cl.init();
  return cl;
};
