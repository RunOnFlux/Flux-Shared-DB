/* eslint-disable no-unused-vars */
const mySql = require('mysql2/promise');
const config = require('./config');
const log = require('../lib/log');

class DBClient {
  constructor() {
    this.connection = {};
    this.connected = false;
    this.InitDB = '';
  }

  /**
  * [init]
  */
  async init() {
    if (config.dbType === 'mysql') {
      this.connection = await mySql.createConnection({
        host: config.dbHost,
        user: config.dbUser,
        password: config.dbPass,
        port: config.dbPort,
      });
      this.connection.once('error', () => {
        this.connected = false;
        console.log(`mysql connected: ${this.connected }`);
      });
      this.connected = true;
    }
  }

  /**
  * [query]
  * @param {string} query [description]
  */
  async query(query, rawResult = false) {
    if (config.dbType === 'mysql') {
      
      try {
        if(!this.connected){
          log.info(`DB connecten was lost, reconnecting...`);
          await this.init();
          this.setDB(this.InitDB);
        }
        const [rows, fields, err] = await this.connection.query(query);
        if (rawResult) return [rows, fields, err];
        return rows;
      } catch (err) {
        log.info(err);
        return [null,null,err];
      }
    }
    return null;
  }
  /**
  * [execute]
  * @param {string} query [description]
  * @param {array} params [description]
  */
  async execute(query, params, rawResult = false) {
    if (config.dbType === 'mysql') {
      try {
        if(!this.connected){
          await this.init();
        }
        const [rows, fields, err] = await this.connection.execute(query, params);
        if (rawResult) return [rows, fields, err];
        return rows;
      } catch (err) {
        log.info(err);
        return [null,null,err];
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
      this.InitDB = dbName;
      this.connection.changeUser({
        database : dbName
      }, (err) => {
          if (err) {
            console.log('Error in changing database', err);
            return;
          }
      })
    }
  }
}

// eslint-disable-next-line func-names
exports.createClient = async function () {
  try {
    const cl = new DBClient();
    await cl.init();
    return cl;
  } catch (err) {
    log.info(err);
    return null;
  }
};
