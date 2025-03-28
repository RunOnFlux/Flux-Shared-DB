/* eslint-disable no-unused-vars */
const mySql = require('mysql2/promise');
const net = require('net');
const config = require('./config');
const Security = require('./Security');
const log = require('../lib/log');

class DBClient {
  constructor() {
    this.connection = {};
    this.connected = false;
    this.InitDB = '';
    this.stream = null;
    this.socketCallBack = null;
    this.socketId = null;
    this.enableSocketWrite = false;
  }

  /**
  * [init]
  */
  async createStream() {
    this.stream = net.connect({
      host: config.dbHost,
      port: config.dbPort,
    });
    const { stream } = this;
    return new Promise((resolve, reject) => {
      stream.once('connect', () => {
        stream.removeListener('error', reject);
        resolve(stream);
      });
      stream.once('error', (err) => {
        stream.removeListener('connection', resolve);
        stream.removeListener('data', resolve);
        reject(err);
      });
    });
  }

  /**
  * [rawCallback]
  */
  rawCallback(data) {
    if (this.socketCallBack && this.enableSocketWrite) {
      this.socketCallBack.write(data);
      // log.info(`writing to ${this.socketId}: ${data.length} bytes`);
    }
  }

  /**
  * [setSocket]
  */
  setSocket(func, id = null) {
    if (func === null) log.info('socket set to null');
    this.socketCallBack = func;
    this.socketId = id;
    this.enableSocketWrite = true;
  }

  /**
  * [disableSocketWrite]
  */
  disableSocketWrite() {
    // log.info(`socket write disabled for ${this.socketId}`);
    this.enableSocketWrite = false;
    this.socketId = null;
  }

  /**
  * [init]
  */
  async init() {
    if (config.dbType === 'mysql') {
      try {
        await this.createStream();
        this.stream.on('data', (data) => {
          this.rawCallback(data);
        });
        this.connection = await mySql.createConnection({
          password: Security.getKey(),
          user: config.dbUser,
          stream: this.stream,
          connectTimeout: 60000,
        });
        this.connection.on('error', (err) => {
          this.connected = false;
          log.info(`Connection to ${this.InitDB} DB was lost: ${err.message}`);
          this.reconnect();
        });
        this.connected = true;
      } catch (err) {
        this.connected = false;
        log.error(`Initial connection error: ${err.message}`);
        setTimeout(() => this.reconnect(), 1000);
      }
    }
  }

  async reconnect() {
    if (this.connected) return;
    // log.info('Attempting to reconnect to the database...');
    try {
      await this.init();
      // log.info('Reconnected to the database.');
    } catch (err) {
      // log.error(`Reconnection failed: ${err.message}`);
      // setTimeout(() => this.reconnect(), 5000); // Retry after 5 seconds
    }
  }

  /**
  * [query]
  * @param {string} query [description]
  */
  async query(query, rawResult = false, fullQuery = '') {
    if (config.dbType === 'mysql') {
      // log.info(`running Query: ${query}`);
      try {
        if (!this.connected) {
          log.info(`Connecten to ${this.InitDB} DB was lost, reconnecting...`);
          await this.init();
          this.setDB(this.InitDB);
        }
        if (rawResult) {
          const [rows, fields, err] = await this.connection.query(query);
          if (err) log.error(err);
          return [rows, fields, err];
        // eslint-disable-next-line no-else-return
        } else {
          const [rows, err] = await this.connection.query(query);
          if (err && err.toString().includes('Error')) log.error(`Error running query: ${err.toString()}, ${fullQuery}`, 'red');
          return rows;
        }
      } catch (err) {
        if (err && err.toString().includes('Error')) log.error(`Error running query: ${err.toString()}, ${fullQuery}`, 'red');
        return [null, null, err];
      }
    }
    return null;
  }

  /**
  * [execute]
  * @param {string} query [description]
  * @param {array} params [description]
  */
  async execute(query, params, rawResult = false, fullQuery = '') {
    if (config.dbType === 'mysql') {
      try {
        if (!this.connected) {
          await this.init();
        }
        const [rows, fields, err] = await this.connection.execute(query, params);
        if (err && err.toString().includes('Error')) log.error(`Error executing query: ${err.toString()}, ${fullQuery}`, 'red');
        if (rawResult) return [rows, fields, err];
        return rows;
      } catch (err) {
        if (err && err.toString().includes('Error')) log.error(`Error executing query: ${err.toString()}, ${fullQuery}`, 'red');
        return [null, null, err];
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
        log.info(`DB ${dbName} exists`);
      }
    }
    return null;
  }

  /**
  * [setDB]
  * @param {string} dbName [description]
  */
  async setDB(dbName) {
    try {
      if (config.dbType === 'mysql') {
        this.InitDB = dbName;
        // log.info(`seting db to ${dbName}`);
        if (this.connection) {
          this.connection.changeUser({
            database: dbName,
          }).catch((err) => {
            if (err) {
              log.error(`Error changing database: ${err}`);
              this.reconnect();
            }
          });
        }
      }
    } catch (err) {
      log.info(err);
    }
  }

  /**
  * [setPassword]
  * @param {string} key [description]
  */
  async setPassword(key) {
    if (config.dbType === 'mysql') {
      await this.query(`SET PASSWORD FOR 'root'@'localhost' = PASSWORD('${key}');SET PASSWORD FOR 'root'@'%' = PASSWORD('${key}');FLUSH PRIVILEGES;`);
    }
  }
}

// eslint-disable-next-line func-names
exports.createClient = async function () {
  try {
    const cl = new DBClient();
    await cl.init();
    if (!cl.connected) return null;
    return cl;
  } catch (err) {
    log.info(JSON.stringify(err));
    if (config.dbType === 'mysql') {
      if (err.code === 'ER_ACCESS_DENIED_ERROR') return 'WRONG_KEY';
    }
    return null;
  }
};
