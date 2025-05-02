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
    this.InitDB = ''; // Tracks the currently selected DB via setDB
    this.stream = null;
    this.socketCallBack = null; // For potential raw streaming (not used in these APIs)
    this.socketId = null;
    this.enableSocketWrite = false;
    this.initializing = false; // Flag to prevent concurrent initializations
    this.reconnecting = false; // Flag to prevent concurrent reconnections
    this.initPromise = null; // Store initialization promise
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
        stream.removeListener('data', resolve); // Remove data listener on error too
        reject(err);
      });
      // Handle stream closing unexpectedly
      stream.once('close', () => {
        log.warn('DB stream closed unexpectedly.');
        this.connected = false;
        // Optional: trigger reconnect or notify
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
  * [init] - Public init method, ensures only one init runs at a time
  */
  async init() {
    // Prevent concurrent initialization attempts
    if (this.initializing) {
      log.warn('Initialization already in progress.');
      // Optionally wait for the existing promise
      return this.initPromise;
    }
    this.initializing = true;
    // eslint-disable-next-line no-underscore-dangle
    this.initPromise = this._doInit(); // Store the promise

    try {
      await this.initPromise;
    } finally {
      this.initializing = false; // Reset flag whether success or failure
    }
    return this.initPromise; // Return the completed promise result
  }

  /**
   * [_doInit] - Internal initialization logic
   */
  // eslint-disable-next-line no-underscore-dangle
  async _doInit() { // Internal init logic
    if (config.dbType === 'mysql') {
      try {
        // Close existing stream/connection if necessary
        if (this.stream) {
          this.stream.destroy();
          this.stream = null;
        }
        if (this.connection && typeof this.connection.end === 'function') {
          try { await this.connection.end(); } catch (e) { log.warn('Error closing previous connection:', e.message); }
          this.connection = {};
        }
        this.connected = false; // Mark as disconnected during init

        await this.createStream();
        this.stream.on('data', (data) => {
          this.rawCallback(data);
        });

        // --- *** DATE FIX STARTS HERE *** ---
        // Define the typeCast function to handle date/time types
        const typeCast = (field, next) => {
          // Check if the field is a date/time type that should be returned as a string
          if (
            field.type === 'DATETIME'
              || field.type === 'TIMESTAMP'
              || field.type === 'DATE'
              || field.type === 'NEWDATE' // Sometimes used for DATE type
          ) {
            // Return the raw string value from the database
            const value = field.string();
            // Return null if the value is actually NULL in the DB
            return value === null ? null : value;
          }
          // For all other types, use the default casting behavior
          return next();
        };
          // --- *** DATE FIX ENDS HERE *** ---

        this.connection = await mySql.createConnection({
          password: Security.getKey(),
          user: config.dbUser,
          stream: this.stream,
          connectTimeout: 15000, // Reduced timeout
          // --- *** DATE FIX: Apply typeCast function *** ---
          typeCast,
          // --- *** END DATE FIX *** ---
          // multipleStatements: true // Be cautious if enabling this
        });

        this.connection.on('error', async (err) => { // Make listener async
          this.connected = false;
          log.error(`DB connection error for ${this.InitDB || 'initial connection'}: ${err.code} - ${err.message}`);
          // Attempt reconnect on specific errors
          if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
            log.info('Attempting to reconnect due to connection error...');
            await this.reconnect(); // Await the reconnect attempt
          } else {
            // Handle other errors (e.g., auth errors might not be recoverable here)
            log.error('Unhandled DB connection error:', err);
          }
        });

        this.connected = true;
        log.info('DB connection established successfully.');
        // If an initial DB was set, re-apply it
        if (this.InitDB) {
          await this.setDB(this.InitDB);
        }
      } catch (err) {
        this.connected = false;
        log.error(`Initial DB connection error: ${err.message}`);
        if (this.stream) { // Clean up stream on init failure
          this.stream.destroy();
          this.stream = null;
        }
        // Don't automatically retry here, let caller handle initial failure
        throw err; // Re-throw error to signal failure
      }
    } else {
      throw new Error(`Unsupported dbType: ${config.dbType}`);
    }
  }

  /**
   * [reconnect] - Handles reconnection attempts
   */
  async reconnect() {
    if (this.connected) return; // Already connected
    if (this.reconnecting) {
      log.warn('Reconnect already in progress.');
      return; // Avoid concurrent reconnects
    }

    this.reconnecting = true;
    log.info('Attempting to reconnect to the database...');

    // Simple backoff strategy
    let attempts = 0;
    const maxAttempts = 5;
    const delay = 2000; // 2 seconds

    while (attempts < maxAttempts && !this.connected) {
      attempts += 1;
      log.info(`Reconnect attempt ${attempts}/${maxAttempts}...`);
      try {
        // Use _doInit directly to avoid the initializing flag issue during reconnect
        // eslint-disable-next-line no-await-in-loop, no-underscore-dangle
        await this._doInit();
        if (this.connected) {
          log.info('Reconnected to the database successfully.');
          break; // Exit loop on success
        }
      } catch (err) {
        log.error(`Reconnection attempt ${attempts} failed: ${err.message}`);
        if (attempts < maxAttempts) {
          // eslint-disable-next-line no-await-in-loop, no-loop-func, no-promise-executor-return
          await new Promise((resolve) => setTimeout(resolve, delay * 2 ** (attempts - 1))); // Exponential backoff
        }
      }
    }

    this.reconnecting = false; // Reset flag

    if (!this.connected) {
      log.error('Failed to reconnect to the database after multiple attempts.');
      // Optional: Implement further actions (e.g., shutdown, notify admin)
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
    await cl.init(); // Await the initialization
    if (!cl.connected) {
      log.error('DBClient failed to connect during factory creation.');
      throw new Error('DBClient connection failed.');
    }
    log.info('DBClient created and connected successfully.');
    return cl;
  } catch (err) {
    log.error(`Error creating DBClient: ${err.message}`);
    if (config.dbType === 'mysql') {
      if (err.code === 'ER_ACCESS_DENIED_ERROR') {
        log.error('Database access denied. Check credentials (DB_USER/DB_PASSWORD).');
        throw new Error('WRONG_KEY');
      }
      if (err.code === 'ECONNREFUSED') {
        log.error('Database connection refused. Is the DB server running and accessible?');
        throw new Error('CONN_REFUSED');
      }
    }
    return null; // Return null on failure
  }
};
