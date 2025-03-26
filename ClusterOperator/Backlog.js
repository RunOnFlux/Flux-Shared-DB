/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const timer = require('timers/promises');
const queryCache = require('memory-cache');
const fs = require('fs');
const path = require('path');
const dbClient = require('./DBClient');
const config = require('./config');
const log = require('../lib/log');
const Security = require('./Security');
const ConnectionPool = require('../lib/ConnectionPool');
const utill = require('../lib/utill');
const mysqldump = require('../lib/mysqldump');

class BackLog {
  static buffer = [];

  static sequenceNumber = 0;

  static bufferSequenceNumber = 0;

  static bufferStartSequenceNumber = 0;

  static compressionTask = -1;

  static BLClient = null;

  static UserDBClient = null;

  static writeLock = false;

  static executeLogs = true;

  static BLqueryCache = queryCache;

  /**
  * [createBacklog]
  * @param {object} params [description]
  */
  static async createBacklog(UserDBClient) {
    this.BLClient = await dbClient.createClient();
    this.UserDBClient = UserDBClient;
    try {
      if (config.dbType === 'mysql') {
        const dbList = await this.BLClient.query(`SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${config.dbBacklog}'`);
        if (dbList.length === 0) {
          log.info('Backlog DB not defined yet, creating backlog DB...');
          await this.BLClient.createDB(config.dbBacklog);
        } else {
          log.info('Backlog DB already exists, moving on...');
        }
        await this.BLClient.setDB(config.dbBacklog);
        let tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
          WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogCollection}'`);
        if (tableList.length === 0) {
          log.info('Backlog table not defined yet, creating backlog table...');
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogCollection} (seq bigint, query longtext, timestamp bigint) ENGINE=InnoDB;`);
          await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogCollection}\`
            MODIFY COLUMN \`seq\` bigint(0) UNSIGNED NOT NULL FIRST,
            ADD PRIMARY KEY (\`seq\`),
            ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
        } else {
          log.info('Backlog table already exists, moving on...');
          this.sequenceNumber = await this.getLastSequenceNumber();
        }
        tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
          WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogBuffer}'`);
        if (tableList.length === 0) {
          log.info('Backlog buffer table not defined yet, creating buffer table...');
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogBuffer} (seq bigint, query longtext, timestamp bigint) ENGINE=InnoDB;`);
          await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogBuffer}\` 
            MODIFY COLUMN \`seq\` bigint(0) UNSIGNED NOT NULL FIRST,
            ADD PRIMARY KEY (\`seq\`),
            ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
        } else {
          log.info('Backlog buffer table already exists, moving on...');
        }
        tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
          WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbOptions}'`);
        if (tableList.length === 0) {
          log.info('Backlog options table not defined yet, creating options table...');
          await this.BLClient.query(`CREATE TABLE ${config.dbOptions} (k varchar(64), value text, PRIMARY KEY (k)) ENGINE=InnoDB;`);
        } else {
          log.info('Backlog options table already exists, moving on...');
        }
        log.info(`Last Seq No: ${this.sequenceNumber}`);
      }
    } catch (e) {
      log.error(`Error creating backlog: ${e}`);
    }
  }

  /**
  * [pushQuery]
  * @param {string} query [description]
  * @param {int} timestamp [description]
  * @return {Array}
  */
  static async pushQuery(query, seq = 0, timestamp, buffer = false, connId = false, fullQuery = '') {
    // eslint-disable-next-line no-param-reassign
    if (timestamp === undefined) timestamp = Date.now();
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        if (buffer) {
          if (this.bufferStartSequenceNumber === 0) this.bufferStartSequenceNumber = seq;
          this.bufferSequenceNumber = seq;
          await this.BLClient.execute(
            `INSERT INTO ${config.dbBacklogBuffer} (seq, query, timestamp) VALUES (?,?,?)`,
            [seq, query, timestamp],
          );
          return [null, seq, timestamp];
        } else {
          this.writeLock = true;
          let result = null;
          if (seq === 0) { this.sequenceNumber += 1; } else {
            this.sequenceNumber = seq;
          }
          const seqForThis = this.sequenceNumber;
          const BLResult = await this.BLClient.execute(
            `INSERT INTO ${config.dbBacklogCollection} (seq, query, timestamp) VALUES (?,?,?)`,
            [seqForThis, query, timestamp],
          );
          if (this.executeLogs) log.info(`executed ${seqForThis}`);
          /*
          this.BLqueryCache.put(seqForThis, {
            query, seq: seqForThis, timestamp, connId, ip: false,
          }, 20 * 60);
          */
          this.writeLock = false;
          // Abort query execution if there is an error in backlog insert
          if (Array.isArray(BLResult) && BLResult[2]) {
            log.error(`Error in SQL: ${JSON.stringify(BLResult[2])}`);
          } else {
            if (connId === false) {
              result = await this.UserDBClient.query(query, false, fullQuery);
            } else if (connId >= 0) {
              result = await ConnectionPool.getConnectionById(connId).query(query, false, fullQuery);
            }
            if (Array.isArray(result) && result[2]) {
              log.error(`Error in SQL: ${JSON.stringify(result[2])}`);
            }
          }
          return [result, seqForThis, timestamp];
        }
      }
    } catch (e) {
      this.writeLock = false;
      log.error(`error executing query, ${query}, ${seq}`);
    }
    return [];
  }

  /**
  * [getLogs]
  * @param {int} startFrom [description]
  * @param {int} pageSize [description]
  * @return {Array}
  */
  static async getLogs(startFrom, pageSize) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogCollection} WHERE seq >= ${startFrom} ORDER BY seq LIMIT ${pageSize}`);
        const trimedRecords = utill.trimArrayToSize(totalRecords, 3 * 1024 * 1024);
        log.info(`sending backlog records ${startFrom},${pageSize}, records: ${trimedRecords.length}`);
        return trimedRecords;
      }
    } catch (e) {
      log.error(e);
    }
    return [];
  }

  /**
  * [getLogsByTime]
  * @param {int} startFrom [description]
  * @param {int} length [description]
  * @return {Array}
  */
  static async getLogsByTime(startFrom, length) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.execute(`SELECT seq, LEFT(query,10) as query, timestamp FROM ${config.dbBacklogCollection} WHERE timestamp >= ? AND timestamp < ? ORDER BY seq`, [startFrom, Number(startFrom) + Number(length)]);
        return totalRecords;
      }
    } catch (e) {
      log.error(e);
    }
    return [];
  }

  /**
  * [getLogs]
  * @param {int} index [description]
  * @return {object}
  */
  static async getLog(index) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const record = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogCollection} WHERE seq=${index}`);
        // log.info(`backlog records ${startFrom},${pageSize}:${JSON.stringify(totalRecords)}`);
        return record;
      }
    } catch (e) {
      log.error(e);
    }
    return [];
  }

  /**
  * [getDateRange]
  * @return {object}
  */
  static async getDateRange() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const record = await this.BLClient.execute(`SELECT MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp FROM ${config.dbBacklogCollection}`);
        log.info(record);
        return record[0];
      }
    } catch (e) {
      log.error(e);
    }
    return [];
  }

  /**
  * [getTotalLogsCount]
  * @return {int}
  */
  static async getTotalLogsCount() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    } else {
      try {
        if (config.dbType === 'mysql') {
          const totalRecords = await this.BLClient.query(`SELECT count(*) as total FROM ${config.dbBacklogCollection}`);
          log.info(`Total Records: ${JSON.stringify(totalRecords)}`);
          return totalRecords[0].total;
        }
      } catch (e) {
        log.error(e);
      }
    }
    return 0;
  }

  /**
  * [getLastSequenceNumber]
  * @return {int}
  */
  static async getLastSequenceNumber(buffer = false) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    } else {
      try {
        if (config.dbType === 'mysql') {
          let records = [];
          if (buffer) {
            records = await this.BLClient.query(`SELECT seq as seqNo FROM ${config.dbBacklogBuffer} ORDER BY seq DESC LIMIT 1`);
          } else {
            records = await this.BLClient.query(`SELECT seq as seqNo FROM ${config.dbBacklogCollection} ORDER BY seq DESC LIMIT 1`);
          }
          if (records.length) return records[0].seqNo;
        }
      } catch (e) {
        log.error(e);
      }
    }
    return 0;
  }

  /**
  * [getFirstSequenceNumber]
  * @return {int}
  */
  static async getFirstSequenceNumber(buffer = false) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    } else {
      try {
        if (config.dbType === 'mysql') {
          let records = [];
          if (buffer) {
            records = await this.BLClient.query(`SELECT seq as seqNo FROM ${config.dbBacklogBuffer} ORDER BY seq ASC LIMIT 1`);
          } else {
            records = await this.BLClient.query(`SELECT seq as seqNo FROM ${config.dbBacklogCollection} ORDER BY seq ASC LIMIT 1`);
          }
          if (records.length) return records[0].seqNo;
        }
      } catch (e) {
        log.error(e);
      }
    }
    return 0;
  }

  /**
  * [getNumberOfUpdates]
  * @return {int}
  */
  static async getNumberOfUpdates(buffer = false) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    } else {
      try {
        if (config.dbType === 'mysql') {
          let records = [];
          if (buffer) {
            records = await this.BLClient.query(`SELECT COUNT(*) as count FROM ${config.dbBacklogBuffer} WHERE query LIKE 'update%' OR query LIKE 'set%'`);
          } else {
            records = await this.BLClient.query(`SELECT COUNT(*) as count FROM ${config.dbBacklogCollection} WHERE query LIKE 'update%' OR query LIKE 'set%'`);
          }
          if (records.length) return records[0].count;
        }
      } catch (e) {
        log.error(e);
      }
    }
    return 0;
  }

  /**
  * [shiftBacklogSeqNo]
  * @return {int}
  */
  static async shiftBacklogSeqNo(shiftSize = 0) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    } else {
      try {
        if (config.dbType === 'mysql') {
          if (typeof shiftSize === 'number') await this.BLClient.query(`UPDATE ${config.dbBacklogCollection} set seq = seq + ${shiftSize}`);
          this.sequenceNumber = await this.getLastSequenceNumber();
          log.info(`shifted backlog, current sequenceNumber: ${this.sequenceNumber}`);
        }
      } catch (e) {
        log.error(e);
        return false;
      }
    }
    return true;
  }

  /**
  * [keepConnections]
  */
  static async keepConnections() {
    if (config.dbType === 'mysql' && this.BLClient) {
      await this.BLClient.setDB(config.dbBacklog);
      await this.UserDBClient.setDB(config.dbInitDB);
    }
  }

  /**
  * [clearLogs]
  */
  static async clearLogs(seqNo = 0) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        if (seqNo !== 0) {
          await this.BLClient.execute(`DELETE FROM ${config.dbBacklogCollection} where seq<=?`, [seqNo]);
        } else {
          await this.BLClient.query(`DELETE FROM ${config.dbBacklogCollection}`);
          this.sequenceNumber = 0;
        }
      }
    } catch (e) {
      log.error(e);
    }
    log.info('backlog data removed successfully.');
  }

  /**
  * [rebuildDatabase]
  */
  static async rebuildDatabase(seqNo) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DROP DATABASE ${config.dbInitDB}`);
        await this.BLClient.createDB(config.dbInitDB);
        this.UserDBClient.setDB(config.dbInitDB);
        await this.BLClient.setDB(config.dbBacklog);
        const records = await this.BLClient.execute('SELECT * FROM backlog WHERE seq<=? ORDER BY seq', [seqNo]);
        // console.log(records);
        for (const record of records) {
          log.info(`executing seq(${record.seq})`);
          try {
            // eslint-disable-next-line no-await-in-loop, no-unused-vars
            const result = await this.UserDBClient.query(record.query);
          } catch (e) {
            log.error(e);
          }
          // eslint-disable-next-line no-await-in-loop
        }
        await this.BLClient.execute('DELETE FROM backlog WHERE seq>?', [seqNo]);
        await this.clearBuffer();
      }
    } catch (e) {
      log.error(e);
    }
    this.buffer = [];
    log.info(`DB and backlog rolled back to ${seqNo}`);
  }

  /**
  * [destroyBacklog]
  */
  static async destroyBacklog() {
    if (!this.BLClient) this.BLClient = await dbClient.createClient();
    try {
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DROP DATABASE ${config.dbBacklog}`);
        this.sequenceNumber = 0;
      }
    } catch (e) {
      log.error(e);
    }
    log.info(`${config.dbBacklog} database and all it's data erased successfully.`);
  }

  /**
  * [clearBacklog]
  */
  static async clearBacklog() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DELETE FROM ${config.dbBacklogCollection}`);
        await this.BLClient.query(`DROP TABLE ${config.dbBacklogCollection}`);
        await timer.setTimeout(100);
        await this.BLClient.query(`CREATE TABLE ${config.dbBacklogCollection} (seq bigint, query longtext, timestamp bigint) ENGINE=InnoDB;`);
        await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogCollection}\`
          MODIFY COLUMN \`seq\` bigint(0) UNSIGNED NOT NULL FIRST,
          ADD PRIMARY KEY (\`seq\`),
          ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
        this.sequenceNumber = 0;
      }
    } catch (e) {
      log.error(e);
    }
    this.buffer = [];
    log.info('Backlog table recreated successfully.');
  }

  /**
  * [clearBuffer]
  */
  static async clearBuffer() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DELETE FROM ${config.dbBacklogBuffer}`);
        this.bufferSequenceNumber = 0;
        this.bufferStartSequenceNumber = 0;
      }
    } catch (e) {
      log.error(e);
    }
    this.buffer = [];
    log.info('All buffer data removed successfully.');
  }

  static async pushToBacklog(query, seq = false, timestamp = false) {
    // eslint-disable-next-line no-param-reassign
    if (timestamp === false) timestamp = Date.now();
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        if (this.sequenceNumber === undefined) this.sequenceNumber = 0;
        // log.info(`executing ${this.sequenceNumber}`);
        this.sequenceNumber += 1;
        const seqForThis = this.sequenceNumber;
        const BLResult = await this.BLClient.execute(
          `INSERT INTO ${config.dbBacklogCollection} (seq, query, timestamp) VALUES (?,?,?)`,
          [seqForThis, query, timestamp],
        );
        return [BLResult, seqForThis, timestamp];
      }
    } catch (e) {
      log.error(`error executing query, ${query}, ${seq}`);
    }
    return [];
  }

  /**
  * [moveBufferToBacklog]
  */
  static async moveBufferToBacklog() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }

    if (config.dbType === 'mysql') {
      const records = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogBuffer} ORDER BY seq`);
      for (const record of records) {
        try {
          if (record.seq === this.sequenceNumber + 1) {
            log.info(`copying seq(${record.seq}) from buffer`);
            // eslint-disable-next-line no-await-in-loop
            await this.pushQuery(record.query, record.seq, record.timestamp);
          }
        } catch (e) {
          log.error(e);
        }
        // eslint-disable-next-line no-await-in-loop
        await this.BLClient.execute(`DELETE FROM ${config.dbBacklogBuffer} WHERE seq=?`, [record.seq]);
      }
      const records2 = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogBuffer} ORDER BY seq`);
      if (records2.length > 0) {
        this.bufferStartSequenceNumber = records2[0].seq;
        if (records2.length > 20) await this.moveBufferToBacklog();
      } else {
        this.bufferStartSequenceNumber = 0;
      }
    }
    // this.clearBuffer();
    log.info('All buffer data moved to backlog successfully.');
  }

  /**
  * [pushKey]
  */
  static async pushKey(key, value, encrypt = true) {
    const encryptedValue = (encrypt) ? Security.encrypt(value) : value;
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const record = await this.BLClient.execute(`SELECT * FROM ${config.dbOptions} WHERE k=?`, [key]);
        if (record.length) {
          await this.BLClient.execute(`UPDATE ${config.dbOptions} SET value=? WHERE k=?`, [encryptedValue, key]);
        } else {
          await this.BLClient.execute(`INSERT INTO ${config.dbOptions} (k, value) VALUES (?,?)`, [key, encryptedValue]);
        }
      }
    } catch (e) {
      log.error(e);
    }
    this.buffer = [];
    // log.info('Key pushed.');
  }

  /**
  * [getKey]
  */
  static async getKey(key, decrypt = true) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const records = await this.BLClient.execute(`SELECT * FROM ${config.dbOptions} WHERE k=?`, [key]);
        if (records.length) {
          return (decrypt) ? Security.encryptComm(Security.decrypt(records[0].value)) : records[0].value;
        }
      }
    } catch (e) {
      log.error(e);
    }
    return null;
  }

  /**
  * [removeKey]
  */
  static async removeKey(key) {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const records = await this.BLClient.execute(`DELETE FROM ${config.dbOptions} WHERE k=?`, [key]);
        if (records.length) {
          return true;
        }
      }
    } catch (e) {
      log.error(e);
    }
    return false;
  }

  /**
  * [getAllKeys]
  */
  static async getAllKeys() {
    const keys = {};
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        const records = await this.BLClient.execute(`SELECT * FROM ${config.dbOptions}`);
        for (const record of records) {
          keys[record.k] = Security.encryptComm(Security.decrypt(record.value));
        }
      }
    } catch (e) {
      log.error(e);
    }
    return keys;
  }

  /**
  * [dumpBackup]
  */
  static async dumpBackup(filename = null) {
    const timestamp = new Date().getTime();
    // eslint-disable-next-line no-param-reassign
    if (!filename) filename = `B_${timestamp}`;
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (this.BLClient && config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    log.info(`creating backup file :${filename}.sql, SeqNo: ${this.sequenceNumber}`);
    if (this.BLClient) {
      const startTime = Date.now(); // Record the start time
      await mysqldump({
        connection: {
          host: config.dbHost,
          port: config.dbPort,
          user: config.dbUser,
          password: Security.getKey(),
          database: config.dbInitDB,
        },
        dump: {
          schema: {
            table: {
              dropIfExist: true,
            },
          },
          data: {
            verbose: false,
          },
        },
        dumpToFile: `./dumps/${filename}.sql`,
      });
      const endTime = Date.now(); // Record the end time
      log.info(`Backup file created in (${endTime - startTime} ms): ${filename}.sql`);
      return (filename);
    } else {
      log.info('Can not connect to the DB');
      return (null);
    }
  }

  /**
  * [listSqlFiles]
  */
  static async listSqlFiles() {
    const folderPath = './dumps/';
    try {
      const files = fs.readdirSync(folderPath);

      const sqlFilesInfo = files.map((file) => {
        const filePath = path.join(folderPath, file);
        const fileStats = fs.statSync(filePath);
        const isSqlFile = path.extname(file) === '.sql';

        if (isSqlFile) {
          return {
            fileName: file,
            fileSize: fileStats.size, // in bytes
            createdDateTime: fileStats.birthtime, // creation date/time
          };
        } else {
          return null; // Ignore non-SQL files
        }
      });

      // Filter out null entries (non-SQL files) and return the result
      return sqlFilesInfo.filter((info) => info !== null);
    } catch (error) {
      log.error(`Error reading folder: ${error}`);
      return [];
    }
  }

  /**
  * [deleteBackupFile]
  */
  static async deleteBackupFile(fileName, withExtention = false) {
    try {
      if (withExtention) {
        fs.unlinkSync(`./dumps/${fileName}`);
      } else {
        fs.unlinkSync(`./dumps/${fileName}.sql`);
      }
      log.info(`File "${fileName}" has been deleted.`);
    } catch (error) {
      log.error(`Error deleting file "${fileName}": ${error.message}`);
    }
  }

  static async testDB() {
    try {
      const dbList = await this.BLClient.query(`SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${config.dbBacklog}'`);
      if (dbList.length === 0) {
        log.error('DB test failed', 'red');
        return false;
      } else {
        log.info('DB test passes', 'green');
        return true;
      }
    } catch (error) {
      log.error('DB test failed', 'red');
      return false;
    }
  }

  static async adjustBeaconFile(object) {
    try {
      fs.writeFileSync('./dumps/beacon.json', JSON.stringify(object, null, 2));
    } catch (error) {
      console.error('Error writing to file:', error);
    }
  }

  static async readBeaconFile() {
    try {
      if (fs.existsSync('./dumps/beacon.json')) {
        const fileContent = fs.readFileSync('./dumps/beacon.json', 'utf8');
        const parsedContent = JSON.parse(fileContent);
        return parsedContent;
      }
      return null;
    } catch (error) {
      console.error('Error reading to file:', error);
      return null;
    }
  }

  /**
  * [purgeBinLogs]
  */
  static async purgeBinLogs() {
    try {
      if (config.dbType === 'mysql') {
        log.info('PURGING BINLOGS', 'cyan');
        await this.UserDBClient.query('FLUSH LOGS');
        await this.UserDBClient.query("PURGE BINARY LOGS BEFORE '2036-04-03'");
      }
    } catch (e) {
      log.error(e);
    }
  }
}// end class

// eslint-disable-next-line func-names
module.exports = BackLog;
