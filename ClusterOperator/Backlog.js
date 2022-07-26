/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */

const dbClient = require('./DBClient');
const config = require('./config');
const log = require('../lib/log');
const ConnectionPool = require('../lib/ConnectionPool');

class BackLog {
  static buffer = [];

  static sequenceNumber = 0;

  static bufferSequenceNumber = 0;

  static bufferStartSequenceNumber = 0;

  static BLClient = null;

  static UserDBClient = null;

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
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogCollection} (seq bigint, query longtext, timestamp bigint) ENGINE=MyISAM;`);
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
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogBuffer} (seq bigint, query longtext, timestamp bigint) ENGINE=MyISAM;`);
          await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogBuffer}\` 
            MODIFY COLUMN \`seq\` bigint(0) UNSIGNED NOT NULL FIRST,
            ADD PRIMARY KEY (\`seq\`),
            ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
        } else {
          log.info('Backlog buffer table already exists, moving on...');
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
  static async pushQuery(query, seq = 0, timestamp, buffer = false, connId = false) {
    // eslint-disable-next-line no-param-reassign
    if (timestamp === undefined) timestamp = Date.now();
    if (!this.BLClient) {
      log.error('Backlog not created yet. Call createBacklog() first.');
      return [];
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
        } else if (seq === 0 || this.sequenceNumber + 1 === seq) {
          if (seq === 0) { this.sequenceNumber += 1; } else { this.sequenceNumber = seq; }
          const seqForThis = this.sequenceNumber;
          let result2 = null;
          if (connId === false) {
            result2 = await this.UserDBClient.query(query);
          } else {
            result2 = await ConnectionPool.getConnectionById(connId).query(query);
          }
          await this.BLClient.execute(
            `INSERT INTO ${config.dbBacklogCollection} (seq, query, timestamp) VALUES (?,?,?)`,
            [seqForThis, query, timestamp],
          );
          return [result2, seqForThis, timestamp];
        } else {
          log.error(`Wrong query order skipping pushQuery. ${this.sequenceNumber} + 1 <> ${seq}`);
          return [];
        }
      }
    } catch (e) {
      log.error('error executing query');
      log.error(e);
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
      log.error('Backlog not created yet. Call createBacklog() first.');
      return [];
    }
    try {
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogCollection} LIMIT ${startFrom},${pageSize}`);
        // log.info(`backlog records ${startFrom},${pageSize}:${JSON.stringify(totalRecords)}`);
        return totalRecords;
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
      log.error('Backlog not created yet. Call createBacklog() first.');
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
      log.error('Backlog not created yet. Call createBacklog() first.');
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
  * [clearLogs]
  */
  static async clearLogs() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try {
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DELETE FROM ${config.dbBacklogCollection}`);
        this.sequenceNumber = 0;
      }
    } catch (e) {
      log.error(e);
    }
    log.info('All backlog data removed successfully.');
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
  * [clearBuffer]
  */
  static async clearBuffer() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
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

  /**
  * [moveBufferToBacklog]
  */
  static async moveBufferToBacklog() {
    if (!this.BLClient) {
      this.BLClient = await dbClient.createClient();
      if (config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }

    if (config.dbType === 'mysql') {
      const records = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogBuffer} ORDER BY seq`);
      for (const record of records) {
        log.info(`copying seq(${record.seq}) from buffer`);
        try {
          // eslint-disable-next-line no-await-in-loop
          await this.pushQuery(record.query, record.seq, record.timestamp);
        } catch (e) {
          log.error(e);
        }
        // eslint-disable-next-line no-await-in-loop
        await this.BLClient.execute(`DELETE FROM ${config.dbBacklogBuffer} WHERE seq=?`, [record.seq]);
      }
    }
    this.clearBuffer();
    log.info('All buffer data moved to backlog successfully.');
  }
}

// eslint-disable-next-line func-names
module.exports = BackLog;
