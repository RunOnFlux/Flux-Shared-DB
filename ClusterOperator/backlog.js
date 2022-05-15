/* eslint-disable no-empty-function */
/* eslint-disable no-unused-vars */
const dbClient = require('./dbClient');
const config = require('./config');
const log = require('../lib/log');

class BackLog {

  static buffer = [];
  static sequenceNumber = 0;
  static BufferSequenceNumber = 0;
  static BLClient = null;
  /**
  * [createBacklog]
  * @param {object} params [description]
  */
  static async createBacklog(params) {
    this.BLClient = await dbClient.createClient();
    if (config.dbType === 'mysql') {
        let dbList = await this.BLClient.query(`SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${config.dbBacklog}'`);
        if(dbList.length === 0){
          log.info('backlog DB not defined yet, creating backlog DB');
          await this.BLClient.query(`CREATE DATABASE ${config.dbBacklog}`);
        }else{
          log.info('backlog DB exists');
        }
        await this.BLClient.setDB(config.dbBacklog);
        let tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
        WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogCollection}'`);
        if(tableList.length === 0){
          log.info('backlog table not defined yet, creating backlog table');
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogCollection} (sec bigint, query text, timestamp bigint);`);
          await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogCollection}\`
          MODIFY COLUMN \`sec\` bigint(0) NOT NULL FIRST,
          ADD PRIMARY KEY (\`sec\`),
          ADD UNIQUE INDEX \`sec\`(\`sec\`);`);
        }else{
          log.info('backlog table exists');
          this.sequenceNumber = await this.getLastSequenceNumber();
        }
        tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
        WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogBuffer}'`);
        if(tableList.length === 0){
          log.info('backlog buffer table not defined yet, creating buffer table');
          await this.BLClient.query(`CREATE TABLE ${config.dbBacklogBuffer} (sec bigint, query text, timestamp bigint);`);
          await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogBuffer}\` 
          MODIFY COLUMN \`sec\` bigint(0) NOT NULL FIRST,
          ADD PRIMARY KEY (\`sec\`),
          ADD UNIQUE INDEX \`sec\`(\`sec\`);`);
        }else{
          log.info('backlog buffer table exists');
        }
    }
  }

  /**
  * [pushQuery]
  * @param {string} query [description]
  * @param {int} timestamp [description]
  */
  static async pushQuery(query, timestamp) {
    try{
      if (config.dbType === 'mysql') {
        this.sequenceNumber +=1;
        await this.BLClient.query(`INSERT INTO ${config.dbBacklogCollection} (sec, query, timestamp) VALUES (${this.sequenceNumber},'${query}',${timestamp});`);
        
      }
    }catch(e){
      log.error(e);
    }
  }

  /**
  * [getLogs]
  * @param {int} startFrom [description]
  * @param {int} pageSize [description]
  */
  static async getLogs(startFrom, pageSize) {
    if (config.dbType === 'mysql') {
      const totalRecords = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogCollection} LIMIT ${startFrom},${pageSize} `);
      return totalRecords
    }
  }

  /**
  * [getTotalLogsCount]
  */
  static async getTotalLogsCount() {
    if (config.dbType === 'mysql') {
      const totalRecords = await this.BLClient.query(`SELECT count(*) as total FROM ${config.dbBacklogCollection}`);
      log.info(`Total Records: ${JSON.stringify(totalRecords)}`);
      return totalRecords[0].total
    }
  }

  /**
  * [getLastSequenceNumber]
  */
  static async getLastSequenceNumber() {
    if (config.dbType === 'mysql') {
      const totalRecords = await this.BLClient.query(`SELECT count(*) as total FROM ${config.dbBacklogCollection}`);
      log.info(`Last Sec No: ${JSON.stringify(totalRecords)}`);
      return totalRecords[0].total
    }
  }

  /**
  * [clearLogs]
  */
  static async clearLogs() {
    if (config.dbType === 'mysql') {
      await this.BLClient.query(`DELETE * FROM ${config.dbBacklogCollection}`);
    }
  }
  /**
  * [clearBuffer]
  */
   static async clearBuffer() {
    if (config.dbType === 'mysql') {
      await this.BLClient.query(`DELETE * FROM ${config.dbBacklogBuffer}`);
    }
    this.buffer = [];
  }
}

// eslint-disable-next-line func-names
module.exports = {
  BackLog,
};
