/* eslint-disable no-empty-function */
/* eslint-disable no-unused-vars */
const dbClient = require('./DBClient');
const config = require('./config');
const log = require('../lib/log');

class BackLog {

  static buffer = [];
  static sequenceNumber = 0;
  static bufferSequenceNumber = 0;
  static BLClient = null;
  static UserDBClient = null;
  /**
  * [createBacklog]
  * @param {object} params [description]
  */
  static async createBacklog(params) {
    this.BLClient = await dbClient.createClient();
    this.UserDBClient = await dbClient.createClient();
    try{
      if (config.dbType === 'mysql') {
          let dbList = await this.BLClient.query(`SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${config.dbBacklog}'`);
          if(dbList.length === 0){
            log.info('Backlog DB not defined yet, creating backlog DB...');
            await this.BLClient.createDB(config.dbBacklog);
          }else{
            log.info('Backlog DB already exists, moving on...');
          }
          await this.BLClient.setDB(config.dbBacklog);
          await this.UserDBClient.setDB(config.dbInitDB);
          let tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
          WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogCollection}'`);
          if(tableList.length === 0){
            log.info('Backlog table not defined yet, creating backlog table...');
            await this.BLClient.query(`CREATE TABLE ${config.dbBacklogCollection} (seq bigint, query text, timestamp bigint) ENGINE=MyISAM;`);
            await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogCollection}\`
            MODIFY COLUMN \`seq\` bigint(0) NOT NULL FIRST,
            ADD PRIMARY KEY (\`seq\`),
            ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
          }else{
            log.info('Backlog table already exists, moving on...');
            this.sequenceNumber = await this.getLastSequenceNumber();
          }
          tableList = await this.BLClient.query(`SELECT * FROM INFORMATION_SCHEMA.tables 
          WHERE table_schema = '${config.dbBacklog}' and table_name = '${config.dbBacklogBuffer}'`);
          if(tableList.length === 0){
            log.info('Backlog buffer table not defined yet, creating buffer table...');
            await this.BLClient.query(`CREATE TABLE ${config.dbBacklogBuffer} (seq bigint, query text, timestamp bigint) ENGINE=MyISAM;`);
            await this.BLClient.query(`ALTER TABLE \`${config.dbBacklog}\`.\`${config.dbBacklogBuffer}\` 
            MODIFY COLUMN \`seq\` bigint(0) NOT NULL FIRST,
            ADD PRIMARY KEY (\`seq\`),
            ADD UNIQUE INDEX \`seq\`(\`seq\`);`);
          }else{
            log.info('Backlog buffer table already exists, moving on...');
          }
          log.info(`Last Seq No: ${this.sequenceNumber}`);
      }
    }catch(e){
      log.error(`Error creating backlog: ${e}`);
    }
  }

  /**
  * [pushQuery]
  * @param {string} query [description]
  * @param {int} timestamp [description]
  */
  static async pushQuery(query, timestamp) {
    if(!this.BLClient) {
      log.error(`Backlog not created yet. Call createBacklog() first.`)
      return;
    }
    try{
      if (config.dbType === 'mysql') {
        this.sequenceNumber +=1;
        await this.BLClient.query(`INSERT INTO ${config.dbBacklogCollection} (seq, query, timestamp) VALUES (${this.sequenceNumber},'${query}',${timestamp});`);
        const result = await UserDBClient.query(query);
        return result;
        
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
    if(!this.BLClient) {
      log.error(`Backlog not created yet. Call createBacklog() first.`)
      return;
    }
    try{
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.query(`SELECT * FROM ${config.dbBacklogCollection} LIMIT ${startFrom},${pageSize} `);
        return totalRecords
      }
    }catch(e){
      log.error(e);
    }
  }

  /**
  * [getTotalLogsCount]
  */
  static async getTotalLogsCount() {
    if(!this.BLClient) {
      log.error(`Backlog not created yet. Call createBacklog() first.`)
      return;
    }
    try{
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.query(`SELECT count(*) as total FROM ${config.dbBacklogCollection}`);
        log.info(`Total Records: ${JSON.stringify(totalRecords)}`);
        return totalRecords[0].total
    }
    }catch(e){
      log.error(e);
    }
  }

  /**
  * [getLastSequenceNumber]
  */
  static async getLastSequenceNumber() {
    if(!this.BLClient) {
      log.error(`Backlog not created yet. Call createBacklog() first.`)
      return;
    }
    try{
      if (config.dbType === 'mysql') {
        const totalRecords = await this.BLClient.query(`SELECT seq as total FROM ${config.dbBacklogCollection} ORDER BY seq DESC LIMIT 1`);
        if(totalRecords.length)
          return totalRecords[0].total;
        else 
          return 0;
      }
    }catch(e){
      log.error(e);
    }
  }

  /**
  * [clearLogs]
  */
  static async clearLogs() {
    if(!this.BLClient){ 
      this.BLClient = await dbClient.createClient();
      if (config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try{
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DELETE FROM ${config.dbBacklogCollection}`);
      }
    }catch(e){
      log.error(e);
    }
    log.info(`All backlog data removed successfully.`);
  }
   /**
  * [destroyBacklog]
  */
    static async destroyBacklog() {
      if(!this.BLClient) this.BLClient = await dbClient.createClient();
      try{
        if (config.dbType === 'mysql') {
          await this.BLClient.query(`DROP DATABASE ${config.dbBacklog}`);
        }
      }catch(e){
        log.error(e);
      }
      log.info(`${config.dbBacklog} database and all it's data erased successfully.`);
    }
  /**
  * [clearBuffer]
  */
   static async clearBuffer() {
    if(!this.BLClient){ 
      this.BLClient = await dbClient.createClient();
      if (config.dbType === 'mysql') await this.BLClient.setDB(config.dbBacklog);
    }
    try{
      if (config.dbType === 'mysql') {
        await this.BLClient.query(`DELETE FROM ${config.dbBacklogBuffer}`);
      }
    }catch(e){
      log.error(e);
    }
    this.buffer = [];
    log.info(`All buffer data removed successfully.`);
  }
}

// eslint-disable-next-line func-names
module.exports = BackLog;
