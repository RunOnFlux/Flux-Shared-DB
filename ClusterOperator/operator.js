/* eslint-disable no-await-in-loop */
/* eslint-disable no-unused-vars */
const timer = require('timers/promises');
const BackLog = require('./backlog');
const dbClient = require('./dbClient');
const log = require('../lib/log');
const config = require('./config');

class Operator {

  static localDB = null;
  static dbNodes = [];
  static clientNodes = [];
  /**
  * [initLocalDB]
  */
  static async initLocalDB() {
    await BackLog.createBacklog();
    if(config.dbInitDB){ 
      await this.localDB.createDB(config.dbInitDB);
      log.info(`${config.dbInitDB} database created on local DB.`);
    }
  }

  /**
  * [initInBoundConnections]
  * @param {string} serverType [description]
  */
  static async initInBoundConnections(serverType) {}

  /**
  * [syncLocalDB]
  */
  static async syncLocalDB() {}

  /**
  * [getSyncStatus]
  */
  static async getSyncStatus() {}

  /**
  * [getMaster]
  */
  static async findMaster() {}

  /**
  * [ConnectLocalDB]
  */
  static async ConnectLocalDB() {
    // wait for local db to boot up
    this.localDB = await dbClient.createClient();
    while (this.localDB === null) {
      log.info('Waiting for local DB to boot up...');
      await timer.setTimeout(2000);
      this.localDB = await dbClient.createClient();
    }
    log.info('Connected to local DB.');
  }

  /**
  * [init]
  */
  static async init() {
    await this.ConnectLocalDB();
    await this.initLocalDB();
  }
}
module.exports = Operator;
