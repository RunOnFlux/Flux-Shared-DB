/* eslint-disable no-await-in-loop */
/* eslint-disable no-unused-vars */
const timer = require('timers/promises');
const BackLog = require('./Backlog');
const dbClient = require('./DBClient');
const log = require('../lib/log');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const net = require('net');
const mySQLServer = require('../lib/mysqlServer');
const mySQLConsts = require('../lib/mysqlConstants');
const md5 = require('md5');

class Operator {

  static localDB = null;
  static OpNodes = [];
  static clientNodes = [];
  static nodeInstances = 0;
  static masterNode = null;
  static IamMaster = false;
  static apiKey = null;
  static myIP = null;
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
  static initInBoundConnections(serverType) {
    if(serverType==='mysql'){
      //init mysql port
      net.createServer((so) => {
        const server = mySQLServer.createServer({
          socket: so,
          onAuthorize: this.handleAuthorize,
          onCommand: this.handleCommand,
          localDB: this.localDB,
        });
      }).listen(config.externalDBPort);
      
      log.info(`Started mysql server on port ${config.externalDBPort}`);
    }
  }

  static handleAuthorize(param) {
    log.info('Auth Info:');
    log.info(param);
    // Yup you are authorized
    return true;
  }
  
  static async handleCommand({ command, extra }) {
    // command is a numeric ID, extra is a Buffer
    switch (command) {
      case mySQLConsts.COM_QUERY:
        const query = extra.toString(); 
        console.log(`Got Query: ${query}`);
        //forward the query to the server
        var result = await this.localDB.query(query,true);
        console.log(result);
        // Then send it back to the user in table format
        if(result[1]){
          let fieldNames = [];
          for (let definition of result[1]) fieldNames.push(definition.name);
          this.sendDefinitions(result[1]);
          let finalResult = [];
          for (let row of result[0]){
            let newRow =[];
            for(let filed of fieldNames){
              newRow.push(row[filed]);
            }
            finalResult.push(newRow);
          }

          this.sendRows(finalResult);
        } else if(result[0]){
          this.sendOK({ message: 'OK' });
        }else{
          this.sendError({ message: result[3] });
        }
        
        break;
      case mySQLConsts.COM_PING:
        this.sendOK({ message: 'OK' });
        break;
      case null:
      case undefined:
      case mySQLConsts.COM_QUIT:
        log.info('Disconnecting');
        this.end();
        break;
      case mySQLConsts.COM_INIT_DB:
        var result = await this.localDB.query(`use ${extra}`);
        log.info(`extra is ${extra}`)
        this.sendOK({ message: 'OK' });
        break;
      default:
        log.info(`Unknown Command: ${command}`);
        this.sendError({ message: 'Unknown Command' });
        break;
    }
  }

  /**
  * [syncLocalDB]
  */
  static async syncLocalDB() {}

  /**
  * [getSyncStatus]
  */
  static async getSyncStatus() {}

  /**
  * [findMaster]
  */
  static async findMaster() {
    //get dbappspecs
    if(config.DBAppName){
      const Specifications = await fluxAPI.getApplicationSpecs(config.DBAppName);
      this.nodeInstances = Specifications.instances;
      // wait for all nodes to spawn
      let ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      while (ipList.length < this.nodeInstances) {
        log.info(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
        await timer.setTimeout(2000);
        ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      }
      this.OpNodes = [];
      for(let i=0; i<this.nodeInstances; i++){
        this.OpNodes.push({ip:ipList[i].ip, hash:md5(ipList[i].ip)});
      }
      await getMyIp();
      this.OpNodes.sort((a, b) => (a.hash > b.hash) ? 1 : -1);
      
      if(this.myIP === this.OpNodes[0]){
        //I could be the master, ask second candidate for confirmation.
        let MasterIP = await fluxAPI.getMaster(this.OpNodes[1],config.apiPort);
        //try next node if not responding
        let tries = 0;
        while(MasterIP === "null") {
          MasterIP = await fluxAPI.getMaster(this.OpNodes[2],config.apiPort);
          await timer.setTimeout(2000);
          tries ++;
          log.info(`Node ${this.OpNodes[2]} not responding.`);
          if(tries>5) return this.findMaster();
        }
        if(MasterIP === this.myIP) {
          //I am the master node
          this.masterNode = MasterIP;
          this.IamMaster = true;
        }
      }else{
        //ask first node who the master is
        let MasterIP = await fluxAPI.getMaster(this.OpNodes[0],config.apiPort);
        let tries = 0;
        while(MasterIP === "null") {
          await timer.setTimeout(2000);
          MasterIP = await fluxAPI.getMaster(this.OpNodes[0],config.apiPort);
          tries ++;
          log.info(`Node ${this.OpNodes[0]} not responding.`);
          if(tries>5) return this.findMaster();
        }
        this.masterNode = MasterIP;
      }
      log.info(`Master node is ${this.masterNode}`);
      
    }else{
      log.info(`DB_APPNAME environment variabele is not defined.`)
    }
  }
  /**
  * [getMaster]
  */
  static getMaster() {
    if(this.masterNode === null){
      if(this.OpNodes.length > 2){
        return this.OpNodes[0];
      }
    }else{
      return this.masterNode;
    }
    return null;
  }

  /**
  * [getMyIp]
  */
    static async getMyIp() {
      if(this.myIP === null){
        return this.myIP
      }else{
        let ipList = [];
        for(let i=0; i < this.OpNodes.length || i < 3; i++){
          ipList.push(await fluxAPI.getMyIp(this.OpNodes[i]));
        }
        //find the highest occurrence in the array 
        const myIP = ipList.sort((a,b) =>ipList.filter(v => v===a).length - ipList.filter(v => v===b).length).pop();
        this.myIP = myIP;
        return myIP;
      }
    }

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
    this.initInBoundConnections(config.dbType);
  }
}
module.exports = Operator;
