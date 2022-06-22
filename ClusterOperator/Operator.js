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
    const remoteIp = param.remoteIP;
    const whiteList = config.whiteListedIps.split(',');
    if(whiteList.length && whiteList.includes(remoteIp) || remoteIp.startsWith('80.239.140.')){
        return true;  
    }
    return false; 
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
  * [getSyncStatus]
  */
   static async updateAppInfo(retry=true) {
    const Specifications = await fluxAPI.getApplicationSpecs(config.DBAppName);
    this.nodeInstances = Specifications.instances;
    // wait for all nodes to spawn
    let ipList = await fluxAPI.getApplicationIP(config.DBAppName);
    while (ipList.length < this.nodeInstances) {
      log.info(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
      await timer.setTimeout(10000);
      ipList = await fluxAPI.getApplicationIP(config.DBAppName);
    }
    this.OpNodes = [];
    var myIPList = [];
    for(let i=0; i<ipList.length; i++){
      //extraxt ip from upnp nodes
      if(ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
      var myIp = await fluxAPI.getMyIp(ipList[i].ip, config.containerApiPort);
      myIPList.push(myIp);
      this.OpNodes.push({ip:ipList[i].ip, active:myIp});
    }
    
    
    const myIP = myIPList.sort((a,b) =>myIPList.filter(v => v===a).length - myIPList.filter(v => v===b).length).pop();
    for(let i=0; i<this.OpNodes.length; i++){
      if(this.OpNodes[i].active===myIP || this.OpNodes[i].ip===myIP) 
      this.OpNodes[i].active = true; 
      else 
      this.OpNodes[i].active = false;
    }
    log.info(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);
    if(myIP!==null){
      this.myIP = myIP;
      log.info(`My ip is ${myIP}`);
    }else{
      if(retry){
        log.info(`other nodes are not responding to api port ${config.containerApiPort}, retriying again ...`);
        await timer.setTimeout(15000);
        await this.updateAppInfo();
      }
    }
  
   }

  /**
  * [findMaster]
  */
  static async findMaster() {
    //get dbappspecs
    if(config.DBAppName){
      await this.updateAppInfo();
      //await this.getMyIp();
      //find master candidate
      var masterCandidates=[];
      for(i=0; i<this.OpNodes.length; i++){
        if(this.OpNodes[i].active) masterCandidates.push(this.OpNodes[i].ip);
      }
      //if first candidate is me i'm the master
      if(masterCandidates[0]===this.myIP){
        this.IamMaster = true;
        this.masterNode = this.myIP;
      }else{
        //ask first candidate who the master is
        log.info(`asking master from ${masterCandidates[0]}`);
        let MasterIP = await fluxAPI.getMaster(masterCandidates[0],config.containerApiPort);
        log.info(`response from ${masterCandidates[0]} was ${MasterIP}`);
        if(MasterIP !== null && MasterIP === "null"){
          log.info(`asking master for confirmation @ ${MasterIP}:${config.containerApiPort}`);
          let MasterIP2 = await fluxAPI.getMaster(MasterIP,config.containerApiPort);
          log.info(`response from ${MasterIP} was ${MasterIP2}`);
          if(MasterIP2===MasterIP){
            this.masterNode = MasterIP;
          }else{
            log.info(`master node not matching, retrying...`);
            return this.findMaster();
          }
        }
      }
      log.info(`Master node is ${this.masterNode}`);
      return this.masterNode;
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
        return this.OpNodes[0].ip;
      }
    }else{
      return this.masterNode;
    }
    return null;
  }

  /**
  * [getMyIp]
  */
  static async getMyIp(retries=1) {
    if(this.myIP !== null){
      return this.myIP
    }else{
      let ipList = [];
      for(let i=0; i < this.OpNodes.length && i < 5; i++){
        log.info(`asking myip from ${this.OpNodes[i].ip}`);
        let tempIp = await fluxAPI.getMyIp(this.OpNodes[i].ip, config.containerApiPort);
        log.info(`response from ${this.OpNodes[i].ip} was ${tempIp}`);
        /*let j=1;
        while(tempIp===null && j < 6){
          log.info(`node ${this.OpNodes[i].ip} not responding to api port ${config.containerApiPort}, retrying ${j}/5...`);
          await timer.setTimeout(5000);
          tempIp = await fluxAPI.getMyIp(this.OpNodes[i].ip, config.containerApiPort);
          j++;
        }*/
        if(tempIp!==null){
          this.myIP = tempIp;
          log.info(`My ip is ${JSON.stringify(tempIp)}`);
          return tempIp;
        }
      }
      log.info(`other nodes are not responding to api port ${config.containerApiPort}, retriying again ${retries}...`);
      await this.updateAppInfo();
      await timer.setTimeout(15000 * retries);
      return this.getMyIp(retries+1);
      /*log.info(`all response list: ${JSON.stringify(ipList)}`);
      //find the highest occurrence in the array 
      if(ipList.length>=2){
        const myIP = ipList.sort((a,b) =>ipList.filter(v => v===a).length - ipList.filter(v => v===b).length).pop();
        this.myIP = myIP;
        log.info(`My ip is ${JSON.stringify(myIP)}`);
        return myIP;
      }else{
        log.info(`other nodes are not responding to api port ${config.containerApiPort}, retriying again ${retries}...`);
        await this.updateAppInfo();
        await timer.setTimeout(15000 * retries);
        return this.getMyIp(retries+1);
      }*/
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
