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
const WebSocket = require('ws');
const { io } = require("socket.io-client");
const md5 = require('md5');
const sqlAnalyzer = require('../lib/sqlAnalyzer');

class Operator {

  static localDB = null;
  static OpNodes = [];
  static clientNodes = [];
  static nodeInstances = 0;
  static masterNode = null;
  static IamMaster = false;
  static apiKey = null;
  static myIP = null;
  static MasterWSConn = null;
  static status = 'initializing';
  static serverSocket;

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
  * [initMasterConnection]
  */
  static initMasterConnection() {
    log.info(`establishing persistent connection to master node...${this.masterNode},${this.IamMaster}`);
    if(this.masterNode && !this.IamMaster){ 
      
      try {
        this.MasterWSConn = io.connect(`http://${this.masterNode}:${config.containerApiPort}`,{
          reconnection: false
        } );
        this.MasterWSConn.on("connect", (socket) => {
          const engine = this.MasterWSConn.io.engine;
          log.info(`connected to master...`);
          this.syncLocalDB();
          engine.once("upgrade", () => {
            log.info(`transport protocol: ${engine.transport.name}`); // in most cases, prints "websocket"
          });
        });
        this.MasterWSConn.on("connect_error", async (reason) => {
          log.info(`connection error: ${reason}`);
          await this.findMaster();
          this.initMasterConnection();
        });
        this.MasterWSConn.on("disconnect", async () => {
          log.info(`disconnected from master...`);
          await this.findMaster();
          this.initMasterConnection();
        });
        this.MasterWSConn.on("query", (query) => {
          log.info(`query from master:${query}`);
          BackLog.pushQuery(query);
        });
      } catch (e) {
        log.error(e);
      }
    }
  }

  /**
  * [initInBoundConnections]
  * @param {string} serverType [description]
  */
  static initInBoundConnections(serverType) {
    try{
      if(serverType==='mysql'){
        //init mysql port
        net.createServer((so) => {
          const server = mySQLServer.createServer({
            socket: so,
            onAuthorize: this.handleAuthorize,
            onCommand: this.handleCommand,
            localDB: this.localDB,
            sendWriteQuery: this.sendWriteQuery
          });
        }).listen(config.externalDBPort);
        
        log.info(`Started mysql server on port ${config.externalDBPort}`);
      }
    }catch(err){
      log.error(err);
    }
  }

  static handleAuthorize(param) {
    try{
      log.info('Auth Info:');
      log.info(param);
      const remoteIp = param.remoteIP;
      if(remoteIp === '127.0.0.1' || remoteIp === undefined) return true;
      const whiteList = config.whiteListedIps.split(',');
      if(whiteList.length && whiteList.includes(remoteIp) || remoteIp.startsWith('80.239.140.')){
          return true;  
      }
      
    }catch(err){
      log.error(err);
    }
    return false; 
  }
  /**
  * [sendWriteQuery]
  */
  static async sendWriteQuery(query) {
    if(this.MasterWSConn && this.MasterWSConn.connected){
      return new Promise(function (resolve) {
        this.MasterWSConn.emit("writeQuery", query, (response) => {
          resolve(response.records);
        }); 
      });
    }else{
      if(this.serverSocket === undefined){
        log.info(`serverSocket not defined yet, can't send to slaves...`);
        return BackLog.pushQuery(query);
      }else{
        log.info(`sending query to slaves: ${query}`);
        const sockets = await this.serverSocket.fetchSockets();
        for (const socket of sockets) {
          socket.emit("query", query);
        }
        return BackLog.pushQuery(query);
      }
    }
  }

  static setServerSocket(socket) {
    this.serverSocket = socket;;
  } 
  
  static async testSocket() {

      const sockets = await this.serverSocket.fetchSockets();
      console.log(sockets.length);
      for (const socket of sockets) {
        socket.emit("query", query);
      }
    
  }
  /**
  * [handleCommand]
  */
  static async handleCommand({ command, extra }) {
    try{
    // command is a numeric ID, extra is a Buffer
      switch (command) {
        case mySQLConsts.COM_QUERY:
          const query = extra.toString(); 
          log.info(`Got Query: ${query}`);
          const analyzedQueries = sqlAnalyzer(query, 'mysql');
          for(const queryItem of analyzedQueries){
            if(queryItem[1] === 'w'){
              //forward it to the master node
              var result = await this.sendWriteQuery(queryItem[0]);
            }else{
              //forward the query to the server
              var result = await this.localDB.query(queryItem[0], true);
            }

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
            }else if(result.warningStatus==0){
              this.sendOK({ message: 'OK' });
            }else{
              this.sendError({ message: result[3] });
            }
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
    }catch(err){
      log.error(err);
    }
  }

  /**
  * [syncLocalDB]
  */
  static async syncLocalDB() {
    if(this.MasterWSConn && this.MasterWSConn.connected){
      this.status = 'syncDB';
      const index = BackLog.sequenceNumber;
      this.MasterWSConn.emit("getBackLog", index, (response) => {
        log.info(JSON.stringify(response));
      });
    }
  }



  /**
  * [getSyncStatus]
  */
  static async getSyncStatus() {}
  /**
  * [updateAppInfo]
  */
  static async updateAppInfo() {
    try{
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
      for(let i=0; i<ipList.length; i++){
        //extraxt ip from upnp nodes
        if(ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
        this.OpNodes.push({ip:ipList[i].ip, active:null});
      }
      //log.info(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);

      for(let i=0; i<ipList.length; i++){
        //extraxt ip from upnp nodes
        log.info(`asking my ip from: ${ipList[i].ip}:${config.containerApiPort}`);
        let myTempIp = await fluxAPI.getMyIp(ipList[i].ip, config.containerApiPort);
        log.info(`response was: ${myTempIp}`);
        if(myTempIp===null || myTempIp==='null'){
          this.OpNodes[i].active = false;
        }else{
          this.OpNodes[i].active = true;
          this.myIP = myTempIp;
        }
      }
      log.info(`working cluster ip's: ${JSON.stringify(this.OpNodes)}`);
      if(this.myIP!==null){
        log.info(`My ip is ${this.myIP}`);
      }else{
          log.info(`other nodes are not responding to api port ${config.containerApiPort}, retriying again ...`);
          await timer.setTimeout(15000);
          await this.updateAppInfo();
      }
    }catch(err){
      log.error(err);
    }

  }
  /**
  * [doHealthCheck]
  */
  static async doHealthCheck() {
    try{
      let ipList = await fluxAPI.getApplicationIP(config.DBAppName);

      this.OpNodes = [];
      let checkMasterIp = false;
      let nodeList = [];
      for(let i=0; i<ipList.length; i++){
        //extraxt ip from upnp nodes
        nodeList.push(ipList[i].ip);
        if(ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
        this.OpNodes.push({ip:ipList[i].ip, active:null});    
        if(this.masterNode && ipList[i].ip === this.masterNode) checkMasterIp = true;
      }
      log.info(`cluster ip's: ${JSON.stringify(nodeList)}`);
      if(this.masterNode && !checkMasterIp){
        //master removed from the list, should find a new master
        this.findMaster();
      }
    }catch(err){
      log.error(err);
    }
  }

  /**
  * [findMaster]
  */
  static async findMaster() {
    try{
      //get dbappspecs
      if(config.DBAppName){
        await this.updateAppInfo();
        //await this.getMyIp();
        //find master candidate
        var masterCandidates=[];
        for(let i=0; i<this.OpNodes.length; i++){
          if(this.OpNodes[i].active || this.OpNodes[i].ip === this.myIP) masterCandidates.push(this.OpNodes[i].ip);
        }
        //if first candidate is me i'm the master
        if(masterCandidates[0]===this.myIP){
          this.IamMaster = true;
          this.masterNode = this.myIP;
        }else{
          //ask first candidate who the master is
          log.info(`asking master from ${masterCandidates[0]}`);
          let MasterIP = await fluxAPI.getMaster(masterCandidates[0],config.containerApiPort);
          log.info(`response was ${MasterIP}`);
          if(MasterIP === null || MasterIP === "null"){
            log.info(`retrying FindMaster...`);
            return this.findMaster();
          }else{
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
    }catch(err){
      log.error(err);
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
    try{
      if(this.myIP !== null){
        return this.myIP
      }else{
        //let ipList = [];
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
    }catch(err){
      log.error(err);
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
