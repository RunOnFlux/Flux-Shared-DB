/* eslint-disable no-case-declarations */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-unused-vars */
const timer = require('timers/promises');
const net = require('net');
const { io } = require('socket.io-client');
const md5 = require('md5');
const BackLog = require('./Backlog');
const dbClient = require('./DBClient');
const log = require('../lib/log');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const mySQLServer = require('../lib/mysqlServer');
const mySQLConsts = require('../lib/mysqlConstants');
const sqlAnalyzer = require('../lib/sqlAnalyzer');

class Operator {
  static localDB = null;

  static OpNodes = [];

  static AppNodes = [];

  static clientNodes = [];

  static nodeInstances = 0;

  static masterNode = null;

  static IamMaster = false;

  static apiKey = null;

  static myIP = null;

  static masterWSConn;

  static status = 'INIT';

  static serverSocket;

  /**
  * [initLocalDB]
  */
  static async initLocalDB() {
    await BackLog.createBacklog(this.localDB);
    if (config.dbInitDB) {
      await this.localDB.createDB(config.dbInitDB);
      log.info(`${config.dbInitDB} database created on local DB.`);
    }
  }

  /**
  * [initMasterConnection]
  */
  static initMasterConnection() {
    if (this.masterNode && !this.IamMaster) {
      log.info(`establishing persistent connection to master node...${this.masterNode}`);
      try {
        this.masterWSConn = io.connect(`http://${this.masterNode}:${config.containerApiPort}`, {
          transports: ['websocket'],
          reconnection: false,
          timeout: 2000,
        });
        this.masterWSConn.on('connect', (socket) => {
          const { engine } = this.masterWSConn.io;
          log.info('connected to master...');
          this.syncLocalDB();
          engine.once('upgrade', () => {
            log.info(`transport protocol: ${engine.transport.name}`); // in most cases, prints "websocket"
          });
        });
        this.masterWSConn.on('connect_error', async (reason) => {
          log.info(`connection error: ${reason}`);
          this.masterWSConn.removeAllListeners();
          await this.findMaster();
          this.initMasterConnection();
        });
        this.masterWSConn.on('disconnect', async () => {
          log.info('disconnected from master...');
          this.masterWSConn.removeAllListeners();
          await this.findMaster();
          this.initMasterConnection();
        });
        this.masterWSConn.on('query', async (query, sequenceNumber, timestamp, sendToClient) => {
          log.info(`query from master:${query},${sequenceNumber},${timestamp},${sendToClient}`);
          if (this.status === 'OK') {
            await BackLog.pushQuery(query, sequenceNumber, timestamp, false, sendToClient);
          } else {
            await BackLog.pushQuery(query, sequenceNumber, timestamp, true);
          }
        });
      } catch (e) {
        log.error(e);
        this.masterWSConn.removeAllListeners();
      }
    }
  }

  /**
  * [initInBoundConnections]
  * @param {string} serverType [description]
  */
  static initInBoundConnections(serverType) {
    try {
      if (serverType === 'mysql') {
        // init mysql port
        net.createServer((so) => {
          const server = mySQLServer.createServer({
            socket: so,
            onAuthorize: this.handleAuthorize,
            onCommand: this.handleCommand,
            localDB: this.localDB,
            serverSocket: this.serverSocket,
            masterWSConn: this.masterWSConn,
            BACKLOG_DB: config.dbBacklog,
            IamMaster: this.IamMaster,
            appIPList: this.appIPList,
            isNotBacklogQuery: this.isNotBacklogQuery,
            sendWriteQuery: this.sendWriteQuery,
          });
        }).listen(config.externalDBPort);

        log.info(`Started mysql server on port ${config.externalDBPort}`);
      }
    } catch (err) {
      log.error(err);
    }
  }

  /**
  * [handleAuthorize]
  * @param {object} param [description]
  */
  static handleAuthorize(param) {
    try {
      log.info('Auth Info:');
      log.info(JSON.stringify(param));
      const remoteIp = param.remoteIP;
      if (remoteIp === '127.0.0.1' || remoteIp === undefined) return true;
      const whiteList = config.whiteListedIps.split(',');
      if ((whiteList.length && whiteList.includes(remoteIp)) || remoteIp.startsWith('80.239.140.')) {
        return true;
      }
      if (this.appIPList.includes(remoteIp)) return true;
      log.info(`DB connection rejected from ${remoteIp}`);
    } catch (err) {
      log.error(err);
    }
    return false;
  }

  /**
  * [sendWriteQuery]
  * @param {string} query [description]
  */
  static async sendWriteQuery(query) {
    if (!this.IamMaster) {
      const { masterWSConn } = this;
      return new Promise((resolve) => {
        masterWSConn.emit('writeQuery', query, (response) => {
          resolve(response.result);
        });
      });
    }
    log.info(`sending query to slaves: ${query}`);
    const result = await BackLog.pushQuery(query);
    if (result) this.serverSocket.emit('query', query, result[1], result[2]);
    return result[0];
  }

  /**
  * [setServerSocket]
  * @param {socket} socket [description]
  */
  static async setServerSocket(socket) {
    this.serverSocket = socket;
    const sockets = await this.serverSocket.fetchSockets();
  }

  /**
  * [isNotBacklogQuery]
  * @param {string} query [description]
  * @param {string} BACKLOG_DB [description]
  */
  static isNotBacklogQuery(query, BACKLOG_DB) {
    return !query.includes(BACKLOG_DB);
  }

  /**
  * [handleCommand]
  * @param {int} command [description]
  * @param {string} extra [description]
  */
  static async handleCommand({ command, extra }) {
    try {
    // command is a numeric ID, extra is a Buffer
      switch (command) {
        case mySQLConsts.COM_QUERY:
          const query = extra.toString();
          const analyzedQueries = sqlAnalyzer(query, 'mysql');
          this.localDB.setSocket(this.socket);
          for (const queryItem of analyzedQueries) {
            log.info(`got Query: ${queryItem}`);
            if (queryItem[1] === 'w' && this.isNotBacklogQuery(queryItem[0], this.BACKLOG_DB)) {
              // forward it to the master node
              await this.sendWriteQuery(queryItem[0]);
              // this.localDB.enableSocketWrite = false;
              // let result = await this.localDB.query(queryItem[0], true);
              // this.sendOK({ message: 'OK' });
            } else {
              // forward it to the local DB
              // eslint-disable-next-line prefer-const
              let result = await this.localDB.query(queryItem[0], true);
              // log.info(`result: ${JSON.stringify(result)}`);
            }
            // log.info(result);
            // Then send it back to the user in table format
            /*
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
              break;
            } else if(result[0]){
              this.sendOK({ message: 'OK' });
              break;
            }else if(result.warningStatus==0){
              this.sendOK({ message: 'OK' });
              break;
            }else{
              //this.sendError({ message: result[3] });
              //break;
            } */
          }

          break;
        case mySQLConsts.COM_PING:
          console.log('got ping');
          this.sendOK({ message: 'OK' });
          break;
        case null:
        case undefined:
        case mySQLConsts.COM_QUIT:
          log.info('Disconnecting');
          this.end();
          break;
        case mySQLConsts.COM_INIT_DB:
          this.localDB.setSocket(this.socket);
          await this.localDB.query(`use ${extra}`);
          // this.sendOK({ message: 'OK' });
          break;
        default:
          log.info(`Unknown Command: ${command}`);
          this.sendError({ message: 'Unknown Command' });
          break;
      }
    } catch (err) {
      log.error(err);
    }
  }

  /**
  * [syncLocalDB]
  */
  static async syncLocalDB() {
    if (this.masterWSConn && this.masterWSConn.connected) {
      this.status = 'SYNC';
      let masterSN = BackLog.sequenceNumber + 1;
      let copyBuffer = false;
      while (BackLog.sequenceNumber < masterSN && !copyBuffer) {
        const index = BackLog.sequenceNumber;
        const response = await fluxAPI.getBackLog(index, this.masterWSConn);
        masterSN = response.sequenceNumber;
        log.info(`sync backlog: ${JSON.stringify(response)}`);
        for (const record of response.records) {
          await BackLog.pushQuery(record.query, record.seq, record.timestamp);
        }
        if (BackLog.bufferStartSequenceNumber > 0 && BackLog.bufferStartSequenceNumber <= BackLog.sequenceNumber) copyBuffer = true;
      }
      if (copyBuffer) await BackLog.moveBufferToBacklog();
      this.status = 'OK';
    }
  }

  /**
  * [updateAppInfo]
  */
  static async updateAppInfo() {
    try {
      const Specifications = await fluxAPI.getApplicationSpecs(config.DBAppName);
      this.nodeInstances = Specifications.instances;
      // wait for all nodes to spawn
      let ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      while (ipList.length < this.nodeInstances) {
        log.info(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
        await timer.setTimeout(10000);
        ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      }
      let appIPList = [];
      if (config.DBAppName === config.AppName) {
        appIPList = ipList;
      } else {
        appIPList = await fluxAPI.getApplicationIP(config.AppName);
      }
      this.OpNodes = [];
      for (let i = 0; i < ipList.length; i += 1) {
        // extraxt ip from upnp nodes
        // eslint-disable-next-line prefer-destructuring
        if (ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
        this.OpNodes.push({ ip: ipList[i].ip, active: null });
      }
      for (let i = 0; i < appIPList.length; i += 1) {
        // eslint-disable-next-line prefer-destructuring
        if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
        this.AppNodes.push(appIPList[i].ip);
      }
      // log.info(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);

      for (let i = 0; i < ipList.length; i += 1) {
        // extraxt ip from upnp nodes
        log.info(`asking my ip from: ${ipList[i].ip}:${config.containerApiPort}`);
        const myTempIp = await fluxAPI.getMyIp(ipList[i].ip, config.containerApiPort);
        log.info(`response was: ${myTempIp}`);
        if (myTempIp === null || myTempIp === 'null') {
          this.OpNodes[i].active = false;
        } else {
          this.OpNodes[i].active = true;
          this.myIP = myTempIp;
        }
      }
      log.info(`working cluster ip's: ${JSON.stringify(this.OpNodes)}`);
      if (this.myIP !== null) {
        log.info(`My ip is ${this.myIP}`);
      } else {
        log.info(`other nodes are not responding to api port ${config.containerApiPort}, retriying again ...`);
        await timer.setTimeout(15000);
        await this.updateAppInfo();
      }
    } catch (err) {
      log.error(err);
    }
  }

  /**
  * [doHealthCheck]
  */
  static async doHealthCheck() {
    try {
      const ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      let appIPList = [];
      if (config.DBAppName === config.AppName) {
        appIPList = ipList;
      } else {
        appIPList = await fluxAPI.getApplicationIP(config.AppName);
      }
      this.OpNodes = [];
      this.AppNodes = [];
      let checkMasterIp = false;
      const nodeList = [];
      for (let i = 0; i < ipList.length; i += 1) {
        // extraxt ip from upnp nodes
        nodeList.push(ipList[i].ip);
        // eslint-disable-next-line prefer-destructuring
        if (ipList[i].ip.includes(':')) ipList[i].ip = ipList[i].ip.split(':')[0];
        this.OpNodes.push({ ip: ipList[i].ip, active: null });
        if (this.masterNode && ipList[i].ip === this.masterNode) checkMasterIp = true;
      }
      for (let i = 0; i < appIPList.length; i += 1) {
        // eslint-disable-next-line prefer-destructuring
        if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
        this.AppNodes.push(appIPList[i].ip);
      }
      log.info(`cluster ip's: ${JSON.stringify(nodeList)}`);
      log.info(`app ip's: ${JSON.stringify(this.AppNodes)}`);
      if (this.masterNode && !checkMasterIp) {
        log.info('master removed from the list, should find a new master');
        this.findMaster();
      }
    } catch (err) {
      log.error(err);
    }
  }

  /**
  * [findMaster]
  */
  static async findMaster() {
    try {
      // get dbappspecs
      if (config.DBAppName) {
        await this.updateAppInfo();
        // find master candidate
        const masterCandidates = [];
        for (let i = 0; i < this.OpNodes.length; i += 1) {
          if (this.OpNodes[i].active || this.OpNodes[i].ip === this.myIP) masterCandidates.push(this.OpNodes[i].ip);
        }
        // if first candidate is me i'm the master
        if (masterCandidates[0] === this.myIP) {
          this.IamMaster = true;
          this.masterNode = this.myIP;
        } else {
          // ask first candidate who the master is
          log.info(`asking master from ${masterCandidates[0]}`);
          const MasterIP = await fluxAPI.getMaster(masterCandidates[0], config.containerApiPort);
          log.info(`response was ${MasterIP}`);
          if (MasterIP === null || MasterIP === 'null') {
            log.info('retrying FindMaster...');
            return this.findMaster();
          }
          log.info(`asking master for confirmation @ ${MasterIP}:${config.containerApiPort}`);
          const MasterIP2 = await fluxAPI.getMaster(MasterIP, config.containerApiPort);
          log.info(`response from ${MasterIP} was ${MasterIP2}`);
          if (MasterIP2 === MasterIP) {
            this.masterNode = MasterIP;
          } else {
            log.info('master node not matching, retrying...');
            return this.findMaster();
          }
        }
        log.info(`Master node is ${this.masterNode}`);
        return this.masterNode;
      }
      log.info('DB_APPNAME environment variabele is not defined.');
    } catch (err) {
      log.error(err);
    }
    return null;
  }

  /**
  * [getMaster]
  */
  static getMaster() {
    if (this.masterNode === null) {
      if (this.OpNodes.length > 2) {
        return this.OpNodes[0].ip;
      }
    } else {
      return this.masterNode;
    }
    return null;
  }

  /**
  * [getMyIp]
  */
  /*
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
          let j=1;

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
        log.info(`all response list: ${JSON.stringify(ipList)}`);
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
        }
      }
    }catch(err){
      log.error(err);
    }
  }
  */

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
