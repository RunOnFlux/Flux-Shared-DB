/* eslint-disable no-else-return */
/* eslint-disable no-case-declarations */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-unused-vars */
const timer = require('timers/promises');
const net = require('net');
const { networkInterfaces } = require('os');
const axios = require('axios');
const { io } = require('socket.io-client');
const missingQueryBuffer = require('memory-cache');
const BackLog = require('./Backlog');
const dbClient = require('./DBClient');
const log = require('../lib/log');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const mySQLServer = require('../lib/mysqlServer');
const mySQLConsts = require('../lib/mysqlConstants');
const sqlAnalyzer = require('../lib/sqlAnalyzer');
const ConnectionPool = require('../lib/ConnectionPool');
const Security = require('./Security');

// const e = require('cors');

class Operator {
  static localDB = null;

  static OpNodes = [];

  static masterCandidates = [];

  static AppNodes = [];

  static clientNodes = [];

  static nodeInstances = 0;

  static authorizedApp = null;

  static masterNode = null;

  static IamMaster = false;

  static apiKey = null;

  static myIP = null;

  static masterWSConn;

  static status = 'INIT';

  static dbConnStatus = 'NOT_CONNECTED';

  static serverSocket;

  static keys = {};

  static authorized = {};

  static lastBufferSeqNo = 0;

  static firstBufferSeqNo = 0;

  static prevWriteQuery = '';

  static connectionDrops = 0;

  static ghosted = false;

  static masterQueue = [];

  static ticket = 0;

  static sessionQueries = {};

  static buffer = {};

  /**
  * [initLocalDB]
  */
  static async initLocalDB() {
    await BackLog.createBacklog();
    if (config.dbInitDB) {
      try {
        await this.localDB.createDB(config.dbInitDB);
        BackLog.UserDBClient = this.localDB;
        BackLog.UserDBClient.setDB(config.dbInitDB);
        log.info(`${config.dbInitDB} database created on local DB.`);
        await ConnectionPool.init({ numberOfConnections: 10, maxConnections: 100, db: config.dbInitDB });
      } catch (err) {
        log.info(err);
      }
    }
  }

  /**
  * [initLocalDB]
  */
  static getTicket() {
    this.ticket += 1;
    return this.ticket;
  }

  /**
  * [initMasterConnection]
  */
  static initMasterConnection() {
    log.info(`master node: ${this.masterNode}`);
    if (this.masterNode && !this.IamMaster) {
      log.info(`establishing persistent connection to master node...${this.masterNode}`);
      try {
        this.masterWSConn = io.connect(`http://${this.masterNode}:${config.containerApiPort}`, {
          transports: ['websocket'],
          reconnection: false,
          timeout: 2000,
        });
        this.masterWSConn.on('connect', async (socket) => {
          const { engine } = this.masterWSConn.io;
          log.info('connected to master, Sharing keys...');
          try {
            const keys = await fluxAPI.shareKeys(Security.publicKey, this.masterWSConn);
            // log.info(Security.privateDecrypt(keys.commAESKey));
            Security.setCommKeys(Security.privateDecrypt(keys.commAESKey), Security.privateDecrypt(keys.commAESIV));
            // log.info(`commAESKey is: ${Security.privateDecrypt(keys.commAESKey)}`);
            // log.info(`commAESIV is: ${Security.privateDecrypt(keys.commAESIV)}`);
            if (this.dbConnStatus === 'WRONG_KEY' && keys.key) {
              const myKeys = Security.privateDecrypt(keys.key).split(':');
              log.info(`myKeys is: ${myKeys[0]}:${myKeys[1]}`);
              // Security.setKey(myKeys[0]);
              Security.setIV(myKeys[1]);
              await this.initDB();
            } else {
              await fluxAPI.updateKey(Security.encryptComm(`N${this.myIP}`), Security.encryptComm(`${Security.getKey()}:${Security.getIV()}`), this.masterWSConn);
            }
          } catch (err) {
            log.error(err);
          }

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
          log.info('disconnected from master...', 'red');
          this.connectionDrops += 1;
          this.masterWSConn.removeAllListeners();
          await this.findMaster();
          this.initMasterConnection();
        });
        this.masterWSConn.on('query', async (query, sequenceNumber, timestamp, connId) => {
          log.info(`query from master:${sequenceNumber},${timestamp},${connId}`);
          if (this.status === 'OK') {
            // if it's the next sequnce number in line push it to the backlog, else put it in buffer
            if (sequenceNumber === BackLog.sequenceNumber + 1) {
              const result = await BackLog.pushQuery(query, sequenceNumber, timestamp, false, connId);
              // push queries from buffer until there is a gap or the buffer is empty
              while (this.buffer[BackLog.sequenceNumber + 1] !== undefined) {
                const nextQuery = this.buffer[BackLog.sequenceNumber + 1];
                if (nextQuery !== undefined && nextQuery !== null) {
                  log.info(JSON.stringify(nextQuery), 'magenta');
                  log.info(`moving seqNo ${nextQuery.sequenceNumber} from buffer to backlog`, 'magenta');
                  await BackLog.pushQuery(nextQuery.query, nextQuery.sequenceNumber, nextQuery.timestamp, false, nextQuery.connId);
                  this.buffer[nextQuery.sequenceNumber] = undefined;
                }
              }
              if (this.lastBufferSeqNo > BackLog.sequenceNumber + 1) {
                let i = 1;
                while (this.buffer[BackLog.sequenceNumber + 1] === undefined && i < 5) {
                  if (missingQueryBuffer.get(BackLog.sequenceNumber + i) !== true) {
                    log.info(`missing seqNo ${BackLog.sequenceNumber + i}, asking master to resend`, 'magenta');
                    missingQueryBuffer.put(BackLog.sequenceNumber + i, true, 5000);
                    fluxAPI.askQuery(BackLog.sequenceNumber + 1, this.masterWSConn);
                    i += 1;
                  }
                }
              }
            } else if (sequenceNumber > BackLog.sequenceNumber + 1) {
              if (this.buffer[sequenceNumber] === undefined) {
                this.buffer[sequenceNumber] = {
                  query, sequenceNumber, timestamp, connId,
                };
                log.info(`pushing seqNo ${sequenceNumber} to the buffer`, 'magenta');
                this.lastBufferSeqNo = sequenceNumber;
                if (this.buffer[BackLog.sequenceNumber + 1] === undefined && missingQueryBuffer.get(BackLog.sequenceNumber + 1) !== true) {
                  let i = 1;
                  while (this.buffer[BackLog.sequenceNumber + 1] === undefined && i < 5) {
                    if (missingQueryBuffer.get(BackLog.sequenceNumber + i) !== true) {
                      log.info(`missing seqNo ${BackLog.sequenceNumber + i}, asking master to resend`, 'magenta');
                      missingQueryBuffer.put(BackLog.sequenceNumber + i, true, 5000);
                      fluxAPI.askQuery(BackLog.sequenceNumber + i, this.masterWSConn);
                      i += 1;
                    }
                  }
                }
              }
            }
          } else if (this.status === 'SYNC') {
            const result = await BackLog.pushQuery(query, sequenceNumber, timestamp, true, connId);
          } else {
            log.info(`omitted query status: ${this.status}`);
          }
        });
        this.masterWSConn.on('updateKey', async (key, value) => {
          const decKey = Security.decryptComm(key);
          log.info(`updateKey master:${decKey},${value}`);
          await BackLog.pushKey(decKey, value);
          Operator.keys[decKey] = value;
        });
        this.masterWSConn.on('rollBack', async (seqNo) => {
          if (this.status === 'SYNC') {
            this.status = 'ROLLBACK';
            await BackLog.rollBack(seqNo);
            this.syncLocalDB();
          } else {
            const tempStatus = this.status;
            this.status = 'ROLLBACK';
            await BackLog.rollBack(seqNo);
            this.status = tempStatus;
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
          mySQLServer.createServer({
            socket: so,
            onAuthorize: this.handleAuthorize,
            onCommand: this.handleCommand,
            operator: this,
            authorizedApp: this.authorizedApp,
            localDB: this.localDB,
            serverSocket: this.serverSocket,
            masterWSConn: this.masterWSConn,
            BACKLOG_DB: config.dbBacklog,
            IamMaster: this.IamMaster,
            masterNode: this.masterNode,
            appIPList: this.appIPList,
            status: this.status,
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
      // log.info(`DB auth from ${param.remoteIP}`);
      // log.info(JSON.stringify(param));
      if (this.status !== 'OK' || this.operator.ghosted) {
        // log.info(`status: ${this.status},${this.operator.status}, rejecting connection`);
        return false;
      }
      const remoteIp = param.remoteIP;
      if (this.authorizedApp === null) this.authorizedApp = remoteIp;
      const whiteList = config.whiteListedIps.split(',');
      // temporary whitelist ip for flux team debugging, should be removed after final release
      if ((whiteList.length && whiteList.includes(remoteIp)) || remoteIp === '167.235.234.45') {
        return true;
      }
      if (!this.operator.IamMaster && (config.AppName.includes('wordpress') || config.authMasterOnly)) return false;
      if (remoteIp === this.authorizedApp) {
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
  static async sendWriteQuery(query, connId) {
    if (this.masterNode !== null) {
      // log.info(`master node: ${this.masterNode}`);
      if (!this.IamMaster) {
        const { masterWSConn } = this;
        return new Promise((resolve) => {
          masterWSConn.emit('writeQuery', query, connId, (response) => {
            resolve(response.result);
          });
        });
      }
      /*
      if (BackLog.writeLock) {
        const myTicket = this.operator.getTicket();
        log.info(`put into queue: ${myTicket}, in queue: ${this.operator.masterQueue.length}`, 'cyan');
        this.operator.masterQueue.push(myTicket);
        while (BackLog.writeLock || this.operator.masterQueue[0] !== myTicket) {
          await timer.setTimeout(5);
        }
        BackLog.writeLock = true;
        this.operator.masterQueue.shift();
        log.info(`out of queue: ${myTicket}, in queue: ${this.operator.masterQueue.length}`, 'cyan');
      }
      */
      const result = await BackLog.pushQuery(query, 0, Date.now(), false, connId);
      // log.info(`sending query to slaves: ${JSON.stringify(result)}`);
      if (result) this.serverSocket.emit('query', query, result[1], result[2], false);
      return result[0];
    }
    return null;
  }

  /**
  * [rollBack]
  * @param {int} seq [description]
  */
  static async rollBack(seqNo) {
    if (this.IamMaster) {
      this.status = 'ROLLBACK';
      log.info(`rooling back to ${seqNo}`);
      this.serverSocket.emit('rollback', seqNo);
      await BackLog.rebuildDatabase(seqNo);
      this.status = 'OK';
    } else {
      const { masterWSConn } = this;
      masterWSConn.emit('rollBack', seqNo, (response) => {
        log.info(response.result);
      });
    }
  }

  /**
  * [setServerSocket]
  * @param {socket} socket [description]
  */
  static async setServerSocket(socket) {
    this.serverSocket = socket;
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
  static async handleCommand({ command, extra, id }) {
    try {
      // command is a numeric ID, extra is a Buffer
      switch (command) {
        case mySQLConsts.COM_QUERY:
          const query = extra.toString();
          const analyzedQueries = sqlAnalyzer(query, 'mysql');
          // if (analyzedQueries.length > 2) log.info(JSON.stringify(analyzedQueries));
          for (const queryItem of analyzedQueries) {
            // log.query(queryItem, 'white', id);
            if (queryItem[1] === 'w' && this.isNotBacklogQuery(queryItem[0], this.BACKLOG_DB)) {
              // wait untill there are incomming connections
              if (this.operator.IamMaster && this.operator.serverSocket.engine.clientsCount < 1) {
                log.warn(`no incomming connections: ${this.operator.serverSocket.engine.clientsCount}`, 'yellow');
                break;
              }
              // forward it to the master node
              // log.info(`${id},${queryItem[0]}`);
              //  log.info(`incoming write ${id}`);
              if (this.operator.sessionQueries[id] !== undefined) {
                await this.sendWriteQuery(this.operator.sessionQueries[id], -1);
                this.operator.sessionQueries[id] = undefined;
              }
              await this.sendWriteQuery(queryItem[0], id);
              // log.info(`finish write ${id}`);
              // this.localDB.enableSocketWrite = false;
              // let result = await this.localDB.query(queryItem[0], true);
              // this.sendOK({ message: 'OK' });
            } else if (queryItem[1] === 's') {
              // eslint-disable-next-line prefer-destructuring
              this.operator.sessionQueries[id] = queryItem[0];
              // log.info(`incoming set session ${id}`);
              await ConnectionPool.getConnectionById(id).query(queryItem[0], true);
              // log.info(`finish set session ${id}`);
            } else {
              // forward it to the local DB
              // eslint-disable-next-line prefer-const
              // log.info(`incoming read ${id}`);
              await ConnectionPool.getConnectionById(id).query(queryItem[0], true);
              // log.info(`finish read ${id}`);
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
          // console.log('got ping');
          this.sendOK({ message: 'OK' });
          break;
        case null:
        case undefined:
        case mySQLConsts.COM_QUIT:
          // log.info(`Disconnecting from ${id}`);
          this.end();
          break;
        case mySQLConsts.COM_INIT_DB:
          // this.localDB.setSocket(this.socket, id);
          await ConnectionPool.getConnectionById(id).query(`use ${extra}`);
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
      try {
        const response = await fluxAPI.getKeys(this.masterWSConn);
        const keys = JSON.parse(Security.decryptComm(Buffer.from(response.keys, 'hex')));
        // console.log(keys);
        // eslint-disable-next-line guard-for-in
        for (const key in keys) {
          BackLog.pushKey(key, keys[key]);
          Operator.keys[key] = keys[key];
        }
      } catch (err) {
        log.error(err);
      }
      let masterSN = BackLog.sequenceNumber + 1;
      let copyBuffer = false;
      while (BackLog.sequenceNumber < masterSN && !copyBuffer) {
        const index = BackLog.sequenceNumber;
        const response = await fluxAPI.getBackLog(index, this.masterWSConn);
        masterSN = response.sequenceNumber;
        const percent = Math.round((index / masterSN) * 1000);
        log.info(`sync backlog from ${index} to ${index + response.records.length} - [${'='.repeat(Math.floor(percent / 50))}>${'-'.repeat(Math.floor((1000 - percent) / 50))}] %${percent / 10}`, 'cyan');
        // log.info(JSON.stringify(response.records));
        BackLog.executeLogs = false;
        for (const record of response.records) {
          if (this.status !== 'SYNC') {
            log.warn('Sync proccess halted.', 'red');
            return;
          }
          await BackLog.pushQuery(record.query, record.seq, record.timestamp);
        }
        if (BackLog.bufferStartSequenceNumber > 0 && BackLog.bufferStartSequenceNumber <= BackLog.sequenceNumber) copyBuffer = true;
        BackLog.executeLogs = true;
      }
      log.info(`sync finished, moving remaining records from backlog, copyBuffer:${copyBuffer}`, 'cyan');
      if (copyBuffer) await BackLog.moveBufferToBacklog();
      log.info('Status OK', 'green');
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
        let upnp = false;
        if (ipList[i].ip.includes(':')) {
          upnp = true;
          // eslint-disable-next-line prefer-destructuring
          ipList[i].ip = ipList[i].ip.split(':')[0];
        }
        this.OpNodes.push({
          ip: ipList[i].ip, active: null, seqNo: 0, upnp,
        });
      }
      for (let i = 0; i < appIPList.length; i += 1) {
        // eslint-disable-next-line prefer-destructuring
        if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
        this.AppNodes.push(appIPList[i].ip);
      }
      // log.info(`cluster ip's: ${JSON.stringify(this.OpNodes)}`);
      let activeNodes = 1;
      for (let i = 0; i < ipList.length; i += 1) {
        // extraxt ip from upnp nodes
        log.info(`asking my ip from: ${ipList[i].ip}:${config.containerApiPort}`);
        // const myTempIp = await fluxAPI.getMyIp(ipList[i].ip, config.containerApiPort);
        const status = await fluxAPI.getStatus(ipList[i].ip, config.containerApiPort);
        log.info(`response was: ${JSON.stringify(status)}`);
        if (status === null || status === 'null') {
          this.OpNodes[i].active = false;
        } else {
          activeNodes += 1;
          this.OpNodes[i].seqNo = status.sequenceNumber;
          this.OpNodes[i].active = true;
          this.myIP = status.remoteIP;
        }
      }
      const activeNodePer = 100 * (activeNodes / ipList.length);
      log.info(`${activeNodePer} percent of nodes are active`);
      if (this.myIP !== null && activeNodePer >= 50) {
        log.info(`My ip is ${this.myIP}`);
      } else {
        log.info('Not enough active nodes, retriying again...');
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
      ConnectionPool.keepFreeConnections();
      BackLog.keepConnections();
      // update node list
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
      if (this.masterNode && !checkMasterIp) {
        log.info('master removed from the list, should find a new master', 'yellow');
        await this.findMaster();
        this.initMasterConnection();
      }
      if (this.IamMaster && this.serverSocket.engine.clientsCount < 1) {
        log.info('No incomming connections, should find a new master', 'yellow');
        await this.findMaster();
        this.initMasterConnection();
      }
      // check connection stability
      if (this.connectionDrops > 3) {
        this.ghosted = true;
        log.info(`health check! connection drops: ${this.connectionDrops}`, 'red');
        log.info('Ghosted', 'red');
      } else if (this.ghosted) {
        log.info(`health check! connection drops: ${this.connectionDrops}`, 'green');
        log.info('Ghosting removed', 'red');
        this.ghosted = false;
      }
      this.connectionDrops = 0;
    } catch (err) {
      log.error(err);
    }
  }

  /**
  * [findMaster]
  */
  static async findMaster() {
    try {
      this.status = 'INIT';
      this.masterNode = null;
      this.IamMaster = false;
      // get dbappspecs
      if (config.DBAppName) {
        await this.updateAppInfo();
        // find master candidate
        this.masterCandidates = [];
        for (let i = 0; i < this.OpNodes.length; i += 1) {
          if (this.OpNodes[i].ip === this.myIP) {
            this.OpNodes[i].active = true;
            this.OpNodes[i].seqNo = BackLog.sequenceNumber;
          }
        }
        // eslint-disable-next-line no-confusing-arrow, no-nested-ternary
        this.OpNodes.sort((a, b) => (a.seqNo < b.seqNo) ? 1 : ((b.seqNo < a.seqNo) ? -1 : 0));
        const masterSeqNo = this.OpNodes[0].seqNo;
        for (let i = 0; i < this.OpNodes.length; i += 1) {
          if (this.OpNodes[i].active && this.OpNodes[i].seqNo === masterSeqNo) {
            if (this.OpNodes[i].upnp) {
              this.masterCandidates.push(this.OpNodes[i].ip);
            } else {
              this.masterCandidates.unshift(this.OpNodes[i].ip);
            }
          }
        }
        log.info(`working cluster ip's: ${JSON.stringify(this.OpNodes)}`);
        log.info(`masterCandidates: ${JSON.stringify(this.masterCandidates)}`);
        // if first candidate is me i'm the master
        if (this.masterCandidates[0] === this.myIP) {
          let MasterIP = this.myIP;
          // ask second candidate for confirmation
          if (this.masterCandidates.length > 1) MasterIP = await fluxAPI.getMaster(this.masterCandidates[1], config.containerApiPort);

          if (MasterIP === this.myIP) {
            this.IamMaster = true;
            this.masterNode = this.myIP;
            this.status = 'OK';
          } else if (MasterIP === null || MasterIP === 'null') {
            log.info('retrying FindMaster...');
            return this.findMaster();
          } else {
            this.masterNode = MasterIP;
          }
        } else {
          // ask first candidate who the master is
          log.info(`asking master from ${this.masterCandidates[0]}`);
          const MasterIP = await fluxAPI.getMaster(this.masterCandidates[0], config.containerApiPort);
          log.info(`response was ${MasterIP}`);
          if (MasterIP === null || MasterIP === 'null') {
            log.info('retrying FindMaster...');
            return this.findMaster();
          }
          if (MasterIP === this.myIP) {
            this.IamMaster = true;
            this.masterNode = this.myIP;
            this.status = 'OK';
            return this.masterNode;
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
        log.info(`Master node is ${this.masterNode}`, 'yellow');
        return this.masterNode;
      }
      log.info('DB_APPNAME environment variabele is not defined.');
    } catch (err) {
      log.info('error while finding master');
      log.error(err);
      return this.findMaster();
    }
    return null;
  }

  /**
  * [getMaster]
  */
  static getMaster() {
    if (this.masterNode === null) {
      if (this.masterCandidates.length) {
        return this.masterCandidates[0];
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

    if (this.localDB === 'WRONG_KEY') {
      return false;
    } else {
      while (this.localDB === null) {
        log.info('Waiting for local DB to boot up...');
        await timer.setTimeout(2000);
        this.localDB = await dbClient.createClient();
      }
      log.info('Connected to local DB.');
    }
    return true;
  }

  /**
  * [initDB]
  */
  static async initDB() {
    if (await this.ConnectLocalDB()) {
      await this.initLocalDB();
      this.initInBoundConnections(config.dbType);
      this.dbConnStatus = 'CONNECTED';
      // Security.setKey(Security.generateNewKey());
      // TODO: RESET DB PASS
    } else {
      this.dbConnStatus = 'WRONG_KEY';
    }
  }

  /**
  * [init]
  */
  static async init() {
    await this.initDB();
  }
}
module.exports = Operator;
