/* eslint-disable no-else-return */
/* eslint-disable no-case-declarations */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-unused-vars */
const timer = require('timers/promises');
const fs = require('fs');
const net = require('net');
const http = require('http');
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
const IdService = require('./IdService');
const SqlImporter = require('../lib/mysqlimport');

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

  static downloadingBackup = false;

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
        BackLog.UserDBClient.query("SET GLOBAL sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES'");
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
    if (this.masterWSConn) {
      try {
        this.masterWSConn.removeAllListeners();
        this.masterWSConn.disconnect();
      } catch (err) {
        log.error(err);
      }
    }
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
            /*
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
            */
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
              await BackLog.pushQuery(query, sequenceNumber, timestamp, false, connId);
              // push queries from buffer until there is a gap or the buffer is empty
              while (this.buffer[BackLog.sequenceNumber + 1] !== undefined) {
                const nextQuery = this.buffer[BackLog.sequenceNumber + 1];
                if (nextQuery !== undefined && nextQuery !== null) {
                  // log.info(JSON.stringify(nextQuery), 'magenta');
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
                    missingQueryBuffer.put(BackLog.sequenceNumber + i, true, 10000);
                    await fluxAPI.askQuery(BackLog.sequenceNumber + i, this.masterWSConn);
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
                      missingQueryBuffer.put(BackLog.sequenceNumber + i, true, 10000);
                      await fluxAPI.askQuery(BackLog.sequenceNumber + i, this.masterWSConn);
                      i += 1;
                    }
                  }
                }
              }
            }
          } else if (this.status === 'SYNC' || this.status === 'CLEANUP') {
            await BackLog.pushQuery(query, sequenceNumber, timestamp, true, connId);
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
        this.masterWSConn.on('userSession', (op, key, value) => {
          if (op === 'add') { IdService.addNewSession(key, value); } else { IdService.removeSession(key); }
        });
        this.masterWSConn.on('rollBack', async (seqNo) => {
          log.info(`rollback request from master, rewinding to ${seqNo}`);
          if (this.status === 'SYNC') {
            this.status = 'ROLLBACK';
            await BackLog.rebuildDatabase(seqNo);
            this.syncLocalDB();
          } else {
            const tempStatus = this.status;
            this.status = 'ROLLBACK';
            await BackLog.rebuildDatabase(seqNo);
            this.status = tempStatus;
          }
        });
        this.masterWSConn.on('compressbacklog', async (filename, filesize) => {
          log.info(`compressbacklog request from master, filename: ${filename} with ${filesize} bytes`);
          await this.comperssBacklog(filename, filesize);
          this.syncLocalDB();
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
      if (this.status !== 'OK' || this.operator.ghosted) {
        // log.info(`status: ${this.status},${this.operator.status}, rejecting connection`);
        return false;
      }
      // wait untill there are incomming connections
      if (this.operator.IamMaster && this.operator.serverSocket.engine.clientsCount < 1) {
        log.warn('no incomming connections: refusing DB client auth', 'yellow');
        return false;
      }
      const remoteIp = param.remoteIP;
      if (this.authorizedApp === null) this.authorizedApp = remoteIp;
      const whiteList = config.whiteListedIps.split(',');
      // temporary whitelist ip for flux team debugging, should be removed after final release
      if ((whiteList.length && whiteList.includes(remoteIp)) || remoteIp === '167.235.234.45') {
        return true;
      }
      // apps only can connect to the master node
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
  static async sendWriteQuery(query, connId = false, fullQuery = null, masterSocket = null) {
    if (this.masterNode !== null) {
      // log.info(`master node: ${this.masterNode}`);
      if (!this.IamMaster) {
        const { masterWSConn } = this;
        if (masterWSConn) {
          return new Promise((resolve) => {
            masterWSConn.emit('writeQuery', query, connId, (response) => {
              resolve(response.result);
            });
          });
        }
      }
      const result = await BackLog.pushQuery(query, 0, Date.now(), false, connId, fullQuery || query);
      // log.info(`sending query to slaves: ${JSON.stringify(result)}`);
      if (result) {
        // log.info(`emitting ${result[1]}`);
        if (this.serverSocket) {
          this.serverSocket.emit('query', query, result[1], result[2], false);
        } else {
          masterSocket.emit('query', query, result[1], result[2], false);
        }
      }
      return result;
    }
    return null;
  }

  /**
  * [rollBack]
  * @param {int} seq [description]
  */
  static async rollBack(seqNo) {
    try {
      if (this.status !== 'ROLLBACK') {
        if (this.IamMaster) {
          this.status = 'ROLLBACK';
          log.info(`rolling back to ${seqNo}`);
          this.serverSocket.emit('rollBack', seqNo);
          await BackLog.rebuildDatabase(seqNo);
          this.status = 'OK';
        } else {
          const { masterWSConn } = this;
          if (masterWSConn) {
            return new Promise((resolve) => {
              masterWSConn.emit('rollBack', seqNo, (response) => {
                resolve(response.result);
              });
            });
          }
        }
      }
      return null;
    } catch (e) {
      log.error(JSON.stringify(e));
      return null;
    }
  }

  static async pushToBacklog(query, seq = false, timestamp = false) {
    return BackLog.pushToBacklog(query, seq, timestamp);
  }

  static async doCompressCheck() {
    const currentHour = new Date().getHours();
    if (this.IamMaster && BackLog.sequenceNumber > 500000 && currentHour >= 1) {
      this.comperssBacklog();
    }
  }

  /**
  * [downloadBackup]
  *
  */
  static async downloadBackup(filename, filesize) {
    return new Promise((resolve, reject) => {
      const fileUrl = `http://${this.masterNode}:${config.debugUIPort}/${filename}/${filesize}`;
      const filePath = `./dumps/${filename}.sql`;
      log.info(`downloading backup from ${fileUrl}`);
      const file = fs.createWriteStream(filePath);
      const request = http.get(fileUrl, (response) => {
        if (response.statusCode !== 200) {
          log.error(`Failed to download file. Status code: ${response.statusCode}`);
          fs.unlink(filePath, (err) => {
            if (err) {
              log.error(`Error deleting partially created file: ${err}`);
            }
          });
          reject(new Error(`Failed to download file. Status code: ${response.statusCode}`));
          return;
        }

        response.pipe(file);

        file.on('finish', () => {
          log.info('download finished.');
          file.close();
          resolve(); // Resolve the promise after successful download
        });
      });

      request.on('error', (error) => {
        log.error(`Error downloading file: ${error.message}`);
        fs.unlink(filePath, (err) => {
          if (err) {
            log.error(`Error deleting partially created file: ${err}`);
          }
        });
        reject(error); // Reject the promise with the error
      });
    });
  }

  /**
  * [comperssBacklog]
  *
  */
  static async comperssBacklog(filename = false, filesize = 0) {
    try {
      this.status = 'COMPRESSING';
      await timer.setTimeout(500);
      // create a snapshot
      let backupFilename = filename;
      if (backupFilename) {
        let tries = 0;
        while (!fs.existsSync(`./dumps/${backupFilename}.sql`)) {
          // reset master if file is not being replicated.
          if (tries > 30) {
            // clear backlog
            await BackLog.clearBacklog();
            await BackLog.clearBuffer();
            if (this.masterWSConn) {
              try {
                this.masterWSConn.removeAllListeners();
                this.masterWSConn.disconnect();
              } catch (err) {
                log.error(err);
              }
            }
            await this.findMaster();
            this.initMasterConnection();
            return;
          }
          log.info(`Waiting for ./dumps/${backupFilename}.sql to be created...`);
          await timer.setTimeout(3000);
          tries += 1;
        }
        while (fs.statSync(`./dumps/${backupFilename}.sql`).size !== filesize) {
          log.info(`filesize don't match ${fs.statSync(`./dumps/${backupFilename}.sql`).size}, ${filesize}`);
          await timer.setTimeout(3000);
        }
      } else {
        log.info('creating snapshot...');
        backupFilename = await BackLog.dumpBackup();
        const fileStats = fs.statSync(`./dumps/${backupFilename}.sql`);
        // eslint-disable-next-line no-param-reassign
        filesize = fileStats.size;
      }
      if (backupFilename && filesize > 1000000) {
        if (this.IamMaster) {
          this.serverSocket.emit('compressbacklog', backupFilename, filesize);
        }
        // clear backlog
        await BackLog.clearBacklog();
        await BackLog.clearBuffer();
        await timer.setTimeout(200);
        // restore backlog from snapshot
        const importer = new SqlImporter({
          callback: this.pushToBacklog,
          serverSocket: false,
        });
        importer.onProgress((progress) => {
          const percent = Math.floor((progress.bytes_processed / progress.total_bytes) * 10000) / 100;
          BackLog.compressionTask = percent;
          log.info(`${percent}% Completed`, 'cyan');
        });
        importer.setEncoding('utf8');
        await importer.import(`./dumps/${backupFilename}.sql`).then(async () => {
          const filesImported = importer.getImported();
          log.info(`${filesImported.length} SQL file(s) imported to backlog.`);
          this.status = 'OK';
          BackLog.compressionTask = -1;
          if (this.IamMaster) {
            const files = await BackLog.listSqlFiles();
            for (let i = 0; i < files.length - 1; i += 1) BackLog.deleteBackupFile(files[i].fileName, true);
          }
        }).catch((err) => {
          log.error(err);
        });
      }
    } catch (e) {
      this.status = 'OK';
      log.error(JSON.stringify(e));
    }
  }

  /**
  * [emitUserSession]
  * @param {string} key [description]
  * @param {string} value [description]
  */
  static async emitUserSession(op, key, value) {
    if (this.IamMaster && this.serverSocket) {
      this.serverSocket.emit('userSession', op, key, value);
    } else {
      const { masterWSConn } = this;
      return new Promise((resolve) => {
        if (masterWSConn) {
          masterWSConn.emit('userSession', op, key, value, (response) => {
            resolve(response.result);
          });
        }
      });
    }
    return null;
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
          for (const queryItem of analyzedQueries) {
            if (queryItem[1] === 'w' && this.isNotBacklogQuery(queryItem[0], this.BACKLOG_DB)) {
              if (this.operator.sessionQueries[id] !== undefined) {
                await this.sendWriteQuery(this.operator.sessionQueries[id], -1);
                this.operator.sessionQueries[id] = undefined;
              }
              await this.sendWriteQuery(queryItem[0], id, query);
            } else if (queryItem[1] === 's') {
              // eslint-disable-next-line prefer-destructuring
              this.operator.sessionQueries[id] = queryItem[0];
              await ConnectionPool.getConnectionById(id).query(queryItem[0], true);
            } else {
              // forward it to the local DB
              await ConnectionPool.getConnectionById(id).query(queryItem[0], true);
            }
          }

          break;
        case mySQLConsts.COM_PING:
          this.sendOK({ message: 'OK' });
          break;
        case null:
        case undefined:
        case mySQLConsts.COM_QUIT:
          this.end();
          break;
        case mySQLConsts.COM_INIT_DB:
          await ConnectionPool.getConnectionById(id).query(`use ${extra}`);
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
      /*
      try {
        const response = await fluxAPI.getKeys(this.masterWSConn);
        const keys = JSON.parse(Security.decryptComm(Buffer.from(response.keys, 'hex')));
        // eslint-disable-next-line guard-for-in
        for (const key in keys) {
          BackLog.pushKey(key, keys[key]);
          Operator.keys[key] = keys[key];
        }
      } catch (err) {
        log.error(err);
      }
      */
      let masterSN = BackLog.sequenceNumber + 1;
      log.info(`current seq no: ${masterSN}`);
      let copyBuffer = false;
      while (BackLog.sequenceNumber < masterSN && !copyBuffer) {
        try {
          const index = BackLog.sequenceNumber;
          const response = await fluxAPI.getBackLog(index + 1, this.masterWSConn);
          masterSN = response.sequenceNumber;
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
          let percent = Math.round(((index + response.records.length) / masterSN) * 1000);
          if (masterSN === 0) percent = 0;
          log.info(`sync backlog from ${index + 1} to ${index + response.records.length} - [${'='.repeat(Math.floor(percent / 50))}>${'-'.repeat(Math.floor((1000 - percent) / 50))}] %${percent / 10}`, 'cyan');
        } catch (err) {
          log.error(err);
        }
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
      const prevMaster = await BackLog.getKey('masterIP', false);
      const myip = await BackLog.getKey('myIP', false);
      if (prevMaster) {
        log.info(`previous master was ${prevMaster}`);
        if (ipList.some((obj) => obj.ip.includes(prevMaster))) {
          log.info('previous master is in the node list. continue...');
        } else {
          log.info('previous master is NOT in the node list.');
          while (ipList.length < this.nodeInstances / 2) {
            log.info(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
            await timer.setTimeout(10000);
            ipList = await fluxAPI.getApplicationIP(config.DBAppName);
          }
        }
      } else {
        log.info('no master node defined before.');
        while (ipList.length < this.nodeInstances / 2) {
          log.info(`Waiting for all nodes to spawn ${ipList.length}/${this.nodeInstances}...`);
          await timer.setTimeout(10000);
          ipList = await fluxAPI.getApplicationIP(config.DBAppName);
        }
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
          ip: ipList[i].ip, active: false, seqNo: 0, upnp,
        });
      }
      for (let i = 0; i < appIPList.length; i += 1) {
        // eslint-disable-next-line prefer-destructuring
        if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
        this.AppNodes.push(appIPList[i].ip);
      }
      let activeNodes = 1;
      for (let i = 0; i < ipList.length; i += 1) {
        if (myip !== ipList[i].ip) {
          // extraxt ip from upnp nodes
          log.info(`asking status from: ${ipList[i].ip}:${config.containerApiPort}`);
          const status = await fluxAPI.getStatus(ipList[i].ip, config.containerApiPort);
          log.info(`${ipList[i].ip}'s response was: ${JSON.stringify(status)}`);
          if (status === null || status === 'null') {
            this.OpNodes[i].active = false;
          } else {
            activeNodes += 1;
            this.OpNodes[i].seqNo = status.sequenceNumber;
            this.OpNodes[i].active = true;
            this.myIP = status.remoteIP;
          }
        }
      }
      const activeNodePer = 100 * (activeNodes / ipList.length);
      log.info(`${activeNodePer} percent of nodes are active`);
      if (this.myIP !== null && activeNodePer >= 50) {
        log.info(`My ip is ${this.myIP}`);
        BackLog.pushKey('myIP', this.myIP, false);
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
      if (appIPList.length > 0) {
        this.OpNodes = [];
        this.AppNodes = [];
        let checkMasterIp = false;
        const nodeList = [];
        for (let i = 0; i < ipList.length; i += 1) {
          // extraxt ip from upnp nodes
          nodeList.push(ipList[i].ip);
          let nodeReachable = false;
          let seqNo = 0;
          let upnp = false;
          if (ipList[i].ip.includes(':')) {
            // eslint-disable-next-line prefer-destructuring
            ipList[i].ip = ipList[i].ip.split(':')[0];
            upnp = true;
          }
          if (this.myIP && ipList[i].ip === this.myIP) {
            nodeReachable = true;
            seqNo = BackLog.sequenceNumber;
          } else {
            const status = await fluxAPI.getStatus(ipList[i].ip, config.containerApiPort, 5000);
            if (status !== null && status !== 'null') {
              nodeReachable = true;
              seqNo = status.sequenceNumber;
            }
          }
          this.OpNodes.push({
            ip: ipList[i].ip, active: nodeReachable, seqNo, upnp,
          });
          if (this.masterNode && ipList[i].ip === this.masterNode) checkMasterIp = true;
        }
        for (let i = 0; i < appIPList.length; i += 1) {
          // eslint-disable-next-line prefer-destructuring
          if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
          this.AppNodes.push(appIPList[i].ip);
        }
        // check if master is working
        if (!this.IamMaster && this.masterNode && this.status !== 'INIT' && this.status !== 'COMPRESSING') {
          let MasterIP = await fluxAPI.getMaster(this.masterNode, config.containerApiPort);
          let tries = 1;
          while ((MasterIP === null || MasterIP === 'null') && tries < 5) {
            tries += 1;
            log.info(`master not responding, tries :${tries}`);
            MasterIP = await fluxAPI.getMaster(this.masterNode, config.containerApiPort);
          }
          // log.debug(`checking master node ${this.masterNode}: ${MasterIP}`);
          if (MasterIP === null || MasterIP === 'null' || MasterIP !== this.masterNode) {
            log.info('master not responding, running findMaster...');
            await this.findMaster();
            this.initMasterConnection();
          }
        }
        if (this.masterNode && !checkMasterIp) {
          log.info('master removed from the list, should find a new master', 'yellow');
          this.masterNode = null;
          this.IamMaster = false;
          await this.findMaster();
          this.initMasterConnection();
        }
        if (this.IamMaster && this.serverSocket.engine.clientsCount < 1 && this.status !== 'INIT' && this.status !== 'COMPRESSING') {
          log.info('No incomming connections, should find a new master', 'yellow');
          await this.findMaster();
          this.initMasterConnection();
        }
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
          log.info(`asking second candidate for confirmation: ${MasterIP}`);
          if (MasterIP === this.myIP || !this.masterCandidates.includes(MasterIP)) {
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
            BackLog.pushKey('masterIP', this.masterNode, false);
            log.info(`Master node is ${this.masterNode}`, 'yellow');
            return this.masterNode;
          }
          log.info(`asking master for confirmation @ ${MasterIP}:${config.containerApiPort}`);
          const MasterIP2 = await fluxAPI.getMaster(MasterIP, config.containerApiPort);
          log.info(`response from ${MasterIP} was ${MasterIP2}`);
          if (MasterIP2 === MasterIP && this.masterCandidates.includes(MasterIP)) {
            this.masterNode = MasterIP;
          } else {
            log.info('master node not matching, retrying...');
            return this.findMaster();
          }
        }
        log.info(`Master node is ${this.masterNode}`, 'yellow');
        BackLog.pushKey('masterIP', this.masterNode, false);
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
