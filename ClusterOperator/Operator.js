/* eslint-disable no-param-reassign */
/* eslint-disable no-nested-ternary */
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

  static ClusterStatus = [];

  static masterCandidates = [];

  static AppNodes = [];

  static clientNodes = [];

  static appLocations = [];

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

  static dbConnectionFails = 0;

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

  static closeMasterConnection() {
    if (this.masterWSConn) {
      try {
        this.masterWSConn.removeAllListeners();
        this.masterWSConn.disconnect();
        this.masterWSConn = null;
      } catch (err) {
        log.error(err);
      }
    }
  }

  /**
  * [initMasterConnection]
  */
  static initMasterConnection() {
    this.closeMasterConnection();
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
          this.closeMasterConnection();
          if (this.status !== 'COMPRESSING') {
            await this.findMaster();
            this.initMasterConnection();
          }
          await this.findMaster();
          this.initMasterConnection();
        });
        this.masterWSConn.on('disconnect', async () => {
          log.info('disconnected from master...', 'red');
          this.connectionDrops += 1;
          this.closeMasterConnection();
          if (this.status !== 'COMPRESSING') {
            await this.findMaster();
            this.initMasterConnection();
          }
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
          } else if (this.status === 'SYNC' || this.status === 'COMPRESSING') {
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
        this.masterWSConn.on('compressionStart', async (seqNo) => {
          log.info(`compressionStart request, seqNo: ${seqNo}`);
          await BackLog.pushKey('lastCompression', seqNo, false);
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
      if (!this.operator.IamMaster && (config.authMasterOnly)) return false;
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
    const randomNumber = Math.floor(Math.random() * 2000);
    let prevSeqNo = await BackLog.getKey('lastCompression', false);
    if (!prevSeqNo) {
      const beaconContent = BackLog.readBeaconFile();
      if (beaconContent && beaconContent.seqNo) {
        prevSeqNo = beaconContent.seqNo;
      }
    }
    log.info(`lastCompression ${prevSeqNo}`, 'cyan');
    if (prevSeqNo) {
      if (!this.IamMaster && this.status === 'OK' && BackLog.sequenceNumber > (Number(prevSeqNo) + 10000 + randomNumber)) this.comperssBacklog();
    } else if (!this.IamMaster && this.status === 'OK' && BackLog.sequenceNumber > 10000 + randomNumber) {
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
  static async comperssBacklog() {
    try {
      this.status = 'COMPRESSING';
      log.info('Status COMPRESSING', 'cyan');
      // delete old snapshots
      const files = await BackLog.listSqlFiles();
      for (let i = 0; i < files.length - 1; i += 1) BackLog.deleteBackupFile(files[i].fileName, true);
      const seqNo = BackLog.sequenceNumber;
      log.info(seqNo, 'cyan');
      await BackLog.pushKey('lastCompression', seqNo, false);
      log.info('key set', 'cyan');
      this.emitCompressionStart(seqNo);
      log.info('key emmited', 'cyan');
      // create snapshot
      const backupFilename = await BackLog.dumpBackup();
      const fileStats = fs.statSync(`./dumps/${backupFilename}.sql`);
      // eslint-disable-next-line no-param-reassign
      const BackupFilesize = fileStats.size;
      // update beacon file
      await BackLog.adjustBeaconFile({ seqNo, backupFilename, BackupFilesize });
      // clear old backlogs
      await BackLog.clearLogs(seqNo);
      log.info('Compression finished, moving buffer records to backlog', 'cyan');
      await BackLog.moveBufferToBacklog();
      // find a new master if old connection is lost
      if (this.masterWSConn === null) {
        await this.findMaster();
        this.initMasterConnection();
      } else {
        log.info('Status OK', 'green');
        this.status = 'OK';
      }
    } catch (e) {
      log.error(`error happened while compressing backlog, moving buffer records to backlog ${JSON.stringify(e)}`, 'red');
      await BackLog.moveBufferToBacklog();
      this.status = 'OK';
      log.error(JSON.stringify(e));
    }
  }

  /**
  * [comperssBacklog]
  *
  */
  static async comperssBacklogOld(filename = false, filesize = 0) {
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
            this.closeMasterConnection();
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
  * [emitCompressionStart]
  * @param {string} key [description]
  * @param {string} value [description]
  */
  static async emitCompressionStart(seqNo) {
    const { masterWSConn } = this;
    return new Promise((resolve) => {
      if (masterWSConn) {
        masterWSConn.emit('compressionStart', seqNo, (response) => {
          resolve(response.result);
        });
      }
    });
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
      // check for beacon file presence
      let status = await fluxAPI.getStatus(this.masterNode, config.containerApiPort);
      while (!status) {
        await timer.setTimeout(3000);
        status = await fluxAPI.getStatus(this.masterNode, config.containerApiPort);
      }
      log.info(JSON.stringify(status));
      if ('firstSequenceNumber' in status && status.firstSequenceNumber > BackLog.sequenceNumber) {
        let beaconContent = await BackLog.readBeaconFile();
        while (beaconContent) {
          log.info('Waiting for beacon file to be created...');
          await timer.setTimeout(3000);
          beaconContent = await BackLog.readBeaconFile();
          log.info(JSON.stringify(beaconContent));
        }
        log.info(JSON.stringify(beaconContent));
        if (beaconContent.seqNo > BackLog.sequenceNumber) {
          while (!fs.existsSync(`./dumps/${beaconContent.backupFilename}.sql`)) {
            log.info(`Waiting for ${beaconContent.backupFilename}.sql to be created...`);
            await timer.setTimeout(3000);
          }
          while (fs.statSync(`./dumps/${beaconContent.backupFilename}.sql`).size !== beaconContent.BackupFilesize) {
            log.info(`filesize don't match ${fs.statSync(`./dumps/${beaconContent.backupFilename}.sql`).size}, ${beaconContent.BackupFilesize}`);
            await timer.setTimeout(3000);
          }
          await BackLog.clearBacklog();
          await BackLog.clearBuffer();
          await timer.setTimeout(200);
          log.info(`importing ${beaconContent.backupFilename}.sql`, 'cyan');
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
          await importer.import(`./dumps/${beaconContent.backupFilename}.sql`).then(async () => {
            const filesImported = importer.getImported();
            log.info(`${filesImported.length} SQL file(s) imported to backlog.`);
            BackLog.shiftBacklogSeqNo(beaconContent.seqNo - BackLog.sequenceNumber);
            this.syncLocalDB();
          }).catch((err) => {
            log.error(err);
          });
          return;
        }
      }
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
      log.info(`current seq no: ${masterSN - 1}`);
      let copyBuffer = false;
      while (BackLog.sequenceNumber < masterSN && !copyBuffer) {
        try {
          const index = BackLog.sequenceNumber;
          const response = await fluxAPI.getBackLog(index + 1, this.masterWSConn);
          if (response && response.status === 'OK') {
            masterSN = response.sequenceNumber;
            BackLog.executeLogs = false;
            for (const record of response.records) {
              if (this.status !== 'SYNC') {
                log.warn('Sync proccess halted.', 'red');
                return;
              }
              await BackLog.pushQuery(record.query, record.seq, record.timestamp);
            }
            // if (BackLog.bufferStartSequenceNumber > 0 && BackLog.bufferStartSequenceNumber <= BackLog.sequenceNumber)
            copyBuffer = true;
            BackLog.executeLogs = true;
            let percent = 0;
            if (masterSN !== 0) percent = Math.round(((index + response.records.length) / masterSN) * 1000);
            log.info(`sync backlog from ${index + 1} to ${index + response.records.length} - [${'='.repeat(Math.floor(percent / 50))}>${'-'.repeat(Math.floor((1000 - percent) / 50))}] %${percent / 10}`, 'cyan');
          }
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
      if (this.nodeInstances === 0) {
        const Specifications = await fluxAPI.getApplicationSpecs(config.DBAppName);
        this.nodeInstances = Specifications.instances;
      }
      // fetch cluster ip's
      if (this.appLocations.length === 0) {
        log.info('fetching cluster list...');
        this.appLocations = await fluxAPI.getApplicationIP(config.DBAppName);
        setTimeout(() => {
          this.appLocations = [];
        }, 2 * 60 * 1000);
      }
      let ipList = this.appLocations;
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
        if (ipList[i].ip.includes(':')) {
          // eslint-disable-next-line prefer-destructuring
          ipList[i].ip = ipList[i].ip.split(':')[0];
        }
        this.OpNodes.push({
          ip: ipList[i].ip, active: false, seqNo: 0, staticIp: ipList[i].staticIp, osUptime: ipList[i].osUptime,
        });
      }
      for (let i = 0; i < appIPList.length; i += 1) {
        // eslint-disable-next-line prefer-destructuring
        if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
        this.AppNodes.push(appIPList[i].ip);
      }
      /*
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
      */
      let activeNodes = 1;
      const statusPromises = this.OpNodes
        .filter((ipObj) => myip !== ipObj.ip) // Skip own IP upfront
        .map((ipObj, index) => {
          log.info(`Asking status from: ${ipObj.ip}:${config.containerApiPort}`);
          return fluxAPI.getStatus(ipObj.ip, config.containerApiPort)
            .then((status) => {
              log.info(`${ipObj.ip}'s response was: ${JSON.stringify(status)}`);
              if (!status || status === 'null') {
                ipObj.active = false;
              } else {
                activeNodes += 1;
                ipObj.seqNo = status.sequenceNumber;
                ipObj.active = true;
                this.myIP = status.remoteIP;
              }
              return { index, status }; // Preserve original array index
            })
            .catch((error) => {
              log.error(`Error from ${ipObj.ip}: ${error.message}`);
              return { index, status: null }; // Handle errors as null status
            });
        });

      // Wait for all calls to complete
      const results = await Promise.all(statusPromises);
      log.info(`results: ${JSON.stringify(results)}`);
      // Process results after all calls finish
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
      // check db connection
      /*
      log.info('health check');
      if (await BackLog.testDB()) {
        this.dbConnectionFails = 0;
      } else {
        this.dbConnectionFails += 1;
      }
      if (this.dbConnectionFails > 3) {
        log.error('DB connection tests failed, node marked for uninstall.', 'red');
        this.status = 'UNINSTALL';
        return;
      }
      */
      ConnectionPool.keepFreeConnections();
      BackLog.keepConnections();
      await this.doCompressCheck();
      // abort health check if doing compression
      if (this.status === 'COMPRESSING') return;
      // check if beacon file has ben updated.
      if (this.status === 'OK') {
        const beaconContent = await BackLog.readBeaconFile();
        log.info(JSON.stringify(beaconContent));
        if (beaconContent) {
          const firstSequenceNumber = await BackLog.getFirstSequenceNumber();
          log.info(`checking if cleanup needed ${firstSequenceNumber},${beaconContent.seqNo},${BackLog.sequenceNumber}`, 'cyan');
          if (beaconContent.seqNo > firstSequenceNumber && beaconContent.seqNo < BackLog.sequenceNumber + 1000) {
            // clear old backlogs
            log.info(`clearing logs older than ${beaconContent.seqNo}`);
            await BackLog.clearLogs(beaconContent.seqNo);
          }
        }
      }
      // update node list
      const ipList = await fluxAPI.getApplicationIP(config.DBAppName);
      let appIPList = [];
      if (config.DBAppName === config.AppName) {
        appIPList = ipList;
      } else {
        appIPList = await fluxAPI.getApplicationIP(config.AppName);
      }
      if (appIPList.length > 0) {
        const OpNodesTmp = [];
        const ClusterStatusTmp = [];
        let checkMasterIp = false;
        let masterConflicts = 0;
        for (let i = 0; i < ipList.length; i += 1) {
          let nodeReachable = false;
          let seqNo = 0;
          let masterIP = '';
          const fullIP = ipList[i].ip;
          if (ipList[i].ip.includes(':')) {
            // eslint-disable-next-line prefer-destructuring
            ipList[i].ip = ipList[i].ip.split(':')[0];
          }
          if (this.myIP && ipList[i].ip === this.myIP) {
            nodeReachable = true;
            seqNo = BackLog.sequenceNumber;
            masterIP = this.masterNode;
          } else {
            const status = await fluxAPI.getStatus(ipList[i].ip, config.containerApiPort, 5000);
            if (status !== null && status !== 'null') {
              nodeReachable = true;
              seqNo = status.sequenceNumber;
              masterIP = status.masterIP;
              if (this.masterNode && status.masterIP !== 'null' && status.masterIP !== null && status.masterIP !== this.masterNode) masterConflicts += 1;
            }
          }
          OpNodesTmp.push({
            ip: ipList[i].ip, active: nodeReachable, seqNo, staticIp: ipList[i].staticIp, osUptime: ipList[i].osUptime,
          });
          ClusterStatusTmp.push({
            ip: fullIP, active: nodeReachable, seqNo, staticIp: ipList[i].staticIp, osUptime: ipList[i].osUptime, masterIP,
          });
          if (this.masterNode && ipList[i].ip === this.masterNode) checkMasterIp = true;
        }

        this.OpNodes = OpNodesTmp;

        if (masterConflicts > 1) {
          log.info('master conflicts detected, should find a new master', 'yellow');
          this.closeMasterConnection();
          await this.findMaster();
          this.initMasterConnection();
          return;
        }
        ClusterStatusTmp.sort((a, b) => {
          // Priority 0: Master node
          if (a.ip.split(':')[0] === this.masterNode) {
            return -1;
          }
          if (b.ip.split(':')[0] === this.masterNode) {
            return 1;
          }
          // Priority 1: Sort by seqNo in descending order
          if (a.seqNo !== b.seqNo) {
            return b.seqNo - a.seqNo; // Higher seqNo comes first
          }
          // Priority 2: Sort by staticIp, with true preferred
          const aStaticIp = a.staticIp === true ? 2 : a.staticIp === false ? 1 : 0;
          const bStaticIp = b.staticIp === true ? 2 : b.staticIp === false ? 1 : 0;
          if (aStaticIp !== bStaticIp) {
            return bStaticIp - aStaticIp; // Higher staticIp value comes first
          }
          // Priority 3: Sort by osUptime in descending order
          const aUptime = a.osUptime || 0; // Use 0 if osUptime is null
          const bUptime = b.osUptime || 0;
          if (aUptime !== bUptime) {
            return bUptime - aUptime; // Higher osUptime comes first
          }
          return 0; // All priorities are equal
        });
        this.AppNodes = [];
        this.ClusterStatus = ClusterStatusTmp;
        for (let i = 0; i < appIPList.length; i += 1) {
          // eslint-disable-next-line prefer-destructuring
          if (appIPList[i].ip.includes(':')) appIPList[i].ip = appIPList[i].ip.split(':')[0];
          this.AppNodes.push(appIPList[i].ip);
        }
        // check if master is working
        if (!this.IamMaster && this.masterNode && this.status !== 'INIT') {
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
            this.closeMasterConnection();
            await this.findMaster();
            this.initMasterConnection();
            return;
          }
        }
        if (this.masterNode && !checkMasterIp) {
          log.info('master removed from the list, should find a new master', 'yellow');
          this.closeMasterConnection();
          this.masterNode = null;
          this.IamMaster = false;
          await this.findMaster();
          this.initMasterConnection();
          return;
        }
        if (this.IamMaster && this.serverSocket.engine.clientsCount < 1 && this.status !== 'INIT') {
          log.info('No incomming connections, should find a new master', 'yellow');
          this.closeMasterConnection();
          await this.findMaster();
          this.initMasterConnection();
          return;
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
  static async findMaster(resetMasterCandidates = true) {
    try {
      if (this.status === 'UNINSTALL') return null;
      this.status = 'INIT';
      this.masterNode = null;
      this.IamMaster = false;
      // get dbappspecs
      if (config.DBAppName) {
        if (resetMasterCandidates) this.masterCandidates = [];
        await this.updateAppInfo();
        // find master candidate
        for (let i = 0; i < this.OpNodes.length; i += 1) {
          if (this.OpNodes[i].ip === this.myIP) {
            this.OpNodes[i].active = true;
            this.OpNodes[i].seqNo = BackLog.sequenceNumber;
          }
        }
        // eslint-disable-next-line no-confusing-arrow, no-nested-ternary
        this.OpNodes.sort((a, b) => {
          // Priority 1: Sort by seqNo in descending order
          if (a.seqNo !== b.seqNo) {
            return b.seqNo - a.seqNo; // Higher seqNo comes first
          }
          // Priority 2: Sort by staticIp, with true preferred
          const aStaticIp = a.staticIp === true ? 2 : a.staticIp === false ? 1 : 0;
          const bStaticIp = b.staticIp === true ? 2 : b.staticIp === false ? 1 : 0;
          if (aStaticIp !== bStaticIp) {
            return bStaticIp - aStaticIp; // Higher staticIp value comes first
          }
          // Priority 3: Sort by osUptime in descending order
          const aUptime = a.osUptime || 0; // Use 0 if osUptime is null
          const bUptime = b.osUptime || 0;
          if (aUptime !== bUptime) {
            return bUptime - aUptime; // Higher osUptime comes first
          }
          return 0; // All priorities are equal
        });
        this.masterCandidates = [];
        const masterSeqNo = this.OpNodes[0].seqNo;
        for (let i = 0; i < this.OpNodes.length; i += 1) {
          if (this.OpNodes[i].active && this.OpNodes[i].seqNo === masterSeqNo) {
            this.masterCandidates.push(this.OpNodes[i].ip);
          }
        }
        log.info(`working cluster ip's: ${JSON.stringify(this.OpNodes)}`);
        log.info(`masterCandidates: ${JSON.stringify(this.masterCandidates)}`);
        await timer.setTimeout(500);
        // if first candidate is me i'm the master
        if (this.masterCandidates[0] === this.myIP) {
          let MasterIP = this.myIP;
          // ask second candidate for confirmation
          if (this.masterCandidates.length > 1) {
            log.info(`asking second candidate for confirmation: ${MasterIP}`);
            MasterIP = await fluxAPI.getMaster(this.masterCandidates[1], config.containerApiPort);
          }
          if (MasterIP === this.myIP) {
            this.IamMaster = true;
            this.masterNode = this.myIP;
            this.status = 'OK';
            log.info('Status OK', 'green');
          } else if (MasterIP === null || MasterIP === 'null') {
            log.info('retrying FindMaster...');
            return this.findMaster(false);
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
            return this.findMaster(false);
          }
          if (MasterIP === this.myIP) {
            this.IamMaster = true;
            this.masterNode = this.myIP;
            this.status = 'OK';
            log.info('Status OK', 'green');
            BackLog.pushKey('masterIP', this.masterNode, false);
            log.info(`Master node is ${this.masterNode}`, 'yellow');
            return this.masterNode;
          }
          if (this.masterCandidates[0] !== MasterIP) {
            log.info(`asking master for confirmation @ ${MasterIP}:${config.containerApiPort}`);
            const MasterIP2 = await fluxAPI.getMaster(MasterIP, config.containerApiPort);
            log.info(`response from ${MasterIP} was ${MasterIP2}`);
            if (MasterIP2 === MasterIP && this.masterCandidates.includes(MasterIP)) {
              this.masterNode = MasterIP;
            } else {
              log.info('master node not matching, retrying...');
              return this.findMaster(false);
            }
          } else {
            this.masterNode = MasterIP;
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
      return this.findMaster(false);
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
    let tries = 0;
    if (this.localDB === 'WRONG_KEY') {
      return false;
    } else {
      while (this.localDB === null) {
        log.info('Waiting for local DB to boot up...');
        tries += 1;
        if (tries > 450) { // more than 15 minutes
          log.info('db check failed.', 'red');
          this.status = 'UNINSTALL';
        }
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
