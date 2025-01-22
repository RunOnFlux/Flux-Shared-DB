/* eslint-disable no-plusplus */
/* eslint-disable consistent-return */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-unused-vars */
const { Server } = require('socket.io');
const https = require('https');
const timer = require('timers/promises');
const express = require('express');
const RateLimit = require('express-rate-limit');
const fileUpload = require('express-fileupload');
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const qs = require('qs');
const sanitize = require('sanitize-filename');
const queryCache = require('memory-cache');
const Operator = require('./Operator');
const BackLog = require('./Backlog');
const IdService = require('./IdService');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const Security = require('./Security');
const SqlImporter = require('../lib/mysqlimport');

/**
* [auth]
* @param {string} ip [description]
*/
function auth(ip) {
  const whiteList = config.whiteListedIps.split(',');
  if (whiteList.length && whiteList.includes(ip)) return true;
  // only operator nodes can connect
  const idx = Operator.OpNodes.findIndex((item) => item.ip === ip);
  if (idx === -1) return false;
  return true;
}
/**
* [authUser]
*/
function authUser(req) {
  let remoteIp = utill.convertIP(req.ip);
  if (!remoteIp) remoteIp = req.socket.address().address;
  let loginphrase = false;
  if (req.headers.loginphrase) {
    loginphrase = req.headers.loginphrase;
  } else {
    loginphrase = req.cookies.loginphrase;
  }
  if (loginphrase && IdService.verifySession(loginphrase, remoteIp)) {
    return true;
  }
  return false;
}
/**
 * To check if a parameter is an object and if not, return an empty object.
 * @param {*} parameter Parameter of any type.
 * @returns {object} Returns the original parameter if it is an object or returns an empty object.
 */
function ensureObject(parameter) {
  if (typeof parameter === 'object') {
    return parameter;
  }
  if (!parameter) {
    return {};
  }
  let param;
  try {
    param = JSON.parse(parameter);
  } catch (e) {
    param = qs.parse(parameter);
  }
  if (typeof param !== 'object') {
    return {};
  }
  return param;
}
/**
* Starts UI service
*/
function startUI() {
  const app = express();
  app.use(cors());
  app.use(cookieParser());
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(fileUpload());
  const limiter = RateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // max 100 requests per windowMs
  });
  fs.writeFileSync('errors.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('warnings.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('info.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('query.txt', `version: ${config.version}<br>`);
  fs.appendFileSync('debug.txt', `------------------------------------------------------<br>version: ${config.version}<br>`);

  app.options('/*', (req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Content-Length, X-Requested-With');
    res.send(200);
  });
  app.get('/', (req, res) => {
    const { host } = req.headers;
    if (host) {
      if (authUser(req)) {
        res.sendFile(path.join(__dirname, '../ui/index.html'));
      } else {
        res.sendFile(path.join(__dirname, '../ui/login.html'));
      }
    } else if (Operator.IamMaster) { // request comming from fdm
      res.send('OK');
    } else {
      res.status(500).send('Bad Request');
    }
  });

  app.get('/assets/zelID.svg', (req, res) => {
    res.sendFile(path.join(__dirname, '../ui/assets/zelID.svg'));
  });
  app.get('/assets/Flux_white-blue.svg', (req, res) => {
    res.sendFile(path.join(__dirname, '../ui/assets/Flux_white-blue.svg'));
  });
  // apply rate limiter to all requests below
  app.use(limiter);
  app.get('/logs/:file?', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    let { file } = req.params;
    file = file || req.query.file;
    const whiteList = config.whiteListedIps.split(',');
    let logFile = 'errors.txt';
    switch (file) {
      case 'info':
        logFile = 'info.txt';
        break;
      case 'warnings':
        logFile = 'warnings.txt';
        break;
      case 'debug':
        logFile = 'debug.txt';
        break;
      case 'query':
        logFile = 'query.txt';
        break;
      default:
        logFile = 'errors.txt';
        break;
    }

    if (whiteList.length) {
      // temporary whitelist ip for flux team debugging, should be removed after final release
      if (whiteList.includes(remoteIp) || remoteIp === '167.235.234.45' || remoteIp === '45.89.52.198') {
        res.send(`<html><style>
        .t {color:#2cb92c;}
        .yellow {color:yellow;}
        .red {color:#ff8100;}
        .green {color:green;}
        .magenta {color:magenta;}
        .cyan {color:cyan;}
        .lb {color:#00d0ff;}
        .m {margin-left:10px;}
        </style><body style="
          font-family: monospace;
          background-color: #2a2a32;
          color: white;
          font-size: 12;
          ">FluxDB Debug Screen<br>${fs.readFileSync(logFile).toString()}</body></html>`);
      }
    }
  });

  app.post('/rollback', async (req, res) => {
    if (authUser(req)) {
      let { seqNo } = req.body;
      seqNo = seqNo || req.query.seqNo;
      if (seqNo) {
        await Operator.rollBack(seqNo);
        res.send({ status: 'OK' });
      }
    }
  });

  app.get('/nodelist', (req, res) => {
    if (authUser(req)) {
      res.send(Operator.OpNodes);
      res.end();
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.get('/getstatus', async (req, res) => {
    res.status(403).send('Bad Request');
    /*
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('X-Accel-Buffering', 'no');
    let count = 1;
    while (true) {
      res.write(`${JSON.stringify({
        type: 'stream',
        chunk: count++,
      })}\r\n\r\n`);
      await timer.setTimeout(2000);
      // console.log(count);
    }
    */
  });

  app.get('/status', (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'X-Requested-With');
    res.send({
      status: Operator.status,
      sequenceNumber: BackLog.sequenceNumber,
      masterIP: Operator.getMaster(),
      taskStatus: BackLog.compressionTask,
    });
    res.end();
  });

  app.get('/getLogDateRange', async (req, res) => {
    if (authUser(req)) {
      res.send(await BackLog.getDateRange());
      res.end();
    } else {
      res.status(403).send('Bad Request');
    }
  });

  app.get('/getLogsByTime', async (req, res) => {
    if (authUser(req)) {
      const { starttime } = req.query;
      const { length } = req.query;
      res.send(await BackLog.getLogsByTime(starttime, length));
      res.end();
    } else {
      res.status(403).send('Bad Request');
    }
  });

  app.get('/disableWrites', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        log.info('Writes Disabled');
        Operator.status = 'DISABLED';
      }
    }
  });

  app.get('/enableWrites', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        log.info('Writes Enabled');
        Operator.status = 'OK';
      }
    }
  });

  app.get('/secret/:key', async (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        const { key } = req.params;
        const value = await BackLog.getKey(`_sk${key}`);
        if (value) {
          res.send(value);
        } else {
          res.status(404).send('Key not found');
        }
      }
    }
  });

  app.post('/secret/', async (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    let secret = req.body;
    BackLog.pushKey(`_sk${secret.key}`, secret.value, true);
    // console.log(secret.key);
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        secret = req.body;
        const value = await BackLog.pushKey(`_sk${secret.key}`, secret.value, true);
        if (value) {
          res.send('OK');
        }
      }
    }
    res.status(404).send('Key not found');
  });

  app.delete('/secret/:key', async (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        const { key } = req.params;
        if (await BackLog.removeKey(`_sk${key}`)) {
          res.send('OK');
        }
      }
    }
    res.status(404).send('Key not found');
  });
  app.get('/listbackups', async (req, res) => {
    if (authUser(req)) {
      res.send(await BackLog.listSqlFiles());
      res.end();
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.get('/getbackupfile/:filename', async (req, res) => {
    if (authUser(req)) {
      const { filename } = req.params;
      res.download(path.join(__dirname, `../dumps/${sanitize(filename)}.sql`), `${sanitize(filename)}.sql`, (err) => {
        if (err) {
          // Handle errors, such as file not found
          res.status(404).send('File not found');
        }
      });
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.get('/getbackupfile2/:filename/:filesize', async (req, res) => {
    const { filename } = req.params;
    const { filesize } = req.params;
    if (fs.existsSync(`./dumps/${sanitize(filename)}.sql`) && fs.statSync(`./dumps/${sanitize(filename)}.sql`).size === sanitize(filesize)) {
      res.download(path.join(__dirname, `../dumps/${sanitize(filename)}.sql`), `${sanitize(filename)}.sql`, (err) => {
        if (err) {
          // Handle errors, such as file not found
          res.status(404).send('File not found');
        }
      });
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/upload-sql', async (req, res) => {
    if (authUser(req)) {
      if (!req.files || !req.files.sqlFile) {
        return res.status(400).send('No file uploaded.');
      }
      const { sqlFile } = req.files;
      const uploadPath = path.join(__dirname, '../dumps/', sqlFile.name); // Adjust the destination folder as needed
      // Move the uploaded .sql file to the specified location
      sqlFile.mv(uploadPath, (err) => {
        if (err) {
          return res.status(500).send(`Error uploading file: ${err.message}`);
        }
        res.send('File uploaded successfully.');
      });
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/generatebackup', async (req, res) => {
    if (authUser(req)) {
      res.send(await BackLog.dumpBackup());
      res.end();
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/deletebackup', async (req, res) => {
    if (authUser(req)) {
      const { body } = req;
      if (body) {
        const { filename } = body;
        res.send(await BackLog.deleteBackupFile(sanitize(filename)));
        res.end();
      }
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/executebackup', async (req, res) => {
    if (authUser(req)) {
      if (Operator.IamMaster) {
        const { body } = req;
        if (body) {
          const { filename } = body;
          // create a snapshot
          // await BackLog.dumpBackup();
          // removing old db + resetting secuence numbers:
          await Operator.rollBack(0);
          await timer.setTimeout(2000);
          const importer = new SqlImporter({
            callback: Operator.sendWriteQuery,
            serverSocket: Operator.serverSocket,
          });
          importer.onProgress((progress) => {
            const percent = Math.floor((progress.bytes_processed / progress.total_bytes) * 10000) / 100;
            log.info(`${percent}% Completed`, 'cyan');
          });
          importer.setEncoding('utf8');
          await importer.import(`./dumps/${sanitize(filename)}.sql`).then(async () => {
            const filesImported = importer.getImported();
            log.info(`${filesImported.length} SQL file(s) imported.`);
            res.send('OK');
          }).catch((err) => {
            res.status(500).send(JSON.stringify(err));
            log.error(err);
          });
          res.end();
        }
      } else {
        res.status(500).send('operation is only allowed on master node');
      }
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/compressbacklog', async (req, res) => {
    if (authUser(req)) {
      if (Operator.IamMaster) {
        await Operator.comperssBacklog();
      } else {
        res.status(500).send('operation is only allowed on master node');
      }
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.get('/isloggedin/', (req, res) => {
    if (authUser(req)) {
      res.cookie('loginphrase', req.headers.loginphrase);
      res.send('OK');
    } else {
      res.status(403).send('Bad Request');
    }
  });
  app.post('/verifylogin/', (req, res) => {
    let { body } = req;
    if (body) {
      const { signature } = body;
      const { message } = body;
      if (IdService.verifyLogin(message, signature)) {
        let remoteIp = utill.convertIP(req.ip);
        if (!remoteIp) remoteIp = req.socket.address().address;
        IdService.addNewSession(message, remoteIp);
        Operator.emitUserSession('add', message, remoteIp);
        res.cookie('loginphrase', message);
        res.send('OK');
      } else {
        res.send('SIGNATURE NOT VALID');
      }
    }
    req.on('data', (data) => {
      body += data;
    });
    req.on('end', async () => {
      try {
        const processedBody = ensureObject(body);
        let { signature } = processedBody;
        if (!signature) signature = req.query.signature;
        const message = processedBody.loginPhrase || processedBody.message || req.query.message;
        if (IdService.verifyLogin(message, signature)) {
          let remoteIp = utill.convertIP(req.ip);
          if (!remoteIp) remoteIp = req.socket.address().address;
          IdService.addNewSession(message, remoteIp);
          Operator.emitUserSession('add', message, remoteIp);
          res.cookie('loginphrase', message);
          res.send('OK');
        } else {
          res.send('SIGNATURE NOT VALID');
        }
      } catch (error) {
        log.error(error);
      }
      res.send('Error');
    });
  });

  app.get('/loginphrase/', (req, res) => {
    res.send(IdService.generateLoginPhrase());
  });

  app.get('/logout/', (req, res) => {
    if (authUser(req)) {
      IdService.removeSession(req.cookies.loginphrase);
      Operator.emitUserSession('remove', req.cookies.loginphrase, '');
      res.send('OK');
    } else {
      res.status(403).send('Bad Request');
    }
  });

  if (config.ssl) {
    const keys = Security.generateRSAKey();
    const httpsOptions = {
      key: keys.pemPrivateKey,
      cert: keys.pemCertificate,
    };
    https.createServer(httpsOptions, app).listen(config.debugUIPort, () => {
      log.info(`starting SSL interface on port ${config.debugUIPort}`);
    });
  } else {
    app.listen(config.debugUIPort, () => {
      log.info(`starting interface on port ${config.debugUIPort}`);
    });
  }
}
/**
* [validate]
* @param {string} ip [description]
*/
async function validate(ip) {
  if (Operator.AppNodes.includes(ip)) return true;
  return false;
  // const validateApp = await fluxAPI.validateApp(config.DBAppName, ip);
  // if (validateApp) return true;
  // return false;
}
/**
* [initServer]
*/
async function initServer() {
  Security.init();
  startUI();
  await Operator.init();
  const io = new Server(config.apiPort, { transports: ['websocket', 'polling'], maxHttpBufferSize: 4 * 1024 * 1024 });
  // const app = new App();
  // io.attachApp(app);
  Operator.setServerSocket(io);

  io.on('connection', async (socket) => {
    const ip = utill.convertIP(socket.handshake.address);
    log.debug(`connection from ${ip}`, 'red');
    if (auth(ip)) {
      // log.info(`validating ${ip}: ${await auth(ip)}`);
      socket.on('disconnect', (reason) => {
        log.info(`disconnected from ${ip}`, 'red');
      });
      socket.on('getStatus', async (callback) => {
        // log.info(`getStatus from ${ip}`);
        callback({
          status: Operator.status,
          sequenceNumber: BackLog.sequenceNumber,
          remoteIP: utill.convertIP(socket.handshake.address),
          masterIP: Operator.getMaster(),
        });
      });
      socket.on('getMaster', async (callback) => {
        // log.info(`getMaster from ${ip}`);
        callback({ status: 'success', message: Operator.getMaster() });
      });
      socket.on('getMyIp', async (callback) => {
        // log.info(`getMyIp from ${ip}`);
        callback({ status: 'success', message: utill.convertIP(socket.handshake.address) });
      });
      socket.on('getBackLog', async (start, callback) => {
        const records = await BackLog.getLogs(start, 200);
        callback({ status: Operator.status, sequenceNumber: BackLog.sequenceNumber, records });
      });
      socket.on('writeQuery', async (query, connId, callback) => {
        log.info(`writeQuery from ${utill.convertIP(socket.handshake.address)}:${connId}`);
        const result = await BackLog.pushQuery(query);
        // log.info(`forwarding query to slaves: ${JSON.stringify(result)}`);
        socket.broadcast.emit('query', query, result[1], result[2], false);
        socket.emit('query', query, result[1], result[2], connId);
        // cache write queries for 20 seconds
        queryCache.put(result[1], {
          query, seq: result[1], timestamp: result[2], connId, ip,
        }, 1000 * 60);
        callback({ status: Operator.status, result: result[0] });
      });
      socket.on('askQuery', async (index, callback) => {
        log.info(`${ip} asking for seqNo: ${index}`, 'magenta');
        const record = queryCache.get(index);
        let connId = false;
        if (record) {
          if (record.ip === ip && record.connId) connId = record.connId;
          log.info(`sending query: ${index}`, 'magenta');
          socket.emit('query', record.query, record.seq, record.timestamp, connId);
        } else {
          log.warn(`query ${index} not in query cache`, 'red');
          let BLRecord = BackLog.BLqueryCache.get(index);
          log.info(JSON.stringify(BLRecord), 'red');
          if (!BLRecord) {
            BLRecord = await BackLog.getLog(index);
            log.info(`from DB : ${JSON.stringify(BLRecord)}`, 'red');
            try {
              socket.emit('query', BLRecord[0].query, BLRecord[0].seq, BLRecord[0].timestamp, connId);
            } catch (err) {
              log.error(JSON.stringify(err));
            }
          }
        }
        callback({ status: Operator.status });
      });
      socket.on('shareKeys', async (pubKey, callback) => {
        const nodeip = utill.convertIP(socket.handshake.address);
        // log.info(`shareKeys from ${nodeip}`);
        let nodeKey = null;
        if (!(`N${nodeip}` in Operator.keys)) {
          Operator.keys = await BackLog.getAllKeys();
          if (`N${nodeip}` in Operator.keys) nodeKey = Operator.keys[`N${nodeip}`];
          if (nodeKey) nodeKey = Security.publicEncrypt(pubKey, Buffer.from(nodeKey, 'hex'));
        }
        callback({
          status: Operator.status,
          commAESKey: Security.publicEncrypt(pubKey, Security.getCommAESKey()),
          commAESIV: Security.publicEncrypt(pubKey, Security.getCommAESIv()),
          key: nodeKey,
        });
      });
      socket.on('updateKey', async (key, value, callback) => {
        const decKey = Security.decryptComm(key);
        log.info(`updateKey from ${decKey}`);
        await BackLog.pushKey(decKey, value);
        Operator.keys[decKey] = value;
        socket.broadcast.emit('updateKey', key, value);
        callback({ status: Operator.status });
      });
      socket.on('getKeys', async (callback) => {
        const keysToSend = {};
        const nodeip = utill.convertIP(socket.handshake.address);
        for (const key in Operator.keys) {
          if ((key.startsWith('N') || key.startsWith('_')) && key !== `N${nodeip}`) {
            keysToSend[key] = Operator.keys[key];
          }
        }
        keysToSend[`N${Operator.myIP}`] = Security.encryptComm(`${Security.getKey()}:${Security.getIV()}`);
        callback({ status: Operator.status, keys: Security.encryptComm(JSON.stringify(keysToSend)) });
      });
      socket.on('resetMaster', async (callback) => {
        if (Operator.IamMaster) {
          Object.keys(io.sockets.sockets).forEach((s) => {
            io.sockets.sockets[s].disconnect(true);
          });
          Operator.findMaster();
        }
        callback({ status: Operator.status });
      });
      socket.on('rollBack', async (seqNo, callback) => {
        if (Operator.IamMaster) {
          Operator.rollBack(seqNo);
        }
        callback({ status: Operator.status });
      });
      socket.on('userSession', async (op, key, value, callback) => {
        if (op === 'add') { IdService.addNewSession(key, value); } else { IdService.removeSession(key); }
        socket.broadcast.emit('userSession', op, key, value);
        callback({ status: Operator.status });
      });
    } else {
      log.warn(`rejected from ${ip}`);
      socket.disconnect();
    }
    if (await validate(ip)) {
      // log.info(`auth: ${ip} is validated`);
    } else {
      log.warn(`validation failed for ${ip}`, 'red');
      socket.disconnect();
    }
  });
  IdService.init();
  log.info(`Api Server started on port ${config.apiPort}`);
  await Operator.findMaster();
  log.info(`find master finished, master is ${Operator.masterNode}`);
  if (!Operator.IamMaster) {
    Operator.initMasterConnection();
  }
  setInterval(async () => {
    Operator.doHealthCheck();
  }, 120000);
  setInterval(async () => {
    // Operator.doCompressCheck();
  }, 2 * 60 * 60 * 1000); // 2 hour
  setInterval(async () => {
    BackLog.purgeBinLogs();
  }, 48 * 60 * 60 * 1000);
}

initServer();
