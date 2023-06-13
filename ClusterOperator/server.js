/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-unused-vars */
const { Server } = require('socket.io');
const timer = require('timers/promises');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const queryCache = require('memory-cache');
const Operator = require('./Operator');
const BackLog = require('./Backlog');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const Security = require('./Security');
const fluxAPI = require('../lib/fluxAPI');

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
function authUser() {
  return false;
}
/**
* Starts UI service
*/
function startUI() {
  const app = express();
  app.use(cors());
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  fs.writeFileSync('errors.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('warnings.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('info.txt', `version: ${config.version}<br>`);
  fs.appendFileSync('debug.txt', `------------------------------------------------------<br>version: ${config.version}<br>`);

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
      default:
        logFile = 'errors.txt';
        break;
    }
    if (whiteList.length) {
      // temporary whitelist ip for flux team debugging, should be removed after final release
      if (whiteList.includes(remoteIp) || remoteIp === '206.79.215.43' || remoteIp === '45.89.52.198') {
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
    let remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (req.headers['x-forwarded-for']) {
      remoteIp = req.headers['x-forwarded-for'];
    }
    let { seqNo } = req.body;
    seqNo = seqNo || req.query.seqNo;
    console.log(req.body);
    console.log(seqNo);
    if (whiteList.length && seqNo) {
      if (whiteList.includes(remoteIp)) {
        console.log(seqNo);
        await Operator.rollBack(seqNo);
        res.send({ status: 'OK' });
      }
    }
  });

  app.get('/stats', (req, res) => {
    let remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (req.headers['x-forwarded-for']) {
      remoteIp = req.headers['x-forwarded-for'];
    }
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        res.send(Operator.OpNodes);
      }
    }
  });

  app.get('/getLogDateRange', async (req, res) => {
    let remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (req.headers['x-forwarded-for']) {
      remoteIp = req.headers['x-forwarded-for'];
    }
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        res.send(await BackLog.getDateRange());
      }
    }
  });

  app.get('/getLogsByTime', async (req, res) => {
    let remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (req.headers['x-forwarded-for']) {
      remoteIp = req.headers['x-forwarded-for'];
    }
    const { starttime } = req.query;
    const { length } = req.query;
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        res.send(await BackLog.getLogsByTime(starttime, length));
      }
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

  app.get('/secret/:key', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        const { key } = req.params;
        const value = BackLog.getKey(`k_${key}`);
        if (value) {
          res.send(value);
        } else {
          res.status(404).send('Key not found');
        }
      }
    }
  });

  app.post('/secret/', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    let secret = req.body;
    let value = BackLog.pushKey(`k_${secret.key}`, secret.value, true);
    // console.log(secret.key);
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        secret = req.body;
        value = BackLog.pushKey(`k_${secret.key}`, secret.value);
        if (value) {
          res.send('OK');
        }
      }
    }
    res.status(404).send('Key not found');
  });

  app.delete('/secret/:key', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        const { key } = req.params;
        if (BackLog.removeKey(`k_${key}`)) {
          res.send('OK');
        }
      }
    }
    res.status(404).send('Key not found');
  });

  app.get('/', (req, res) => {
    let remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (req.headers['x-forwarded-for']) {
      remoteIp = req.headers['x-forwarded-for'];
    }
    // log.info(JSON.stringify(req.headers));
    // log.info(`UI access from ${remoteIp}`);
    if (authUser()) {
      res.sendFile(path.join(__dirname, '../ui/index.html'));
    } else {
      res.sendFile(path.join(__dirname, '../ui/login.html'));
    }
  });

  app.get('/assets/zelID.svg', (req, res) => {
    res.sendFile(path.join(__dirname, '../ui/assets/zelID.svg'));
  });

  app.listen(config.debugUIPort, () => {
    log.info(`starting interface on port ${config.debugUIPort}`);
  });
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
  const io = new Server(config.apiPort);
  // const app = new App();
  // io.attachApp(app);
  Operator.setServerSocket(io);

  io.on('connection', async (socket) => {
    const ip = utill.convertIP(socket.handshake.address);
    if (auth(ip)) {
      // log.info(`validating ${ip}: ${await auth(ip)}`);
      socket.on('disconnect', (reason) => {
        // log.info(`disconnected from ${ip}`);
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
        /*
        if (BackLog.writeLock) {
          const myTicket = Operator.getTicket();
          log.info(`put into queue: ${myTicket}, in queue: ${Operator.masterQueue.length}`, 'cyan');
          Operator.masterQueue.push(myTicket);
          while (BackLog.writeLock || Operator.masterQueue[0] !== myTicket) {
            await timer.setTimeout(5);
          }
          BackLog.writeLock = true;
          Operator.masterQueue.shift();
          log.info(`out of queue: ${myTicket}, in queue: ${Operator.masterQueue.length}`, 'cyan');
        }
        */
        const result = await BackLog.pushQuery(query);
        // log.info(`forwarding query to slaves: ${JSON.stringify(result)}`);
        socket.broadcast.emit('query', query, result[1], result[2], false);
        socket.emit('query', query, result[1], result[2], connId);
        // cache write queries for 20 seconds
        queryCache.put(result[1], {
          query, seq: result[1], timestamp: result[2], connId, ip,
        }, 1000 * 20);
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
          // record = await BackLog.getLog(index);
        }
        // if (record) {

        // log.info(`record type: ${Array.isArray(record)}`, 'magenta');
        // if (Array.isArray(record)) {
        // socket.emit('query', record[0].query, record[0].seq, record[0].timestamp, false);
        // log.warn(`query ${index} not in query cache`, 'red');
        // } else {

        // }
        // }
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
    } else {
      // log.info(`rejected from ${ip}`);
      socket.disconnect();
    }
    if (await validate(ip)) {
      // log.info(`auth: ${ip} is validated`);
    } else {
      log.info(`validation failed for ${ip}`, 'red');
      socket.disconnect();
    }
  });
  /*
  app.listen(config.apiPort, (token) => {
    if (!token) {
      log.warn(`port ${config.apiPort} already in use`);
    }
  });
 */
  log.info(`Api Server started on port ${config.apiPort}`);
  //await Operator.findMaster();
  log.info(`find master finished, master is ${Operator.masterNode}`);
  if (!Operator.IamMaster) {
    Operator.initMasterConnection();
  }
  setInterval(async () => {
    Operator.doHealthCheck();
  }, 120000);
}

initServer();
