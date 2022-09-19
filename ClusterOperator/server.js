/* eslint-disable no-restricted-syntax */
/* eslint-disable no-unused-vars */
const { App } = require('uWebSockets.js');
const { Server } = require('socket.io');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const e = require('express');
const Operator = require('./Operator');
const BackLog = require('./Backlog');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const Security = require('./Security');
const fluxAPI = require('../lib/fluxAPI');

/**
* Starts UI service
*/
function startUI() {
  const app = express();
  app.use(cors());
  app.use(bodyParser.json());
  fs.writeFileSync('logs.txt', `version: ${config.version}\n`);

  app.get('/logs', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      //if (whiteList.includes(remoteIp)) {
        res.send(`<html><body style="
          font-family: monospace;
          background-color: #404048;
          color: white;
          font-size: 12;
          ">FluxDB Debug Screen<br>${utill.htmlEscape(fs.readFileSync('logs.txt').toString())}</body></html>`);
      //}
    }
  });

  app.listen(config.debugUIPort, () => {
    log.info(`starting interface on port ${config.debugUIPort}`);
  });
}
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
* [validate]
* @param {string} ip [description]
*/
async function validate(ip) {
  const validateApp = await fluxAPI.validateApp(config.DBAppName, ip);
  if (validateApp) return true;
  return false;
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
      // socket.on('disconnect', (reason) => {
      //   log.info(`disconnected from ${ip}`);
      // });
      socket.on('getStatus', async (callback) => {
        log.info(`getStatus from ${ip}`);
        callback({
          status: Operator.status,
          sequenceNumber: BackLog.sequenceNumber,
          remoteIP: utill.convertIP(socket.handshake.address),
          masterIP: Operator.getMaster(),
        });
      });
      socket.on('getMaster', async (callback) => {
        log.info(`getMaster from ${ip}`);
        callback({ status: 'success', message: Operator.getMaster() });
      });
      socket.on('getMyIp', async (callback) => {
        log.info(`getMyIp from ${ip}`);
        callback({ status: 'success', message: utill.convertIP(socket.handshake.address) });
      });
      socket.on('getBackLog', async (start, callback) => {
        const records = await BackLog.getLogs(start, 200);
        callback({ status: Operator.status, sequenceNumber: BackLog.sequenceNumber, records });
      });
      socket.on('writeQuery', async (query, connId, callback) => {
        log.info(`writeQuery from ${utill.convertIP(socket.handshake.address)}:${connId}`);
        const result = await BackLog.pushQuery(query);
        log.info(`forwarding query to slaves: ${JSON.stringify(result)}`);
        socket.broadcast.emit('query', query, result[1], result[2], false);
        socket.emit('query', query, result[1], result[2], connId);
        // io.emit('query', query, result[1], result[2]);
        callback({ status: Operator.status, result: result[0] });
      });
      socket.on('shareKeys', async (pubKey, callback) => {
        const nodeip = utill.convertIP(socket.handshake.address);
        log.info(`shareKeys from ${nodeip}`);
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
    } else {
      // log.info(`rejected from ${ip}`);
      socket.disconnect();
    }
    if (await validate(ip)) {
      // log.info(`auth: ${ip} is validated`);
    } else {
      // log.info(`validation failed for ${ip}`);
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
  await Operator.findMaster();
  log.info(`find master finished, master is ${Operator.masterNode}`);
  if (!Operator.IamMaster) {
    Operator.initMasterConnection();
  }
  setInterval(async () => {
    Operator.doHealthCheck();
  }, 120000);
}

initServer();
