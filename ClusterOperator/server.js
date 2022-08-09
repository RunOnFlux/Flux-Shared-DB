/* eslint-disable no-unused-vars */
const { Server } = require('socket.io');
const express = require('express');
const fs = require('fs');
const Operator = require('./Operator');
const BackLog = require('./Backlog');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');

/**
* Starts UI service
*/
function startUI() {
  const app = express();
  fs.writeFileSync('logs.txt', `version: ${config.version}\n`);

  app.get('/', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length) {
      if (whiteList.includes(remoteIp)) {
        res.send(`<html><body style="
          font-family: monospace;
          background-color: #404048;
          color: white;
          font-size: 12;
          ">FluxDB Debug Screen<br>${utill.htmlEscape(fs.readFileSync('logs.txt').toString())}</body></html>`);
      }
    }
  });

  app.listen(config.debugUIPort, () => {
    log.info(`starting debug interface on port ${config.debugUIPort}`);
  });
}
/**
* [auth]
* @param {string} ip [description]
*/
function auth(ip) {
  const whiteList = config.whiteListedIps.split(',');
  if ((whiteList.length && whiteList.includes(ip))) return true;
  // only operator nodes can connect
  const idx = Operator.OpNodes.findIndex((item) => item.ip === ip);
  if (idx === -1) return false;
  // only one connection per ip allowed
  // idx = clients.findIndex(item => item.ip==ip);
  // if(idx === -1) return true; else return false;
  return true;
}
/**
* [initServer]
*/
async function initServer() {
  startUI();
  await Operator.init();
  const io = new Server(config.apiPort);
  Operator.setServerSocket(io);

  io.on('connection', (socket) => {
    const ip = utill.convertIP(socket.handshake.address);
    if (auth(ip)) {
      console.info(`Client connected [id=${socket.id}, ip=${ip}]`);
      socket.on('disconnect', (reason) => {
      });
      socket.on('getStatus', (callback) => {
        callback({
          status: Operator.status,
          sequenceNumber: BackLog.sequenceNumber,
          remoteIP: utill.convertIP(socket.handshake.address),
          masterIP: Operator.getMaster(),
        });
      });
      socket.on('getMyIp', (callback) => {
        callback({ status: 'success', message: utill.convertIP(socket.handshake.address) });
      });
      socket.on('getMaster', (callback) => {
        callback({ status: 'success', message: Operator.getMaster() });
      });
      socket.on('getBackLog', async (start, callback) => {
        log.info(`getBackLog from ${utill.convertIP(socket.handshake.address)} : ${start}`);
        const records = await BackLog.getLogs(start, 100);
        // log.info(`backlog records: ${JSON.stringify(records)}`);
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
    } else {
      socket.disconnect();
    }
  });

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
