const Operator = require('./Operator');
const { Server } = require("socket.io");
const BackLog = require('./Backlog');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const express = require('express');
const fs = require('fs');

function htmlEscape(text) {
  return text.replace(/&/g, '&amp;').
    replace(/</g, '&lt;').
    replace(/"/g, '&quot;').
    replace(/'/g, '&#039;').
    replace(/\n/g, '</br>');
}

const app = express();
fs.writeFileSync('logs.txt', `version: ${config.version}\n`);

app.get('/', (req, res) => {
  const remoteIp = utill.convertIP(req.ip);
  //const whiteList = config.whiteListedIps.split(',');
  //if(whiteList.length){
    //if(whiteList.includes(remoteIp))
      res.send(`<html><body style="
      font-family: monospace;
      background-color: #404048;
      color: white;
      font-size: 12;
      ">FluxDB Debug Screen<br>${htmlEscape(fs.readFileSync('logs.txt').toString())}</body></html>`);
  //}
})

app.listen(config.debugUIPort, () => {
  log.info(`starting debug interface on port ${config.debugUIPort}`);
})


function auth(ip){
  const whiteList = config.whiteListedIps.split(',');
  if(whiteList.length && whiteList.includes(ip) || ip.startsWith('80.239.140.')) return true;
  //only operator nodes can connect
  let idx = Operator.OpNodes.findIndex(item => item.ip==ip);
  if(idx === -1) return false;
  //only one connection per ip allowed
  //idx = clients.findIndex(item => item.ip==ip);
  //if(idx === -1) return true; else return false;
  return true;
}

async function initServer(){
  await Operator.init();
  const io = new Server(config.apiPort);
  Operator.setServerSocket(io);
  
  io.on("connection", (socket) => {
    
    var ip = utill.convertIP(socket.handshake.address);
    if(auth(ip)){
      console.info(`Client connected [id=${socket.id}, ip=${ip}]`);
      socket.on("disconnect", (reason) => {
        log.info(`${utill.convertIP(socket.handshake.address)} disconnected ${reason}`);
      });
      socket.on("getStatus", (callback) => {
        callback({status: "ok"});
      });
      socket.on("getMyIp", (callback) => {
        log.info(`sending remoteIp: ${utill.convertIP(socket.handshake.address)}`);
        callback({status: "success", message: utill.convertIP(socket.handshake.address)});
      });
      socket.on("getMaster", (callback) => {
        log.info(`sending masterIP: ${JSON.stringify(Operator.getMaster())}`);
        callback({status: "success", message: Operator.getMaster()});
      });
      socket.on("getBackLog", (start, callback) => {
        console.log(`getBackLog from ${utill.convertIP(socket.handshake.address)} : ${start}`);
        const records = BackLog.getLogs(start, 100);
        console.log(`backlog records: ${JSON.stringify(records)}`);
        callback({status: "success", sequenceNumber: BackLog.sequenceNumber,  records: JSON.stringify(records)});
      });
      socket.on("writeQuery", (query, callback) => {
        console.log(`writeQuery from ${utill.convertIP(socket.handshake.address)} : ${query}`);
        const result = BackLog.pushQuery(query);
        socket.emit("writeQuery", query);
        callback({status: "success", sequenceNumber: BackLog.sequenceNumber,  records: JSON.stringify(result)});
      });
    }else{
      log.info(`socket connection rejected from ${ip}`);
      socket.disconnect();
    }
  });
  

  log.info(`Api Server started on port ${config.apiPort}`);
  
  await Operator.findMaster();
  console.log(`find master finished, master is ${Operator.masterNode}`);
  
  if(Operator.IamMaster){

  }else{

    Operator.initMasterConnection();
  }
}

initServer();
const updateAppInterval =  setInterval(async function() {
  const x = Operator.doHealthCheck();
}, 120000);


