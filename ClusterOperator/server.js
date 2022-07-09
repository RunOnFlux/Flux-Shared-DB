const Operator = require('./Operator');
const { Server } = require("socket.io");
const BackLog = require('./Backlog');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const express = require('express');
const fs = require('fs');



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
      ">FluxDB Debug Screen<br>${utill.htmlEscape(fs.readFileSync('logs.txt').toString())}</body></html>`);
  //}
})

app.listen(config.debugUIPort, () => {
  log.info(`starting debug interface on port ${config.debugUIPort}`);
})


async function initServer(){
  await Operator.init();
  const io = new Server(config.apiPort);
  Operator.setServerSocket(io);
  
  io.on("connection", (socket) => {
    
    var ip = utill.convertIP(socket.handshake.address);
    if(utill.auth(ip)){
      console.info(`Client connected [id=${socket.id}, ip=${ip}]`);
      socket.on("disconnect", (reason) => {
      });
      socket.on("getStatus", (callback) => {
        callback({status: "ok"});
      });
      socket.on("getMyIp", (callback) => {
        callback({status: "success", message: utill.convertIP(socket.handshake.address)});
      });
      socket.on("getMaster", (callback) => {
        callback({status: "success", message: Operator.getMaster()});
      });
      socket.on("getBackLog", async (start, callback) => {
        log.info(`getBackLog from ${utill.convertIP(socket.handshake.address)} : ${start}`);
        const records = await BackLog.getLogs(start, 100);
        log.info(`backlog records: ${JSON.stringify(records)}`);
        callback({status: "success", sequenceNumber: BackLog.sequenceNumber,  records: records});
      });
      socket.on("writeQuery", async (query, callback) => {
        log.info(`writeQuery from ${utill.convertIP(socket.handshake.address)} : ${query}`);
        const result = await BackLog.pushQuery(query);
        log.info(`forwarding query to slaves: ${query}`);
        io.emit("query", query);
        callback({status: "success", sequenceNumber: BackLog.sequenceNumber, result});
      });
    }else{
      socket.disconnect();
    }
  });
  
  log.info(`Api Server started on port ${config.apiPort}`);
  await Operator.findMaster();
  log.info(`find master finished, master is ${Operator.masterNode}`);
  if(!Operator.IamMaster){
    Operator.initMasterConnection();
  }
  const updateAppInterval =  setInterval(async function() {
    const x = Operator.doHealthCheck();
  }, 120000);
}

initServer();



