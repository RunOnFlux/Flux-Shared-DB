const Operator = require('./Operator');
//const { WebSocketServer } = require('ws');
const { Server } = require("socket.io");
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


//const wss = new WebSocketServer({ port: config.apiPort });
let clients = [];
/*
function handleAPICommand(ws, command, message){
  switch (command) {
    case 'GET_MASTER':
      log.info(`sending masterIP: ${JSON.stringify(Operator.getMaster())}`);
      ws.send(`{"status":"success","message":${JSON.stringify(Operator.getMaster())}}`);
      break;
    case 'GET_MYIP':
      let idx = clients.findIndex(item => item.ws==ws);
      if(idx>=0){
        //log.info(`sending remoteIp: ${clients[idx].ip}`);
        ws.send(`{"status":"success","message":"${clients[idx].ip}"}`);
      }else{
        //log.info(`ws and ip not found, sending null`);
        ws.send(`{"status":"success","message":"null"}`);
      }
      break;
    case 'GET_BACKLOG':
      break;
    case 'QUERY':
      break;
    default:
      log.info(`Unknown Command: ${command}`);
      break;
  }
}
*/

function auth(ip){
  const whiteList = config.whiteListedIps.split(',');
  if(whiteList.length && whiteList.includes(ip) || ip.startsWith('80.239.140.')) return true;
  //only operator nodes can connect
  let idx = Operator.OpNodes.findIndex(item => item.ip==ip);
  if(idx === -1) return false;
  //only one connection per ip allowed
  idx = clients.findIndex(item => item.ip==ip);
  if(idx === -1) return true; else return false;
}

async function initServer(){

  const io = new Server(config.apiPort);
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
    }else{
      log.info(`socket connection rejected from ${ip}`);
      socket.disconnect();
    }
  });
  
/*
  wss.on('connection', function connection(ws, req) {
    var ip = utill.convertIP(req.socket.remoteAddress);

    if(auth(ip)){
      clients.push({ws:ws, ip:ip});
      ws.isAlive = true;
      ws.on('pong', function heartbeat() {
        this.isAlive = true;
      });
      ws.on('message', function message(data) {
        //log.info(`received: ${data}`);
        try{
          let jsonData = JSON.parse(data);
          handleAPICommand(ws, jsonData.command, jsonData.message);
        }catch(err){
          log.info(`Unrecognized command:${data}, ${err}`);
        }
      });
      ws.on('close', function close() {    
        let idx = clients.findIndex(item => item.ws==ws);
        //log.info(`socket closed id:${idx}`);
        if(idx>=0){
          log.info(`socket from ${clients[idx].ip} closed.`);
          clients = clients.splice(idx,0); 
        }
      });
      ws.on('error', (error) => {
        //log.info(`error in ws`);
        //log.error(error);
        ws.terminate();
      })
      //log.info(`socket connected from ${ip}`);
      //ws.send(`{"status":"connected","from":"${ip}"}`);
    }else{
      log.info(`socket connection rejected from ${ip}`);
      ws.send(`{"status":"rejected"}`);
      ws.terminate();
    }
  });
  wss.on('error', (error) => {
    log.info(`error in wss`);
    log.error(error);
  })
  
  const interval = setInterval(function ping() {
    wss.clients.forEach(function each(ws) {
      try{
        if (ws.isAlive === false) {
          let idx = clients.findIndex(item => item.ws==ws);
          log.info(`connection from ${clients[idx].ip} timed out, terminating socket.`);
          clients = clients.splice(idx,0); 
          return ws.terminate();
        }
    
        ws.isAlive = false;
        ws.ping();
        log.info(`sending ping`);
      }catch{error}{
        log.error(error);
      }
    });
  }, 5000);
  
  wss.on('close', function close() {
    clearInterval(interval);
  });
  */
  log.info(`Api Server started on port ${config.apiPort}`);
  await Operator.init();
  await Operator.findMaster();
  console.log(`find master finished, master is ${Operator.masterNode}`);
  
  if(Operator.IamMaster){

  }else{
    //connect to master
    Operator.initMasterConnection();

    //sync database

    //relay data
  }
}

initServer();
const updateAppInterval =  setInterval(async function() {
  const x = Operator.doHealthCheck();
}, 120000);


