const Operator = require('./Operator');
const { WebSocketServer } = require('ws');
const log = require('../lib/log');
const config = require('./config');




const wss = new WebSocketServer({ port: config.apiPort });
let clients = [];

function handleAPICommand(ws, command, message){
  switch (command) {
    case 'GET_MASTER':
      ws.send(`{"status":"success","message":"${Operator.getMaster()}"}`);
      break;
    case 'GET_MYIP':
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

function auth(ip){
  //only operator nodes can connect
  //let idx = Operator.OpNodes.findIndex(item => item.ip==ip);
  //if(idx === -1) return false;
  //only one connection per ip allowed
  idx = clients.findIndex(item => item.ip==ip);
  if(idx === -1) return true; else return false;
}






async function initServer(){
  await Operator.init();

  wss.on('connection', function connection(ws, req) {
    const ip = req.socket.remoteAddress;
    
    if(auth(ip)){
      clients.push({ws:ws, ip:ip});
      ws.isAlive = true;
      ws.on('pong', function heartbeat() {
        this.isAlive = true;
      });
      ws.on('message', function message(data) {
        log.info(`received: ${data}`);
        try{
          let jsonData = JSON.parse(data);
          handleAPICommand(ws, jsonData.command, jsonData.message);
        }catch(err){
          log.info('Unrecognized command.');
        }
      });
      ws.on('close', function close() {    
        let idx = clients.findIndex(item => item.ws==ws);
        log.info(`socket from ${clients[idx].ip} closed.`);
        clients = clients.splice(idx,0); 
      });
      log.info(`socket connected from ${ip}`);
      ws.send(`{"status":"connected","from":"${ip}"}`);
    }else{
      log.info(`socket connection rejected from ${ip}`);
      ws.terminate();
    }
  });
  
  const interval = setInterval(function ping() {
    wss.clients.forEach(function each(ws) {
      if (ws.isAlive === false) {
        let idx = clients.findIndex(item => item.ws==ws);
        log.info(`connection from ${clients[idx].ip} timed out, terminating socket.`);
        clients = clients.splice(idx,0); 
        return ws.terminate();
      }
  
      ws.isAlive = false;
      ws.ping();
    });
  }, 3000);
  
  wss.on('close', function close() {
    clearInterval(interval);
  });
  log.info(`Api Server started on port ${config.apiPort}`);
  await Operator.findMaster();
}

initServer();


