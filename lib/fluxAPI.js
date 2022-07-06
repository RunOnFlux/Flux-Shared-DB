const axios = require('axios');
const log = require('./log');
const WebSocket = require('ws');
const { io } = require("socket.io-client");
const utill = require('./utill');
const timer = require('timers/promises');
/**
 * [getApplicationSpecs Retrieves app specifications]
 * @param {string} appName [description]
 * @return {Array}         [description]
 */
async function getApplicationSpecs(appName) {
  try {
    const fluxnodeList = await axios.get(`https://api.runonflux.io/apps/appspecifications/${appName}`, { timeout: 13456 });
    if (fluxnodeList.data.status === 'success') {
      return fluxnodeList.data.data || [];
    }
    return [];
  } catch (e) {
    log.error(e);
    return [];
  }
}

/**
 * [getApplicationIP Retrieves IP's that a given application is running on]
 * @param {string} appName [description]
 * @return {Array}         [description]
 */
 async function getApplicationIP(appName) {
  try {
    const fluxnodeList = await axios.get(`https://api.runonflux.io/apps/location/${appName}`, { timeout: 13456 });
    if (fluxnodeList.data.status === 'success') {
      return fluxnodeList.data.data || [];
    }
    return [];
  } catch (e) {
    log.error(e);
    return [];
  }
}

/**
 * [getMaster ]
 * @param {string} ip [description]
 * @param {string} port [description]
 * @return {json}         [description]
 */
 async function getMaster(ip, port, key) {
  try {
    
    return new Promise(function (resolve) {
      const client = io.connect(`http://${ip}:${port}`, { transports: ["websocket"], reconnection: false, timeout:1000});
      const timeout = setTimeout(function(){
        client.disconnect();
        resolve(null);
      }, 1000); 
      client.emit("getMaster", (response) => {
        client.disconnect();
        clearTimeout(timeout);
        resolve(response.message);
      }); 
    });
    /*
    const client = new WebSocket(`ws://${ip}:${port}`,{handshakeTimeout:1000});
    //log.info(`connecting ${ip}:${port}`);
    wsStatus = await new Promise(resolve => {
      client.once('open', resolve);
      client.on('error', (error) => {
        //log.error(error);
        resolve(null);
      })
    });
    log.info(`connected ${ip}:${port}`);
    client.send(JSON.stringify({ command: 'GET_MASTER', message: null }));
    return new Promise(resolve => {  
      client.on('message', function message(data, isBinary) {
        let JsonData = JSON.parse(data);
        //log.info(`message recieved: ${data}`);
        if(JsonData.status==='success'){
          client.terminate();
          resolve(JsonData.message);
        }
      });
      client.on('error', (error) => {
        //log.info(`error in client`);
        //log.error(error);
        resolve(null);
      });
    });
    */

  } catch (e) {
    log.error(e);
    return [];
  }
}

/**
 * [getMyIp]
 * @param {string} ip [description]
 * @param {string} port [description]
 * @return {json}         [description]
 */
 async function getMyIp(ip, port) {
  try {
    return new Promise(async function (resolve) {
      const client = io.connect(`http://${ip}:${port}`,{ transports: ["polling"], reconnection: false, timeout:1000});
      
      const timeout = setTimeout(function(){
        client.disconnect();
        resolve(null);
      }, 1000); 
      client.on("connect_error", (reason) => {
        log.error(reason);
        clearTimeout(timeout);
        resolve(null);
      });
      client.emit("getMyIp", (response) => {
        client.disconnect();
        clearTimeout(timeout);
        resolve(response.message);
      });

       
    });
    /*
    const client = new WebSocket(`ws://${ip}:${port}`,{handshakeTimeout:2000});
    //log.info(`connecting ${ip}:${port}`);
    wsStatus = await new Promise(resolve => {
      client.once('open', resolve);
      client.on('error', (error) => {
        log.error(error);
        resolve(null);
      })
    });

    if(wsStatus===null) return wsStatus;
    
    console.log(`ws connected to ${ip}:${port}`);
    client.send(JSON.stringify({ command: 'GET_MYIP', message: null }));
    
    return new Promise(resolve => {  
      client.on('message', function message(data, isBinary) {
        let JsonData = JSON.parse(data);
        log.info(`message recieved: ${data}`);
        if(JsonData.status==='success'){
          client.terminate();
          var myip = utill.convertIP(JsonData.message);
          resolve(myip);
        }else{
          //log.info(`connection rejected.`);
          resolve(null);
        }
      });
      client.on('error', (error) => {
        //log.info(`error in client`);
        log.error(error);
        resolve(null);
      })
      
    });*/

  } catch (e) {
    log.error(`socket connection failed.`);
    log.error(e);
    return null;
  }
}

module.exports = {
  getApplicationIP,
  getApplicationSpecs,
  getMaster,
  getMyIp,
};