const axios = require('axios');
const log = require('./log');
const WebSocket = require('ws');
const utill = require('./utill');
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
    const client = new WebSocket(`ws://${ip}:${port}`);
    await new Promise(resolve => client.once('open', resolve));

    client.send(JSON.stringify({ command: 'GET_MASTER', message: null }));
    return new Promise(resolve => {  
      client.on('message', function message(data, isBinary) {
        let JsonData = JSON.parse(data);
        if(JsonData.status==='success'){
          client.terminate();
          resolve(JsonData);
        }
      });
    });

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
    const client = new WebSocket(`ws://${ip}:${port}`);
    await new Promise(resolve => client.once('open', resolve));
    client.send(JSON.stringify({ command: 'GET_MASTER', message: null }));
    console.log(`ws connected to ${ip}:${port}`);
    return new Promise(resolve => {  
      client.on('message', function message(data, isBinary) {
        let JsonData = JSON.parse(data);
        console.log(`message recieved ${JsonData}`);
        if(JsonData.status==='connected'){
          client.terminate();
          var myip = utill.convertIP(JsonData.from);
          resolve(myip);
        }
      });
    });

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