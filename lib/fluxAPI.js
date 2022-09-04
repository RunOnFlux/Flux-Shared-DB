const axios = require('axios');
const { io } = require('socket.io-client');
const log = require('./log');
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
async function getMaster(ip, port) {
  try {
    return new Promise((resolve) => {
      const client = io.connect(`http://${ip}:${port}`, { transports: ['websocket'], reconnection: false, timeout: 3000 });
      const timeout = setTimeout(() => {
        client.disconnect();
        resolve(null);
      }, 3000);
      client.emit('getMaster', (response) => {
        client.disconnect();
        clearTimeout(timeout);
        resolve(response.message);
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
    return new Promise((resolve) => {
      const client = io.connect(`http://${ip}:${port}`, { transports: ['websocket'], reconnection: false, timeout: 3000 });

      const timeout = setTimeout(() => {
        client.disconnect();
        resolve(null);
      }, 3000);
      client.on('connect_error', (reason) => {
        log.error(reason);
        clearTimeout(timeout);
        resolve(null);
      });
      client.emit('getMyIp', (response) => {
        client.disconnect();
        clearTimeout(timeout);
        resolve(response.message);
      });
    });
  } catch (e) {
    log.error('socket connection failed.');
    log.error(e);
    return null;
  }
}

/**
 * [getBackLog]
 * @param {string} index [description]
 * @param {socket} socket [description]
 * @return {json}        [description]
 */
async function getBackLog(index, socket) {
  try {
    return new Promise((resolve) => {
      socket.emit('getBackLog', index, (response) => {
        // log.info(JSON.stringify(response));
        resolve(response);
      });
    });
  } catch (e) {
    log.error(e);
    return null;
  }
}

/**
 * [shareKeys]
 * @param {string} pubKey [description]
 * @param {socket} socket [description]
 * @return {json}        [description]
 */
async function shareKeys(pubKey, socket) {
  try {
    return new Promise((resolve) => {
      socket.emit('shareKeys', pubKey, (response) => {
        log.info(JSON.stringify(response));
        resolve(response);
      });
    });
  } catch (e) {
    log.error(e);
    return null;
  }
}

/**
 * [updateKey]
 * @param {string} key [description]
 * @param {socket} socket [description]
 * @return {json}        [description]
 */
async function updateKey(key, value, socket) {
  try {
    return new Promise((resolve) => {
      socket.emit('updateKey', key, value, (response) => {
        log.info(JSON.stringify(response));
        resolve(response);
      });
    });
  } catch (e) {
    log.error(e);
    return null;
  }
}

/**
 * [getKeys]
 * @param {socket} socket [description]
 * @return {json}        [description]
 */
async function getKeys(socket) {
  try {
    return new Promise((resolve) => {
      socket.emit('getKeys', (response) => {
        log.info(JSON.stringify(response));
        resolve(response);
      });
    });
  } catch (e) {
    log.error(e);
    return null;
  }
}

module.exports = {
  getApplicationIP,
  getApplicationSpecs,
  getMaster,
  getMyIp,
  getBackLog,
  shareKeys,
  updateKey,
  getKeys,
};
