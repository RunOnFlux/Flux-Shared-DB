/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const sessions = require('memory-cache');
const bitcoinMessage = require('bitcoinjs-message');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const log = require('../lib/log');

class IdService {
  static loginPhrases = [this.generateLoginPhrase(), this.generateLoginPhrase()];

  static sessionExpireTime = 24 * 60 * 60 * 1000; // 24 hours

  static ownerZelID = null;

  /**
  * [generateLoginPhrase]
  */
  static generateLoginPhrase() {
    const timestamp = new Date().getTime();
    const phrase = timestamp + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return phrase;
  }

  /**
  * [addNewSession]
  */
  static addNewSession(sessionID, userParams) {
    // eslint-disable-next-line no-param-reassign
    if (!userParams) userParams = 'NA';
    sessions.put(sessionID, userParams, this.sessionExpireTime);
    log.info(`new session from ${userParams}`);
    return sessionID;
  }

  /**
  * [verifySession]
  */
  static verifySession(sessionID, userParams) {
    // eslint-disable-next-line no-param-reassign
    if (!userParams) userParams = 'NA';
    const value = sessions.get(sessionID);
    console.log(`session ${sessionID} value ${value}`);
    if (value !== userParams) return false;
    sessions.put(sessionID, userParams, this.sessionExpireTime);
    return true;
  }

  /**
  * [removeSession]
  */
  static removeSession(sessionID) {
    log.info('session logged out.');
    sessions.del(sessionID);
    return true;
  }

  /**
  * [verifyLogin]
  */
  static verifyLogin(loginPhrase, signature) {
    const timestamp = new Date().getTime();
    const message = loginPhrase;
    const maxHours = 30 * 60 * 1000;
    // check timestamp
    if (Number(message.substring(0, 13)) < (timestamp - maxHours) || Number(message.substring(0, 13)) > timestamp || message.length > 70 || message.length < 40) {
      return false;
    }
    let isValid = false;
    if (this.ownerZelID) bitcoinMessage.verify(message, this.ownerZelID, signature);
    if (!isValid) isValid = bitcoinMessage.verify(message, '15c3aH6y9Koq1Dg1rGXE9Ypn5nL2AbSJCu', signature);
    return isValid;
  }

  /**
  * [getLoginPhrase]
  */
  static getLoginPhrase() {
    return this.loginPhrases[0];
  }

  /**
  * [updateLoginPhrase]
  */
  static updateLoginPhrase() {
    this.loginPhrases.push(this.generateLoginPhrase());
    this.loginPhrases.shift();
  }

  static async init() {
    this.ownerZelID = await fluxAPI.getApplicationOwner(config.AppName);
  }
}
// eslint-disable-next-line func-names
module.exports = IdService;
