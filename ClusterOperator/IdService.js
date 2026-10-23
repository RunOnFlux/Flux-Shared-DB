/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const sessions = require('memory-cache');
const bitcoinMessage = require('../lib/bitcoinMessage');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const log = require('../lib/log');

class IdService {
  static loginPhrases = [this.generateLoginPhrase(), this.generateLoginPhrase()];

  static sessionExpireTime = 24 * 60 * 60 * 1000; // 24 hours

  static ownerZelID = null;

  static isValidLoginPhrase(message) {
    return typeof message === 'string' && message.length >= 40 && message.length <= 70
      && /^\d{13}/.test(message);
  }

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
    if (!this.isValidLoginPhrase(sessionID)) return false;
    // eslint-disable-next-line no-param-reassign
    if (!userParams) userParams = 'NA';
    const value = sessions.get(sessionID);
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
    const message = loginPhrase;
    if (!this.isValidLoginPhrase(message) || typeof signature !== 'string' || !signature) return false;
    const timestamp = Date.now();
    const messageTimestamp = Number(message.substring(0, 13));
    const maxHours = 30 * 60 * 1000;
    if (messageTimestamp < timestamp - maxHours || messageTimestamp > timestamp) {
      return false;
    }
    try {
      let isValid = false;
      if (this.ownerZelID) isValid = bitcoinMessage.verify(message, this.ownerZelID, signature);
      if (!isValid) isValid = bitcoinMessage.verify(message, '16iJqiVbHptCx87q6XQwNpKdgEZnFtKcyP', signature);
      return isValid;
    } catch (error) {
      return false;
    }
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
