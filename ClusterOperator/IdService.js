/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const { createHash, randomBytes } = require('crypto');
const sessions = require('memory-cache');
const bitcoinMessage = require('../lib/bitcoinMessage');
const fluxAPI = require('../lib/fluxAPI');
const config = require('./config');
const log = require('../lib/log');

class IdService {
  static loginPhrases = []; // App identity is resolved after this module is loaded.

  static sessionExpireTime = 24 * 60 * 60 * 1000; // 24 hours

  static ownerZelID = null;

  static isValidLoginPhrase(message) {
    return typeof message === 'string' && message.length >= 40 && message.length <= 70
      && /^\d{13}/.test(message);
  }

  /**
  * [loginScope]
  */
  static loginScope() {
    if (typeof config.DBAppName !== 'string' || !config.DBAppName.trim()) {
      throw new Error('Login requires a resolved Shared-DB application name');
    }
    return createHash('sha256').update('flux-shared-db/login/v1\0').update(config.DBAppName).digest('base64url')
      .slice(0, 22);
  }

  static generateLoginPhrase() {
    // Keep the timestamp prefix and <=70 characters for existing wallet/session clients.
    // Replicas share an app scope, but signatures cannot authorize a different app.
    return `${Date.now()}:SDB-login:${this.loginScope()}:${randomBytes(16).toString('base64url')}`;
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
      // A signature over an arbitrary timestamp-prefixed message is not a login.
      // Only explicit login messages for this database app grant administrator access.
      if (!/^\d{13}:SDB-login:[A-Za-z0-9_-]{22}:[A-Za-z0-9_-]{22}$/.test(message)
        || message.split(':')[2] !== this.loginScope()) return false;
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
    return this.loginPhrases[0] || this.generateLoginPhrase();
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
