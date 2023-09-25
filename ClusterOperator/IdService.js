/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const sessions = require('memory-cache');
const log = require('../lib/log');

class IdService {
  static loginPhrases = [this.generateLoginPhrase(), this.generateLoginPhrase()];
  static sessionExpireTime = 30 * 60 * 1000;

  /**
  * [generateLoginPhrase]
  */
  static generateLoginPhrase() {
    const timestamp = new Date().getTime();
    const phrase = timestamp + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return phrase;
  }

  /**
  * [getNewSession]
  */
  static getNewSession(userParams) {
    const timestamp = new Date().getTime();
    const sessionID = timestamp + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    sessions.put(sessionID, userParams, this.sessionExpireTime);
    return sessionID;
  }

  /**
  * [verifySession]
  */
  static verifySession(sessionID, userParams) {
    const value = sessions.get(sessionID);
    if (value !== userParams) return false;
    sessions.put(sessionID, userParams, this.sessionExpireTime);
    return true;
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
}
// eslint-disable-next-line func-names
module.exports = IdService;
