/* eslint-disable no-else-return */
/* eslint-disable no-restricted-syntax */
const log = require('../lib/log');

class IdService {
  static loginPhrases = [this.generateLoginPhrase(), this.generateLoginPhrase()];

  /**
  * [generateLoginPhrase]
  */
  static async generateLoginPhrase() {
    const timestamp = new Date().getTime();
    const phrase = timestamp + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return phrase;
  }

  /**
  * [getLoginPhrase]
  */
  static async getLoginPhrase() {
    return this.loginPhrases[1];
  }

  /**
  * [updateLoginPhrase]
  */
  static async updateLoginPhrase() {
    this.loginPhrases.push(this.generateLoginPhrase());
    this.loginPhrases.shift();
  }
}
// eslint-disable-next-line func-names
module.exports = IdService;
