const {
  generateKeyPairSync, randomBytes, createCipheriv, createDecipheriv, publicDecrypt, publicEncrypt, privateDecrypt, privateEncrypt,
} = require('crypto');
const log = require('../lib/log');
const config = require('./config');

class Security {
  static publicKey;

  static #privateKey;

  static #key;

  static #initVector;

  static #securityKey;

  static #commAESKey;

  static #commAESIv;

  static async init() {
    let { privateKey, publicKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      publicKeyEncoding: {
        type: 'spki',
        format: 'pem',
      },
      privateKeyEncoding: {
        type: 'pkcs8',
        format: 'pem',
      },
    });
    this.#initVector = randomBytes(16);
    this.#securityKey = randomBytes(32);
    this.#commAESIv = randomBytes(16);
    this.#commAESKey = randomBytes(32);
    this.#key = this.encrypt(config.dbPass, this.#securityKey, this.#initVector);
    this.publicKey = publicKey;
    this.#privateKey = this.encrypt(privateKey, this.#securityKey, this.#initVector);
    privateKey = null;
    publicKey = null;
    log.info(`commAESIv is: ${this.#commAESIv.toString('hex')}`);
    log.info(`commAESKey is: ${this.#commAESKey.toString('hex')}`);
    log.info(`pubKey is: ${this.publicKey}`);
  }

  static encrypt(message, key = Buffer.from(this.getKey(), 'hex'), iv = this.#initVector) {
    try {
      const cipher = createCipheriv('aes-256-cbc', key, iv);
      return cipher.update(message, 'utf-8', 'hex') + cipher.final('hex');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static decrypt(message, key = Buffer.from(this.getKey(), 'hex'), iv = this.#initVector) {
    try {
      const decipher = createDecipheriv('aes-256-cbc', key, iv);
      return decipher.update(message, 'hex', 'utf-8') + decipher.final('utf-8');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static encryptComm(message, key = this.#commAESKey, iv = this.#commAESIv) {
    try {
      const cipher = createCipheriv('aes-256-cbc', key, iv);
      return cipher.update(message, 'utf-8', 'hex') + cipher.final('hex');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static decryptComm(message, key = this.#commAESKey, iv = this.#commAESIv) {
    try {
      const decipher = createDecipheriv('aes-256-cbc', key, iv);
      return decipher.update(message, 'hex', 'utf-8') + decipher.final('utf-8');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static getKey() {
    return this.decrypt(this.#key, this.#securityKey, this.#initVector);
  }

  static getIV() {
    return this.#initVector.toString('hex');
  }

  static generateNewKey() {
    return randomBytes(32).toString('hex');
  }

  static setKey(key) {
    this.#key = this.encrypt(key, this.#securityKey, this.#initVector);
  }

  static setIV(iv) {
    this.#initVector = Buffer.from(iv, 'hex');
  }

  static setCommKeys(key, iv) {
    this.#commAESIv = Buffer.from(iv, 'hex');
    this.#commAESKey = Buffer.from(key, 'hex');
  }

  static getCommAESIv() {
    return this.#commAESIv.toString('hex');
  }

  static getCommAESKey() {
    return this.#commAESKey.toString('hex');
  }

  static #getPrivateKey() {
    return this.decrypt(this.#privateKey, this.#securityKey, this.#initVector);
  }

  static publicDecrypt(key, buffer) {
    try {
      return publicDecrypt(key, buffer).toString('utf-8');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static publicEncrypt(key, buffer) {
    try {
      return publicEncrypt(key, buffer);
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static privateDecrypt(buffer) {
    try {
      return privateDecrypt(this.#getPrivateKey(), buffer).toString('utf-8');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static privateEncrypt(buffer) {
    try {
      return privateEncrypt(this.#getPrivateKey(), buffer);
    } catch (err) {
      log.error(err);
      return null;
    }
  }
}
module.exports = Security;
