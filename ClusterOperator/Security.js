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
    this.#key = this.encrypt(config.dbPass, this.#securityKey, this.#initVector);
    this.publicKey = publicKey;
    this.#privateKey = this.encrypt(privateKey, this.#securityKey, this.#initVector);
    privateKey = null;
    publicKey = null;
  }

  static encrypt(message, key, iv) {
    const cipher = createCipheriv('aes-256-cbc', key, iv);
    return cipher.update(message, 'utf-8', 'hex') + cipher.final('hex');
  }

  static decrypt(message, key, iv) {
    const decipher = createDecipheriv('aes-256-cbc', key, iv);
    return decipher.update(message, 'hex', 'utf-8') + decipher.final('utf-8');
  }

  static getKey() {
    return this.decrypt(this.#key, this.#securityKey, this.#initVector);
  }

  static generateNewKey() {
    return randomBytes(32).toString('hex');
  }

  static setKey(key) {
    this.#key = this.encrypt(key, this.#securityKey, this.#initVector);
  }

  static #getPrivateKey() {
    return this.decrypt(this.#privateKey, this.#securityKey, this.#initVector);
  }

  static publicDecrypt(key, buffer) {
    return publicDecrypt(key, buffer).toString('utf-8');
  }

  static publicEncrypt(key, buffer) {
    return publicEncrypt(key, buffer);
  }

  static privateDecrypt(buffer) {
    return privateDecrypt(this.#getPrivateKey(), buffer).toString('utf-8');
  }

  static privateEncrypt(buffer) {
    return privateEncrypt(this.#getPrivateKey(), buffer);
  }
}
module.exports = Security;
