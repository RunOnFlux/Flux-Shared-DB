/* eslint-disable no-unused-vars */
const {
  generateKeyPairSync, randomBytes, createCipheriv, createDecipheriv, publicDecrypt, publicEncrypt, privateDecrypt, privateEncrypt,
} = require('crypto');
const forge = require('node-forge');
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
  }

  static encrypt(message, key = Buffer.from(this.getKey(), 'hex'), iv = this.#initVector) {
    try {
      // console.log(message);
      const utfMessage = message.toString();
      const cipher = createCipheriv('aes-256-cbc', key, iv);
      return cipher.update(utfMessage, 'utf-8', 'hex') + cipher.final('hex');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static decrypt(message, key = Buffer.from(this.getKey(), 'hex'), iv = this.#initVector) {
    try {
      const decipher = createDecipheriv('aes-256-cbc', key, iv);
      return decipher.update(message, 'hex') + decipher.final();
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static encryptComm(message, key = this.#commAESKey, iv = this.#commAESIv) {
    try {
      const utfMessage = message.toString();
      const cipher = createCipheriv('aes-256-cbc', key, iv);
      return cipher.update(utfMessage, 'utf-8', 'hex') + cipher.final('hex');
    } catch (err) {
      log.error(err);
      return null;
    }
  }

  static decryptComm(message, key = this.#commAESKey, iv = this.#commAESIv) {
    try {
      const decipher = createDecipheriv('aes-256-cbc', key, iv);
      return decipher.update(message, 'hex') + decipher.final();
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

  static generateRSAKey() {
    const keys = forge.pki.rsa.generateKeyPair(2048);
    const cert = forge.pki.createCertificate();
    cert.publicKey = keys.publicKey;
    cert.serialNumber = '01';
    cert.validity.notBefore = new Date();
    cert.validity.notAfter = new Date();
    cert.validity.notAfter.setFullYear(cert.validity.notBefore.getFullYear() + 1);
    const attrs = [
      { name: 'commonName', value: 'runonflux.io' },
      { name: 'countryName', value: 'US' },
      { shortName: 'ST', value: 'New York' },
      { name: 'localityName', value: 'New York' },
      { name: 'organizationName', value: 'InFlux Technologies' },
      { shortName: 'OU', value: 'IT' },
    ];
    cert.setSubject(attrs);
    cert.setIssuer(attrs);
    cert.sign(keys.privateKey);
    const pemPrivateKey = forge.pki.privateKeyToPem(keys.privateKey);
    const pemCertificate = forge.pki.certificateToPem(cert);
    return { pemPrivateKey, pemCertificate };
  }
}
module.exports = Security;
