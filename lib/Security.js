const { generateKeyPairSync, publicDecrypt, publicEncrypt } = require('crypto');
const log = require('./log');

class Security {
  static publicKey;

  static #privateKey;

  static init() {
    const { privateK, publicK } = generateKeyPairSync('rsa', {
      modulusLength: 4096,
      publicKeyEncoding: {
        type: 'spki',
        format: 'pem',
      },
      privateKeyEncoding: {
        type: 'pkcs8',
        format: 'pem',
        cipher: 'aes-256-cbc',
      },
    }, (err) => {
      log.info(err);
    });

    this.publicKey = publicK;
    this.#privateKey = privateK;
  }


}
module.exports = Security;
