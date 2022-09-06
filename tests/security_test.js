const Security = require('../ClusterOperator/Security');

async function test() {
  Security.init();
  const en = Security.encryptComm('test');
  const keys = {
    'N89.58.26.67': 'd9c1fa0352be20f37ee2f60a1e4284db4af054cfc2cdb270737999cb773bb502:e0f0e5080fdf0a91dd0ecc523e6e53e5'
  };
  console.log(Security.getKey());
  console.log(Security.getCommAESKey());
  const encrypted = Security.publicEncrypt(Security.publicKey, Security.getCommAESKey());
  //const encrypted2 = Security.publicEncrypt(Security.publicKey, Buffer.from(null, 'hex'));
  const decrypted = Security.privateDecrypt(encrypted);
  console.log(Buffer.from(decrypted.toString('hex'), 'hex'));
  Security.setCommKeys(decrypted, Security.getCommAESIv());
  Security.setKey(Security.generateNewKey());
  const de = Security.decryptComm(en);
  const de2 = Security.decryptComm(keys['N89.58.26.67'].toString('hex'));
  console.log(de);
  console.log(Security.getIV());
  console.log(Security.decrypt(Security.encrypt(Security.getIV())));
  // Security.setKey(Security.generateNewKey());
  console.log(Security.getKey());
}
test();
