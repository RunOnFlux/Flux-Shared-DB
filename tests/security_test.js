const Security = require('../ClusterOperator/Security');

async function test() {
  Security.init();
  const en = Security.encryptComm('test');
  
  console.log(Security.getKey());
  console.log(Security.getCommAESKey());
  const encrypted = Security.publicEncrypt(Security.publicKey, Security.getCommAESKey());
  const encrypted2 = Security.publicEncrypt(Security.publicKey, Buffer.from('null', 'hex'));
  const decrypted = Security.privateDecrypt(encrypted);
  console.log(Buffer.from(decrypted.toString('hex'), 'hex'));
  Security.setCommKeys(decrypted, Security.getCommAESIv());
  Security.setKey(Security.generateNewKey());
  const de = Security.decryptComm(en);
  console.log(de);
  console.log(Security.getIV());
  console.log(Security.decrypt(Security.encrypt(Security.getIV())));
  // Security.setKey(Security.generateNewKey());
  console.log(Security.getKey());
}
test();
