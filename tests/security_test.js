const Security = require('../ClusterOperator/Security');

async function test() {
  Security.init();
  console.log(Security.getKey());

  const encrypted = Security.privateEncrypt(Buffer.from('test message'));
  console.log(encrypted);
  const decrypted = Security.publicDecrypt(Security.publicKey, encrypted);
  console.log(decrypted);
  Security.generateNewKey();
  console.log(Security.getKey());
}
test();
