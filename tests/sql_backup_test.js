const BackLog = require('../ClusterOperator/Backlog');
const Security = require('../ClusterOperator/Security');

async function test() {
  await BackLog.dumpBackup();
  await BackLog.deleteBackupFile('BU_1695997497871');
  console.log(await BackLog.listSqlFiles());
}
Security.init();
test();
