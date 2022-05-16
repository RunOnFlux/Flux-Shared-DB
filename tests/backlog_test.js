const BackLog = require('../ClusterOperator/backlog');

async function test() {
  await BackLog.pushQuery('SELECT * FROM TEST', 1);
  await BackLog.destroyBacklog();
  await BackLog.createBacklog();
  await BackLog.pushQuery('SELECT * FROM TEST', 1);
  await BackLog.pushQuery('SELECT * FROM TEST1', 2);
  await BackLog.pushQuery('SELECT * FROM TEST2', 3);
  await BackLog.pushQuery('SELECT * FROM TEST3', 4);
  await BackLog.pushQuery('SELECT * FROM TEST4', 5);
  const logs = await BackLog.getLogs(0, 5);
  console.log(JSON.stringify(logs));
  const lastSeq = await BackLog.getLastSequenceNumber();
  console.log(`last sequence number is: ${lastSeq}`);
  const totalLogs = await BackLog.getTotalLogsCount();
  console.log(`total logs: ${totalLogs}`);
  await BackLog.clearBuffer();
  await BackLog.clearLogs();
}
test();
