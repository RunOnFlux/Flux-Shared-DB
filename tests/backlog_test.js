const BackLog = require('../ClusterOperator/backlog');

async function test() {
  await BackLog.BackLog.createBacklog();
  await BackLog.BackLog.pushQuery('SELECT * FROM TEST', 1);
  await BackLog.BackLog.pushQuery('SELECT * FROM TEST1', 2);
  await BackLog.BackLog.pushQuery('SELECT * FROM TEST2', 3);
  await BackLog.BackLog.pushQuery('SELECT * FROM TEST3', 4);
  await BackLog.BackLog.pushQuery('SELECT * FROM TEST4', 5);
  const logs = await BackLog.BackLog.getLogs(0, 5);
  console.log(JSON.stringify(logs));
}
test();
