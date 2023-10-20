const sessions = require('memory-cache');
const timer = require('timers/promises');

async function test() {
  sessions.put('1', '1', 700);
  await timer.setTimeout(500);
  sessions.put('1', '1', 700);
  console.log(sessions.get(1));
  await timer.setTimeout(500);
  console.log(sessions.get(1));
  console.log(sessions.size());
}
test();
