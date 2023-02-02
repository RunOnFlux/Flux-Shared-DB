const buffer = require('memory-cache');
const log = require('../lib/log');

const request = {
  query: 'select * from table',
  sequenceNumber: 123,
  timestamp: 1231343214313,
};
buffer.put(123, request);
console.log(buffer.get(122 + 1));
console.log(buffer.get(122));
console.log(buffer.size());
console.log(buffer.del(122 + 1));
console.log(buffer.size());
buffer.put(123, request);
buffer.clear();
console.log(buffer.size());
log.error('tttt', { class: 't' });
