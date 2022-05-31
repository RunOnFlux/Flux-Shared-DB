const WebSocket = require('ws');

const ws1 = new WebSocket('ws://localhost:7071');

ws1.on('open', function open() {
  ws1.send('{"command":"test"}');
  //ws1.terminate();
});

const ws2 = new WebSocket('ws://localhost:7071');

ws2.on('open', function open() {
  ws2.send('{"command":"test2"}');
  //ws2.terminate();
});

const ws3 = new WebSocket('ws://localhost:7071');

ws3.on('open', function open() {
  ws3.send('{"command":"test3"}');
  //ws3.terminate();
});



