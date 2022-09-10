/* eslint-disable */
const { App } = require('uWebSockets.js');
const { Server } = require('socket.io');
const timer = require('timers/promises');
const fluxAPI = require('../lib/fluxAPI');
const utill = require('../lib/utill');

function startServer() {
  const io = new Server();
  const app = new App();
  io.attachApp(app);
  io.on('connection', async (socket) => {
    const ip = socket.handshake.address;
    console.log(ip);
    socket.on('getStatus', async (callback) => {
      console.log(`getStatus from ${ip}`);
      callback({ status: 'success', message: utill.convertIP(socket.handshake.address) });
    });
  });
  
  app.listen(3002, (token) => {
    if (!token) {
      console.log(`port ${3002} already in use`);
    }
  });
  
}
async function testClient() {
  console.log(await fluxAPI.getStatus('localhost', 3002));
}
startServer();
testClient()

