/* eslint-disable no-unused-vars */
const net = require('net');
const mysql = require('mysql2/promise');
const dbClient = require('../ClusterOperator/DBClient');
const mySQLServer = require('../lib/mysqlServer');
const consts = require('../lib/mysqlConstants');

let localDBClient = null;
let appDBClient = null;

async function init() {
  localDBClient = await dbClient.createClient();
  appDBClient = await mysql.createConnection({
    password: 'secret',
    port: 3307,
    host: 'localhost',
  });
}
function handleAuthorize(param) {
  console.log('Auth Info:');
  console.log(param);
  // Yup you are authorized
  return true;
}

function handleQuery(result) {
  // Take the query, print it out
  // console.log(`Got Result: ${JSON.stringify(result)}`);
  this.sendPacket(result);
}
async function handleCommand({ command, extra }) {
  // command is a numeric ID, extra is a Buffer
  switch (command) {
    case consts.COM_QUERY:
      console.log(`Got query: ${extra.toString()}`);
      this.localDBClient.setSocket(this.socket);
      await this.localDBClient.query(extra.toString(), true);
      break;
    case consts.COM_PING:
      this.sendOK({ message: 'OK' });
      break;
    case null:
    case undefined:
    case consts.COM_QUIT:
      console.log('Disconnecting');
      this.end();
      break;
    case consts.COM_INIT_DB:
      await this.localDBClient.query(`use ${extra}`);
      console.log(`extra is ${extra}`);
      this.sendOK({ message: 'OK' });
      break;
    default:
      console.log(`Unknown Command: ${command}`);
      this.sendError({ message: 'Unknown Command' });
      break;
  }
}

net.createServer((so) => {
  const server = mySQLServer.createServer({
    socket: so,
    onAuthorize: handleAuthorize,
    onCommand: handleCommand,
    localDBClient,
  });
}).listen(3307);

console.log('Started server on port 3307');

init();
