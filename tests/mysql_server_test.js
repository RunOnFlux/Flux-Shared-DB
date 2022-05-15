/* eslint-disable no-unused-vars */
const net = require('net');
const mysql = require('mysql');
const mySQLServer = require('../lib/mysqlServer');
const consts = require('../lib/mysqlConstants');

function handleAuthorize(param) {
  console.log('Auth Info:');
  console.log(param);
  // Yup you are authorized
  return true;
}

function handleQuery(query) {
  // Take the query, print it out
  console.log(`Got Query: ${query}`);

  // Then send it back to the user in table format
  this.sendDefinitions([this.newDefinition({ name: 'TheCommandYouSent' })]);
  this.sendRows([
    [query],
  ]);
}
function handleCommand({ command, extra }) {
  // command is a numeric ID, extra is a Buffer
  switch (command) {
    case consts.COM_QUERY:
      handleQuery.call(this, extra.toString());
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
  });
}).listen(3306);

console.log('Started server on port 3306');

const connection = mysql.createConnection({
  host: 'localhost',
  user: 'me',
  password: 'secret',
  database: 'my_db',
});

connection.connect();

connection.query('Show Databases', (error, results, fields) => {
  if (error) throw error;
  console.log('The solution is: ', results[0]);
});

connection.end();
