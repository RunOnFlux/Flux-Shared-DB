/* eslint-disable no-unused-vars */
const net = require('net');
const mysql = require('mysql2/promise');
const mySQLServer = require('../lib/mysqlServer');
const consts = require('../lib/mysqlConstants');

async function test(query, port){
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'secret',
    database: 'test_db',
    port: port
  });
  const [rows, fields, err] = await connection.query(query);

  connection.end();
  return [rows, fields, err];
}
function handleAuthorize(param) {
  console.log('Auth Info:');
  console.log(param);
  // Yup you are authorized
  return true;
}

function handleQuery(result) {
  // Take the query, print it out
  console.log(`Got Result: ${JSON.stringify(result[0])}`);

  let fieldNames = [];
  for (let definition of result[1]) fieldNames.push(definition.name);
  this.sendDefinitions(result[1]);
  let finalResult = [];
  for (let row of result[0]){
    
    let newRow =[];
    for(let filed of fieldNames){
      //console.log(`pushing filed ${filed}: ${JSON.stringify(row[filed])}`);
      newRow.push(row[filed]);
    }
    finalResult.push(newRow);
  }
  this.sendRows(finalResult);
}
async function handleCommand({ command, extra }) {
  // command is a numeric ID, extra is a Buffer
  switch (command) {
    case consts.COM_QUERY:
      handleQuery.call(this, await test(extra.toString(),3306));
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
}).listen(3307);

console.log('Started server on port 3307');






test('select * from wp_posts',3307);