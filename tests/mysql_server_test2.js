const mysql = require('mysql2/promise');

async function test() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'me',
    password: 'secret',
    database: 'my_db',
    port: '3307',
  });

  console.log(await connection.query('Show Databases'));

  // connection.end();

  const connection2 = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '123',
    port: '3307',
  });
  console.log(await connection2.query('SHOw Databases'));

  const connection3 = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '123',
    port: '3307',
  });

  console.log(await connection3.query('SHOW Databases'));

  connection.end();
  connection2.end();
  connection3.end();
}
test();
