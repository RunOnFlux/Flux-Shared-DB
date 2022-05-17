const mysql = require('mysql2');

let connection = mysql.createConnection({
    host: 'localhost',
    user: 'me',
    password: 'secret',
    database: 'my_db',
    port: '3306',
  });
  
  connection.connect();
  
  connection.query('Show Databases', (error, results, fields) => {
    if (error) throw error;
    console.log('The solution is: ', [results,fields]);
  });
  
  connection.end();

connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '123',
    port: '3307',
  });
  
  connection.connect();
  
  connection.query('Show Databases', (error, results, fields) => {
    if (error) throw error;
    console.log('The solution is: ', [results,fields]);
  });
  
  connection.end();
  