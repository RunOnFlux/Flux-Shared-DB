const sqlAnalyzer = require('../lib/sqlAnalyzer');
const fs = require('fs');

console.log(sqlAnalyzer(`select * from table;`,'mysql'));

try {
    const queries = fs.readFileSync('./tests/sql/test.sql', 'utf8');
    console.log(sqlAnalyzer(queries,'mysql'));
  } catch (err) {
    console.error(err);
  }