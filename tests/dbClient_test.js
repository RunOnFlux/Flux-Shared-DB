const client = require('../ClusterOperator/dbClient');

async function test() {
  const mySqlClient = await client.createClient();

  await mySqlClient.query('CREATE DATABASE test123;');
  await mySqlClient.setDB('test123');
  await mySqlClient.query('CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20), species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);');
  console.log(await mySqlClient.query('SHOW DATABASES;'));
  console.log(await mySqlClient.query('SHOW TABLES;'));
}
test();
