const Importer = require('../modules/mysql-import');

function callback(query) {
  console.log(query);
}

const importer = new Importer({
  host: 'localhost',
  user: 'root',
  password: 'secret',
  database: 'test_db2',
  callback,
});

importer.onProgress((progress) => {
  const percent = Math.floor((progress.bytes_processed / progress.total_bytes) * 10000) / 100;
  console.log(`${percent}% Completed`);
});
importer.setEncoding('utf8');
importer.import('./dumps/dump.sql').then(() => {
  const filesImported = importer.getImported();
  console.log(`${filesImported.length} SQL file(s) imported.`);
}).catch((err) => {
  console.error(err);
});
