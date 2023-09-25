const Importer = require('mysql-import');

const importer = new Importer({
  host: 'localhost',
  user: 'root',
  password: 'secret',
  database: 'test_db2',
});

importer.onProgress((progress) => {
  const percent = Math.floor(progress.bytes_processed / progress.total_bytes * 10000) / 100;
  console.log(`${percent}% Completed`);
});
importer.setEncoding('utf8');
importer.import('./dump.sql').then(() => {
  const files_imported = importer.getImported();
  console.log(`${files_imported.length} SQL file(s) imported.`);
}).catch((err) => {
  console.error(err);
});
