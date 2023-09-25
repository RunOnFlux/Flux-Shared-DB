const mysqldump = require('mysqldump');

mysqldump({
  connection: {
    host: 'localhost',
    user: 'root',
    password: 'secret',
    database: 'test_db',
  },
  dump: {
    schema: {
      table: {
        dropIfExist: true,
      },
    },
    data: {
      verbose: false,
    },
  },
  dumpToFile: './dump.sql',
});
