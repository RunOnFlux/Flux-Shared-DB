module.exports = {
  dbHost: 'host.docker.internal',
  dbType: 'mysql',
  dbUser: 'root',
  dbPass: 'secret',
  dbPort: 3306,
  dbBacklog: 'flux_backlog',
  dbBacklogCollection: 'backlog',
  dbBacklogBuffer: 'backlog_buffer',
  dbInitDB: 'test_db',
  connectionServer: 'mysql',
  externalDBPort: 3307,
  apiPort: 7071,
  DBAppName: process.env.DB_APPNAME,
  AppName: process.env.APPNAME,
  clusterList: [], // Temporary
};
