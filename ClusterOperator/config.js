module.exports = {
  dbHost: process.env.DB_COMPONENT_NAME || '',
  dbType: 'mysql',
  dbUser: 'root',
  dbPass: process.env.DB_INIT_PASS || '',
  dbPort: 3306,
  dbBacklog: 'flux_backlog',
  dbBacklogCollection: 'backlog',
  dbBacklogBuffer: 'backlog_buffer',
  dbInitDB: 'test_db',
  connectionServer: 'mysql',
  externalDBPort: 3307,
  apiPort: 7071,
  debugUIPort: 8008,
  containerDBPort: process.env.DB_PORT || 33950,
  containerApiPort: process.env.API_PORT || 33951,
  DBAppName: process.env.DB_APPNAME || 'dbfluxtest4',
  AppName: process.env.CLIENT_APPNAME || '',
  version: '0.9.26',
  whiteListedIps: process.env.WHITELIST || '::1',
  clusterList: [], // Temporary
};
