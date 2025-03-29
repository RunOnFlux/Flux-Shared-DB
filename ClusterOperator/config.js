module.exports = {
  dbHost: process.env.DB_COMPONENT_NAME || 'localhost',
  dbType: process.env.DB_TYPE || 'mysql',
  dbUser: 'root',
  dbPass: process.env.DB_INIT_PASS || 'secret',
  dbPort: 3306,
  dbBacklog: 'flux_backlog',
  dbBacklogCollection: 'backlog',
  dbBacklogBuffer: 'backlog_buffer',
  dbOptions: 'options',
  dbInitDB: process.env.INIT_DB_NAME || 'test_db',
  externalDBPort: process.env.EXT_DB_PORT || 3307,
  apiPort: 7071,
  debugUIPort: 8008,
  containerDBPort: String(process.env.DB_PORT || 33949).trim(),
  containerApiPort: String(process.env.API_PORT || 33950).trim(),
  DBAppName: process.env.DB_APPNAME || '',
  AppName: process.env.CLIENT_APPNAME || '',
  version: '1.6.2',
  whiteListedIps: process.env.WHITELIST || '127.0.0.1',
  debugMode: true,
  containerDataPath: '',
  authMasterOnly: process.env.AUTH_MASTER_ONLY || false,
  ssl: false,
};
