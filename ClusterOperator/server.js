/* eslint-disable prefer-destructuring */
/* eslint-disable no-plusplus */
/* eslint-disable consistent-return */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-unused-vars */
const { Server } = require('socket.io');
const https = require('https');
const timer = require('timers/promises');
const express = require('express');
const RateLimit = require('express-rate-limit');
const fileUpload = require('express-fileupload');
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const qs = require('qs');
const sanitize = require('sanitize-filename'); // Needed for DB Manager routes potentially
const queryCache = require('memory-cache');
const Operator = require('./Operator');
const BackLog = require('./Backlog');
const IdService = require('./IdService');
const log = require('../lib/log');
const utill = require('../lib/utill');
const config = require('./config');
const Security = require('./Security');
const SqlImporter = require('../lib/mysqlimport');
// Import the factory function, not the class directly
const { createClient } = require('./DBClient'); // Assuming DBClient.js exports createClient

// --- Global variable to hold the DB Client instance ---
let dbClientInstance;

/**
* [auth]
* @param {string} ip [description]
*/
function auth(ip) {
  const whiteList = config.whiteListedIps.split(',');
  if (whiteList.length && whiteList.includes(ip)) return true;
  // only operator nodes can connect
  const idx = Operator.OpNodes.findIndex((item) => item.ip === ip);
  if (idx === -1) {
    log.info(`opnodes: ${JSON.stringify(Operator.OpNodes)}`);
    return false;
  }
  return true;
}
/**
* [authUser]
*/
function authUser(req) {
  let remoteIp = utill.convertIP(req.ip);
  if (!remoteIp) remoteIp = req.socket.address().address;
  let loginphrase = false;
  if (req.headers.loginphrase) {
    loginphrase = req.headers.loginphrase;
  } else {
    loginphrase = req.cookies.loginphrase;
  }
  if (loginphrase && IdService.verifySession(loginphrase, remoteIp)) {
    return true;
  }
  return false;
}
/**
 * To check if a parameter is an object and if not, return an empty object.
 * @param {*} parameter Parameter of any type.
 * @returns {object} Returns the original parameter if it is an object or returns an empty object.
 */
function ensureObject(parameter) {
  if (typeof parameter === 'object') {
    return parameter;
  }
  if (!parameter) {
    return {};
  }
  let param;
  try {
    param = JSON.parse(parameter);
  } catch (e) {
    param = qs.parse(parameter);
  }
  if (typeof param !== 'object') {
    return {};
  }
  return param;
}
/**
* Helper function to safely quote database/table/column identifiers.
* Prevents SQL injection through identifiers.
* @param {string} identifier The identifier to quote.
* @returns {string} The quoted identifier (e.g., `table_name`) or throws error if invalid.
*/
function quoteIdentifier(identifier) {
  if (typeof identifier !== 'string' || identifier.length === 0) {
    throw new Error('Invalid identifier provided for quoting.');
  }
  // Basic check: Allow alphanumeric and underscores. Reject others.
  // More robust validation might be needed depending on DB specifics.
  if (!/^[a-zA-Z0-9_]+$/.test(identifier)) {
    throw new Error(`Invalid characters in identifier: ${identifier}. Only alphanumeric and underscores are allowed.`);
  }
  return `\`${identifier.replace(/`/g, '')}\``; // Remove any backticks just in case and quote
}
/**
* Starts UI service
*/
async function startUI() { // Make async to potentially await DB client init if needed later
  const app = express();
  app.use(cors()); // Configure CORS appropriately
  app.use(cookieParser());
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(fileUpload());
  const limiter = RateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // max 100 requests per windowMs (adjust as needed)
  });
  // Apply limiter globally or selectively
  // app.use(limiter); // Apply to all routes below this line

  fs.writeFileSync('errors.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('warnings.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('info.txt', `version: ${config.version}<br>`);
  fs.writeFileSync('query.txt', `version: ${config.version}<br>`);
  fs.appendFileSync('debug.txt', `------------------------------------------------------<br>version: ${config.version}<br>`);

  app.options('/*', (req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*'); // Adjust for production
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Content-Length, X-Requested-With, loginphrase'); // Add loginphrase if used in headers
    res.sendStatus(200);
  });

  // --- Static Files and Root Route ---
  app.get('/', (req, res) => {
    const { host } = req.headers;
    if (host) {
      if (authUser(req)) {
        res.sendFile(path.join(__dirname, '../ui/index.html'));
      } else {
        res.sendFile(path.join(__dirname, '../ui/login.html'));
      }
    } else if (Operator.IamMaster) { // request coming from fdm
      res.send('OK');
    } else {
      res.status(500).send('Bad Request');
    }
  });

  // Serve assets (consider using express.static for the whole assets folder)
  app.get('/assets/zelID.svg', (req, res) => {
    res.sendFile(path.join(__dirname, '../ui/assets/zelID.svg'));
  });
  app.get('/assets/Flux_white-blue.svg', (req, res) => {
    res.sendFile(path.join(__dirname, '../ui/assets/Flux_white-blue.svg'));
  });

  app.use(limiter); // Apply rate limiter to all API routes below

  app.get('/logs/:file?', (req, res) => {
    const remoteIp = utill.convertIP(req.ip);
    let { file } = req.params;
    file = file || req.query.file;
    const whiteList = config.whiteListedIps.split(',');
    let logFile = 'errors.txt';
    switch (file) {
      case 'info':
        logFile = 'info.txt';
        break;
      case 'warnings':
        logFile = 'warnings.txt';
        break;
      case 'debug':
        logFile = 'debug.txt';
        break;
      case 'query':
        logFile = 'query.txt';
        break;
      default:
        logFile = 'errors.txt';
        break;
    }

    if (whiteList.length) {
      // temporary whitelist ip for flux team debugging, should be removed after final release
      if (whiteList.includes(remoteIp) || remoteIp === '167.235.234.45' || remoteIp === '45.89.52.198') {
        res.send(`<html><style>
        .t {color:#2cb92c;}
        .yellow {color:yellow;}
        .red {color:#ff8100;}
        .green {color:green;}
        .magenta {color:magenta;}
        .cyan {color:cyan;}
        .lb {color:#00d0ff;}
        .m {margin-left:10px;}
        </style><body style="
          font-family: monospace;
          background-color: #2a2a32;
          color: white;
          font-size: 12;
          ">${logFile} Debug Screen<br>${fs.readFileSync(logFile).toString()}</body></html>`);
      } else {
        res.status(403).send('Access Denied'); // Deny access if not whitelisted
      }
    } else {
      res.status(403).send('Access Denied'); // Deny access if whitelist is empty
    }
  });

  app.post('/rollback', async (req, res) => {
    if (authUser(req)) {
      let { seqNo } = req.body;
      seqNo = seqNo || req.query.seqNo;
      if (seqNo !== undefined && seqNo !== null) { // Check if seqNo is provided
        await Operator.rollBack(seqNo);
        res.send({ status: 'OK' });
      } else {
        res.status(400).send({ error: 'Missing sequence number (seqNo).' });
      }
    } else {
      res.status(401).send('Unauthorized'); // Use 401 for auth errors
    }
  });

  app.get('/nodelist', (req, res) => {
    if (authUser(req)) {
      res.send(Operator.OpNodes);
      // res.end(); // Not needed with res.send()
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.get('/getstatus', async (req, res) => {
    // This endpoint seems intentionally disabled in the original code.
    // If it needs to be enabled, implement the streaming logic carefully.
    res.status(501).send('Not Implemented'); // Use 501 for not implemented
  });

  app.get('/status', (req, res) => {
    // This endpoint seems intended for internal node communication, no authUser check needed?
    // Add CORS headers if accessed directly by browser/external tools
    // res.header('Access-Control-Allow-Origin', '*');
    // res.header('Access-Control-Allow-Headers', 'X-Requested-With');
    res.send({
      status: Operator.status,
      sequenceNumber: BackLog.sequenceNumber,
      masterIP: Operator.getMaster(),
      taskStatus: BackLog.compressionTask,
      clusterStatus: Operator.ClusterStatus,
    });
    // res.end(); // Not needed
  });

  app.get('/getLogDateRange', async (req, res) => {
    if (authUser(req)) {
      res.send(await BackLog.getDateRange());
      // res.end();
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.get('/getLogsByTime', async (req, res) => {
    if (authUser(req)) {
      const { starttime, length } = req.query;
      if (starttime === undefined || length === undefined) {
        return res.status(400).send({ error: 'Missing starttime or length query parameter.' });
      }
      try {
        const logs = await BackLog.getLogsByTime(starttime, length);
        res.send(logs);
      } catch (error) {
        log.error(`Error fetching logs by time: ${error.message}`);
        res.status(500).send({ error: 'Failed to fetch logs.' });
      }
      // res.end();
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  // --- Backup/Restore Routes ---
  app.get('/listbackups', async (req, res) => {
    if (authUser(req)) {
      try {
        res.send(await BackLog.listSqlFiles());
      } catch (error) {
        log.error(`Error listing backup files: ${error.message}`);
        res.status(500).send({ error: 'Failed to list backup files.' });
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.get('/getbackupfile/:filename', async (req, res) => {
    if (authUser(req)) {
      const { filename } = req.params;
      const safeFilename = sanitize(filename);
      if (!safeFilename) {
        return res.status(400).send('Invalid filename.');
      }
      const filePath = path.join(__dirname, `../dumps/${safeFilename}.sql`);

      // Check if file exists before attempting download
      fs.access(filePath, fs.constants.R_OK, (err) => {
        if (err) {
          log.error(`Backup file not found or not readable: ${filePath}`);
          return res.status(404).send('File not found');
        }
        res.download(filePath, `${safeFilename}.sql`, (downloadErr) => {
          if (downloadErr) {
            // Handle errors during download (e.g., connection closed)
            log.error(`Error downloading backup file ${safeFilename}: ${downloadErr.message}`);
            // Avoid sending another response if headers already sent
            if (!res.headersSent) {
              res.status(500).send('Error downloading file');
            }
          }
        });
      });
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.post('/upload-sql', async (req, res) => {
    if (authUser(req)) {
      if (!req.files || !req.files.sqlFile) {
        return res.status(400).send('No file uploaded.');
      }
      const { sqlFile } = req.files;

      // Validate file type/extension
      if (path.extname(sqlFile.name).toLowerCase() !== '.sql') {
        return res.status(400).send('Invalid file type. Only .sql files are allowed.');
      }

      // Sanitize filename before saving
      const safeFilename = sanitize(sqlFile.name);
      if (!safeFilename) {
        return res.status(400).send('Invalid filename.');
      }

      const uploadPath = path.join(__dirname, '../dumps/', safeFilename);
      const dumpsDir = path.join(__dirname, '../dumps/');

      // Ensure dumps directory exists
      try {
        await fs.promises.mkdir(dumpsDir, { recursive: true });
      } catch (dirErr) {
        log.error(`Failed to ensure dumps directory exists: ${dirErr.message}`);
        return res.status(500).send('Server error during file upload preparation.');
      }

      sqlFile.mv(uploadPath, (err) => {
        if (err) {
          log.error(`Error saving uploaded SQL file: ${err.message}`);
          return res.status(500).send(`Error uploading file: ${err.message}`);
        }
        log.info(`SQL file uploaded successfully: ${safeFilename}`);
        res.send('File uploaded successfully.');
      });
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.post('/generatebackup', async (req, res) => {
    if (authUser(req)) {
      try {
        const result = await BackLog.dumpBackup();
        res.send(result); // Send result from dumpBackup
      } catch (error) {
        log.error(`Error generating backup: ${error.message}`);
        res.status(500).send({ error: 'Failed to generate backup.' });
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.post('/deletebackup', async (req, res) => {
    if (authUser(req)) {
      const { filename } = req.body; // Assuming filename is in body
      if (!filename) {
        return res.status(400).send({ error: 'Missing filename in request body.' });
      }
      const safeFilename = sanitize(filename);
      if (!safeFilename) {
        return res.status(400).send('Invalid filename.');
      }
      try {
        const result = await BackLog.deleteBackupFile(safeFilename);
        res.send(result); // Send result from deleteBackupFile
      } catch (error) {
        log.error(`Error deleting backup ${safeFilename}: ${error.message}`);
        res.status(500).send({ error: `Failed to delete backup file ${safeFilename}.` });
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  });

  app.post('/executebackup', async (req, res) => {
    if (authUser(req)) {
      if (Operator.IamMaster) {
        const { filename } = req.body;
        if (!filename) {
          return res.status(400).send({ error: 'Missing filename in request body.' });
        }
        const safeFilename = sanitize(filename);
        if (!safeFilename) {
          return res.status(400).send('Invalid filename.');
        }
        const filePath = `./dumps/${safeFilename}.sql`;

        // Check if file exists
        try {
          await fs.promises.access(filePath, fs.constants.R_OK);
        } catch (fileErr) {
          log.error(`Backup file not found for execution: ${filePath}`);
          return res.status(404).send({ error: `Backup file '${safeFilename}.sql' not found.` });
        }

        try {
          log.info(`Starting execution of backup file: ${safeFilename}.sql`);
          BackLog.compressionTask = 0;
          // removing old db + resetting sequence numbers:
          log.info('Rolling back DB to sequence 0 before executing backup...');
          await Operator.rollBack(0);
          Operator.status = 'IMPORTING';
          await timer.setTimeout(2000); // Allow time for rollback operations

          const importer = new SqlImporter({
            callback: Operator.sendWriteQuery, // Ensure this callback handles query execution correctly
            serverSocket: Operator.serverSocket, // Pass socket if needed by callback
          });

          importer.onProgress((progress) => {
            const percent = Math.floor((progress.bytes_processed / progress.total_bytes) * 10000) / 100;
            log.info(`Import progress for ${safeFilename}.sql: ${percent}% Completed`, 'cyan');
            BackLog.compressionTask = percent;
            // Consider sending progress via WebSocket if needed for UI feedback
          });

          importer.setEncoding('utf8'); // Or appropriate encoding

          await importer.import(filePath);

          const filesImported = importer.getImported();
          log.info(`${filesImported.length} SQL file(s) imported successfully (${safeFilename}.sql).`);
          Operator.status = 'OK';
          res.send('OK');
          BackLog.compressionTask = -1;
        } catch (err) {
          Operator.status = 'OK';
          log.error(`Error during backup execution (${safeFilename}.sql): ${err.message || err}`);
          res.status(500).send({ error: `Failed to execute backup file: ${err.message || err}` });
        }
      } else {
        res.status(403).send({ error: 'Operation is only allowed on master node' }); // Use 403 Forbidden
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  });
  app.post('/compressbacklog', async (req, res) => {
    if (authUser(req)) {
      if (Operator.IamMaster) {
        await Operator.comperssBacklog();
      } else {
        res.status(500).send('operation is only allowed on master node');
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  });
  // --- Authentication Routes ---
  app.get('/isloggedin/', (req, res) => {
    if (authUser(req)) {
      res.cookie('loginphrase', req.headers.loginphrase);
      res.send('OK');
    } else {
      res.status(401).send('Unauthorized');
    }
  });
  app.post('/verifylogin/', async (req, res) => { // Make async
    // Original code had duplicate logic for body parsing. Simplified.
    try {
      const { signature, message, loginPhrase } = req.body; // Use parsed body
      const msg = loginPhrase || message; // Prefer loginPhrase if present

      if (!signature || !msg) {
        return res.status(400).send('Missing signature or message/loginPhrase.');
      }

      if (IdService.verifyLogin(msg, signature)) {
        let remoteIp = utill.convertIP(req.ip);
        if (!remoteIp) remoteIp = req.socket.address().address;
        IdService.addNewSession(msg, remoteIp);
        Operator.emitUserSession('add', msg, remoteIp);
        res.cookie('loginphrase', msg, { httpOnly: true, secure: config.ssl, sameSite: 'strict' }); // Add cookie options
        res.send('OK');
      } else {
        res.status(401).send('SIGNATURE NOT VALID');
      }
    } catch (error) {
      log.error(`Error during login verification: ${error.message}`);
      res.status(500).send('Error during login verification.');
    }
  });

  app.get('/loginphrase/', (req, res) => {
    res.send(IdService.generateLoginPhrase());
  });

  app.get('/logout/', (req, res) => {
    if (authUser(req)) {
      const { loginphrase } = req.cookies; // Get phrase from cookie
      if (loginphrase) {
        IdService.removeSession(loginphrase);
        Operator.emitUserSession('remove', loginphrase, '');
        res.clearCookie('loginphrase'); // Clear the cookie
        res.send('OK');
      } else {
        res.status(400).send('Logout failed: Session not found.');
      }
    } else {
      // Already logged out or invalid session
      res.clearCookie('loginphrase');
      res.send('OK'); // Send OK even if not logged in, as the goal is to be logged out
    }
  });

  // --- Secret Management Routes (Requires Whitelist) ---
  // Apply a middleware for IP whitelisting to these routes
  const requireWhitelist = (req, res, next) => {
    const remoteIp = utill.convertIP(req.ip);
    const whiteList = config.whiteListedIps.split(',');
    if (whiteList.length && whiteList.includes(remoteIp)) {
      next(); // IP is whitelisted, proceed
    } else {
      log.warn(`Access denied for non-whitelisted IP ${remoteIp} to secret management.`);
      res.status(403).send('Access Denied');
    }
  };

  app.get('/secret/:key', requireWhitelist, async (req, res) => {
    const { key } = req.params;
    if (!key) return res.status(400).send('Missing key.');
    const value = await BackLog.getKey(`_sk${key}`);
    if (value !== null && value !== undefined) { // Check explicitly for null/undefined
      res.send(value);
    } else {
      res.status(404).send('Key not found');
    }
  });

  app.post('/secret/', requireWhitelist, async (req, res) => {
    const { key, value } = req.body;
    if (!key || value === undefined) return res.status(400).send('Missing key or value.');
    // Assuming pushKey returns success/failure or throws error
    try {
      await BackLog.pushKey(`_sk${key}`, value, true); // Assuming true means broadcast/persist
      res.send('OK');
    } catch (error) {
      log.error(`Error setting secret key ${key}: ${error.message}`);
      res.status(500).send('Failed to set secret.');
    }
  });

  app.delete('/secret/:key', requireWhitelist, async (req, res) => {
    const { key } = req.params;
    if (!key) return res.status(400).send('Missing key.');
    try {
      const success = await BackLog.removeKey(`_sk${key}`);
      if (success) {
        res.send('OK');
      } else {
        // removeKey might return false if key didn't exist
        res.status(404).send('Key not found or could not be removed.');
      }
    } catch (error) {
      log.error(`Error deleting secret key ${key}: ${error.message}`);
      res.status(500).send('Failed to delete secret.');
    }
  });

  // ========================================================
  // --- NEW: DB Manager API Routes ---
  // ========================================================
  const dbManagerRouter = express.Router();

  // Middleware: Ensure DB client is connected & user is authenticated for ALL DB Manager routes
  dbManagerRouter.use(async (req, res, next) => {
    // 1. Check Authentication first
    if (!authUser(req)) {
      log.warn(`Unauthorized access attempt to DB Manager API from IP: ${req.ip}`);
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // 2. Check DB Connection
    if (!dbClientInstance || !dbClientInstance.connected) {
      log.warn('DB client not ready for DB Manager request, attempting to ensure connection...');
      try {
        if (dbClientInstance && !dbClientInstance.connected) {
          await dbClientInstance.reconnect();
        }
        if (!dbClientInstance || !dbClientInstance.connected) {
          log.warn('Re-initializing DB client for DB Manager request...');
          dbClientInstance = await createClient();
        }
        if (!dbClientInstance || !dbClientInstance.connected) {
          log.error('Failed to ensure DB client connection for DB Manager request.');
          return res.status(503).json({ error: 'Database connection unavailable' });
        }
        log.info('DB client connection verified for DB Manager request.');
      } catch (error) {
        log.error(`Failed to initialize/reconnect DB client for DB Manager: ${error.message}`);
        const statusCode = error.message === 'WRONG_KEY' ? 401 : 503;
        return res.status(statusCode).json({ error: `Database connection failed: ${error.message}` });
      }
    }
    next(); // Proceed
  });

  /// GET /api/db-manager/databases - List all databases
  dbManagerRouter.get('/databases', async (req, res) => {
    try {
      const result = await dbClientInstance.query('SHOW DATABASES');

      if (result && result.error) {
        return res.status(500).json({ error: `Failed to list databases: ${result.error}`, code: result.code });
      }
      if (!Array.isArray(result)) {
        log.error('Unexpected result format from SHOW DATABASES:', result);
        return res.status(500).json({ error: 'Unexpected error fetching databases.' });
      }

      // Filter out system databases if desired
      const systemDBs = ['information_schema', 'mysql', 'performance_schema', 'sys', 'flux_backlog'];
      const databases = result
        .map((row) => row.Database)
        .filter((dbName) => !systemDBs.includes(dbName.toLowerCase()));

      res.json({ databases });
    } catch (error) {
      log.error(`Error in /databases: ${error.message}`);
      res.status(500).json({ error: 'Internal server error while fetching databases' });
    }
  });

  // GET /api/db-manager/databases/:dbName/tables - List tables in a specific database
  dbManagerRouter.get('/databases/:dbName/tables', async (req, res) => {
    const { dbName } = req.params;
    const safeDbName = sanitize(dbName.replace(/`/g, '')); // Sanitize and remove backticks
    if (!safeDbName) {
      return res.status(400).json({ error: 'Invalid database name provided.' });
    }

    try {
      const setResult = await dbClientInstance.setDB(safeDbName);
      if (setResult && setResult.error) {
        // Handle case where DB doesn't exist or access denied
        if (setResult.code === 'ER_BAD_DB_ERROR') {
          return res.status(404).json({ error: `Database '${safeDbName}' not found or access denied.` });
        }
        return res.status(500).json({ error: `Failed to select database ${safeDbName}: ${setResult.error}`, code: setResult.code });
      }

      const result = await dbClientInstance.query('SHOW TABLES');
      if (result && result.error) {
        return res.status(500).json({ error: `Failed to list tables for ${safeDbName}: ${result.error}`, code: result.code });
      }
      if (!Array.isArray(result)) {
        log.error(`Unexpected result format from SHOW TABLES in ${safeDbName}:`, result);
        return res.status(500).json({ error: `Unexpected error fetching tables for ${safeDbName}.` });
      }

      const tables = result.map((row) => Object.values(row)[0]);
      res.json({ tables });
    } catch (error) {
      log.error(`Error in /databases/${safeDbName}/tables: ${error.message}`);
      res.status(500).json({ error: `Internal server error while fetching tables for ${safeDbName}` });
    }
  });

  // GET /api/db-manager/databases/:dbName/tables/:tableName/structure - Get table structure
  dbManagerRouter.get('/databases/:dbName/tables/:tableName/structure', async (req, res) => {
    const { dbName, tableName } = req.params;
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    const safeTableName = sanitize(tableName.replace(/`/g, ''));
    if (!safeDbName || !safeTableName) {
      return res.status(400).json({ error: 'Invalid database or table name provided.' });
    }

    try {
      const setResult = await dbClientInstance.setDB(safeDbName);
      if (setResult && setResult.error) {
        if (setResult.code === 'ER_BAD_DB_ERROR') {
          return res.status(404).json({ error: `Database '${safeDbName}' not found or access denied.` });
        }
        return res.status(500).json({ error: `Failed to select database ${safeDbName}: ${setResult.error}`, code: setResult.code });
      }

      const result = await dbClientInstance.query(`DESCRIBE \`${safeTableName}\``);
      if (result && result.error) {
        if (result.code === 'ER_NO_SUCH_TABLE') {
          return res.status(404).json({ error: `Table '${safeDbName}.${safeTableName}' not found.` });
        }
        return res.status(500).json({ error: `Failed to get structure for ${safeDbName}.${safeTableName}: ${result.error}`, code: result.code });
      }
      if (!Array.isArray(result)) {
        log.error(`Unexpected result format from DESCRIBE ${safeDbName}.${safeTableName}:`, result);
        return res.status(500).json({ error: `Unexpected error fetching structure for ${safeDbName}.${safeTableName}.` });
      }

      res.json({ columns: result });
    } catch (error) {
      log.error(`Error in /databases/${safeDbName}/tables/${safeTableName}/structure: ${error.message}`);
      res.status(500).json({ error: `Internal server error while fetching structure for ${safeDbName}.${safeTableName}` });
    }
  });

  // GET /api/db-manager/databases/:dbName/tables/:tableName/rows - Get rows (with pagination)
  dbManagerRouter.get('/databases/:dbName/tables/:tableName/rows', async (req, res) => {
    const { dbName, tableName } = req.params;
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    const safeTableName = sanitize(tableName.replace(/`/g, ''));
    if (!safeDbName || !safeTableName || safeDbName.includes('..') || safeTableName.includes('..')) {
      return res.status(400).json({ error: 'Invalid database or table name provided.' });
    }

    const limit = parseInt(req.query.limit, 10) || 100;
    const page = parseInt(req.query.page, 10) || 1; // Use page instead of offset
    const offset = (page - 1) * limit;

    if (limit <= 0 || page <= 0) {
      return res.status(400).json({ error: 'Invalid limit or page number.' });
    }

    try {
      const setResult = await dbClientInstance.setDB(safeDbName);
      if (setResult && setResult.error) { /* ... error handling ... */
        if (setResult.code === 'ER_BAD_DB_ERROR') return res.status(404).json({ error: `Database '${safeDbName}' not found or access denied.` });
        return res.status(500).json({ error: `Failed to select database ${safeDbName}: ${setResult.error}`, code: setResult.code });
      }

      // Use Promise.all to fetch rows and count concurrently
      const [rowsResult, countResult] = await Promise.all([
        dbClientInstance.query(`SELECT * FROM ${quoteIdentifier(safeTableName)} LIMIT ${limit} OFFSET ${offset}`),
        dbClientInstance.query(`SELECT COUNT(*) as totalRows FROM ${quoteIdentifier(safeTableName)}`),
      ]);

      // Check for errors in rows query
      if (rowsResult && rowsResult.error) {
        if (rowsResult.code === 'ER_NO_SUCH_TABLE') return res.status(404).json({ error: `Table '${safeDbName}.${safeTableName}' not found.` });
        return res.status(500).json({ error: `Failed to get rows: ${rowsResult.error}`, code: rowsResult.code });
      }
      if (!Array.isArray(rowsResult)) {
        log.error('Unexpected rows result format:', rowsResult);
        return res.status(500).json({ error: 'Unexpected error fetching rows.' });
      }

      // Check for errors in count query
      let totalRows = 0;
      if (countResult && !countResult.error && Array.isArray(countResult) && countResult.length > 0) {
        totalRows = countResult[0].totalRows;
      } else if (countResult && countResult.error) {
        log.warn(`Failed to get total row count for ${safeDbName}.${safeTableName}: ${countResult.error}`);
        // Proceed without total count, or return error based on requirements
      } else {
        log.warn(`Unexpected count result format for ${safeDbName}.${safeTableName}:`, countResult);
      }

      res.json({
        rows: rowsResult, totalRows, currentPage: page, rowsPerPage: limit,
      });
    } catch (error) {
      log.error(`Error fetching rows/count for ${safeDbName}.${safeTableName}: ${error.message}`);
      res.status(500).json({ error: 'Internal server error while fetching rows' });
    }
  });

  // PUT /api/db-manager/databases/:dbName/tables/:tableName/rows - Update a row
  dbManagerRouter.put('/databases/:dbName/tables/:tableName/rows', async (req, res) => {
    const { dbName, tableName } = req.params;
    const { pkColumn, pkValue, updates } = req.body; // updates is an object { colName: newValue, ... }

    // --- Validation ---
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    const safeTableName = sanitize(tableName.replace(/`/g, ''));
    const safePkColumn = sanitize(pkColumn?.replace(/`/g, '')); // Sanitize PK column name

    if (!safeDbName || !safeTableName || !safePkColumn || pkValue === undefined || !updates || typeof updates !== 'object' || Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'Invalid input: Missing database, table, primary key info, or update data.' });
    }
    if (safeDbName.includes('..') || safeTableName.includes('..') || safePkColumn.includes('..')) {
      return res.status(400).json({ error: 'Invalid characters in names.' });
    }

    // --- Build Query using manual escaping ---
    const setClauses = [];
    // eslint-disable-next-line guard-for-in
    for (const col in updates) {
      // Sanitize column name before quoting
      const safeCol = sanitize(col.replace(/`/g, ''));
      if (!safeCol || safeCol.includes('..')) {
        log.warn(`Skipping update for potentially unsafe column name: ${col}`);
        // eslint-disable-next-line no-continue
        continue; // Skip potentially unsafe column names
      }
      // Manually escape the value using the connection's escape method
      const escapedValue = dbClientInstance.connection.escape(updates[col]); // Use dbClient.connection.escape
      setClauses.push(`${quoteIdentifier(safeCol)} = ${escapedValue}`);
    }

    if (setClauses.length === 0) {
      return res.status(400).json({ error: 'No valid columns provided for update.' });
    }

    // Escape the primary key value for the WHERE clause
    const escapedPkValue = dbClientInstance.connection.escape(pkValue);

    // Construct the final query string
    const query = `UPDATE ${quoteIdentifier(safeTableName)} SET ${setClauses.join(', ')} WHERE ${quoteIdentifier(safePkColumn)} = ${escapedPkValue}`;

    // --- Execute ---
    try {
      if (Operator.IamMaster) {
        // Emit a message to all nodes if needed (e.g., for replication)
        const result = await Operator.sendWriteQuery(query);
        if (result && result.error) {
          if (result.code === 'ER_NO_SUCH_TABLE') return res.status(404).json({ error: `Table '${safeDbName}.${safeTableName}' not found.` });
          if (result.code === 'ER_BAD_FIELD_ERROR') return res.status(400).json({ error: `Invalid column name provided for update: ${result.error}` });
          return res.status(500).json({ error: `Failed to update row: ${result.error}`, code: result.code });
        }
        // Check affectedRows (result[0] for execute often contains metadata)
        if (result && result.affectedRows !== undefined) {
          if (result.affectedRows > 0) {
            res.json({ success: true, message: `Row updated successfully (PK: ${pkValue}).`, affectedRows: result.affectedRows });
          } else {
            res.status(404).json({ error: `Row not found with PK ${safePkColumn}=${pkValue} or no changes made.` });
          }
        } else {
          log.warn('Update executed but result format unexpected:', result);
          res.json({ success: true, message: 'Update command executed, but result format was unexpected.' });
        }
      } else {
        return res.status(500).json({ error: 'Failed: only master nodes can do this' });
      }
    } catch (error) {
      log.error(`Error updating row in ${safeDbName}.${safeTableName}: ${error.message}`);
      res.status(500).json({ error: 'Internal server error while updating row.' });
    }
  });

  // DELETE /api/db-manager/databases/:dbName/tables/:tableName/rows - Delete a row
  dbManagerRouter.delete('/databases/:dbName/tables/:tableName/rows', async (req, res) => {
    const { dbName, tableName } = req.params;
    const { pkColumn, pkValue } = req.body; // Get PK info from body

    // --- Validation ---
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    const safeTableName = sanitize(tableName.replace(/`/g, ''));
    const safePkColumn = sanitize(pkColumn?.replace(/`/g, ''));

    if (!safeDbName || !safeTableName || !safePkColumn || pkValue === undefined) {
      return res.status(400).json({ error: 'Invalid input: Missing database, table, or primary key info.' });
    }
    if (safeDbName.includes('..') || safeTableName.includes('..') || safePkColumn.includes('..')) {
      return res.status(400).json({ error: 'Invalid characters in names.' });
    }

    // --- Build Query ---
    const queryParams = [pkValue];
    const query = `DELETE FROM ${quoteIdentifier(safeTableName)} WHERE ${quoteIdentifier(safePkColumn)} = ${queryParams}`;

    // --- Execute ---
    try {
      if (Operator.IamMaster) {
        // Emit a message to all nodes if needed (e.g., for replication)
        const result = await Operator.sendWriteQuery(query);
        if (result && result.error) {
          if (result.code === 'ER_NO_SUCH_TABLE') return res.status(404).json({ error: `Table '${safeDbName}.${safeTableName}' not found.` });
          return res.status(500).json({ error: `Failed to delete row: ${result.error}`, code: result.code });
        }
        if (result && result.affectedRows !== undefined) {
          if (result.affectedRows > 0) {
            res.json({ success: true, message: `Row deleted successfully (PK: ${pkValue}).`, affectedRows: result.affectedRows });
          } else {
            res.status(404).json({ error: `Row not found with PK ${safePkColumn}=${pkValue}.` });
          }
        } else {
          log.warn('Delete executed but result format unexpected:', result);
          res.json({ success: true, message: 'Delete command executed, but result format was unexpected.' });
        }
      } else {
        return res.status(500).json({ error: 'Failed: only master nodes can do this' });
      }
    } catch (error) {
      log.error(`Error deleting row in ${safeDbName}.${safeTableName}: ${error.message}`);
      res.status(500).json({ error: 'Internal server error while deleting row.' });
    }
  });

  // DELETE /api/db-manager/databases/:dbName/tables/:tableName - Delete a table
  dbManagerRouter.delete('/databases/:dbName/tables/:tableName', async (req, res) => {
    const { dbName, tableName } = req.params;

    // --- Validation ---
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    const safeTableName = sanitize(tableName.replace(/`/g, ''));
    if (!safeDbName || !safeTableName || safeDbName.includes('..') || safeTableName.includes('..')) {
      return res.status(400).json({ error: 'Invalid database or table name provided.' });
    }

    // --- Build Query ---
    // DANGER ZONE: DROP TABLE is irreversible. Use with extreme caution.
    const query = `DROP TABLE IF EXISTS ${quoteIdentifier(safeTableName)}`; // Use IF EXISTS for safety

    // --- Execute ---
    try {
      const setResult = await dbClientInstance.setDB(safeDbName);
      if (setResult && setResult.error) { /* ... error handling ... */
        if (setResult.code === 'ER_BAD_DB_ERROR') return res.status(404).json({ error: `Database '${safeDbName}' not found or access denied.` });
        return res.status(500).json({ error: `Failed to select database ${safeDbName}: ${setResult.error}`, code: setResult.code });
      }

      log.warn(`Executing DROP TABLE query for ${safeDbName}.${safeTableName}. This is irreversible.`);
      if (Operator.IamMaster) {
        // Emit a message to all nodes if needed (e.g., for replication)
        const result = await Operator.sendWriteQuery(query);
        if (result && result.error) {
          // Error codes might differ for DROP TABLE failures
          return res.status(500).json({ error: `Failed to drop table: ${result.error}`, code: result.code });
        }
      } else {
        return res.status(500).json({ error: 'Failed: only master nodes can do this' });
      }

      // DROP TABLE doesn't typically return affectedRows. Success is indicated by lack of error.
      res.json({ success: true, message: `Table ${safeTableName} dropped successfully (if it existed).` });
    } catch (error) {
      log.error(`Error dropping table ${safeDbName}.${safeTableName}: ${error.message}`);
      res.status(500).json({ error: 'Internal server error while dropping table.' });
    }
  });

  // POST /api/db-manager/databases/:dbName/run-query - Run a custom query
  dbManagerRouter.post('/databases/:dbName/run-query', async (req, res) => {
    const { dbName } = req.params;
    const { query } = req.body;

    // --- Validation ---
    const safeDbName = sanitize(dbName.replace(/`/g, ''));
    if (!safeDbName || safeDbName.includes('..')) {
      return res.status(400).json({ error: 'Invalid database name provided.' });
    }
    if (!query || typeof query !== 'string' || query.trim().length === 0) {
      return res.status(400).json({ error: 'Missing or empty query.' });
    }

    // --- SECURITY CHECK ---
    // VERY IMPORTANT: Add checks to prevent dangerous queries.
    // This is a basic example, needs significant enhancement for production.
    const lowerQuery = query.toLowerCase().trim();
    const disallowedKeywords = ['drop ', 'delete ', 'alter ', 'truncate ']; // Example blacklist
    let writeQuery = false;
    // Allow SELECT by default, explicitly block others for now
    if (!lowerQuery.startsWith('select ')) {
      // More fine-grained check: allow specific non-selects if needed, but be careful
      let isDisallowed = false;
      writeQuery = true;
      for (const keyword of disallowedKeywords) {
        if (lowerQuery.startsWith(keyword)) {
          isDisallowed = true;
          break;
        }
      }
      if (isDisallowed) {
        log.warn(`Attempted to run potentially dangerous query type by user: ${query}`);
        return res.status(403).json({ error: 'Query type not allowed. Only SELECT queries are permitted in this basic implementation.' });
      }
      // If it's not SELECT and not explicitly disallowed, maybe allow SHOW, DESCRIBE etc.?
      // Add more allowed keywords if necessary: const allowedNonSelect = ['show ', 'describe '];
    }
    // Add checks for comments that might hide malicious code, multiple statements etc.
    if (query.includes('--') || query.includes('/*') || query.includes('*/') || query.includes(';')) {
      // Basic check for comments or multiple statements (can be bypassed)
      log.warn(`Query contains potentially problematic characters (comments, semicolons): ${query}`);
      // Decide whether to block or attempt sanitization (blocking is safer)
      // return res.status(400).json({ error: 'Query contains potentially unsafe characters or multiple statements.' });
    }

    // --- Execute ---
    try {
      if (!writeQuery) {
        const setResult = await dbClientInstance.setDB(safeDbName);
        if (setResult && setResult.error) { /* ... error handling ... */
          if (setResult.code === 'ER_BAD_DB_ERROR') return res.status(404).json({ error: `Database '${safeDbName}' not found or access denied.` });
          return res.status(500).json({ error: `Failed to select database ${safeDbName}: ${setResult.error}`, code: setResult.code });
        }
        log.info(`Executing custom query in DB ${safeDbName}: ${query}`);
        // Use simple query execution. Prepared statements aren't suitable for arbitrary queries.
        const result = await dbClientInstance.query(query, true); // Get raw result [rows, fields]
        log.debug(`Query result: ${JSON.stringify(result)}`);
        if (result && result.error) {
          // Provide specific DB errors back to the user
          return res.status(400).json({ error: `Query execution failed: ${result.error}`, code: result.code });
        }
        if (Array.isArray(result) && result.length === 2 && Array.isArray(result[0]) && Array.isArray(result[1])) {
          // Likely a SELECT result
          res.json({ success: true, rows: result[0], fields: result[1] });
        } else {
          res.json({ success: true, result });
        }
      } else {
        // eslint-disable-next-line no-lonely-if
        if (Operator.IamMaster) {
          // Emit a message to all nodes if needed (e.g., for replication)
          const result = await Operator.sendWriteQuery(query);
          if (result && result.error) {
            // Provide specific DB errors back to the user
            return res.status(400).json({ error: `Query execution failed: ${result.error}`, code: result.code });
          }
          if (result && result.affectedRows !== undefined) {
            // Likely an INSERT/UPDATE/DELETE result (if allowed)
            res.json({ success: true, affectedRows: result.affectedRows, changedRows: result.changedRows });
          } else {
            // Other results (e.g., SHOW, DESCRIBE might be just rows)
            res.json({ success: true, result });
          }
        } else {
          return res.status(500).json({ error: 'Failed: only master nodes can do this' });
        }
      }
    } catch (error) {
      log.error(`Error running custom query in ${safeDbName}: ${error.message}`);
      // Avoid sending detailed internal errors unless necessary
      res.status(500).json({ error: 'Internal server error while running custom query.' });
    }
  });

  // Mount the DB Manager router under /api/db-manager
  // This path MUST match the 'dbManagerApiPath' in the Vue frontend
  app.use('/api/db-manager', dbManagerRouter);

  // ========================================================
  // --- Server Listening ---
  // ========================================================

  if (config.ssl) {
    const keys = Security.generateRSAKey();
    const httpsOptions = {
      key: keys.pemPrivateKey,
      cert: keys.pemCertificate,
    };
    https.createServer(httpsOptions, app).listen(config.debugUIPort, () => {
      log.info(`starting SSL interface on port ${config.debugUIPort}`);
    });
  } else {
    app.listen(config.debugUIPort, () => {
      log.info(`starting interface on port ${config.debugUIPort}`);
    });
  }
} // End of startUI function

/**
* [validate] - Placeholder/Example validation
* @param {string} ip
*/
async function validate(ip) {
  // Replace with actual validation logic if needed for socket connections
  if (Operator.AppNodes.includes(ip)) return true;
  // log.info(`appnodes: ${JSON.stringify(Operator.AppNodes)}`);
  return false; // Default to false if not in AppNodes
}

/**
* [initServer] - Main server initialization function
*/
async function initServer() {
  try {
    Security.init(); // Initialize security components

    // --- Initialize DB Client Instance ---
    // Do this early so it's available for startUI and socket handlers
    log.info('Initializing Database Client...');
    dbClientInstance = await createClient(); // Use the factory function
    if (!dbClientInstance || !dbClientInstance.connected) {
      // Handle critical failure to connect on startup
      log.error('CRITICAL: Failed to initialize database client on startup. Exiting.');
      process.exit(1); // Exit if DB connection is essential
    }
    log.info('Database Client Initialized Successfully.');

    // --- Start UI and API Server ---
    await startUI(); // Now start the Express app

    // --- Initialize Operator and Socket.IO ---
    await Operator.init(dbClientInstance); // Pass DB client to Operator if needed
    const io = new Server(config.apiPort, { transports: ['websocket', 'polling'], maxHttpBufferSize: 4 * 1024 * 1024 });
    Operator.setServerSocket(io);

    // --- Socket.IO Connection Handling ---
    io.on('connection', async (socket) => {
      const ip = utill.convertIP(socket.handshake.address);
      let isOperatorNode = false;
      let isValidatedAppNode = false;

      try {
        isOperatorNode = auth(ip); // Check if it's an operator node
        isValidatedAppNode = await validate(ip); // Check if it's a validated app node

        if (isOperatorNode) {
          log.debug(`Operator node connected: ${ip}`);
          // Operator node specific event listeners
          socket.on('disconnect', (reason) => {
            // log.info(`Operator node disconnected: ${ip}, Reason: ${reason}`);
          });
          socket.on('getStatus', async (callback) => {
            log.info(`getStatus from Operator ${ip}`);
            try {
              callback({
                status: Operator.status,
                sequenceNumber: BackLog.sequenceNumber,
                firstSequenceNumber: await BackLog.getFirstSequenceNumber(),
                remoteIP: ip, // Use already determined IP
                masterIP: Operator.getMaster(),
              });
            } catch (e) { log.error('Error in getStatus callback:', e); }
          });
          socket.on('getMaster', (callback) => { // No need for async if just returning value
            log.info(`getMaster from Operator ${ip}`);
            try { callback({ status: 'success', message: Operator.getMaster() }); } catch (e) { log.error('Error in getMaster callback:', e); }
          });
          socket.on('getMyIp', (callback) => {
            log.info(`getMyIp from Operator ${ip}`);
            try { callback({ status: 'success', message: ip }); } catch (e) { log.error('Error in getMyIp callback:', e); }
          });
          socket.on('getBackLog', async (start, callback) => {
            log.info(`getBackLog from Operator ${ip}, starting at ${start}`);
            try {
              const records = await BackLog.getLogs(start, 400); // Limit records fetched
              callback({ status: Operator.status, sequenceNumber: BackLog.sequenceNumber, records });
            } catch (e) {
              log.error(`Error fetching backlog for ${ip}:`, e);
              callback({ status: 'error', message: 'Failed to fetch backlog', records: [] });
            }
          });
          socket.on('writeQuery', async (query, connId, callback) => {
            // log.info(`writeQuery from Operator ${ip}:${connId}`);
            try {
              const result = await BackLog.pushQuery(query); // [result, seq, timestamp]
              if (result && result.length >= 3) {
                // log.info(`Broadcasting query seq ${result[1]} to peers.`);
                socket.broadcast.emit('query', query, result[1], result[2], false); // Broadcast to other operators
                socket.emit('query', query, result[1], result[2], connId); // Echo back to sender with connId
                // Cache write queries
                queryCache.put(result[1], {
                  query, seq: result[1], timestamp: result[2], connId, ip,
                }, 5 * 60 * 1000); // 5 min cache
                callback({ status: Operator.status, result: result[0] });
              } else {
                log.error('Failed to push query to backlog or invalid result format.');
                callback({ status: 'error', result: 'Failed to process query' });
              }
            } catch (e) {
              log.error(`Error processing writeQuery from ${ip}:`, e);
              callback({ status: 'error', result: 'Error processing query' });
            }
          });
          socket.on('askQuery', async (index, callback) => {
            log.info(`${ip} asking for seqNo: ${index}`, 'magenta');
            let record = queryCache.get(index);
            let connId = false;
            let source = 'cache';
            try {
              if (!record) {
                source = 'db';
                const BLRecord = await BackLog.getLog(index);
                if (BLRecord && BLRecord.length > 0) {
                  record = BLRecord[0]; // { query, seq, timestamp }
                }
              }

              if (record) {
                // Check if requestor IP matches cached IP for connId echo
                if (record.ip === ip && record.connId) connId = record.connId;
                log.info(`Sending query seq ${index} to ${ip} from ${source}. ConnId: ${connId}`, 'magenta');
                socket.emit('query', record.query, record.seq, record.timestamp, connId);
              } else {
                log.warn(`Query seq ${index} not found in cache or DB for ${ip}.`);
              }
              if (callback) callback({ status: Operator.status });
            } catch (e) {
              log.error(`Error processing askQuery for seq ${index} from ${ip}:`, e);
              if (callback) callback({ status: 'error', message: 'Failed to retrieve query' });
            }
          });
          // --- Other Operator Listeners (shareKeys, updateKey, getKeys, etc.) ---
          socket.on('shareKeys', async (pubKey, callback) => {
            const nodeip = utill.convertIP(socket.handshake.address);
            // log.info(`shareKeys from ${nodeip}`);
            let nodeKey = null;
            if (!(`N${nodeip}` in Operator.keys)) {
              Operator.keys = await BackLog.getAllKeys();
              if (`N${nodeip}` in Operator.keys) nodeKey = Operator.keys[`N${nodeip}`];
              if (nodeKey) nodeKey = Security.publicEncrypt(pubKey, Buffer.from(nodeKey, 'hex'));
            }
            callback({
              status: Operator.status,
              commAESKey: Security.publicEncrypt(pubKey, Security.getCommAESKey()),
              commAESIV: Security.publicEncrypt(pubKey, Security.getCommAESIv()),
              key: nodeKey,
            });
          });
          socket.on('updateKey', async (key, value, callback) => {
            const decKey = Security.decryptComm(key);
            log.info(`updateKey from ${decKey}`);
            await BackLog.pushKey(decKey, value);
            Operator.keys[decKey] = value;
            socket.broadcast.emit('updateKey', key, value);
            callback({ status: Operator.status });
          });
          socket.on('getKeys', async (callback) => {
            const keysToSend = {};
            const nodeip = utill.convertIP(socket.handshake.address);
            for (const key in Operator.keys) {
              if ((key.startsWith('N') || key.startsWith('_')) && key !== `N${nodeip}`) {
                keysToSend[key] = Operator.keys[key];
              }
            }
            keysToSend[`N${Operator.myIP}`] = Security.encryptComm(`${Security.getKey()}:${Security.getIV()}`);
            callback({ status: Operator.status, keys: Security.encryptComm(JSON.stringify(keysToSend)) });
          });
          socket.on('resetMaster', async (callback) => {
            if (Operator.IamMaster) {
              Object.keys(io.sockets.sockets).forEach((s) => {
                io.sockets.sockets[s].disconnect(true);
              });
              Operator.findMaster();
            }
            callback({ status: Operator.status });
          });
          socket.on('rollBack', async (seqNo, callback) => {
            if (Operator.IamMaster) {
              Operator.rollBack(seqNo);
            }
            callback({ status: Operator.status });
          });
          socket.on('userSession', async (op, key, value, callback) => {
            if (op === 'add') { IdService.addNewSession(key, value); } else { IdService.removeSession(key); }
            socket.broadcast.emit('userSession', op, key, value);
            callback({ status: Operator.status });
          });
          socket.on('compressionStart', async (seqNo) => {
            await BackLog.pushKey('lastCompression', seqNo, false);
            socket.broadcast.emit('compressionStart', seqNo);
          });
        } else if (isValidatedAppNode) {
          log.debug(`Validated App node connected: ${ip}`);
          // Application node specific event listeners (if any)
          // Example: Allow read-only queries?
          socket.on('disconnect', (reason) => {
            log.info(`App node disconnected: ${ip}, Reason: ${reason}`);
          });
          // Add app-specific listeners here if needed
        } else {
          log.warn(`Connection rejected for unauthorized IP: ${ip}`);
          socket.disconnect(true); // Force disconnect
        }
      } catch (error) {
        log.error(`Error during socket connection validation for ${ip}: ${error.message}`);
        socket.disconnect(true);
      }
    }); // End io.on('connection')

    // --- Final Steps ---
    IdService.init(); // Initialize session management
    log.info(`API Server started on port ${config.apiPort}`);

    await Operator.findMaster(); // Initial master election
    log.info(`Find master finished, master is ${Operator.masterNode}`);
    if (!Operator.IamMaster) {
      Operator.initMasterConnection(); // Connect to master if slave
    }

    // Start periodic health checks
    setInterval(async () => {
      Operator.doHealthCheck();
    }, 120000); // Every 2 minutes
  } catch (error) {
    log.error(`CRITICAL: Failed to initialize server: ${error.message}`);
    // Perform cleanup if necessary (e.g., close DB connection if partially opened)
    if (dbClientInstance) {
      await dbClientInstance.close();
    }
    process.exit(1); // Exit if essential setup fails
  }
} // End of initServer function

// --- Start the Server ---
initServer();
