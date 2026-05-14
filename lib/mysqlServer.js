/* eslint-disable */
/**
* [This Library emulates the MySQL server protocol, giving you the ability to create MySQL-like service]
*/
const consts = require('./mysqlConstants');
const crypto = require('crypto');
const utill = require('./utill');
const ConnectionPool = require('./ConnectionPool');
const log = require('./log');


class Server {
 constructor(opts) {
  try{
    Object.assign(this,opts);
    
    if (! this.banner) this.banner = "Mysql 8.0";
    if (! this.salt) this.salt = crypto.randomBytes(20); 
    this.connId = null;
    this.sequence = 0;
    this.onPacket = this.helloPacketHandler;
    this.incoming = [];
    this.preparedStatements = new Map(); // stmtId → { sql, numParams, types[] }
    this.nextStmtId = 1;
    this._packetQueue = Promise.resolve(); // serialization chain
    
    this.socket.on('data', this.handleData);
    this.socket.on("error", (err) => {
      ConnectionPool.releaseConnection(this.connId);
    });
    this.socket.on("end", () => {
      ConnectionPool.releaseConnection(this.connId);
    });
    // if (this.handleDisconnect) this.socket.on('end', this.handleDisconnect);
    
    this.sendServerHello();
  }catch(err){
    console.log(err);
  }
 }
 
end = () => {
  try{
    this.socket.end();
    // ConnectionPool.releaseConnection(this.connId);
    // this.operator.sessionQueries[this.connId] = undefined
  }catch(err){
    // console.log(err);
  }
};

 writeHeader(data,len) {
  const seqOut = this.sequence % 256;
  data.writeUIntLE(len - 4,0,3);
  data.writeUInt8(seqOut, 3);
  this.sequence++;
  // log.debug(`[mysqlServer conn=${this.connId}] writeHeader seq_out=${seqOut} pktLen=${len-4}`);
 }

 sendPacket(payload) {
  return this.socket.write(payload);
 }
 
 newDefinition(params) {
  return {
   catalog: params.catalog ? params.catalog : 'def',
   schema: params.db,
   table: params.table,
   orgTable: params.orgTable,
   name: params.name,
   orgName: params.orgName,
   length: params.length ? params.length : 0,
   type: params.type ? params.type : consts.MYSQL_TYPE_STRING,
   flags: params.flags ? params.flags : 0,
   decimals: params.decimals,
   'default': params['default'],
  };
 }

 sendDefinitions(definitions) {
  // Write Definition Header
  let payload = Buffer.alloc(1024);
  let len = 4;
  len = writeLengthCodedBinary(payload,len,definitions.length);
  this.writeHeader(payload,len); 
  this.sendPacket(payload.slice(0,len));

  // Write each definition
  for (let definition of definitions) {
   len = 4;
   for (let field of [ 'catalog','schema','table','orgTable','name','orgName' ]) {
    let val = definition[field] || "";
    len = writeLengthCodedString(payload,len,val);
   }
   // MariaDB clients always set MARIADB_CLIENT_EXTENDED_METADATA and expect
   // a length-encoded uint here (0 = no extended metadata) before the fixed
   // fields block. mysql2 clients do not read this field.
   if (this.mariadbClient) {
    len = payload.writeUInt8(0x00, len);
   }
   len = payload.writeUInt8(0x0C, len);
   len = payload.writeUInt16LE(11, len); // ASCII
   len = payload.writeUInt32LE(definition.columnLength || 0, len);
   len = payload.writeUInt8(definition.columnType != null ? definition.columnType : consts.MYSQL_TYPE_VAR_STRING , len);
   len = payload.writeUInt16LE(definition.flags != null ? definition.flags : 0, len);
   len = payload.writeUInt8(definition.decimals != null ? definition.decimals : 0, len);
   len = payload.writeUInt16LE(0,len); // \0\0 FILLER
   len = writeLengthCodedString(payload,len,definition['default']);
   this.writeHeader(payload,len); 
   this.sendPacket(payload.slice(0,len));
   //console.log('def:' + JSON.stringify(definition));
   //console.log(payload.slice(4,len));
  }
  
  this.sendEOF();
 }

 sendRow(row) {
  let payload = Buffer.alloc(1024);
  let len = 4;
  for (let cell of row) {
   if (cell == null) {
    len = payload.writeUInt8(0xFB,len);
   } else {
    len = writeLengthCodedString(payload,len,cell);
   }
  }
  this.writeHeader(payload,len); 
  this.sendPacket(payload.slice(0,len));
 }

 sendRows(rows = []) {
  for (let row of rows) {
    // console.log(`sendng row: ${JSON.stringify(row)}`);
   this.sendRow(row);
  }
  this.sendEOF();
 }


 sendEOF({warningCount = 0, serverStatus = consts.SERVER_STATUS_AUTOCOMMIT} = {}) {
  // Write EOF
  let payload = Buffer.alloc(16);
  let len = 4;
  len = payload.writeUInt8(0xFE,len);
  len = payload.writeUInt16LE(warningCount,len);
  len = payload.writeUInt16LE(serverStatus,len);
  this.writeHeader(payload,len); 
  this.sendPacket(payload.slice(0,len));
 }

 sendServerHello = () => {
  //## Sending Server Hello...
  let payload = Buffer.alloc(128);
  let pos = 4;
  pos = payload.writeUInt8(10,pos); // Protocol version
  
  pos += payload.write("8.0",pos);
  pos = payload.writeUInt8(0,pos);

  pos = payload.writeUInt32LE(process.pid,pos);

  pos += this.salt.copy(payload,pos, 0,8);
  pos = payload.writeUInt8(0,pos);

  pos = payload.writeUInt16LE(
   consts.CLIENT_LONG_PASSWORD | 
   consts.CLIENT_CONNECT_WITH_DB | 
   consts.CLIENT_PROTOCOL_41 | 
   consts.CLIENT_SECURE_CONNECTION
  , pos);

  if (this.serverCharset) {
   pos = payload.writeUInt8(this.serverCharset,pos);
  } else {
   pos = payload.writeUInt8(0x21,pos); // latin1
  }
  pos = payload.writeUInt16LE(consts.SERVER_STATUS_AUTOCOMMIT,pos);
  payload.fill(0,pos,pos+13);
  pos += 13;

  pos += this.salt.copy(payload,pos,8);
  pos = payload.writeUInt8(0,pos);
  this.writeHeader(payload,pos); 

  return this.sendPacket(payload.slice(0,pos));
 }

 handleData = (data) => {
  try{
    if (data && data.length > 0) {
    this.incoming.push(data);
    }
    this.gatherIncoming();
    if (data == null) {
    // log.info("Connection closed");
    this.socket.destroy();
    }
  }catch(err){
    // console.log(err);
  }
 }
 
 gatherIncoming() {
  try{
    let incoming;
    if (this.incoming.length > 0) {
    let len = 0;
    for (let buf of this.incoming) {
      len += buf.length;
    }
    incoming = Buffer.alloc(len);
    len = 0;
    for (let buf of this.incoming) {
      len += buf.copy(incoming,len); 
    }
    } else {
    incoming = this.incoming[0];
    }
    let remaining = this.readPackets(incoming);
    this.incoming = [Buffer.from(remaining)];
  }catch(err){
    // console.log(err);
  }
 }
 
 readPackets(buf) {
  let offset = 0;
  let pktsThisSegment = 0;
  while (true) {
   let data = buf.slice(offset);
   if (data.length < 4) return data;
  
   let packetLength = data.readUIntLE(0,3);
   if (data.length < packetLength + 4) return data;

   const seqIn = data.readUIntLE(3,1);
   // NOTE: this.sequence is NOT set here — the queued closure sets it right
   // before dispatching each handler so async handlers don't race on sequence.
   offset += packetLength + 4;
   let packet = data.slice(4,packetLength + 4);
   pktsThisSegment++;
   const cmdByte = packet.length > 0 ? packet[0] : 0xFF;
   // log.debug(`[mysqlServer conn=${this.connId}] readPackets pkt#${pktsThisSegment} seq_in=${seqIn} cmd=0x${cmdByte.toString(16).padStart(2,'0')} pktLen=${packetLength} (${buf.length} bytes in segment)`);
   
   // Serialize async packet handlers: each packet waits for the previous one
   // to fully complete before this.sequence is updated for the next packet.
   // Without this, two packets arriving in the same TCP segment (same readPackets
   // call) would both start their async handlers concurrently, causing SEQ DRIFT.
   const capturedPacket = packet;
   const capturedSeqIn  = seqIn;
   this._packetQueue = this._packetQueue.then(() => {
     this.sequence = capturedSeqIn + 1;
     if (typeof this.onPacket !== 'function') {
       // log.debug(`[mysqlServer conn=${this.connId}] readPackets: onPacket is ${this.onPacket} for pkt seq_in=${capturedSeqIn} — packet dropped`);
       return;
     }
     return Promise.resolve(this.onPacket(capturedPacket));
   }).catch((err) => {
     // log.error(`[mysqlServer conn=${this.connId}] packet handler error: ${err && (err.stack || err.message || err)}`);
   });
   this.packetCount++;
  }
 }

 helloPacketHandler = (packet) => {
  //## Reading Client Hello...

  // http://dev.mysql.com/doc/internals/en/the-packet-header.html

  if (packet.length == 0) return this.sendError({ message: "Zero length hello packet" });

  let ptr = 0;

  let clientFlags = packet.slice(ptr,ptr+4);
  ptr += 4;

  let maxPacketSize = packet.slice(ptr,ptr+4);
  ptr += 4;

  this.clientCharset = packet.readUInt8(ptr);
  ptr++;

  // The last 4 bytes of the 23-byte filler carry MariaDB-specific client
  // capabilities (bits 32-63 of the combined capability integer).
  // MARIADB_CLIENT_EXTENDED_METADATA = 1n << 35n  → bit 3 of those 4 bytes.
  const mariadbExtCaps = packet.readUInt32LE(ptr + 19);
  this.mariadbClient = !!(mariadbExtCaps & 0x08);
  ptr += 23; // skip full filler

  let usernameEnd = packet.indexOf(0,ptr);
  let username = packet.toString('ascii',ptr,usernameEnd);
  ptr = usernameEnd + 1;

  let scrambleLength = packet.readUInt8(ptr);
  ptr++;

  if (scrambleLength > 0) {
   this.scramble = packet.slice(ptr,ptr+scrambleLength);
   ptr += scrambleLength;
  }
 
  let database;

  let databaseEnd = packet.indexOf(0,ptr);
  if (databaseEnd >= 0) {
   database = packet.toString('ascii',ptr,databaseEnd);
  }
  this.onPacket = null;
  const remoteIP = utill.convertIP(this.socket.remoteAddress);  
  return Promise.resolve(this.onAuthorize({ clientFlags, maxPacketSize, username, database, remoteIP}))
  .then( async (authorized) => {
   if (! authorized) throw `${remoteIP} Not Authorized`;
    this.connId = await ConnectionPool.getFreeConnection(this.socket);

 
   // log.info(`grabbing ${this.connId}`);
   this.onPacket = this.normalPacketHandler;
   this.gatherIncoming();
   this.sendOK({ message: "OK" });
  })
  .catch( (err) => {
   // log.error(err);
   // this.sendError( { message: "Authorization Failure" } );
   this.socket.destroy();
  });
 }

 normalPacketHandler(packet) {
  const id = this.connId;
  if (packet == null) throw "Empty packet";
  const command = packet.readUInt8(0);
  const extra = packet.length > 1 ? packet.slice(1) : null;
  const cmdName = {
    [consts.COM_QUERY]: 'COM_QUERY',
    [consts.COM_STMT_PREPARE]: 'COM_STMT_PREPARE',
    [consts.COM_STMT_EXECUTE]: 'COM_STMT_EXECUTE',
    [consts.COM_STMT_CLOSE]: 'COM_STMT_CLOSE',
    [consts.COM_STMT_RESET]: 'COM_STMT_RESET',
    [consts.COM_PING]: 'COM_PING',
    [consts.COM_QUIT]: 'COM_QUIT',
    [consts.COM_INIT_DB]: 'COM_INIT_DB',
  }[command] || `CMD_0x${command.toString(16).padStart(2,'0')}`;
  if (command === consts.COM_QUERY && extra) {
    const sql = extra.toString('utf8', 0, Math.min(extra.length, 100));
    // log.debug(`[mysqlServer conn=${id}] received COM_QUERY sql="${sql}"`);
  } else if (command !== consts.COM_STMT_PREPARE) {
    // COM_STMT_PREPARE is already logged in handleStmtPrepare; skip duplicate here
    // log.debug(`[mysqlServer conn=${id}] received ${cmdName} (0x${command.toString(16).padStart(2,'0')})`);
  }
  // Prepared-statement commands are handled inside the emulator so the caller's
  // onCommand only ever sees COM_QUERY (with parameters already interpolated).
  switch (command) {
    case consts.COM_STMT_PREPARE:    return this.handleStmtPrepare(extra);
    case consts.COM_STMT_EXECUTE:    return this.handleStmtExecute(extra, id);
    case consts.COM_STMT_CLOSE:      return this.handleStmtClose(extra);
    case consts.COM_STMT_RESET:      return this.handleStmtReset(extra);
    case consts.COM_SET_OPTION:      return this.sendEOF(); // acknowledge multi-statement toggle
    default:
      return this.onCommand({ command, extra, id });
  }
 }
 // ─── Prepared-statement support ──────────────────────────────────────────────

 // Send a single generic column-definition packet (used for param definitions).
 sendParamDefinition() {
  let payload = Buffer.alloc(256);
  let len = 4;
  for (const field of ['def', '', '', '', '?', '?']) {
   len = writeLengthCodedString(payload, len, field);
  }
  if (this.mariadbClient) len = payload.writeUInt8(0x00, len);
  len = payload.writeUInt8(0x0C, len);
  len = payload.writeUInt16LE(0x21, len);  // utf8 charset
  len = payload.writeUInt32LE(0, len);     // column length
  len = payload.writeUInt8(consts.MYSQL_TYPE_VAR_STRING, len);
  len = payload.writeUInt16LE(0, len);     // flags
  len = payload.writeUInt8(0, len);        // decimals
  len = payload.writeUInt16LE(0, len);     // filler
  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));
 }

 handleStmtPrepare(extra) {
  const sql = extra.toString();
  // Count unquoted ? placeholders
  let numParams = 0;
  let inSingle = false, inDouble = false;
  for (const ch of sql) {
   if (ch === "'" && !inDouble) inSingle = !inSingle;
   else if (ch === '"' && !inSingle) inDouble = !inDouble;
   else if (ch === '?' && !inSingle && !inDouble) numParams++;
  }
  const stmtId = this.nextStmtId++;
  this.preparedStatements.set(stmtId, { sql, numParams, types: [] });
  // log.debug(`[mysqlServer conn=${this.connId}] PREPARE stmtId=${stmtId} numParams=${numParams} sql="${sql.slice(0,120)}"`);

  // COM_STMT_PREPARE_OK: 0x00, stmt_id(4), num_columns(2), num_params(2), 0x00, warnings(2)
  let payload = Buffer.alloc(16);
  let len = 4;
  len = payload.writeUInt8(0x00, len);
  len = payload.writeUInt32LE(stmtId, len);
  len = payload.writeUInt16LE(0, len);         // num_columns (0 — no resultset meta at prepare time)
  len = payload.writeUInt16LE(numParams, len);
  len = payload.writeUInt8(0x00, len);
  len = payload.writeUInt16LE(0, len);
  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));

  // One param-definition packet per ?
  for (let i = 0; i < numParams; i++) this.sendParamDefinition();
  if (numParams > 0) this.sendEOF();
  // num_columns = 0, so no column-definition packets / EOF needed
 }

 handleStmtExecute(extra, connId) {
  let ptr = 0;
  const stmtId = extra.readUInt32LE(ptr); ptr += 4;
  ptr += 1; // flags
  ptr += 4; // iteration-count (always 1)

  const stmt = this.preparedStatements.get(stmtId);
  if (!stmt) {
   return this.sendError({ message: `Unknown prepared statement id: ${stmtId}`, errno: 1243, sqlState: 'HY000' });
  }
  const { sql, numParams } = stmt;
  // log.debug(`[mysqlServer conn=${connId}] EXECUTE stmtId=${stmtId} numParams=${numParams} sql="${sql.slice(0,120)}"`);

  // When onStmtResult is registered (binary-protocol path) we must always go
  // through _sendBinaryResultSet — even for 0-param statements.  Routing to
  // onCommand (text-protocol) while mysql2 expects binary-protocol causes
  // "Encoding not recognized: 'undefined'" and garbage data.
  if (numParams === 0) {
   if (typeof this.onStmtResult === 'function') {
    // log.debug(`[mysqlServer conn=${connId}] EXECUTE 0-param → _sendBinaryResultSet`);
    return this._sendBinaryResultSet(sql, connId);
   }
   // log.debug(`[mysqlServer conn=${connId}] EXECUTE 0-param → onCommand (no onStmtResult)`);
   return this.onCommand({ command: consts.COM_QUERY, extra: Buffer.from(sql), id: connId });
  }

  // Null bitmap
  const nullBitmapLen = Math.ceil(numParams / 8);
  const nullBitmap = extra.slice(ptr, ptr + nullBitmapLen);
  ptr += nullBitmapLen;

  const newParamsBoundFlag = extra.readUInt8(ptr); ptr += 1;
  if (newParamsBoundFlag === 1 || stmt.types.length === 0) {
   stmt.types = [];
   for (let i = 0; i < numParams; i++) {
    stmt.types.push(extra.readUInt16LE(ptr)); ptr += 2;
   }
  }

  const params = [];
  for (let i = 0; i < numParams; i++) {
   if ((nullBitmap[Math.floor(i / 8)] >> (i % 8)) & 1) { params.push(null); continue; }
   const typeCode = stmt.types[i] & 0xFF;
   const unsigned = !!(stmt.types[i] & 0x8000);
   let value;
   const pad = (n) => String(n).padStart(2, '0');
   switch (typeCode) {
    case 1: // TINY
     value = unsigned ? extra.readUInt8(ptr) : extra.readInt8(ptr); ptr += 1; break;
    case 2: case 13: // SHORT, YEAR
     value = unsigned ? extra.readUInt16LE(ptr) : extra.readInt16LE(ptr); ptr += 2; break;
    case 3: case 9: // LONG, INT24
     value = unsigned ? extra.readUInt32LE(ptr) : extra.readInt32LE(ptr); ptr += 4; break;
    case 8: // LONGLONG
     value = (unsigned ? extra.readBigUInt64LE(ptr) : extra.readBigInt64LE(ptr)).toString(); ptr += 8; break;
    case 4: // FLOAT
     value = extra.readFloatLE(ptr); ptr += 4; break;
    case 5: // DOUBLE
     value = extra.readDoubleLE(ptr); ptr += 8; break;
    case 10: case 7: case 12: { // DATE, TIMESTAMP, DATETIME
     const dlen = extra.readUInt8(ptr++);
     if (dlen === 0) { value = typeCode === 10 ? '0000-00-00' : '0000-00-00 00:00:00'; break; }
     const yr = extra.readUInt16LE(ptr); ptr += 2;
     const mo = extra.readUInt8(ptr++), dy = extra.readUInt8(ptr++);
     if (dlen === 4) { value = `${yr}-${pad(mo)}-${pad(dy)}`; break; }
     const hr = extra.readUInt8(ptr++), mi = extra.readUInt8(ptr++), sc = extra.readUInt8(ptr++);
     if (dlen >= 11) ptr += 4; // microseconds
     value = `${yr}-${pad(mo)}-${pad(dy)} ${pad(hr)}:${pad(mi)}:${pad(sc)}`; break;
    }
    case 11: { // TIME
     const tlen = extra.readUInt8(ptr++);
     if (tlen === 0) { value = '00:00:00'; break; }
     const neg = extra.readUInt8(ptr++);
     const days = extra.readUInt32LE(ptr); ptr += 4;
     const hr = extra.readUInt8(ptr++), mi = extra.readUInt8(ptr++), sc = extra.readUInt8(ptr++);
     if (tlen >= 12) ptr += 4;
     value = `${neg ? '-' : ''}${pad(days * 24 + hr)}:${pad(mi)}:${pad(sc)}`; break;
    }
    default: { // VARCHAR, BLOB, DECIMAL, all string-like types
     const lc = readLengthCodedNumber(extra, ptr); ptr = lc.pos;
     value = extra.toString('utf8', ptr, ptr + lc.value); ptr += lc.value; break;
    }
   }
   params.push(value);
  }

  const finalSql = buildQueryWithParams(sql, params);
  // log.debug(`[mysqlServer conn=${connId}] EXECUTE interpolated sql="${finalSql.slice(0,120)}"`);
  // If the caller provides onStmtResult, use it to get structured results and
  // encode them as binary-protocol rows.  Otherwise fall back to the raw-proxy
  // COM_QUERY path (which produces text-protocol rows, fine for mysql2 clients).
  if (typeof this.onStmtResult === 'function') {
   // log.debug(`[mysqlServer conn=${connId}] EXECUTE → _sendBinaryResultSet`);
   return this._sendBinaryResultSet(finalSql, connId);
  }
  // log.debug(`[mysqlServer conn=${connId}] EXECUTE → onCommand (no onStmtResult)`);
  return this.onCommand({ command: consts.COM_QUERY, extra: Buffer.from(finalSql), id: connId });
 }

 async _sendBinaryResultSet(sql, connId) {
  const seqAtEntry = this.sequence;
  // log.debug(`[mysqlServer conn=${connId}] _sendBinaryResultSet ENTER seq=${seqAtEntry} sql="${sql.slice(0,80)}"`);
  try {
   const result = await this.onStmtResult(sql, connId);
   const seqAfterAwait = this.sequence;
   if (seqAfterAwait !== seqAtEntry) {
    // log.debug(`[mysqlServer conn=${connId}] _sendBinaryResultSet SEQ DRIFT: was ${seqAtEntry} before await, now ${seqAfterAwait} after await — another command ran concurrently!`);
   }
   if (!result) {
    // log.debug(`[mysqlServer conn=${connId}] _sendBinaryResultSet: onStmtResult returned null → sendOK`);
    return this.sendOK({ message: 'OK' });
   }
   const { rows, fields } = result;
   // Non-SELECT (INSERT/UPDATE/DELETE) — fields is undefined or empty
   if (!fields || fields.length === 0) {
    const r = rows || {};
    // log.debug(`[mysqlServer conn=${connId}] _sendBinaryResultSet: write OK affectedRows=${r.affectedRows} insertId=${r.insertId}`);
    return this.sendOK({
     affectedRows: r.affectedRows != null ? r.affectedRows : 0,
     insertId: r.insertId != null ? r.insertId : undefined,
     message: 'OK',
    });
   }
   // SELECT — send column definitions (same format as COM_QUERY)
   // log.debug(`[mysqlServer conn=${connId}] _sendBinaryResultSet: SELECT ${Array.isArray(rows) ? rows.length : '?'} rows, ${fields.length} fields`);
   this.sendDefinitions(fields);
   // Send each row in binary-protocol format
   for (const row of rows) {
    this.sendBinaryRow(fields, row);
   }
   this.sendEOF();
  } catch (err) {
   // log.error(`[mysqlServer conn=${connId}] _sendBinaryResultSet error: ${err && (err.stack || err.message || err)}`);
   this.sendError({ message: (err && err.message) || 'Query error', errno: 1064, sqlState: '42000' });
  }
 }

 // Encode a result row using the MySQL binary row protocol:
 // [0x00][null_bitmap][binary_value ...]
 sendBinaryRow(fields, row) {
  const UNSIGNED_FLAG = 32;
  let payload = Buffer.alloc(65536); // generous; covers large rows
  let len = 4;

  const numCols = fields.length;
  const nullBitmapLen = Math.ceil((numCols + 2) / 8);

  len = payload.writeUInt8(0x00, len); // binary row packet marker

  const nullBitmapStart = len;
  payload.fill(0, len, len + nullBitmapLen);
  len += nullBitmapLen;

  for (let i = 0; i < numCols; i++) {
   const f = fields[i];
   const value = row[f.name];

   if (value === null || value === undefined) {
    const byte = Math.floor((i + 2) / 8);
    const bit = (i + 2) % 8;
    payload[nullBitmapStart + byte] |= (1 << bit);
    continue;
   }

   const t = f.columnType;
   const unsigned = !!(f.flags & UNSIGNED_FLAG);

   if (t === 1) {        // TINY
    len = unsigned ? payload.writeUInt8(Number(value), len) : payload.writeInt8(Number(value), len);
   } else if (t === 2) { // SHORT
    len = unsigned ? payload.writeUInt16LE(Number(value), len) : payload.writeInt16LE(Number(value), len);
   } else if (t === 3 || t === 9) { // LONG, INT24
    len = unsigned ? payload.writeUInt32LE(Number(value), len) : payload.writeInt32LE(Number(value), len);
   } else if (t === 8) { // LONGLONG
    const big = typeof value === 'bigint' ? value : BigInt(value);
    len = unsigned ? payload.writeBigUInt64LE(big, len) : payload.writeBigInt64LE(big, len);
   } else if (t === 4) { // FLOAT
    len = payload.writeFloatLE(Number(value), len);
   } else if (t === 5) { // DOUBLE
    len = payload.writeDoubleLE(Number(value), len);
   } else {
    // VARCHAR, TEXT, BLOB, NEWDECIMAL, DATE*, TIME* → length-encoded string
    len = writeLengthCodedString(payload, len, String(value));
   }
  }

  this.writeHeader(payload, len);
  this.sendPacket(payload.slice(0, len));
 }

 handleStmtClose(extra) {
  if (extra && extra.length >= 4) {
   const stmtId = extra.readUInt32LE(0);
   this.preparedStatements.delete(stmtId);
  }
  // COM_STMT_CLOSE sends no response
 }

 handleStmtReset(extra) {
  if (extra && extra.length >= 4) {
   const stmtId = extra.readUInt32LE(0);
   const stmt = this.preparedStatements.get(stmtId);
   if (stmt) stmt.types = []; // clear cached types
  }
  this.sendOK({ message: 'OK' });
 }

 // ─────────────────────────────────────────────────────────────────────────────

 sendOK({ message, affectedRows = 0, insertId, warningCount = 0}) {
  let data = Buffer.alloc(message.length + 64);
  let len = 4;
  len = data.writeUInt8(0,len);
  len = writeLengthCodedBinary(data,len,affectedRows);
  len = writeLengthCodedBinary(data,len,insertId);
  len = data.writeUInt16LE(consts.SERVER_STATUS_AUTOCOMMIT,len);
  len = data.writeUInt16LE(warningCount,len);
  len = writeLengthCodedString(data,len,message);

  this.writeHeader(data,len);
  this.sendPacket(data.slice(0,len));
 }

 sendRequestHeader({ fieldCount = 0,message, affectedRows = 0, insertId, warningCount = 0}) {
  let data = Buffer.alloc(message.length + 64);
  let len = 4;
  len = data.writeUInt8(0,len);
  len = writeLengthCodedBinary(data,len,fieldCount);
  len = writeLengthCodedBinary(data,len,affectedRows);
  len = writeLengthCodedBinary(data,len,insertId);
  len = writeLengthCodedString(data,len,'');
  len = data.writeUInt16LE(consts.SERVER_STATUS_AUTOCOMMIT,len);
  len = data.writeUInt16LE(warningCount,len);
  len = writeLengthCodedString(data,len,message);

  this.writeHeader(data,len);
  this.sendPacket(data.slice(0,len));
 }

 sendError({ message = 'Unknown MySQL error',errno = 2000,sqlState = "HY000"}) {
  //## Sending Error ...
  console.log(message);
  let data = Buffer.alloc(message.length + 64);
  let len = 4;
  len = data.writeUInt8(0xFF,len);
  len = data.writeUInt16LE(errno,len);
  len += data.write("#",len);
  len += data.write(sqlState,len,5);
  len += data.write(message,len);
  len = data.writeUInt8(0,len);

  this.writeHeader(data,len);
  this.sendPacket(data.slice(0,len));
 }
}
function writeLengthCodedString(buf,pos,str) {
 if (str == null) return buf.writeUInt8(0,pos);
 if (typeof str !== 'string') {
  str = str.toString();
 }
 const len = Buffer.byteLength(str);
 // Use compact 1-byte length for strings that fit (covers all typical
 // catalog/schema/table/column names). This is required for clients such
 // as the native mariadb driver that read the length with readUInt8().
 if (len < 251) {
  buf.writeUInt8(len, pos);
  buf.write(str, pos + 1);
  return pos + len + 1;
 } else if (len < 65536) {
  buf.writeUInt8(0xFC, pos);
  buf.writeUInt16LE(len, pos + 1);
  buf.write(str, pos + 3);
  return pos + len + 3;
 } else {
  buf.writeUInt8(0xFD, pos);
  buf.writeUIntLE(len, pos + 1, 3);
  buf.write(str, pos + 4);
  return pos + len + 4;
 }
}

function writeLengthCodedBinary(buf,pos,number) {
 if (number == null) {
  return buf.writeUInt8(251,pos);
 } else if (number < 251) {
  return buf.writeUInt8(number,pos);
 } else if (number < 0x10000) {
  buf.writeUInt8(252,pos);
  buf.writeUInt16LE(number,pos + 1);
  return pos + 3;
 } else if (number < 0x1000000) {
  buf.writeUInt8(253,pos);
  buf.writeUIntLE(number,pos + 1,3);
  return pos + 4;
 } else {
  buf.writeUInt8(254,pos);
  buf.writeUIntLE(number,pos + 1,8);
  return pos + 9;
 }
}
// ─── Prepared-statement helpers ───────────────────────────────────────────────

// Read a length-coded integer from buf at pos; returns { value, pos } where pos
// points to the first byte AFTER the encoded integer.
function readLengthCodedNumber(buf, pos) {
 const first = buf.readUInt8(pos);
 if (first < 251) return { value: first, pos: pos + 1 };
 if (first === 0xFC) return { value: buf.readUInt16LE(pos + 1), pos: pos + 3 };
 if (first === 0xFD) return { value: buf.readUIntLE(pos + 1, 3), pos: pos + 4 };
 if (first === 0xFE) return { value: buf.readUIntLE(pos + 1, 8), pos: pos + 9 };
 return { value: 0, pos: pos + 1 }; // 0xFB = NULL
}

// Substitute positional ? placeholders in sql with their escaped param values.
function buildQueryWithParams(sql, params) {
 let result = '';
 let paramIdx = 0;
 let inSingle = false, inDouble = false;
 for (let i = 0; i < sql.length; i++) {
  const ch = sql[i];
  if (ch === "'" && !inDouble) { inSingle = !inSingle; result += ch; }
  else if (ch === '"' && !inSingle) { inDouble = !inDouble; result += ch; }
  else if (ch === '?' && !inSingle && !inDouble) { result += escapeSqlValue(params[paramIdx++]); }
  else { result += ch; }
 }
 return result;
}

// Safely escape a JavaScript value for embedding in a SQL string.
function escapeSqlValue(value) {
 if (value === null || value === undefined) return 'NULL';
 if (typeof value === 'boolean') return value ? '1' : '0';
 if (typeof value === 'number') return String(value);
 if (typeof value === 'bigint') return value.toString();
 const str = String(value);
 return "'" + str
  .replace(/\\/g, '\\\\')
  .replace(/'/g, "\\'")
  .replace(/\0/g, '\\0')
  .replace(/\n/g, '\\n')
  .replace(/\r/g, '\\r')
  .replace(/\x1a/g, '\\Z') + "'";
}

// ─────────────────────────────────────────────────────────────────────────────

exports.createServer = function (options) {
    return new Server(options);
};