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
    
    this.socket.on('data', this.handleData);
    this.socket.on("error", (err) => {
      ConnectionPool.releaseConnection(this.connId);
      // console.log(err.stack);
    });
    if (this.handleDisconnect) this.socket.on('end', this.handleDisconnect);
    
    this.sendServerHello();
  }catch(err){
    console.log(err);
  }
 }
 
end = () => {
  try{
    this.socket.end();
    ConnectionPool.releaseConnection(this.connId);
  }catch(err){
    // console.log(err);
  }
};

 writeHeader(data,len) {
  data.writeUIntLE(len - 4,0,3);
  data.writeUInt8(this.sequence++ % 256, 3);
  //console.log(`seq: ${this.sequence}`)
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
    log.info("Connection closed");
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
  while (true) {
   let data = buf.slice(offset);
   if (data.length < 4) return data;
  
   let packetLength = data.readUIntLE(0,3);
   if (data.length < packetLength + 4) return data;

   //console.log({ data, offset });
   
   this.sequence = data.readUIntLE(3,1) + 1;
   offset += packetLength + 4;
   let packet = data.slice(4,packetLength + 4);
   
   this.onPacket(packet);
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
 
  packet.slice(ptr,ptr+23);// filler
  ptr += 23;

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
   log.error(err);
   // this.sendError( { message: "Authorization Failure" } );
   this.socket.destroy();
  });
 }

 normalPacketHandler(packet) {
   //console.log(`new packet: ${packet.readUInt8(0)}`);
  const id = this.connId;
  if (packet == null) throw "Empty packet";
  return this.onCommand({
   command: packet.readUInt8(0),
   extra: packet.length > 1 ? packet.slice(1) : null,
   id
  });
 }
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
  //Mangle it
  str = str.toString();
 }
 buf.writeUInt8(253,pos);
 buf.writeUIntLE(str.length,pos + 1, 3);
 buf.write(str,pos + 4);
 return pos + str.length + 4;
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
exports.createServer = function (options) {
    return new Server(options);
};