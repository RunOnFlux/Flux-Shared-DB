/* eslint-disable no-underscore-dangle, no-bitwise */
const { Duplex, Transform } = require('stream');

// mysql2 automatically requests MariaDB extended metadata when advertised.
// Hide that optional capability on the backend greeting so every raw result
// uses the standard column layout negotiated with our frontend clients.
class BackendHandshake extends Transform {
  constructor() {
    super();
    this.pending = Buffer.alloc(0);
    this.complete = false;
  }

  _transform(chunk, _encoding, done) {
    if (this.complete) return done(null, chunk);
    this.pending = Buffer.concat([this.pending, chunk]);
    if (this.pending.length < 4) return done();
    const length = this.pending.readUIntLE(0, 3);
    if (length > 65535) return done(new Error('Backend greeting exceeds 64 KiB'));
    if (this.pending.length < length + 4) return done();
    const packet = this.pending;
    if (packet[4] === 10) {
      const versionEnd = packet.indexOf(0, 5);
      const capabilities = versionEnd + 14;
      const extended = versionEnd + 28;
      if (versionEnd >= 5 && extended + 4 <= length + 4 && !(packet.readUInt16LE(capabilities) & 1)) {
        packet.writeUInt32LE((packet.readUInt32LE(extended) & ~8) >>> 0, extended);
      }
    }
    this.complete = true;
    this.pending = null;
    return done(null, packet);
  }

  _flush(done) {
    return done(this.complete ? null : new Error('Incomplete backend greeting'));
  }
}

function createBackendStream(socket) {
  const greeting = new BackendHandshake();
  const stream = Duplex.from({ readable: greeting, writable: socket });
  socket.on('error', (error) => stream.destroy(error));
  socket.pipe(greeting);
  return stream;
}

module.exports = { BackendHandshake, createBackendStream };
