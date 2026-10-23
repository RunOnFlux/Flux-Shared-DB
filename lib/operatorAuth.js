const {
  hkdfSync, createHmac, timingSafeEqual,
} = require('crypto');
const fs = require('fs');
const https = require('https');
const config = require('../ClusterOperator/config');

const PURPOSE = 'flux-shared-db/operator-auth/v1';

function deriveKey(settings = config) {
  const secret = settings.operatorApiSecret === undefined ? settings.dbPass : settings.operatorApiSecret;
  if (typeof secret !== 'string' || Buffer.byteLength(secret) < 32) {
    throw new Error('Operator authentication requires OPERATOR_API_SECRET or DB_INIT_PASS with at least 32 bytes; use a randomly generated secret');
  }
  if (typeof settings.DBAppName !== 'string' || !settings.DBAppName.trim()) {
    throw new Error('Operator authentication requires DB_APPNAME or a successfully resolved Flux app name');
  }
  return Buffer.from(hkdfSync('sha256', secret, settings.DBAppName, PURPOSE, 32));
}

// Engine.IO assigns a fresh random session ID before the namespace auth exchange.
// Binding the proof to it prevents replay on another connection. It does not
// replace TLS: plaintext traffic remains readable and vulnerable to live relays.
function proof(key, sessionId) {
  if (typeof sessionId !== 'string' || !sessionId) throw new Error('Missing operator transport session');
  return createHmac('sha256', key).update(`${PURPOSE}\0${sessionId}`).digest('hex');
}

function middleware(isAllowed, settings = config) {
  const key = deriveKey(settings);
  return (socket, next) => {
    const received = socket.handshake.auth?.operatorProof;
    if (!isAllowed(socket) || typeof received !== 'string' || !/^[a-f0-9]{64}$/.test(received)) {
      return next(new Error('Operator authentication failed'));
    }
    const expected = Buffer.from(proof(key, socket.conn.id), 'hex');
    if (!timingSafeEqual(expected, Buffer.from(received, 'hex'))) {
      return next(new Error('Operator authentication failed'));
    }
    return next();
  };
}

function tlsEnabled(settings) {
  if (Boolean(settings.operatorTLSCert) !== Boolean(settings.operatorTLSKey)) {
    throw new Error('Set both OPERATOR_TLS_CERT and OPERATOR_TLS_KEY');
  }
  if ((settings.operatorTLSCA || settings.operatorTLSServername) && !settings.operatorTLSCert) {
    throw new Error('Operator TLS CA/servername requires OPERATOR_TLS_CERT and OPERATOR_TLS_KEY');
  }
  return Boolean(settings.operatorTLSCert);
}

function peerURL(ip, port, settings = config) {
  return `${tlsEnabled(settings) ? 'https' : 'http'}://${ip}:${port}`;
}

function serverTLSOptions(settings = config) {
  if (!tlsEnabled(settings)) return null;
  return {
    cert: fs.readFileSync(settings.operatorTLSCert),
    key: fs.readFileSync(settings.operatorTLSKey),
  };
}

function clientOptions(getSessionId, settings = config) {
  const key = deriveKey(settings);
  const tls = tlsEnabled(settings);
  return {
    auth: (callback) => callback({ operatorProof: proof(key, getSessionId()) }),
    rejectUnauthorized: true,
    ...(tls && settings.operatorTLSCA ? { ca: fs.readFileSync(settings.operatorTLSCA) } : {}),
    // Engine.IO filters transport options; an HTTPS agent carries servername
    // through both polling and WebSocket transports.
    ...(tls ? {
      agent: new https.Agent({
        rejectUnauthorized: true,
        ...(settings.operatorTLSCA ? { ca: fs.readFileSync(settings.operatorTLSCA) } : {}),
        ...(settings.operatorTLSServername ? { servername: settings.operatorTLSServername } : {}),
      }),
    } : {}),
  };
}

module.exports = {
  deriveKey, proof, middleware, clientOptions, peerURL, serverTLSOptions,
};
