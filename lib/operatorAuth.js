const {
  hkdfSync, createHmac, timingSafeEqual,
} = require('crypto');
const fs = require('fs');
const https = require('https');
const config = require('../ClusterOperator/config');

const PURPOSE = 'flux-shared-db/operator-auth/v1';
// Fixed release cutoff, not a grace period measured from process startup.
const DEFAULT_ENFORCE_AT = '2026-10-25T17:48:17Z';

function enforcementTime(settings = config) {
  const value = settings.operatorAuthEnforceAt === undefined ? DEFAULT_ENFORCE_AT : settings.operatorAuthEnforceAt;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)) {
    throw new Error('OPERATOR_AUTH_ENFORCE_AT must be an ISO 8601 UTC datetime ending in Z');
  }
  const timestamp = Date.parse(value);
  const canonical = value.includes('.') ? value : value.replace('Z', '.000Z');
  if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString() !== canonical) {
    throw new Error('Invalid OPERATOR_AUTH_ENFORCE_AT datetime');
  }
  return timestamp;
}

function createPolicy(settings, now = Date.now) {
  const enforceAt = enforcementTime(settings);
  let enforced = false;
  return {
    enforceAt,
    isEnforced: () => {
      enforced = enforced || now() >= enforceAt;
      return enforced;
    },
  };
}

function deriveKey(settings = config) {
  const dedicated = settings.operatorApiSecret !== undefined;
  const secret = dedicated ? settings.operatorApiSecret : settings.dbPass;
  // Keep legacy DB password defaults working; explicit API secrets stay strict.
  if (typeof secret !== 'string' || !secret.length || (dedicated && Buffer.byteLength(secret) < 32)) {
    throw new Error('Operator authentication requires a nonempty DB password or OPERATOR_API_SECRET with at least 32 bytes');
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

function middleware(isAllowed, settings = config, policy = createPolicy(settings)) {
  const key = deriveKey(settings);
  return (socket, next) => {
    if (!isAllowed(socket)) return next(new Error('Operator authentication failed'));
    const received = socket.handshake.auth?.operatorProof;
    let authenticated = false;
    if (received !== undefined) {
      // Invalid proofs never receive the legacy exception.
      if (typeof received !== 'string' || !/^[a-f0-9]{64}$/.test(received)) {
        return next(new Error('Operator authentication failed'));
      }
      const expected = Buffer.from(proof(key, socket.conn.id), 'hex');
      if (!timingSafeEqual(expected, Buffer.from(received, 'hex'))) {
        return next(new Error('Operator authentication failed'));
      }
      authenticated = true;
    } else if (policy.isEnforced()) {
      return next(new Error('Operator authentication failed'));
    }
    Object.assign(socket.data, { operatorAuthenticated: authenticated });
    if (!authenticated) {
      // Stop commands at the deadline even before the expiry timer gets CPU time.
      socket.use((_packet, proceed) => {
        if (policy.isEnforced()) {
          socket.disconnect(true);
          return;
        }
        proceed();
      });
    }
    return next();
  };
}

function install(io, isAllowed, settings = config, options = {}) {
  const now = options.now || Date.now;
  const setTimer = options.setTimer || setTimeout;
  const clearTimer = options.clearTimer || clearTimeout;
  const policy = createPolicy(settings, now);
  io.use(middleware(isAllowed, settings, policy));
  let timer;
  let closed = false;
  const enforce = () => {
    if (!policy.isEnforced()) return;
    io.sockets.sockets.forEach((socket) => {
      if (!socket.data.operatorAuthenticated) socket.disconnect(true);
    });
  };
  const schedule = () => {
    if (closed) return;
    if (policy.isEnforced()) {
      enforce();
      if (options.onEnforce) options.onEnforce();
      return;
    }
    // Node timers cannot exceed a signed 32-bit delay. Recheck wall time on wake.
    timer = setTimer(schedule, Math.min(policy.enforceAt - now(), 0x7fffffff));
    if (timer && timer.unref) timer.unref();
  };
  const close = () => { closed = true; clearTimer(timer); };
  io.httpServer.once('close', close);
  schedule();
  return {
    enforce, close, isEnforced: policy.isEnforced, enforceAt: policy.enforceAt,
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
  deriveKey, proof, middleware, install, enforcementTime, DEFAULT_ENFORCE_AT, clientOptions, peerURL, serverTLSOptions,
};
