const {
  hkdfSync, createHmac, timingSafeEqual,
} = require('crypto');
const fs = require('fs');
const https = require('https');
const config = require('../ClusterOperator/config');

const PURPOSE = 'flux-shared-db/operator-auth/v1';
// Fixed release cutoff, not a grace period measured from process startup.
const DEFAULT_ENFORCE_AT = '2026-10-25T17:48:17Z';
const runtimes = new WeakMap();
const policies = new WeakMap();

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

function tlsEnabled(settings) {
  if (Boolean(settings.operatorTLSCert) !== Boolean(settings.operatorTLSKey)) {
    throw new Error('Set both OPERATOR_TLS_CERT and OPERATOR_TLS_KEY');
  }
  if ((settings.operatorTLSCA || settings.operatorTLSServername) && !settings.operatorTLSCert) {
    throw new Error('Operator TLS CA/servername requires OPERATOR_TLS_CERT and OPERATOR_TLS_KEY');
  }
  return Boolean(settings.operatorTLSCert);
}

// Resolve lazily: DBAppName may be populated from Flux hostinfo after imports.
// A snapshot invalidates the cache if configuration is explicitly changed.
function runtime(settings = config) {
  const snapshot = [settings.DBAppName, settings.dbPass, settings.operatorApiSecret, settings.operatorApiSecretSecondary,
    settings.operatorTLSCert, settings.operatorTLSKey, settings.operatorTLSCA, settings.operatorTLSServername];
  const cached = runtimes.get(settings);
  if (cached && snapshot.every((value, index) => value === cached.snapshot[index])) return cached;
  const key = deriveKey(settings);
  const keys = [key];
  if (settings.operatorApiSecretSecondary !== undefined) {
    const secondary = settings.operatorApiSecretSecondary;
    // The retiring key may be an existing short DB password. Never transmit it.
    if (typeof secondary !== 'string' || !secondary.length) throw new Error('OPERATOR_API_SECRET_SECONDARY must not be empty');
    keys.push(Buffer.from(hkdfSync('sha256', secondary, settings.DBAppName, PURPOSE, 32)));
  }
  const tls = tlsEnabled(settings);
  const ca = tls && settings.operatorTLSCA ? fs.readFileSync(settings.operatorTLSCA) : undefined;
  const clientTLS = { rejectUnauthorized: true };
  let serverTLS = null;
  if (tls) {
    serverTLS = { cert: fs.readFileSync(settings.operatorTLSCert), key: fs.readFileSync(settings.operatorTLSKey) };
    Object.assign(clientTLS, {
      ...(ca ? { ca } : {}),
      // An agent carries servername through polling and WebSocket transports.
      agent: new https.Agent({
        rejectUnauthorized: true,
        ...(ca ? { ca } : {}),
        ...(settings.operatorTLSServername ? { servername: settings.operatorTLSServername } : {}),
      }),
    });
  }
  const result = {
    snapshot, key, keys, clientTLS, serverTLS,
  };
  runtimes.set(settings, result);
  return result;
}

function initialize(settings = config) {
  runtime(settings);
  enforcementTime(settings);
}

function hasReplicationPeer(io, settings = config) {
  const sockets = io?.sockets?.sockets;
  if (!sockets) return false;
  const policy = policies.get(io);
  const grace = policy ? !policy.isEnforced() : Date.now() < enforcementTime(settings);
  return Array.from(sockets.values()).some((socket) => socket.connected && socket.data.operatorReplica
    && (socket.data.operatorAuthenticated || grace));
}

// Engine.IO assigns a fresh random session ID before the namespace auth exchange.
// Binding the proof to it prevents replay on another connection. It does not
// replace TLS: plaintext traffic remains readable and vulnerable to live relays.
function proof(key, sessionId) {
  if (typeof sessionId !== 'string' || !sessionId) throw new Error('Missing operator transport session');
  return createHmac('sha256', key).update(`${PURPOSE}\0${sessionId}`).digest('hex');
}

function middleware(isAllowed, settings = config, policy = createPolicy(settings)) {
  const { keys } = runtime(settings);
  return (socket, next) => {
    if (!isAllowed(socket)) return next(new Error('Operator authentication failed'));
    const received = socket.handshake.auth?.operatorProof;
    let authenticated = false;
    if (received !== undefined) {
      // Invalid proofs never receive the legacy exception.
      if (typeof received !== 'string' || !/^[a-f0-9]{64}$/.test(received)) {
        return next(new Error('Operator authentication failed'));
      }
      const candidate = Buffer.from(received, 'hex');
      if (!keys.some((key) => timingSafeEqual(Buffer.from(proof(key, socket.conn.id), 'hex'), candidate))) {
        return next(new Error('Operator authentication failed'));
      }
      authenticated = true;
    } else if (policy.isEnforced()) {
      return next(new Error('Operator authentication failed'));
    }
    Object.assign(socket.data, {
      operatorAuthenticated: authenticated,
      // Older peers have no role field, including during long snapshot imports.
      // Only namespace-admitted legacy peers can reach this point.
      operatorReplica: socket.handshake.auth?.operatorRole === 'replica' || socket.handshake.auth?.operatorRole === undefined,
    });
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
  policies.set(io, policy);
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

function peerURL(ip, port, settings = config) {
  return `${tlsEnabled(settings) ? 'https' : 'http'}://${ip}:${port}`;
}

function serverTLSOptions(settings = config) {
  return runtime(settings).serverTLS;
}

function clientOptions(getSessionId, settings = config, role = 'discovery') {
  const { key, clientTLS } = runtime(settings);
  return {
    ...clientTLS,
    auth: (callback) => callback({ operatorProof: proof(key, getSessionId()), operatorRole: role }),
  };
}

module.exports = {
  deriveKey, proof, middleware, install, enforcementTime, DEFAULT_ENFORCE_AT, clientOptions, peerURL, serverTLSOptions, initialize, hasReplicationPeer,
};
