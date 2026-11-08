const { createHmac, timingSafeEqual } = require('crypto');

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const MUTATING_GETS = new Set(['/logout', '/isloggedin']);

function sessionCredential(req) {
  return req.headers.loginphrase || req.cookies?.loginphrase;
}

// The authenticated session is the secret, so tokens remain valid across
// replicas/restarts without another cookie or a node-local token store.
function tokenForSession(session) {
  if (typeof session !== 'string' || !session) throw new Error('Missing session');
  return createHmac('sha256', session).update('flux-shared-db/ui-csrf/v1').digest('hex');
}

function cookieCsrfProtection(req, res, next) {
  const path = req.path.toLowerCase().replace(/\/+$/, '');
  // Wallet callbacks authenticate their signed payload, not ambient cookies.
  if (path === '/verifylogin' || (SAFE_METHODS.has(req.method) && !MUTATING_GETS.has(path))) return next();

  // Sibling tenant sites are not trusted, even though browsers call them same-site.
  const site = req.headers['sec-fetch-site'];
  if (site && site !== 'same-origin' && site !== 'none') {
    return res.status(403).json({ error: 'Cross-origin browser writes are not allowed.', code: 'CSRF_INVALID' });
  }
  // A nonempty loginphrase header selects explicit credentials in authUser;
  // it never falls back to cookie authentication if those credentials are bad.
  if (req.headers.loginphrase || !req.cookies?.loginphrase) return next();
  const session = req.cookies.loginphrase;
  const token = req.headers['x-csrf-token'];
  if (typeof session !== 'string' || typeof token !== 'string' || !/^[a-f0-9]{64}$/.test(token)
      || !timingSafeEqual(Buffer.from(token, 'hex'), Buffer.from(tokenForSession(session), 'hex'))) {
    return res.status(403).json({ error: 'Missing or invalid CSRF token.', code: 'CSRF_INVALID' });
  }
  return next();
}

module.exports = { cookieCsrfProtection, sessionCredential, tokenForSession };
