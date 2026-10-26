/* global window, document, Vue */
/* eslint-disable no-restricted-syntax, no-await-in-loop, no-underscore-dangle */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const https = require('node:https');
const axios = require('axios');

// Opt in with RUN_UI_BROWSER_TESTS=1, an installed playwright-core package, and
// CHROMIUM_PATH pointing at an existing Chromium executable. No browser downloads.
test('Chromium blocks stored DB XSS and modified CDN scripts; both pages load with SRI', {
  skip: process.env.RUN_UI_BROWSER_TESTS !== '1', timeout: 90000,
}, async (t) => {
  // eslint-disable-next-line global-require, import/no-unresolved
  const { chromium } = require('playwright-core');
  const directory = path.join(__dirname, '../../ui');
  const admin = fs.readFileSync(path.join(directory, 'index.html'), 'utf8');
  const login = fs.readFileSync(path.join(directory, 'login.html'), 'utf8');
  const csrf = fs.readFileSync(path.join(directory, 'assets/csrf.js'), 'utf8');
  const assets = new Map();
  const tags = [...`${admin}${login}`.matchAll(/<(?:script|link)\b[^>]*(?:src|href)="(https:\/\/[^"]+)"[^>]*>/g)];
  for (const [tag, url] of tags) assets.set(url, { integrity: /integrity="([^"]+)"/.exec(tag)[1] });
  // Download the exact CDN bytes, validate every committed hash, then serve those
  // bytes at their original browser URLs with CORS enabled for real SRI checks.
  const agent = new https.Agent({ rejectUnauthorized: true });
  t.after(() => agent.destroy());
  await Promise.all([...assets].map(async ([url, asset]) => {
    const response = await axios.get(url, {
      responseType: 'arraybuffer', httpsAgent: agent, timeout: 20000, maxRedirects: 0,
    });
    Object.assign(asset, { body: Buffer.from(response.data) });
    const actual = `sha384-${crypto.createHash('sha384').update(asset.body).digest('base64')}`;
    assert.equal(actual, asset.integrity, `CDN bytes differ from SRI: ${url}`);
  }));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH, headless: true, args: ['--no-sandbox'] });
  t.after(() => browser.close());
  const context = await browser.newContext();
  let tamper = false;
  await context.route('**/*', async (route) => {
    const url = route.request().url();
    if (assets.has(url)) {
      const asset = assets.get(url);
      const body = tamper && url.includes('/vue@') ? Buffer.from('window.__tamperedCdn = true;') : asset.body;
      return route.fulfill({ body, contentType: url.endsWith('.css') ? 'text/css' : 'application/javascript', headers: { 'Access-Control-Allow-Origin': '*' } });
    }
    const target = new URL(url);
    if (target.origin !== 'http://shared-db.test') return route.abort();
    if (target.pathname === '/') {
      // Capture the production components without starting dashboard/API polling.
      const html = admin.replace("app.mount('#app');", 'window.testComponents = app._context.components;');
      return route.fulfill({ body: html, contentType: 'text/html' });
    }
    if (target.pathname === '/login') return route.fulfill({ body: login, contentType: 'text/html' });
    if (target.pathname === '/assets/csrf.js') return route.fulfill({ body: csrf, contentType: 'application/javascript' });
    if (target.pathname === '/loginphrase') return route.fulfill({ body: `${Date.now()}:SDB-login:${'a'.repeat(22)}:${'b'.repeat(22)}` });
    return route.fulfill({ status: 404, body: '' });
  });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('http://shared-db.test/');
  await page.waitForFunction(() => window.testComponents);
  assert.deepEqual(await page.evaluate(() => ({ vue: typeof Vue, axios: typeof axios, chart: typeof Chart })), { vue: 'object', axios: 'function', chart: 'function' });
  await page.evaluate(async () => {
    const attack = '<img src=x onerror=window.__xss=1>';
    const json = { nested: [attack, '<svg onload=window.__xss=1></svg>'] };
    const modalElement = document.createElement('div'); document.body.append(modalElement);
    window.confirmation = Vue.createApp({ ...window.testComponents['confirm-modal'], mounted() {} }).mount(modalElement);
    const manager = window.testComponents['manage-database-page'];
    const container = document.createElement('div'); container.id = 'security-manager'; document.body.append(container);
    window.manager = Vue.createApp({
      ...manager,
      mounted() {},
      data() {
        return {
          ...manager.data(),
          selectedDatabase: 'example',
          selectedTable: 'items',
          databases: ['example'],
          tables: ['items'],
          columns: [{ Field: 'id', Key: 'PRI' }, { Field: 'json' }, { Field: 'empty' }],
          rows: [{ id: attack, json, empty: null }],
          primaryKeyColumn: 'id',
          totalRows: 1,
          queryResultType: 'rows',
          queryResult: { rows: [{ json }], fields: [{ name: 'json' }] },
        };
      },
    }, {
      apiUrl: window.location.origin, baseApiPath: '/api/db-manager', onShowConfirm: ({ message }) => window.confirmation.show(message),
    }).mount(container);
    window.manager.confirmDeleteRow(window.manager.rows[0], 0);
    await Vue.nextTick();
  });
  const body = await page.locator('#security-manager').textContent();
  assert.ok(body.includes('<img src=x onerror=window.__xss=1>'));
  assert.ok(body.includes('NULL'));
  assert.equal(await page.locator('#security-manager td img, #security-manager td svg, .modal-body img, .modal-body svg').count(), 0);
  assert.equal(await page.locator('#security-manager .query-results-table td').count(), 1);
  assert.ok((await page.locator('#security-manager .query-results-table td').textContent()).includes('<svg onload=window.__xss=1>'));
  assert.ok((await page.locator('.modal-body.text-center').textContent()).includes('<img src=x onerror=window.__xss=1>'));
  assert.equal(await page.evaluate(() => window.__xss), undefined);
  assert.equal(errors.length, 0, errors.join('\n'));

  await page.goto('http://shared-db.test/login');
  await page.waitForFunction(() => window.loginPhrase && document.querySelector('#particles-js canvas'));
  assert.deepEqual(await page.evaluate(() => ({ jquery: typeof $, particles: typeof particlesJS, bootstrap: typeof bootstrap })), { jquery: 'function', particles: 'function', bootstrap: 'object' });
  assert.equal(errors.length, 0, errors.join('\n'));

  tamper = true;
  const changed = await context.newPage();
  const integrityErrors = [];
  changed.on('console', (message) => { if (/integrity|digest/i.test(message.text())) integrityErrors.push(message.text()); });
  await changed.goto('http://shared-db.test/');
  assert.equal(await changed.evaluate(() => window.__tamperedCdn), undefined);
  assert.equal(await changed.evaluate(() => typeof Vue), 'undefined');
  assert.ok(integrityErrors.length > 0, 'Browser did not report an SRI rejection');
});
