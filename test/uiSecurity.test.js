/* eslint-disable no-restricted-syntax */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const admin = fs.readFileSync(path.join(__dirname, '../ui/index.html'), 'utf8');
const login = fs.readFileSync(path.join(__dirname, '../ui/login.html'), 'utf8');

function components() {
  const registered = new Map();
  const app = { component: (name, component) => registered.set(name, component), mount() {} };
  const script = [...admin.matchAll(/<script>([\s\S]*?)<\/script>/g)][0][1];
  vm.runInNewContext(script, { Vue: { createApp: () => app } });
  return registered;
}

test('DB manager renders JSON, arrays, strings and NULL as text in both result views', () => {
  const manager = components().get('manage-database-page');
  const format = manager.methods.formatValue;
  const attack = '<svg onload=window.__xss=1></svg>';
  const nested = { value: [attack, { html: '<img src=x onerror=window.__xss=1>' }] };
  assert.equal(format(nested), JSON.stringify(nested));
  assert.equal(format([attack]), JSON.stringify([attack]));
  assert.equal(format(attack), attack);
  assert.equal(format(null), 'NULL');
  assert.equal(format(42), 42);
  assert.equal(format(false), false);
  assert.equal(format('a'.repeat(101)), `${'a'.repeat(100)}...`);
  const circular = {}; circular.self = circular;
  assert.equal(format(circular), '[Object]');
  assert.doesNotMatch(manager.template, /v-html/);
  assert.match(manager.template, /{{ formatValue\(row\[col.Field\]\) }}/);
  assert.match(manager.template, /{{ formatValue\(resRow\[field.name\]\) }}/);
});

test('confirmation messages containing database values are displayed as text', () => {
  const registered = components();
  const modal = registered.get('confirm-modal');
  assert.doesNotMatch(modal.template, /v-html/);
  assert.match(modal.template, /{{ message }}/);
  let emitted;
  const attack = '<img src=x onerror=window.__xss=1>';
  registered.get('manage-database-page').methods.confirmDeleteRow.call({
    primaryKeyColumn: 'id', $emit: (_event, payload) => { emitted = payload; },
  }, { id: attack }, 0);
  assert.ok(emitted.message.includes(attack));
});

test('all remote admin and login scripts/styles are version-pinned and integrity-checked', () => {
  const hashes = new Map();
  for (const html of [admin, login]) {
    const tags = [...html.matchAll(/<(?:script|link)\b[^>]*(?:src|href)="(https:\/\/[^"]+)"[^>]*>/g)];
    assert.ok(tags.length > 0);
    for (const [tag, url] of tags) {
      assert.match(url, /(?:@|\/)\d+\.\d+\.\d+\//, `Unpinned asset: ${url}`);
      const integrity = /integrity="(sha384-[A-Za-z0-9+/]{64})"/.exec(tag);
      assert.ok(integrity, `Missing SHA-384 integrity: ${url}`);
      assert.match(tag, /crossorigin="anonymous"/);
      if (hashes.has(url)) assert.equal(hashes.get(url), integrity[1]);
      hashes.set(url, integrity[1]);
    }
    assert.doesNotMatch(html, /\$\.getScript\(/, 'Dynamic script loading bypasses SRI');
  }
});
