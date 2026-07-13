const assert = require('node:assert/strict');
const test = require('node:test');
const bitcoinMessage = require('../lib/bitcoinMessage');

test('verifies a known Bitcoin compact-message signature', () => {
  const message = 'This is an example of a signed message.';
  const address = '1F3sAm6ZtwLAUnj7d38pGFxtP3RVEvtsbV';
  const signature = 'H9L5yLFjti0QTHhPyFrZCT1V/MMnBtXKmoiKDZ78NDBjERki6ZTQZdSMCtkgoNmp17By9ItJr8o7ChX0XxY91nk=';

  assert.equal(bitcoinMessage.verify(message, address, signature), true);
  assert.equal(bitcoinMessage.verify(`${message}!`, address, signature), false);
});

test('converts a public key to its expected P2PKH address', () => {
  const publicKey = '048a789e0910b6aa314f63d2cc666bd44fa4b71d7397cb5466902dc594c1a0a0d2e4d234528ff87b83f971ab2b12cd2939ff33c7846716827a5b0e8233049d8aad';
  assert.equal(bitcoinMessage.publicKeyToAddress(publicKey, '2089'), 'znkz4JE6Y4m8xWoo4ryTnpxwBT5F7vFDgNf');
});
