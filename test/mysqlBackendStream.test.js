/* eslint-disable no-underscore-dangle, no-bitwise, no-await-in-loop, no-restricted-syntax */
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { BackendHandshake } = require('../lib/mysqlBackendStream');

function greeting(mysql = false) {
  const version = Buffer.from(mysql ? '8.3.0\0' : '11.4.0-MariaDB\0');
  const packet = Buffer.alloc(4 + 1 + version.length + 50);
  packet.writeUIntLE(packet.length - 4, 0, 3);
  packet[4] = 10;
  version.copy(packet, 5);
  const end = 5 + version.length - 1;
  packet.writeUInt16LE(mysql ? 0xffff : 0xfffe, end + 14);
  packet.writeUInt32LE(0xf000001f, end + 28);
  return { packet, extended: end + 28 };
}

async function transform(chunks) {
  const stream = new BackendHandshake();
  const output = [];
  stream.on('data', (chunk) => output.push(chunk));
  const ended = once(stream, 'end');
  for (const chunk of chunks) stream.write(chunk);
  stream.end();
  await ended;
  return Buffer.concat(output);
}

test('backend greeting removes only extended metadata even when fragmented at every byte', async () => {
  const { packet, extended } = greeting();
  const expected = Buffer.from(packet);
  expected.writeUInt32LE(0xf0000017, extended);
  assert.deepEqual(await transform([...packet].map((byte) => Buffer.from([byte]))), expected);
  assert.equal(packet.readUInt32LE(extended), 0xf000001f);
});

test('backend stream leaves all response packets and MySQL capability bytes unchanged', async () => {
  const { packet } = greeting(true);
  const responses = Buffer.from([1, 0, 0, 1, 0, 2, 0, 0, 2, 1, 2]);
  assert.deepEqual(await transform([Buffer.concat([packet, responses]), responses]), Buffer.concat([packet, responses, responses]));
});

test('coalesced greeting and data preserve every byte except MariaDB metadata capability', async () => {
  const { packet, extended } = greeting();
  const response = Buffer.from([1, 0, 0, 1, 0]);
  const expected = Buffer.concat([packet, response]);
  expected.writeUInt32LE(0xf0000017, extended);
  assert.deepEqual(await transform([Buffer.concat([packet, response])]), expected);
});

test('truncated and oversized backend greetings reject instead of hanging or buffering unbounded data', async () => {
  await assert.rejects(transform([greeting().packet.subarray(0, 9)]), /Incomplete backend greeting/);
  await assert.rejects(transform([Buffer.from([0, 0, 1, 0])]), /exceeds 64 KiB/);
});
