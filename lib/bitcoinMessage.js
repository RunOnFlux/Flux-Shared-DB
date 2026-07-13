const crypto = require('crypto');
const { bech32 } = require('bech32');
const bs58check = require('bs58check');
const { secp256k1 } = require('@noble/curves/secp256k1');

const SEGWIT_TYPES = {
  P2WPKH: 'p2wpkh',
  P2SH_P2WPKH: 'p2sh(p2wpkh)',
};

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest();
}

function hash256(value) {
  return sha256(sha256(value));
}

function hash160(value) {
  const sha = sha256(value);
  return crypto.createHash('ripemd160').update(sha).digest();
}

function encodeVarInt(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError('Invalid varint value');
  if (value < 0xfd) return Buffer.from([value]);
  if (value <= 0xffff) {
    const result = Buffer.allocUnsafe(3);
    result[0] = 0xfd;
    result.writeUInt16LE(value, 1);
    return result;
  }
  if (value <= 0xffffffff) {
    const result = Buffer.allocUnsafe(5);
    result[0] = 0xfe;
    result.writeUInt32LE(value, 1);
    return result;
  }
  const result = Buffer.allocUnsafe(9);
  result[0] = 0xff;
  // eslint-disable-next-line no-undef
  result.writeBigUInt64LE(BigInt(value), 1);
  return result;
}

function magicHash(message, messagePrefix) {
  const prefixValue = messagePrefix || '\u0018Bitcoin Signed Message:\n';
  const prefix = Buffer.isBuffer(prefixValue) ? prefixValue : Buffer.from(prefixValue, 'utf8');
  const body = Buffer.isBuffer(message) ? message : Buffer.from(message, 'utf8');
  return hash256(Buffer.concat([prefix, encodeVarInt(body.length), body]));
}

function decodeSignature(signature) {
  const value = Buffer.isBuffer(signature) ? signature : Buffer.from(signature, 'base64');
  if (value.length !== 65) throw new Error('Invalid signature length');

  const flagByte = value.readUInt8(0) - 27;
  if (flagByte < 0 || flagByte > 15) throw new Error('Invalid signature parameter');

  let segwitType = null;
  if (flagByte >= 8) {
    segwitType = flagByte >= 12 ? SEGWIT_TYPES.P2WPKH : SEGWIT_TYPES.P2SH_P2WPKH;
  }

  return {
    compressed: flagByte >= 4,
    recovery: flagByte % 4,
    segwitType,
    signature: value.subarray(1),
  };
}

function equal(left, right) {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function decodeBase58Hash(address) {
  return Buffer.from(bs58check.decode(address)).subarray(1);
}

function decodeBech32Hash(address) {
  const decoded = bech32.decode(address);
  return Buffer.from(bech32.fromWords(decoded.words.slice(1)));
}

function segwitRedeemHash(publicKeyHash) {
  return hash160(Buffer.concat([Buffer.from('0014', 'hex'), publicKeyHash]));
}

function publicKeyToAddress(publicKey, publicKeyPrefix = '00') {
  const key = Buffer.isBuffer(publicKey) ? publicKey : Buffer.from(publicKey, 'hex');
  const payload = Buffer.concat([Buffer.from(publicKeyPrefix, 'hex'), hash160(key)]);
  return bs58check.encode(payload);
}

function verify(message, address, signature, messagePrefix, checkSegwitAlways = false) {
  const parsed = decodeSignature(signature);
  if (checkSegwitAlways && !parsed.compressed) {
    throw new Error('checkSegwitAlways requires a compressed public key signature');
  }

  const hash = magicHash(message, messagePrefix);
  const publicKey = secp256k1.Signature
    .fromCompact(parsed.signature)
    .addRecoveryBit(parsed.recovery)
    .recoverPublicKey(hash)
    .toRawBytes(parsed.compressed);
  const publicKeyHash = hash160(publicKey);

  if (parsed.segwitType === SEGWIT_TYPES.P2SH_P2WPKH) {
    return equal(segwitRedeemHash(publicKeyHash), decodeBase58Hash(address));
  }
  if (parsed.segwitType === SEGWIT_TYPES.P2WPKH) {
    return equal(publicKeyHash, decodeBech32Hash(address));
  }
  if (!checkSegwitAlways) {
    return equal(publicKeyHash, decodeBase58Hash(address));
  }

  try {
    return equal(publicKeyHash, decodeBech32Hash(address));
  } catch (error) {
    const expected = decodeBase58Hash(address);
    return equal(publicKeyHash, expected) || equal(segwitRedeemHash(publicKeyHash), expected);
  }
}

module.exports = {
  magicHash,
  publicKeyToAddress,
  verify,
};
