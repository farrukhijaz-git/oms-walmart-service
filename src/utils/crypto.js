const crypto = require('crypto');

const ALGORITHM = 'aes-256-gcm';

function getKey() {
  const key = process.env.ENCRYPTION_KEY;
  if (!key) throw new Error('ENCRYPTION_KEY not set');
  return Buffer.from(key, 'hex');
}

function encrypt(text) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, getKey(), iv);
  const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [iv.toString('hex'), encrypted.toString('hex'), tag.toString('hex')].join(':');
}

function decrypt(ciphertext) {
  const [ivHex, encHex, tagHex] = ciphertext.split(':');
  const iv = Buffer.from(ivHex, 'hex');
  const encrypted = Buffer.from(encHex, 'hex');
  const tag = Buffer.from(tagHex, 'hex');
  const decipher = crypto.createDecipheriv(ALGORITHM, getKey(), iv);
  decipher.setAuthTag(tag);
  return decipher.update(encrypted) + decipher.final('utf8');
}

module.exports = { encrypt, decrypt };
