const axios = require('axios');
const pool = require('../db');
const { decrypt } = require('../utils/crypto');

const WALMART_AUTH_URL = 'https://marketplace.walmartapis.com/v3/token';

async function getCredentials() {
  const { rows } = await pool.query('SELECT * FROM walmart.credentials ORDER BY updated_at DESC LIMIT 1');
  if (rows.length === 0) throw new Error('No Walmart credentials configured');
  return rows[0];
}

async function getAccessToken() {
  const creds = await getCredentials();

  // Check if current token is still valid (with 60s buffer)
  if (creds.access_token && creds.token_expires_at) {
    const expiresAt = new Date(creds.token_expires_at);
    if (expiresAt > new Date(Date.now() + 60000)) {
      return creds.access_token;
    }
  }

  // Decrypt credentials and fetch new token
  const clientId = decrypt(creds.client_id);
  const clientSecret = decrypt(creds.client_secret);

  const basicAuth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

  const resp = await axios.post(WALMART_AUTH_URL, 'grant_type=client_credentials', {
    headers: {
      'Authorization': `Basic ${basicAuth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      'WM_QOS.CORRELATION_ID': `oms-${Date.now()}`,
      'WM_SVC.NAME': 'OMS',
      'Accept': 'application/json',
    },
  });

  const token = resp.data.access_token;
  const expiresIn = resp.data.expires_in || 3600; // seconds
  const expiresAt = new Date(Date.now() + (expiresIn * 1000));

  await pool.query(
    'UPDATE walmart.credentials SET access_token = $1, token_expires_at = $2, updated_at = now() WHERE id = $3',
    [token, expiresAt, creds.id]
  );

  return token;
}

module.exports = { getAccessToken, getCredentials };
