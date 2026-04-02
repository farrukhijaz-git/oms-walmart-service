const express = require('express');
const pool = require('../db');
const { encrypt } = require('../utils/crypto');
const { pollOrders, backfillOrders } = require('../services/orderPoller');
const { getCredentials } = require('../services/walmartAuth');
const { restartScheduler } = require('../services/scheduler');
const requireUser = require('../middleware/requireUser');
const requireAdmin = require('../middleware/requireAdmin');

const router = express.Router();

// GET /walmart/sync/status
router.get('/sync/status', requireUser, async (req, res) => {
  try {
    let creds = null;
    try { creds = await getCredentials(); } catch {}

    const { rows: logRows } = await pool.query(
      `SELECT status, orders_pulled, synced_at FROM walmart.sync_log
       ORDER BY synced_at DESC LIMIT 1`
    );

    res.json({
      configured: !!creds,
      last_polled_at: creds?.last_polled_at || null,
      poll_interval_seconds: creds?.poll_interval_seconds || 900,
      last_sync: logRows[0] || null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
  }
});

// POST /walmart/sync/pull - manual poll trigger (Admin only)
router.post('/sync/pull', requireUser, requireAdmin, async (req, res) => {
  try {
    const result = await pollOrders();
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error('Manual poll error:', err);
    res.status(500).json({ error: { code: 'POLL_ERROR', message: err.message } });
  }
});

// POST /walmart/sync/backfill - import all orders from a given date (Admin only)
// Body: { from_date: "2026-03-10" }
router.post('/sync/backfill', requireUser, requireAdmin, async (req, res) => {
  const { from_date } = req.body;
  if (!from_date) {
    return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'from_date is required (YYYY-MM-DD)' } });
  }
  const parsed = new Date(from_date);
  if (isNaN(parsed.getTime())) {
    return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'Invalid from_date' } });
  }
  try {
    const result = await backfillOrders(parsed.toISOString());
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error('Backfill error:', err);
    res.status(500).json({ error: { code: 'BACKFILL_ERROR', message: err.message } });
  }
});

// POST /walmart/credentials - save encrypted credentials (Admin only)
router.post('/credentials', requireUser, requireAdmin, async (req, res) => {
  const { client_id, client_secret } = req.body;
  if (!client_id || !client_secret) {
    return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'client_id and client_secret are required' } });
  }

  try {
    const encryptedId = encrypt(client_id);
    const encryptedSecret = encrypt(client_secret);

    // Upsert credentials (delete existing, insert new)
    await pool.query('DELETE FROM walmart.credentials');
    await pool.query(
      `INSERT INTO walmart.credentials (client_id, client_secret) VALUES ($1, $2)`,
      [encryptedId, encryptedSecret]
    );

    res.json({ ok: true, message: 'Credentials saved' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
  }
});

// PATCH /walmart/settings - update poll interval (Admin only)
router.patch('/settings', requireUser, requireAdmin, async (req, res) => {
  const { poll_interval_seconds } = req.body;
  if (!poll_interval_seconds || poll_interval_seconds < 60) {
    return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'poll_interval_seconds must be >= 60' } });
  }

  try {
    await pool.query(
      'UPDATE walmart.credentials SET poll_interval_seconds = $1, updated_at = now()',
      [poll_interval_seconds]
    );
    restartScheduler();
    res.json({ ok: true, poll_interval_seconds });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
  }
});

// GET /walmart/sync/log - last 20 sync results
router.get('/sync/log', requireUser, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, sync_type, status, orders_pulled, orders_pushed, error_message, synced_at
       FROM walmart.sync_log ORDER BY synced_at DESC LIMIT 20`
    );
    res.json({ log: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
  }
});

module.exports = router;
