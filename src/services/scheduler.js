const cron = require('node-cron');
const pool = require('../db');
const { pollOrders } = require('./orderPoller');

let currentTask = null;

async function getInterval() {
  try {
    const { rows } = await pool.query(
      'SELECT poll_interval_seconds FROM walmart.credentials ORDER BY updated_at DESC LIMIT 1'
    );
    return rows.length > 0 ? rows[0].poll_interval_seconds : 900;
  } catch {
    return 900;
  }
}

function secondsToCron(seconds) {
  // Convert seconds to minutes for cron (minimum 1 minute)
  const minutes = Math.max(1, Math.floor(seconds / 60));
  return `*/${minutes} * * * *`;
}

async function startScheduler() {
  const intervalSeconds = await getInterval();
  const cronExpr = secondsToCron(intervalSeconds);

  console.log(`Starting Walmart order poller: every ${intervalSeconds}s (${cronExpr})`);

  if (currentTask) currentTask.stop();

  // Run once immediately on startup so we don't wait a full interval after a
  // Render spin-up or service restart before catching up on missed orders.
  setImmediate(async () => {
    console.log('Running startup poll...');
    try {
      const result = await pollOrders();
      console.log(`Startup poll: ${result.pulled} pulled, ${result.updated} updated, ${result.skipped} skipped`);
    } catch (err) {
      console.error('Startup poll error:', err.message);
    }
  });

  currentTask = cron.schedule(cronExpr, async () => {
    console.log('Polling Walmart orders...');
    try {
      const result = await pollOrders();
      console.log(`Poll result: ${result.pulled} pulled, ${result.updated} updated, ${result.skipped} skipped`);
    } catch (err) {
      console.error('Poll error:', err.message);
      try {
        await pool.query(
          `INSERT INTO walmart.sync_log (sync_type, status, orders_pulled, orders_pushed, error_message)
           VALUES ('pull_orders', 'failed', 0, 0, $1)`,
          [err.message]
        );
      } catch {}
    }
  });

  return currentTask;
}

function restartScheduler() {
  startScheduler().catch(console.error);
}

module.exports = { startScheduler, restartScheduler };
