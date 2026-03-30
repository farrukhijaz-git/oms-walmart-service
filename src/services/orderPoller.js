const axios = require('axios');
const pool = require('../db');
const { getAccessToken, getCredentials } = require('./walmartAuth');

const WALMART_ORDERS_URL = 'https://marketplace.walmartapis.com/v3/orders';
const ORDERS_SERVICE_URL = process.env.ORDERS_SERVICE_URL || 'http://localhost:3002';

async function pollOrders() {
  const creds = await getCredentials();
  const token = await getAccessToken();

  // Build date filter from last_polled_at
  const lastPolled = creds.last_polled_at
    ? new Date(creds.last_polled_at).toISOString()
    : new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(); // last 24h if never polled

  const resp = await axios.get(WALMART_ORDERS_URL, {
    params: { createdStartDate: lastPolled, limit: 200 },
    headers: {
      'WM_SEC.ACCESS_TOKEN': token,
      'WM_QOS.CORRELATION_ID': `oms-poll-${Date.now()}`,
      'WM_SVC.NAME': 'OMS',
      'Accept': 'application/json',
    },
  });

  const walmartOrders = resp.data?.list?.elements?.order || [];

  let pulled = 0;
  let skipped = 0;
  const errors = [];

  for (const wOrder of walmartOrders) {
    try {
      const externalId = wOrder.purchaseOrderId;

      // Check for duplicate
      const existing = await pool.query(
        'SELECT id FROM orders.orders WHERE external_id = $1',
        [externalId]
      );

      if (existing.rows.length > 0) {
        skipped++;
        continue;
      }

      // Map Walmart order to OMS format
      const shippingAddr = wOrder.shippingInfo?.postalAddress || {};
      const orderLines = wOrder.orderLines?.orderLine || [];

      const items = orderLines.map(line => ({
        sku: line.item?.sku || line.lineNumber,
        name: line.item?.productName || 'Unknown Item',
        quantity: parseInt(line.orderLineQuantity?.amount || '1'),
        unit_price: parseFloat(line.charges?.charge?.[0]?.chargeAmount?.amount || '0'),
      }));

      // POST to orders service
      await axios.post(`${ORDERS_SERVICE_URL}/orders`, {
        external_id: externalId,
        platform: 'walmart',
        customer_name: shippingAddr.name || 'Unknown',
        address_line1: shippingAddr.address1 || '',
        address_line2: shippingAddr.address2 || null,
        city: shippingAddr.city || '',
        state: shippingAddr.state || '',
        zip: shippingAddr.postalCode || '',
        country: shippingAddr.country || 'US',
        items,
      }, {
        headers: {
          'X-User-Id': 'system',
          'X-User-Role': 'admin',
        },
      });

      pulled++;
    } catch (err) {
      errors.push(`Order ${wOrder.purchaseOrderId}: ${err.message}`);
    }
  }

  // Update last_polled_at
  await pool.query(
    'UPDATE walmart.credentials SET last_polled_at = now(), updated_at = now() WHERE id = $1',
    [creds.id]
  );

  // Log result
  await pool.query(
    `INSERT INTO walmart.sync_log (sync_type, status, orders_pulled, orders_pushed, error_message)
     VALUES ('pull_orders', $1, $2, 0, $3)`,
    [
      errors.length === 0 ? 'success' : pulled > 0 ? 'partial' : 'failed',
      pulled,
      errors.length > 0 ? errors.slice(0, 3).join('; ') : null,
    ]
  );

  return { pulled, skipped, errors };
}

module.exports = { pollOrders };
