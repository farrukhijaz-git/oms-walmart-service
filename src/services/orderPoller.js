const axios = require('axios');
const pool = require('../db');
const { getAccessToken, getCredentials } = require('./walmartAuth');

const WALMART_ORDERS_URL = 'https://marketplace.walmartapis.com/v3/orders';
const ORDERS_SERVICE_URL = process.env.ORDERS_SERVICE_URL || 'http://localhost:3002';

/**
 * Map Walmart's order status to OMS status.
 * Returns null for statuses that should be skipped entirely (cancelled).
 */
function mapWalmartStatus(walmartStatus) {
  switch ((walmartStatus || '').toLowerCase()) {
    case 'shipped':    return 'shipped';
    case 'delivered':  return 'delivered';
    case 'cancelled':  return null;
    case 'created':
    case 'acknowledged':
    default:           return 'new';
  }
}

/**
 * Derive the most advanced status across all order lines.
 * Walmart updates status at the line level — the order-level orderStatus field
 * often stays as "Created"/"Acknowledged" even when lines are Shipped/Delivered.
 */
function deriveOrderStatus(orderLines) {
  const PRIORITY = ['Delivered', 'Shipped', 'Cancelled', 'Acknowledged', 'Created'];
  let best = 'Created';
  for (const line of orderLines) {
    const statuses = line.orderLineStatuses?.orderLineStatus || [];
    for (const ls of statuses) {
      if (PRIORITY.indexOf(ls.status) < PRIORITY.indexOf(best)) {
        best = ls.status;
      }
    }
  }
  return best;
}

/**
 * Extract the first tracking number found across all order lines.
 * Walmart tracking lives inside orderLineStatuses.orderLineStatus[].trackingInfo.
 */
function extractTrackingNumber(orderLines) {
  for (const line of orderLines) {
    const statuses = line.orderLineStatuses?.orderLineStatus || [];
    for (const ls of statuses) {
      const tn = ls.trackingInfo?.trackingNumber;
      if (tn) return tn;
    }
  }
  return null;
}

/**
 * OMS statuses that Walmart can legitimately push us toward.
 * We only let Walmart advance an order to shipped/delivered — we don't let
 * it touch OMS-internal statuses like label_generated, packed, etc.
 */
const WALMART_PROMOTABLE = ['shipped', 'delivered'];

/** Returns true if targetStatus is further along than currentStatus */
function isMoreAdvanced(currentStatus, targetStatus) {
  const ORDER = ['new', 'label_generated', 'inventory_ordered', 'packed', 'ready', 'shipped', 'delivered'];
  return ORDER.indexOf(targetStatus) > ORDER.indexOf(currentStatus);
}

/**
 * Core fetch+import loop. Handles Walmart API pagination via nextCursor.
 * fromDate: ISO string for createdStartDate. Defaults to 24h ago.
 */
async function fetchAndImportOrders(token, fromDate) {
  let cursor = null;
  let pulled = 0, updated = 0, skipped = 0;
  const errors = [];

  do {
    const params = { createdStartDate: fromDate, limit: 200 };
    if (cursor) params.nextCursor = cursor;

    const resp = await axios.get(WALMART_ORDERS_URL, {
      params,
      headers: {
        'WM_SEC.ACCESS_TOKEN': token,
        'WM_QOS.CORRELATION_ID': `oms-poll-${Date.now()}`,
        'WM_SVC.NAME': 'OMS',
        'Accept': 'application/json',
      },
    });

    const walmartOrders = resp.data?.list?.elements?.order || [];
    cursor = resp.data?.list?.meta?.nextCursor || null;

    for (const wOrder of walmartOrders) {
      try {
        const externalId = wOrder.purchaseOrderId;
        const orderLines = wOrder.orderLines?.orderLine || [];
        const omsStatus = mapWalmartStatus(deriveOrderStatus(orderLines));

        if (omsStatus === null) { skipped++; continue; }

        const shippingAddr = wOrder.shippingInfo?.postalAddress || {};
        const trackingNumber = extractTrackingNumber(orderLines);

        const existing = await pool.query(
          'SELECT id, status, tracking_number FROM orders.orders WHERE external_id = $1',
          [externalId]
        );

        if (existing.rows.length > 0) {
          const { id: existingId, status: currentStatus, tracking_number: currentTracking } = existing.rows[0];
          const canPromoteStatus = WALMART_PROMOTABLE.includes(omsStatus) && isMoreAdvanced(currentStatus, omsStatus);
          const canAddTracking = trackingNumber && !currentTracking;

          if (canPromoteStatus) {
            const body = { status: omsStatus, note: `Auto-updated from Walmart (${wOrder.orderStatus})` };
            if (canAddTracking) body.tracking_number = trackingNumber;
            await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}/status`, body, {
              headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
            });
            updated++;
          } else if (canAddTracking) {
            await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}`, { tracking_number: trackingNumber }, {
              headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
            });
            updated++;
          } else {
            skipped++;
          }
          continue;
        }

        const items = orderLines.map(line => ({
          sku: line.item?.sku || line.lineNumber,
          name: line.item?.productName || 'Unknown Item',
          quantity: parseInt(line.orderLineQuantity?.amount || '1'),
          unit_price: parseFloat(line.charges?.charge?.[0]?.chargeAmount?.amount || '0'),
        }));

        const payload = {
          external_id: externalId,
          platform: 'walmart',
          customer_name: shippingAddr.name || 'Unknown',
          address_line1: shippingAddr.address1 || '',
          address_line2: shippingAddr.address2 || null,
          city: shippingAddr.city || '',
          state: shippingAddr.state || '',
          zip: shippingAddr.postalCode || '',
          country: shippingAddr.country || 'US',
          status: omsStatus,
          items,
        };
        if (trackingNumber) payload.tracking_number = trackingNumber;

        await axios.post(`${ORDERS_SERVICE_URL}/orders`, payload, {
          headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
        });
        pulled++;
      } catch (err) {
        errors.push(`Order ${wOrder.purchaseOrderId}: ${err.message}`);
      }
    }
  } while (cursor);

  return { pulled, updated, skipped, errors };
}

async function pollOrders() {
  const creds = await getCredentials();
  const token = await getAccessToken();

  // Always look back 24 hours regardless of last_polled_at.
  // Walmart releases orders in a nightly batch at midnight US Eastern (04:00 UTC).
  // The customer order date (createdStartDate) is when the order was placed —
  // potentially 8-12h before the batch drops. A 24h window guarantees coverage.
  // external_id duplicate check makes this safe.
  const fromDate = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const { pulled, updated, skipped, errors } = await fetchAndImportOrders(token, fromDate);

  await pool.query(
    'UPDATE walmart.credentials SET last_polled_at = now(), updated_at = now() WHERE id = $1',
    [creds.id]
  );
  await pool.query(
    `INSERT INTO walmart.sync_log (sync_type, status, orders_pulled, orders_pushed, error_message)
     VALUES ('pull_orders', $1, $2, 0, $3)`,
    [
      errors.length === 0 ? 'success' : pulled + updated > 0 ? 'partial' : 'failed',
      pulled + updated,
      errors.length > 0 ? errors.slice(0, 3).join('; ') : null,
    ]
  );

  return { pulled, updated, skipped, errors };
}

/**
 * One-time backfill from a given date. Does not update last_polled_at so
 * the regular 24h poll schedule is unaffected.
 */
async function backfillOrders(fromDate) {
  const token = await getAccessToken();
  const { pulled, updated, skipped, errors } = await fetchAndImportOrders(token, fromDate);

  await pool.query(
    `INSERT INTO walmart.sync_log (sync_type, status, orders_pulled, orders_pushed, error_message)
     VALUES ('pull_orders', $1, $2, 0, $3)`,
    [
      errors.length === 0 ? 'success' : pulled + updated > 0 ? 'partial' : 'failed',
      pulled + updated,
      errors.length > 0 ? errors.slice(0, 3).join('; ') : null,
    ]
  );

  return { pulled, updated, skipped, errors };
}

module.exports = { pollOrders, backfillOrders };
