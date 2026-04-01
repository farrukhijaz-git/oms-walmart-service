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
 * Extract the first tracking number found across all order lines.
 * Checks both direct trackingInfo and orderLineShipments paths.
 */
function extractTrackingNumber(orderLines) {
  for (const line of orderLines) {
    const direct = line.trackingInfo?.trackingNumber;
    if (direct) return direct;

    const shipments = line.orderLineShipments?.orderLineShipment || [];
    for (const shipment of shipments) {
      const fromShipment = shipment.trackingInfo?.trackingNumber;
      if (fromShipment) return fromShipment;
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

async function pollOrders() {
  const creds = await getCredentials();
  const token = await getAccessToken();

  // Always look back 24 hours regardless of last_polled_at.
  //
  // Walmart releases orders in a nightly batch at midnight US Eastern (04:00 UTC).
  // The customer order date (what createdStartDate filters on) is when the order
  // was placed — potentially 8-12 hours before the batch drops. By the time the
  // batch appears in the API our rolling last_polled_at cursor is already past
  // those order dates, so a short overlap doesn't help.
  //
  // A 24-hour window guarantees we always cover a full batch cycle.
  // The external_id duplicate check makes this safe — existing orders are
  // skipped, never re-created.
  const lastPolled = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

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
  let updated = 0;
  let skipped = 0;
  const errors = [];

  for (const wOrder of walmartOrders) {
    try {
      const externalId = wOrder.purchaseOrderId;
      const omsStatus = mapWalmartStatus(wOrder.orderStatus);

      if (omsStatus === null) {
        skipped++;
        continue;
      }

      const shippingAddr = wOrder.shippingInfo?.postalAddress || {};
      const orderLines = wOrder.orderLines?.orderLine || [];
      const trackingNumber = extractTrackingNumber(orderLines);

      // --- Check if order already exists in OMS ---
      const existing = await pool.query(
        'SELECT id, status, tracking_number FROM orders.orders WHERE external_id = $1',
        [externalId]
      );

      if (existing.rows.length > 0) {
        // Order already exists — only sync if Walmart has a terminal status update
        const { id: existingId, status: currentStatus, tracking_number: currentTracking } = existing.rows[0];

        const canPromoteStatus =
          WALMART_PROMOTABLE.includes(omsStatus) && isMoreAdvanced(currentStatus, omsStatus);
        const canAddTracking = trackingNumber && !currentTracking;

        if (canPromoteStatus) {
          // Update status (and tracking if newly available)
          const body = {
            status: omsStatus,
            note: `Auto-updated from Walmart (${wOrder.orderStatus})`,
          };
          if (canAddTracking) body.tracking_number = trackingNumber;

          await axios.patch(
            `${ORDERS_SERVICE_URL}/orders/${existingId}/status`,
            body,
            { headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' } }
          );
          updated++;
        } else if (canAddTracking) {
          // Status is already up to date, but we now have a tracking number
          await axios.patch(
            `${ORDERS_SERVICE_URL}/orders/${existingId}`,
            { tracking_number: trackingNumber },
            { headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' } }
          );
          updated++;
        } else {
          skipped++;
        }
        continue;
      }

      // --- New order — create it ---
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
        headers: {
          'X-User-Id': '00000000-0000-0000-0000-000000000000',
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
      errors.length === 0 ? 'success' : pulled + updated > 0 ? 'partial' : 'failed',
      pulled + updated,
      errors.length > 0 ? errors.slice(0, 3).join('; ') : null,
    ]
  );

  return { pulled, updated, skipped, errors };
}

module.exports = { pollOrders };
