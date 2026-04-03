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
function normalizeArray(val) {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

function deriveOrderStatus(orderLines) {
  const PRIORITY = ['Delivered', 'Shipped', 'Cancelled', 'Acknowledged', 'Created'];
  let best = 'Created';
  for (const line of orderLines) {
    const statuses = normalizeArray(line.orderLineStatuses?.orderLineStatus);
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
    const statuses = normalizeArray(line.orderLineStatuses?.orderLineStatus);
    for (const ls of statuses) {
      const tn = ls.trackingInfo?.trackingNumber;
      if (tn) return tn;
    }
  }
  return null;
}

/**
 * Extract ship-by date from order-level shippingInfo.
 * Walmart provides estimatedShipDate at shippingInfo level (epoch milliseconds).
 */
function extractShipByDate(wOrder) {
  const estShip = wOrder.shippingInfo?.estimatedShipDate;
  if (estShip) {
    const date = new Date(typeof estShip === 'number' ? estShip : parseInt(estShip, 10));
    return date.toISOString();
  }
  return null;
}

/**
 * Extract deliver-by date from order-level shippingInfo.
 * Walmart provides estimatedDeliveryDate at shippingInfo level (epoch milliseconds).
 */
function extractDeliverByDate(wOrder) {
  const estDel = wOrder.shippingInfo?.estimatedDeliveryDate;
  if (estDel) {
    const date = new Date(typeof estDel === 'number' ? estDel : parseInt(estDel, 10));
    return date.toISOString();
  }
  return null;
}

/**
 * Extract ship node (fulfillment center) from order.
 * Walmart provides shipNode at the top-level order object.
 * This represents the fulfillment center or warehouse handling the order.
 */
function extractShipNode(wOrder) {
  const shipNode = wOrder.shipNode;
  if (shipNode && typeof shipNode === 'string' && shipNode.trim()) {
    return String(shipNode).trim();
  }
  return null;
}

/**
 * Calculate order total from order amount or sum of line charges.
 */
function calculateOrderTotal(wOrder, orderLines) {
  // Try orderTotal from root
  if (wOrder.orderTotal?.amount) {
    return parseFloat(wOrder.orderTotal.amount);
  }
  
  // Fallback: sum charge amounts from all lines
  let total = 0;
  for (const line of orderLines) {
    const charges = normalizeArray(line.charges?.charge);
    for (const charge of charges) {
      if (charge.chargeAmount?.amount) {
        total += parseFloat(charge.chargeAmount.amount);
      }
    }
  }
  return total > 0 ? total : null;
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

    const walmartOrders = normalizeArray(resp.data?.list?.elements?.order);
    cursor = resp.data?.list?.meta?.nextCursor || null;

    for (const wOrder of walmartOrders) {
      try {
        const externalId = wOrder.purchaseOrderId;
        const orderLines = normalizeArray(wOrder.orderLines?.orderLine);
        const omsStatus = mapWalmartStatus(deriveOrderStatus(orderLines));

        if (omsStatus === null) { skipped++; continue; }

        const shippingAddr = wOrder.shippingInfo?.postalAddress || {};
        const trackingNumber = extractTrackingNumber(orderLines);
        const shipByDate = extractShipByDate(wOrder);
        const deliverByDate = extractDeliverByDate(wOrder);
        const shipNode = extractShipNode(wOrder);
        const orderTotal = calculateOrderTotal(wOrder, orderLines);
        const orderDate = wOrder.orderDate ? new Date(wOrder.orderDate).toISOString() : null;

        // DEBUG: Log shipNode value from first few orders to verify extraction
        if (pulled + updated < 3) {
          console.log(`Order ${externalId}: shipNode from API = ${JSON.stringify(wOrder.shipNode)}, extracted = ${shipNode}`);
        }

        // Look up via orders service API — avoids direct cross-schema DB query
        // which is fragile if the walmart service DB user lacks orders schema access.
        let existingOrder = null;
        try {
          const lookupResp = await axios.get(
            `${ORDERS_SERVICE_URL}/orders/by-external-id/${encodeURIComponent(externalId)}`,
            { headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' } }
          );
          existingOrder = lookupResp.data?.order || null;
        } catch (lookupErr) {
          if (lookupErr.response?.status !== 404) throw lookupErr;
        }

        if (existingOrder) {
          const {
            id: existingId, status: currentStatus, tracking_number: currentTracking,
            order_date: currentOrderDate, ship_by_date: currentShipBy,
            deliver_by_date: currentDeliverBy, ship_node: currentShipNode,
            order_total: currentOrderTotal,
          } = existingOrder;
          const canPromoteStatus = WALMART_PROMOTABLE.includes(omsStatus) && isMoreAdvanced(currentStatus, omsStatus);
          const canAddTracking = trackingNumber && !currentTracking;

          // Build metadata patch: fill nulls, always overwrite ship_node (may have stale wrong value)
          const metadataPatch = {};
          if (orderDate && !currentOrderDate) metadataPatch.order_date = orderDate;
          if (shipByDate && !currentShipBy) metadataPatch.ship_by_date = shipByDate;
          if (deliverByDate && !currentDeliverBy) metadataPatch.deliver_by_date = deliverByDate;
          if (shipNode && shipNode !== currentShipNode) metadataPatch.ship_node = shipNode;
          if (orderTotal && !currentOrderTotal) metadataPatch.order_total = orderTotal;
          if (canAddTracking) metadataPatch.tracking_number = trackingNumber;

          if (canPromoteStatus) {
            const body = { status: omsStatus, note: `Auto-updated from Walmart (${wOrder.orderStatus})` };
            await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}/status`, body, {
              headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
            });
            if (Object.keys(metadataPatch).length > 0) {
              await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}`, metadataPatch, {
                headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
              });
            }
            updated++;
          } else if (Object.keys(metadataPatch).length > 0) {
            await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}`, metadataPatch, {
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
        if (orderDate) payload.order_date = orderDate;
        if (shipByDate) payload.ship_by_date = shipByDate;
        if (deliverByDate) payload.deliver_by_date = deliverByDate;
        if (shipNode) payload.ship_node = shipNode;
        if (orderTotal) payload.order_total = orderTotal;

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

function buildLogMessage(pulled, updated, skipped, errors) {
  const parts = [];
  if (pulled > 0)  parts.push(`${pulled} new`);
  if (updated > 0) parts.push(`${updated} updated`);
  if (skipped > 0) parts.push(`${skipped} skipped (already current)`);
  if (errors.length > 0) parts.push(...errors.slice(0, 3));
  return parts.length > 0 ? parts.join('; ') : null;
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
      buildLogMessage(pulled, updated, skipped, errors),
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
      buildLogMessage(pulled, updated, skipped, errors),
    ]
  );

  return { pulled, updated, skipped, errors };
}

module.exports = { pollOrders, backfillOrders };
