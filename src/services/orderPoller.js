'use strict';

const axios = require('axios');
const pool = require('../db');
const { getAccessToken, getCredentials } = require('./walmartAuth');

const WALMART_ORDERS_URL = 'https://marketplace.walmartapis.com/v3/orders';
const ORDERS_SERVICE_URL = process.env.ORDERS_SERVICE_URL || 'http://localhost:3002';

/**
 * Map Walmart's derived order status to OMS internal status.
 */
function mapWalmartStatus(walmartStatus) {
  switch ((walmartStatus || '').toLowerCase()) {
    case 'shipped':    return 'shipped';
    case 'delivered':  return 'delivered';
    case 'cancelled':  return 'cancelled';
    case 'created':
    case 'acknowledged':
    default:           return 'new';
  }
}

/**
 * Normalize Walmart API values that may be a single object or an array.
 * Walmart returns a plain object (not array) for single-item orders.
 */
function normalizeArray(val) {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

/**
 * Derive the most advanced status across all order lines.
 * wOrder.orderStatus stays "Created"/"Acknowledged" even after shipment —
 * actual status lives at the line level.
 */
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
 * Path: orderLineStatuses.orderLineStatus[].trackingInfo.trackingNumber
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
 * Ship-by and deliver-by dates live at ORDER level under wOrder.shippingInfo.
 * Values are epoch milliseconds.
 */
function extractShipByDate(wOrder) {
  const estShip = wOrder.shippingInfo?.estimatedShipDate;
  if (!estShip) return null;
  const date = new Date(typeof estShip === 'number' ? estShip : parseInt(estShip, 10));
  return isNaN(date.getTime()) ? null : date.toISOString();
}

function extractDeliverByDate(wOrder) {
  const estDel = wOrder.shippingInfo?.estimatedDeliveryDate;
  if (!estDel) return null;
  const date = new Date(typeof estDel === 'number' ? estDel : parseInt(estDel, 10));
  return isNaN(date.getTime()) ? null : date.toISOString();
}

/**
 * Ship node lives at ORDER level as an object: { name: "...", id: "..." }
 * .name is the human-readable seller node name shown in Walmart Seller Center.
 */
function extractShipNode(wOrder) {
  const name = wOrder.shipNode?.name;
  return (name && typeof name === 'string' && name.trim()) ? name.trim() : null;
}

function extractShipNodeId(wOrder) {
  const id = wOrder.shipNode?.id;
  return id != null ? String(id) : null;
}

/**
 * Calculate order total from orderTotal field or sum of line charges.
 */
function calculateOrderTotal(wOrder, orderLines) {
  if (wOrder.orderTotal?.amount) {
    return parseFloat(wOrder.orderTotal.amount);
  }
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
 * Sum all TAX charge amounts across all order lines.
 */
function calculateTotalTax(orderLines) {
  let tax = 0;
  for (const line of orderLines) {
    const charges = normalizeArray(line.charges?.charge);
    for (const charge of charges) {
      if ((charge.chargeType || '').toUpperCase() === 'TAX' && charge.chargeAmount?.amount) {
        tax += parseFloat(charge.chargeAmount.amount);
      }
    }
  }
  return tax > 0 ? tax : null;
}

function isMoreAdvanced(currentStatus, targetStatus) {
  const ORDER = ['new', 'label_generated', 'inventory_ordered', 'packed', 'ready', 'shipped', 'delivered'];
  return ORDER.indexOf(targetStatus) > ORDER.indexOf(currentStatus);
}

/**
 * Cancellation overrides any non-cancelled OMS status.
 * Even if a label was generated or order was packed, a Walmart cancellation
 * means the order will not be fulfilled — OMS must reflect that immediately.
 */
function shouldCancel(omsStatus, currentStatus) {
  return omsStatus === 'cancelled' && currentStatus !== 'cancelled';
}

/**
 * Core fetch+import loop. Handles Walmart API pagination via nextCursor.
 * @param {string} token - Walmart access token
 * @param {string} fromDate - ISO string for the date filter
 * @param {'createdStartDate'|'lastModifiedStartDate'} dateField - which Walmart date param to use
 */
async function fetchAndImportOrders(token, fromDate, dateField = 'createdStartDate') {
  let cursor = null;
  let pulled = 0, updated = 0, skipped = 0;
  const errors = [];

  do {
    const params = { [dateField]: fromDate, limit: 200 };
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
        const derivedStatus = deriveOrderStatus(orderLines);
        const omsStatus = mapWalmartStatus(derivedStatus);

        const shippingAddr = wOrder.shippingInfo?.postalAddress || {};
        const trackingNumber = extractTrackingNumber(orderLines);
        const shipByDate = extractShipByDate(wOrder);
        const deliverByDate = extractDeliverByDate(wOrder);
        const shipNode = extractShipNode(wOrder);
        const shipNodeId = extractShipNodeId(wOrder);
        const orderTotal = calculateOrderTotal(wOrder, orderLines);
        const totalTax = calculateTotalTax(orderLines);
        const orderDate = wOrder.orderDate ? new Date(wOrder.orderDate).toISOString() : null;
        const walmartStatus = wOrder.orderStatus || derivedStatus;
        const customerOrderId = wOrder.customerOrderId || null;
        const customerEmail = wOrder.customerEmailId || null;
        const phone = shippingAddr.phone || null;
        const addressType = shippingAddr.addressType || null;
        const shippingMethod = wOrder.shippingInfo?.methodCode || null;
        const carrierMethod = orderLines[0]?.fulfillment?.carrierMethodCode || null;
        const shipMethod = orderLines[0]?.fulfillment?.shipMethod || null;

        // Look up via orders service API — avoids direct cross-schema DB query
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
            id: existingId,
            status: currentStatus,
            tracking_number: currentTracking,
            order_date: currentOrderDate,
            ship_by_date: currentShipBy,
            deliver_by_date: currentDeliverBy,
            ship_node: currentShipNode,
            order_total: currentOrderTotal,
            ship_node_id: currentShipNodeId,
            walmart_status: currentWalmartStatus,
            total_tax: currentTotalTax,
            customer_order_id: currentCustomerOrderId,
            customer_email: currentCustomerEmail,
            phone: currentPhone,
            address_type: currentAddressType,
            shipping_method: currentShippingMethod,
          } = existingOrder;

          const canCancel = shouldCancel(omsStatus, currentStatus);
          const canPromoteStatus = isMoreAdvanced(currentStatus, omsStatus);
          const canAddTracking = trackingNumber && !currentTracking;

          const metadataPatch = {};
          if (orderDate && !currentOrderDate) metadataPatch.order_date = orderDate;
          if (shipByDate && !currentShipBy) metadataPatch.ship_by_date = shipByDate;
          if (deliverByDate && !currentDeliverBy) metadataPatch.deliver_by_date = deliverByDate;
          if (shipNode && shipNode !== currentShipNode) metadataPatch.ship_node = shipNode;
          if (shipNodeId && !currentShipNodeId) metadataPatch.ship_node_id = shipNodeId;
          if (orderTotal && !currentOrderTotal) metadataPatch.order_total = orderTotal;
          if (totalTax && !currentTotalTax) metadataPatch.total_tax = totalTax;
          if (canAddTracking) metadataPatch.tracking_number = trackingNumber;
          if (walmartStatus && walmartStatus !== currentWalmartStatus) metadataPatch.walmart_status = walmartStatus;
          if (customerOrderId && !currentCustomerOrderId) metadataPatch.customer_order_id = customerOrderId;
          if (customerEmail && !currentCustomerEmail) metadataPatch.customer_email = customerEmail;
          if (phone && !currentPhone) metadataPatch.phone = phone;
          if (addressType && !currentAddressType) metadataPatch.address_type = addressType;
          if (shippingMethod && !currentShippingMethod) metadataPatch.shipping_method = shippingMethod;

          if (canCancel) {
            // Cancellation overrides any current OMS status — always apply it.
            // Include a note so the status log shows the previous state for audit.
            const cancelNote = `Cancelled on Walmart (was: ${currentStatus})${trackingNumber ? ` — tracking ${trackingNumber} may still be in transit` : ''}`;
            await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}/status`,
              { status: 'cancelled', note: cancelNote },
              { headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' } }
            );
            if (Object.keys(metadataPatch).length > 0) {
              await axios.patch(`${ORDERS_SERVICE_URL}/orders/${existingId}`, metadataPatch, {
                headers: { 'X-User-Id': '00000000-0000-0000-0000-000000000000', 'X-User-Role': 'admin' },
              });
            }
            updated++;
          } else if (canPromoteStatus) {
            const body = { status: omsStatus, note: `Auto-updated from Walmart (${walmartStatus})` };
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

        // Build per-line items with all new fields
        const items = orderLines.map(line => {
          const statuses = normalizeArray(line.orderLineStatuses?.orderLineStatus);
          const firstStatus = statuses[0] || {};
          const charges = normalizeArray(line.charges?.charge);
          const productCharge = charges.find(c => (c.chargeType || '').toUpperCase() === 'PRODUCT') || charges[0];
          const taxCharge = charges.find(c => (c.chargeType || '').toUpperCase() === 'TAX');
          return {
            sku: line.item?.sku || line.lineNumber,
            name: line.item?.productName || 'Unknown Item',
            quantity: parseInt(line.orderLineQuantity?.amount || '1'),
            unit_price: parseFloat(productCharge?.chargeAmount?.amount || '0'),
            line_number: line.lineNumber || null,
            condition: line.item?.condition || null,
            tax_amount: taxCharge?.chargeAmount?.amount ? parseFloat(taxCharge.chargeAmount.amount) : null,
            line_tracking_number: firstStatus?.trackingInfo?.trackingNumber || null,
            tracking_url: firstStatus?.trackingInfo?.trackingURL || null,
            ship_datetime: firstStatus?.trackingInfo?.shipDateTime
              ? new Date(firstStatus.trackingInfo.shipDateTime).toISOString()
              : null,
            line_status: firstStatus?.status || null,
          };
        });

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
        if (trackingNumber)    payload.tracking_number = trackingNumber;
        if (orderDate)         payload.order_date = orderDate;
        if (shipByDate)        payload.ship_by_date = shipByDate;
        if (deliverByDate)     payload.deliver_by_date = deliverByDate;
        if (shipNode)          payload.ship_node = shipNode;
        if (shipNodeId)        payload.ship_node_id = shipNodeId;
        if (orderTotal)        payload.order_total = orderTotal;
        if (totalTax)          payload.total_tax = totalTax;
        if (walmartStatus)     payload.walmart_status = walmartStatus;
        if (customerOrderId)   payload.customer_order_id = customerOrderId;
        if (customerEmail)     payload.customer_email = customerEmail;
        if (phone)             payload.phone = phone;
        if (addressType)       payload.address_type = addressType;
        if (shippingMethod)    payload.shipping_method = shippingMethod;
        if (carrierMethod)     payload.carrier_method = carrierMethod;
        if (shipMethod)        payload.ship_method = shipMethod;

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

  // Look back 24 hours on both axes:
  // - createdStartDate: catches new orders (Walmart releases in nightly batches)
  // - lastModifiedStartDate: catches status updates (shipped/delivered/cancelled) on older orders
  const fromDate = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const byCreated  = await fetchAndImportOrders(token, fromDate, 'createdStartDate');
  const byModified = await fetchAndImportOrders(token, fromDate, 'lastModifiedStartDate');

  const pulled  = byCreated.pulled  + byModified.pulled;
  const updated = byCreated.updated + byModified.updated;
  const skipped = byCreated.skipped + byModified.skipped;
  const errors  = [...byCreated.errors, ...byModified.errors];

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
