'use strict';

const axios = require('axios');
const { getAccessToken } = require('./walmartAuth');

const WALMART_ORDERS_URL = 'https://marketplace.walmartapis.com/v3/orders';
const ORDERS_SERVICE_URL = process.env.ORDERS_SERVICE_URL || 'http://localhost:3002';

const SYSTEM_HEADERS = {
  'X-User-Id': '00000000-0000-0000-0000-000000000000',
  'X-User-Role': 'admin',
};

/**
 * Push shipment/tracking to Walmart Marketplace for a given OMS order.
 *
 * @param {string} orderId       - Internal OMS order UUID
 * @param {string} trackingNumber - Carrier tracking number
 * @param {string} carrier       - Carrier name: UPS | USPS | FedEx | DHL | Other
 * @param {string|null} shipDateTime - ISO datetime (defaults to now)
 */
async function shipOrder(orderId, trackingNumber, carrier, shipDateTime) {
  // Fetch full order + items from orders service
  const orderResp = await axios.get(`${ORDERS_SERVICE_URL}/orders/${orderId}`, {
    headers: SYSTEM_HEADERS,
  });
  const order = orderResp.data?.order;
  if (!order) throw new Error('Order not found');

  const purchaseOrderId = order.external_id;
  if (!purchaseOrderId) throw new Error('Order has no external_id (Walmart PO number)');

  const items = order.items || [];
  if (items.length === 0) throw new Error('Order has no line items');

  const shipDt = shipDateTime || new Date().toISOString();
  const methodCode = order.shipping_method || 'Standard';

  // Build one entry per order line, all sharing the same tracking number
  const orderLines = items.map((item, idx) => ({
    lineNumber: item.line_number || String(idx + 1),
    orderLineStatuses: {
      orderLineStatus: [
        {
          status: 'Shipped',
          statusQuantity: {
            unitOfMeasurement: 'EACH',
            amount: String(item.quantity || 1),
          },
          trackingInfo: {
            shipDateTime: shipDt,
            carrierName: { carrier },
            methodCode,
            trackingNumber,
          },
        },
      ],
    },
  }));

  const payload = {
    orderShipment: {
      orderLines: { orderLine: orderLines },
    },
  };

  const token = await getAccessToken();

  await axios.post(
    `${WALMART_ORDERS_URL}/${encodeURIComponent(purchaseOrderId)}/shipping`,
    payload,
    {
      headers: {
        'WM_SEC.ACCESS_TOKEN': token,
        'WM_QOS.CORRELATION_ID': `oms-ship-${Date.now()}`,
        'WM_SVC.NAME': 'OMS',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    }
  );

  // Mark tracking as pushed in OMS
  await axios.patch(
    `${ORDERS_SERVICE_URL}/orders/${orderId}`,
    { tracking_pushed_to_walmart: true },
    { headers: SYSTEM_HEADERS }
  );

  return { ok: true, purchaseOrderId };
}

module.exports = { shipOrder };
