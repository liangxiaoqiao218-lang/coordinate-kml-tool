import { createHash } from "node:crypto";
import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";
import { paymentIdentifier, paymentTimestamp } from "./schemas.js";
import { parseAlipayNotificationForm } from "./alipay-rsa2-verifier.js";

function required(fields, key) {
  paymentContractAssert(typeof fields[key] === "string" && fields[key].length > 0,
    PAYMENT_ERROR_CODES.WEBHOOK_INVALID, `notification ${key} required`);
  return fields[key];
}

function amountMinor(text) {
  paymentContractAssert(/^(0|[1-9][0-9]{0,11})\.[0-9]{2}$/.test(text), PAYMENT_ERROR_CODES.INVALID_AMOUNT, "invalid decimal notification amount");
  const [whole, fraction] = text.split(".");
  const value = Number(whole) * 100 + Number(fraction);
  paymentContractAssert(Number.isSafeInteger(value) && value > 0, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "positive minor amount required");
  return value;
}

function paymentTime(text) {
  paymentContractAssert(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text), PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "payment timestamp format invalid");
  const iso = paymentTimestamp(`${text.replace(" ", "T")}+08:00`, "gmt_payment");
  const roundTrip = new Date(new Date(iso).getTime() + 8 * 3600000).toISOString().slice(0, 19).replace("T", " ");
  paymentContractAssert(roundTrip === text, PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "payment timestamp calendar invalid");
  return iso;
}

// resolveOrder must read the original merchant order from a trusted server store.
// It supplies ownership, never buyer identifiers or notification pass-through data.
export function createAlipayNotificationDecoder({ resolveOrder, clock } = {}) {
  paymentContractAssert(typeof resolveOrder === "function" && typeof clock === "function",
    PAYMENT_ERROR_CODES.INVALID_REQUEST, "trusted order resolver and clock required");
  return async function decodeAlipayNotification(rawBody) {
    const fields = parseAlipayNotificationForm(rawBody);
    const identityFields = ["user_id", "userId", "visitor_id", "visitorId", "passback_params"];
    paymentContractAssert(identityFields.every(key => !Object.hasOwn(fields, key)),
      PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH, "notification cannot supply site identity");
    const merchantOrderId = paymentIdentifier(required(fields, "out_trade_no"), "out_trade_no");
    const order = await resolveOrder(merchantOrderId);
    paymentContractAssert(order?.schemaVersion === "payment_order_v1" && order.orderId === merchantOrderId && order.provider === "alipay",
      PAYMENT_ERROR_CODES.ORDER_MISMATCH, "original merchant order required");
    const userId = paymentIdentifier(order.userId, "order.userId");
    paymentContractAssert(order.currency === "CNY" && (!Object.hasOwn(fields, "currency") || fields.currency === order.currency),
      PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "notification currency mismatch");
    const status = required(fields, "trade_status");
    paymentContractAssert(["WAIT_BUYER_PAY", "TRADE_SUCCESS", "TRADE_FINISHED", "TRADE_CLOSED"].includes(status),
      PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "unknown notification trade status");
    const paid = status === "TRADE_SUCCESS" || status === "TRADE_FINISHED";
    return Object.freeze({
      appId: required(fields, "app_id"),
      sellerId: required(fields, "seller_id"),
      merchantOrderId,
      providerTransactionId: required(fields, "trade_no"),
      providerEventId: required(fields, "notify_id"),
      userId,
      amountMinor: amountMinor(required(fields, "total_amount")),
      currency: order.currency,
      status,
      paidAt: paid ? paymentTime(required(fields, "gmt_payment")) : null,
      observedAt: paymentTimestamp(clock(), "observedAt"),
      payloadHash: createHash("sha256").update(rawBody).digest("hex"),
      rawReference: required(fields, "notify_id")
    });
  };
}
