import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";
import { PaymentProviderAdapter, validateWebhookEnvelope } from "./provider-interface.js";
import {
  assertVerifiedPaymentIdentity,
  createVerifiedProviderEvidence,
  paymentAmountMinor,
  paymentCurrency,
  paymentIdentifier,
  paymentTimestamp
} from "./schemas.js";

const verifiedAlipayRecords = new WeakSet();
const SUCCESS_STATUSES = new Set(["TRADE_SUCCESS", "TRADE_FINISHED"]);
const STATE_BY_STATUS = Object.freeze({
  WAIT_BUYER_PAY: "PENDING",
  TRADE_SUCCESS: "PAID",
  TRADE_FINISHED: "PAID",
  TRADE_CLOSED: "CLOSED"
});

function requiredText(value, name) {
  return paymentIdentifier(value, name);
}

function normalizeRecord(value, verificationSource) {
  paymentContractAssert(value && typeof value === "object", PAYMENT_ERROR_CODES.INVALID_REQUEST, "normalized Alipay record required");
  const providerStatus = requiredText(value.status, "status").toUpperCase();
  const paymentStateEvidence = STATE_BY_STATUS[providerStatus] || "FAILED";
  const paid = SUCCESS_STATUSES.has(providerStatus);
  const payloadHash = String(value.payloadHash ?? "").trim().toLowerCase();
  paymentContractAssert(/^[a-f0-9]{64}$/.test(payloadHash), PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "payloadHash required");
  const record = Object.freeze({
    provider: "alipay",
    appId: requiredText(value.appId, "appId"),
    sellerId: requiredText(value.sellerId, "sellerId"),
    merchantOrderId: requiredText(value.merchantOrderId, "merchantOrderId"),
    providerTransactionId: requiredText(value.providerTransactionId, "providerTransactionId"),
    providerEventId: requiredText(value.providerEventId, "providerEventId"),
    userId: requiredText(value.userId, "userId"),
    amountMinor: paymentAmountMinor(value.amountMinor),
    currency: paymentCurrency(value.currency),
    providerStatus,
    paymentStateEvidence,
    paidAt: paid ? paymentTimestamp(value.paidAt, "paidAt") : null,
    observedAt: paymentTimestamp(value.observedAt, "observedAt"),
    payloadHash,
    rawReference: value.rawReference ? requiredText(value.rawReference, "rawReference") : null,
    verificationSource
  });
  verifiedAlipayRecords.add(record);
  return record;
}

export class AlipayPaymentAdapter extends PaymentProviderAdapter {
  constructor({ appId, sellerId, currency = "CNY", signatureVerifier, notificationDecoder, queryTransport } = {}) {
    super("alipay");
    this.appId = requiredText(appId, "appId");
    this.sellerId = requiredText(sellerId, "sellerId");
    this.currency = paymentCurrency(currency);
    paymentContractAssert(this.currency === "CNY", PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "Alipay settlement currency must be CNY");
    paymentContractAssert(typeof signatureVerifier === "function", PAYMENT_ERROR_CODES.INVALID_REQUEST, "signatureVerifier required");
    paymentContractAssert(typeof notificationDecoder === "function", PAYMENT_ERROR_CODES.INVALID_REQUEST, "notificationDecoder required");
    paymentContractAssert(typeof queryTransport === "function", PAYMENT_ERROR_CODES.INVALID_REQUEST, "queryTransport required");
    this.signatureVerifier = signatureVerifier;
    this.notificationDecoder = notificationDecoder;
    this.queryTransport = queryTransport;
  }

  async verifyAndParseWebhook(envelope) {
    const checked = validateWebhookEnvelope(envelope);
    const signatureValid = await this.signatureVerifier({ headers: checked.headers, rawBody: checked.rawBody });
    paymentContractAssert(signatureValid === true, PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "Alipay notification signature rejected");
    const decoded = await this.notificationDecoder(checked.rawBody);
    return normalizeRecord(decoded, "verified_webhook");
  }

  async queryPayment({ order } = {}) {
    paymentContractAssert(order?.schemaVersion === "payment_order_v1", PAYMENT_ERROR_CODES.INVALID_REQUEST, "payment order required");
    const decoded = await this.queryTransport(Object.freeze({
      orderId: requiredText(order.orderId, "orderId"),
      providerOrderId: order.providerOrderId ? requiredText(order.providerOrderId, "providerOrderId") : null
    }));
    const record = normalizeRecord(decoded, "verified_query");
    paymentContractAssert(record.merchantOrderId === order.orderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "query response order mismatch");
    paymentContractAssert(
      !order.providerOrderId || record.providerTransactionId === order.providerOrderId,
      PAYMENT_ERROR_CODES.ORDER_MISMATCH,
      "query response transaction mismatch"
    );
    return record;
  }

  createEvidence({ record, order, identity } = {}) {
    paymentContractAssert(verifiedAlipayRecords.has(record), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED, "verified Alipay record required");
    const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
    paymentContractAssert(order?.schemaVersion === "payment_order_v1", PAYMENT_ERROR_CODES.INVALID_REQUEST, "payment order required");
    paymentContractAssert(order.provider === "alipay", PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "order provider must be Alipay");
    paymentContractAssert(order.userId === verifiedIdentity.userId, PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH, "payment identity does not own order");
    paymentContractAssert(record.userId === verifiedIdentity.userId, PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH, "provider record user mismatch");
    paymentContractAssert(record.appId === this.appId, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "Alipay application mismatch");
    paymentContractAssert(record.sellerId === this.sellerId, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "Alipay payee mismatch");
    paymentContractAssert(record.merchantOrderId === order.orderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "merchant order mismatch");
    paymentContractAssert(record.amountMinor === order.amountMinor, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "Alipay amount mismatch");
    paymentContractAssert(record.currency === this.currency && record.currency === order.currency, PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "Alipay currency mismatch");
    paymentContractAssert(!order.providerOrderId || order.providerOrderId === record.providerTransactionId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "provider transaction mismatch");
    return createVerifiedProviderEvidence({
      provider: "alipay",
      providerOrderId: record.providerTransactionId,
      providerEventId: record.providerEventId,
      orderId: record.merchantOrderId,
      amountMinor: record.amountMinor,
      currency: record.currency,
      providerStatus: record.providerStatus,
      paymentStateEvidence: record.paymentStateEvidence,
      paidAt: record.paidAt,
      occurredAt: record.observedAt,
      verifiedAt: record.observedAt,
      verificationSource: record.verificationSource,
      rawReference: record.rawReference
    });
  }
}
