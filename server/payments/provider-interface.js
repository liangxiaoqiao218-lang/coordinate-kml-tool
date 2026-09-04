import { PAYMENT_PROVIDERS } from "./contracts.js";
import { PAYMENT_ERROR_CODES, PaymentContractError, paymentContractAssert } from "./errors.js";
import { createNormalizedProviderResult, paymentAmountMinor, paymentCurrency, paymentIdentifier, paymentProvider } from "./schemas.js";

export const PAYMENT_PROVIDER_METHODS = Object.freeze([
  "createPayment",
  "queryPayment",
  "closePayment",
  "refundPayment",
  "queryRefund",
  "verifyAndParseWebhook"
]);

export function validatePaymentProviderContext(context = {}) {
  paymentContractAssert(context && typeof context === "object", PAYMENT_ERROR_CODES.INVALID_REQUEST, "provider context required");
  return Object.freeze({
    provider: paymentProvider(context.provider),
    requestId: paymentIdentifier(context.requestId, "requestId")
  });
}

export function validateCreatePaymentRequest(value = {}) {
  paymentContractAssert(value.order?.schemaVersion === "payment_order_v1", PAYMENT_ERROR_CODES.INVALID_REQUEST, "payment order required");
  paymentContractAssert(value.order.status === "CREATED", PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, "provider creation requires CREATED order");
  paymentContractAssert(!value.grantDefinition && !value.entitlement, PAYMENT_ERROR_CODES.INVALID_REQUEST, "provider request cannot grant entitlement");
  return Object.freeze({ order: value.order, description: String(value.description ?? "").trim().slice(0, 127) });
}

export function validatePaymentReference(value = {}) {
  return Object.freeze({
    orderId: paymentIdentifier(value.orderId, "orderId"),
    providerOrderId: value.providerOrderId ? paymentIdentifier(value.providerOrderId, "providerOrderId") : null
  });
}

export function validateRefundRequest(value = {}) {
  return Object.freeze({
    refundId: paymentIdentifier(value.refundId, "refundId"),
    orderId: paymentIdentifier(value.orderId, "orderId"),
    providerOrderId: paymentIdentifier(value.providerOrderId, "providerOrderId"),
    amountMinor: paymentAmountMinor(value.amountMinor),
    currency: paymentCurrency(value.currency),
    reason: String(value.reason ?? "").trim().slice(0, 240)
  });
}

export function validateRefundReference(value = {}) {
  return Object.freeze({
    refundId: paymentIdentifier(value.refundId, "refundId"),
    providerRefundId: value.providerRefundId ? paymentIdentifier(value.providerRefundId, "providerRefundId") : null
  });
}

export function validateWebhookEnvelope(value = {}) {
  paymentContractAssert(value.headers && typeof value.headers === "object", PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "webhook headers required");
  const validRawBody = typeof value.rawBody === "string" || value.rawBody instanceof Uint8Array;
  paymentContractAssert(validRawBody && value.rawBody.length > 0, PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "non-empty rawBody required");
  return Object.freeze({ headers: Object.freeze({ ...value.headers }), rawBody: value.rawBody });
}

export class PaymentProviderAdapter {
  constructor(provider) {
    this.provider = paymentProvider(provider);
  }

  unavailable(method) {
    throw new PaymentContractError(PAYMENT_ERROR_CODES.PROVIDER_UNAVAILABLE, `${this.provider}.${method} is not implemented`);
  }

  async createPayment() { return this.unavailable("createPayment"); }
  async queryPayment() { return this.unavailable("queryPayment"); }
  async closePayment() { return this.unavailable("closePayment"); }
  async refundPayment() { return this.unavailable("refundPayment"); }
  async queryRefund() { return this.unavailable("queryRefund"); }
  async verifyAndParseWebhook() { return this.unavailable("verifyAndParseWebhook"); }
}

export function assertPaymentProviderAdapter(adapter) {
  paymentContractAssert(PAYMENT_PROVIDERS.includes(adapter?.provider), PAYMENT_ERROR_CODES.INVALID_REQUEST, "provider adapter identity required");
  for (const method of PAYMENT_PROVIDER_METHODS) {
    paymentContractAssert(typeof adapter[method] === "function", PAYMENT_ERROR_CODES.INVALID_REQUEST, `provider adapter missing ${method}`);
  }
  return adapter;
}

export { createNormalizedProviderResult };
