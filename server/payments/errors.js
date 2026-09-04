export const PAYMENT_ERROR_CODES = Object.freeze({
  INVALID_REQUEST: "PAYMENT_INVALID_REQUEST",
  INVALID_STATE_TRANSITION: "PAYMENT_INVALID_STATE_TRANSITION",
  INVALID_AMOUNT: "PAYMENT_INVALID_AMOUNT",
  CURRENCY_MISMATCH: "PAYMENT_CURRENCY_MISMATCH",
  IDENTITY_REQUIRED: "PAYMENT_IDENTITY_REQUIRED",
  IDENTITY_PROVENANCE_INVALID: "PAYMENT_IDENTITY_PROVENANCE_INVALID",
  PROVIDER_EVIDENCE_UNVERIFIED: "PAYMENT_PROVIDER_EVIDENCE_UNVERIFIED",
  PROVIDER_MISMATCH: "PAYMENT_PROVIDER_MISMATCH",
  ORDER_MISMATCH: "PAYMENT_ORDER_MISMATCH",
  STATE_APPLICATION_REQUIRED: "PAYMENT_STATE_APPLICATION_REQUIRED",
  CONFIRMED_PROVENANCE_INVALID: "PAYMENT_CONFIRMED_PROVENANCE_INVALID",
  PRODUCT_NOT_FOUND: "PAYMENT_PRODUCT_NOT_FOUND",
  PROVIDER_UNAVAILABLE: "PAYMENT_PROVIDER_UNAVAILABLE",
  IDEMPOTENCY_CONFLICT: "PAYMENT_IDEMPOTENCY_CONFLICT",
  ORDER_OWNERSHIP_MISMATCH: "PAYMENT_ORDER_OWNERSHIP_MISMATCH",
  REFUND_EXCEEDS_PAID_AMOUNT: "PAYMENT_REFUND_EXCEEDS_PAID_AMOUNT",
  WEBHOOK_INVALID: "PAYMENT_WEBHOOK_INVALID"
});

export class PaymentContractError extends Error {
  constructor(code, message, details = undefined) {
    super(message || code);
    this.name = "PaymentContractError";
    this.code = code;
    if (details !== undefined) this.details = structuredClone(details);
  }
}

export function paymentContractAssert(condition, code, message, details = undefined) {
  if (!condition) throw new PaymentContractError(code, message, details);
}

export function publicPaymentError(error) {
  const code = Object.values(PAYMENT_ERROR_CODES).includes(error?.code)
    ? error.code
    : PAYMENT_ERROR_CODES.INVALID_REQUEST;
  return Object.freeze({ code, message: "Payment request could not be processed." });
}
