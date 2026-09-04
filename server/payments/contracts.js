export const PAYMENT_ORDER_SCHEMA_VERSION = "payment_order_v1";
export const PAYMENT_EVENT_SCHEMA_VERSION = "payment_event_v1";
export const PAYMENT_REFUND_SCHEMA_VERSION = "payment_refund_v1";
export const BILLING_EVENT_SCHEMA_VERSION = "billing_event_v1";
export const BILLING_PRODUCT_SCHEMA_VERSION = "billing_product_v1";
export const PAYMENT_IDEMPOTENCY_SCHEMA_VERSION = "payment_idempotency_v1";
export const PAYMENT_PROVIDER_RESULT_SCHEMA_VERSION = "payment_provider_result_v1";
export const VERIFIED_PAYMENT_PROVIDER_EVIDENCE_SCHEMA_VERSION = "verified_payment_provider_evidence_v1";
export const PAYMENT_STATE_APPLICATION_SCHEMA_VERSION = "payment_state_application_v1";

export const PAYMENT_IDENTITY_REQUIREMENT = "SERVER_VERIFIED_USER_ID_ONLY";
export const PAYMENT_IDENTITY_AUTHORITY = "SERVER_VERIFIED_IDENTITY_PROVENANCE_ONLY";
export const PAYMENT_PROVIDER_EVIDENCE_AUTHORITY = "VERIFIED_PROVIDER_PROVENANCE_ONLY";
export const PAYMENT_STATE_APPLICATION_AUTHORITY = "STATE_MACHINE_ONLY";
export const PAYMENT_SECURITY_FAIL_MODE = "FAIL_CLOSED";

export const PAYMENT_PROVIDERS = Object.freeze(["wechat", "alipay", "stripe", "other"]);
export const PAYMENT_PURCHASE_TYPES = Object.freeze(["subscription", "quota_package", "one_time"]);
export const PAYMENT_ORDER_STATUSES = Object.freeze([
  "CREATED",
  "PENDING",
  "PAID",
  "FAILED",
  "CLOSED",
  "EXPIRED",
  "PARTIALLY_REFUNDED",
  "REFUNDED"
]);
export const PAYMENT_REFUND_STATUSES = Object.freeze(["CREATED", "PENDING", "SUCCEEDED", "FAILED", "CLOSED"]);
export const PAYMENT_EVENT_TYPES = Object.freeze([
  "PAYMENT_CREATED",
  "PAYMENT_PENDING",
  "PAYMENT_CONFIRMED",
  "PAYMENT_FAILED",
  "PAYMENT_CLOSED",
  "PAYMENT_EXPIRED",
  "REFUND_PENDING",
  "REFUND_SUCCEEDED",
  "REFUND_FAILED"
]);
export const PAYMENT_IDEMPOTENCY_SCOPES = Object.freeze([
  "create_order",
  "payment_event",
  "entitlement_grant",
  "refund_request"
]);

export const PAYMENT_ORDER_FIELD_AUTHORITY = Object.freeze({
  geokitLab: Object.freeze([
    "orderId",
    "userId",
    "provider",
    "productId",
    "purchaseType",
    "amountMinor",
    "currency",
    "status",
    "idempotencyKey",
    "productSnapshot",
    "createdAt",
    "updatedAt",
    "expiredAt",
    "closedAt",
    "refundedAmountMinor"
  ]),
  providerEvidence: Object.freeze(["providerOrderId", "providerStatus", "paidAt", "refundEvidence"]),
  clientForbidden: Object.freeze([
    "userId",
    "user_id",
    "visitorId",
    "amountMinor",
    "currency",
    "providerOrderId",
    "providerStatus",
    "status",
    "paidAt",
    "purchaseType",
    "productSnapshot",
    "grantDefinition",
    "entitlement"
  ])
});

export const PAYMENT_BILLING_BOUNDARY = Object.freeze({
  paymentOwns: Object.freeze(["orders", "provider_operations", "payment_facts", "refund_facts"]),
  billingOwns: Object.freeze(["products", "plans", "purchase_types", "billing_events"]),
  entitlementOwns: Object.freeze(["feature_access", "quota", "subscriptions", "grants"]),
  providerCanGrantEntitlement: false,
  frontendPaymentSuccessAuthority: false,
  crossingEvent: "PAYMENT_CONFIRMED"
});
