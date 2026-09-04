import crypto from "node:crypto";
import {
  PAYMENT_EVENT_SCHEMA_VERSION,
  PAYMENT_EVENT_TYPES,
  PAYMENT_IDEMPOTENCY_SCHEMA_VERSION,
  PAYMENT_IDEMPOTENCY_SCOPES,
  PAYMENT_IDENTITY_REQUIREMENT,
  PAYMENT_ORDER_FIELD_AUTHORITY,
  PAYMENT_ORDER_SCHEMA_VERSION,
  PAYMENT_ORDER_STATUSES,
  PAYMENT_PROVIDER_RESULT_SCHEMA_VERSION,
  PAYMENT_PROVIDERS,
  PAYMENT_PURCHASE_TYPES,
  PAYMENT_REFUND_SCHEMA_VERSION,
  PAYMENT_REFUND_STATUSES,
  VERIFIED_PAYMENT_PROVIDER_EVIDENCE_SCHEMA_VERSION
} from "./contracts.js";
import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";

const verifiedPaymentIdentities = new WeakSet();
const verifiedProviderEvidence = new WeakSet();
const verifiedPaymentEvents = new WeakSet();
const paymentOrders = new WeakSet();

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  for (const item of Object.values(value)) deepFreeze(item);
  return Object.freeze(value);
}

function copy(value) {
  return value == null ? value : structuredClone(value);
}

export function paymentIdentifier(value, name) {
  const result = String(value ?? "").trim();
  paymentContractAssert(result.length > 0 && result.length <= 128, PAYMENT_ERROR_CODES.INVALID_REQUEST, `${name} required`);
  return result;
}

export function paymentTimestamp(value, name, { nullable = false } = {}) {
  if (nullable && (value == null || value === "")) return null;
  const date = new Date(value);
  paymentContractAssert(Number.isFinite(date.getTime()), PAYMENT_ERROR_CODES.INVALID_REQUEST, `${name} must be an ISO timestamp`);
  return date.toISOString();
}

export function paymentAmountMinor(value, name = "amountMinor", { allowZero = false } = {}) {
  paymentContractAssert(Number.isSafeInteger(value), PAYMENT_ERROR_CODES.INVALID_AMOUNT, `${name} must be a safe integer`);
  paymentContractAssert(allowZero ? value >= 0 : value > 0, PAYMENT_ERROR_CODES.INVALID_AMOUNT, `${name} must be ${allowZero ? "non-negative" : "positive"}`);
  return value;
}

export function paymentCurrency(value) {
  const currency = String(value ?? "").trim().toUpperCase();
  paymentContractAssert(/^[A-Z]{3}$/.test(currency), PAYMENT_ERROR_CODES.INVALID_REQUEST, "currency must be a three-letter code");
  return currency;
}

export function paymentProvider(value) {
  const provider = String(value ?? "").trim().toLowerCase();
  paymentContractAssert(PAYMENT_PROVIDERS.includes(provider), PAYMENT_ERROR_CODES.INVALID_REQUEST, "unsupported payment provider");
  return provider;
}

export function createVerifiedPaymentIdentity(value = {}) {
  paymentContractAssert(
    ["authenticated_session", "trusted_service"].includes(value.verification),
    PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID,
    "unsupported identity verification source"
  );
  const forbidden = ["visitorId", "x-visitor-id", "localStorage", "browserGenerated", "browserGeneratedUuid", "requestBodyUserId"]
    .filter(field => Object.hasOwn(value, field));
  paymentContractAssert(forbidden.length === 0, PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID, "browser or request identity is not payment authority");
  const identity = deepFreeze({
    requirement: PAYMENT_IDENTITY_REQUIREMENT,
    userId: paymentIdentifier(value.userId, "userId"),
    verification: value.verification
  });
  verifiedPaymentIdentities.add(identity);
  return identity;
}

export const createServerVerifiedPaymentIdentity = createVerifiedPaymentIdentity;

export function assertVerifiedPaymentIdentity(value) {
  paymentContractAssert(
    value && typeof value === "object" && verifiedPaymentIdentities.has(value),
    PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID,
    "trusted payment identity provenance required"
  );
  return value;
}

function rejectClientAuthority(client) {
  const forbidden = PAYMENT_ORDER_FIELD_AUTHORITY.clientForbidden.filter(field => Object.hasOwn(client || {}, field));
  paymentContractAssert(forbidden.length === 0, PAYMENT_ERROR_CODES.INVALID_REQUEST, "client supplied authoritative payment fields", { forbidden });
}

function validateProductSnapshot(snapshot, authority) {
  paymentContractAssert(snapshot && typeof snapshot === "object", PAYMENT_ERROR_CODES.PRODUCT_NOT_FOUND, "productSnapshot required");
  paymentContractAssert(snapshot.schemaVersion === "billing_product_v1", PAYMENT_ERROR_CODES.PRODUCT_NOT_FOUND, "invalid product snapshot");
  paymentContractAssert(snapshot.productId === authority.productId, PAYMENT_ERROR_CODES.INVALID_REQUEST, "product snapshot mismatch");
  paymentContractAssert(snapshot.purchaseType === authority.purchaseType, PAYMENT_ERROR_CODES.INVALID_REQUEST, "purchase type mismatch");
  paymentContractAssert(snapshot.priceMinor === authority.amountMinor, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "authoritative amount does not match product snapshot");
  paymentContractAssert(snapshot.currency === authority.currency, PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "authoritative currency does not match product snapshot");
  paymentContractAssert(snapshot.paidOrderAllowed === true, PAYMENT_ERROR_CODES.INVALID_REQUEST, "product cannot create a paid order");
}

export function createPaymentOrderContract({ identity, authority = {}, providerEvidence = {}, client = {} } = {}) {
  rejectClientAuthority(client);
  const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
  const provider = paymentProvider(authority.provider);
  const purchaseType = String(authority.purchaseType ?? "").trim();
  paymentContractAssert(PAYMENT_PURCHASE_TYPES.includes(purchaseType), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid purchaseType");
  const amountMinor = paymentAmountMinor(authority.amountMinor);
  const currency = paymentCurrency(authority.currency);
  const productId = paymentIdentifier(authority.productId, "productId");
  const productSnapshot = copy(authority.productSnapshot);
  validateProductSnapshot(productSnapshot, { productId, purchaseType, amountMinor, currency });
  const createdAt = paymentTimestamp(authority.createdAt ?? new Date().toISOString(), "createdAt");
  const status = authority.status ?? "CREATED";
  const refundedAmountMinor = paymentAmountMinor(authority.refundedAmountMinor ?? 0, "refundedAmountMinor", { allowZero: true });
  paymentContractAssert(status === "CREATED", PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, "new payment orders must start at CREATED");
  paymentContractAssert(refundedAmountMinor <= amountMinor, PAYMENT_ERROR_CODES.REFUND_EXCEEDS_PAID_AMOUNT, "refunded amount exceeds order amount");
  const evidence = Object.keys(providerEvidence).length > 0 ? assertVerifiedProviderEvidence(providerEvidence) : null;
  if (evidence) {
    paymentContractAssert(evidence.provider === provider, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "provider evidence does not match order provider");
  }
  const order = deepFreeze({
    schemaVersion: PAYMENT_ORDER_SCHEMA_VERSION,
    orderId: paymentIdentifier(authority.orderId, "orderId"),
    userId: verifiedIdentity.userId,
    provider,
    providerOrderId: evidence?.providerOrderId ?? null,
    productId,
    purchaseType,
    amountMinor,
    currency,
    status,
    idempotencyKey: paymentIdentifier(authority.idempotencyKey, "idempotencyKey"),
    productSnapshot,
    providerStatus: evidence?.providerStatus ?? null,
    createdAt,
    updatedAt: paymentTimestamp(authority.updatedAt ?? createdAt, "updatedAt"),
    paidAt: evidence?.paidAt ?? null,
    expiredAt: paymentTimestamp(authority.expiredAt, "expiredAt", { nullable: true }),
    closedAt: paymentTimestamp(authority.closedAt, "closedAt", { nullable: true }),
    refundedAmountMinor
  });
  paymentOrders.add(order);
  return order;
}

export function assertPaymentOrderContract(value) {
  paymentContractAssert(
    value && typeof value === "object" && paymentOrders.has(value),
    PAYMENT_ERROR_CODES.INVALID_REQUEST,
    "trusted payment order contract required"
  );
  return value;
}

export function createNormalizedProviderResult(value = {}) {
  const result = {
    schemaVersion: PAYMENT_PROVIDER_RESULT_SCHEMA_VERSION,
    provider: paymentProvider(value.provider),
    providerOrderId: paymentIdentifier(value.providerOrderId, "providerOrderId"),
    providerStatus: paymentIdentifier(value.providerStatus, "providerStatus"),
    paymentStateEvidence: String(value.paymentStateEvidence ?? "").trim().toUpperCase(),
    amountMinor: paymentAmountMinor(value.amountMinor),
    currency: paymentCurrency(value.currency),
    paidAt: paymentTimestamp(value.paidAt, "paidAt", { nullable: true }),
    rawReference: value.rawReference ? paymentIdentifier(value.rawReference, "rawReference") : null,
    eventId: value.eventId ? paymentIdentifier(value.eventId, "eventId") : null,
    verified: false
  };
  paymentContractAssert(PAYMENT_ORDER_STATUSES.includes(result.paymentStateEvidence), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid payment state evidence");
  paymentContractAssert(!Object.hasOwn(value, "grantDefinition") && !Object.hasOwn(value, "entitlement"), PAYMENT_ERROR_CODES.INVALID_REQUEST, "provider result cannot contain entitlement authority");
  return deepFreeze(result);
}

export function createVerifiedProviderEvidence(value = {}) {
  paymentContractAssert(!Object.hasOwn(value, "grantDefinition") && !Object.hasOwn(value, "entitlement"), PAYMENT_ERROR_CODES.INVALID_REQUEST, "provider evidence cannot contain entitlement authority");
  const verificationSource = String(value.verificationSource ?? "").trim();
  paymentContractAssert(
    ["verified_webhook", "verified_query", "verified_provider_response"].includes(verificationSource),
    PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED,
    "verified provider provenance required"
  );
  const evidence = deepFreeze({
    schemaVersion: VERIFIED_PAYMENT_PROVIDER_EVIDENCE_SCHEMA_VERSION,
    provider: paymentProvider(value.provider),
    providerOrderId: paymentIdentifier(value.providerOrderId, "providerOrderId"),
    providerEventId: paymentIdentifier(value.providerEventId, "providerEventId"),
    orderId: paymentIdentifier(value.orderId, "orderId"),
    amountMinor: paymentAmountMinor(value.amountMinor),
    currency: paymentCurrency(value.currency),
    providerStatus: paymentIdentifier(value.providerStatus, "providerStatus"),
    paymentStateEvidence: String(value.paymentStateEvidence ?? "").trim().toUpperCase(),
    paidAt: paymentTimestamp(value.paidAt, "paidAt", { nullable: true }),
    occurredAt: paymentTimestamp(value.occurredAt ?? value.verifiedAt, "occurredAt"),
    verifiedAt: paymentTimestamp(value.verifiedAt, "verifiedAt"),
    verificationSource,
    rawReference: value.rawReference ? paymentIdentifier(value.rawReference, "rawReference") : null
  });
  paymentContractAssert(PAYMENT_ORDER_STATUSES.includes(evidence.paymentStateEvidence), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid payment state evidence");
  paymentContractAssert(evidence.paymentStateEvidence !== "PAID" || evidence.paidAt !== null, PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED, "paid evidence requires paidAt");
  verifiedProviderEvidence.add(evidence);
  return evidence;
}

export function assertVerifiedProviderEvidence(value) {
  paymentContractAssert(
    value && typeof value === "object" && verifiedProviderEvidence.has(value),
    PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED,
    "trusted verified provider evidence required"
  );
  return value;
}

export function createPaymentEventContract(value = {}) {
  const evidence = assertVerifiedProviderEvidence(value.verifiedEvidence);
  const eventType = String(value.eventType ?? "").trim().toUpperCase();
  const payloadHash = String(value.payloadHash ?? "").trim().toLowerCase();
  paymentContractAssert(PAYMENT_EVENT_TYPES.includes(eventType), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid payment event type");
  paymentContractAssert(/^[a-f0-9]{64}$/.test(payloadHash), PAYMENT_ERROR_CODES.WEBHOOK_INVALID, "payloadHash must be SHA-256 hex");
  paymentContractAssert(eventType !== "PAYMENT_CONFIRMED" || evidence.paymentStateEvidence === "PAID", PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED, "PAYMENT_CONFIRMED requires paid provider evidence");
  if (Object.hasOwn(value, "provider")) paymentContractAssert(paymentProvider(value.provider) === evidence.provider, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "payment event provider mismatch");
  if (Object.hasOwn(value, "providerOrderId")) paymentContractAssert(paymentIdentifier(value.providerOrderId, "providerOrderId") === evidence.providerOrderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "payment event provider order mismatch");
  if (Object.hasOwn(value, "providerEventId")) paymentContractAssert(paymentIdentifier(value.providerEventId, "providerEventId") === evidence.providerEventId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "payment event provider event mismatch");
  if (Object.hasOwn(value, "orderId")) paymentContractAssert(paymentIdentifier(value.orderId, "orderId") === evidence.orderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "payment event order mismatch");
  if (Object.hasOwn(value, "amountMinor")) paymentContractAssert(paymentAmountMinor(value.amountMinor) === evidence.amountMinor, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "payment event amount mismatch");
  if (Object.hasOwn(value, "currency")) paymentContractAssert(paymentCurrency(value.currency) === evidence.currency, PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "payment event currency mismatch");
  const event = deepFreeze({
    schemaVersion: PAYMENT_EVENT_SCHEMA_VERSION,
    eventId: paymentIdentifier(value.eventId, "eventId"),
    provider: evidence.provider,
    providerOrderId: evidence.providerOrderId,
    providerEventId: evidence.providerEventId,
    orderId: evidence.orderId,
    eventType,
    providerStatus: evidence.providerStatus,
    amountMinor: evidence.amountMinor,
    currency: evidence.currency,
    occurredAt: evidence.occurredAt,
    payloadHash,
    receivedAt: paymentTimestamp(value.receivedAt, "receivedAt")
  });
  verifiedPaymentEvents.add(event);
  return event;
}

export function assertVerifiedPaymentEvent(value) {
  paymentContractAssert(
    value && typeof value === "object" && verifiedPaymentEvents.has(value),
    PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED,
    "trusted verified payment event required"
  );
  return value;
}

export function createPaymentRefundContract(value = {}) {
  const amountMinor = paymentAmountMinor(value.amountMinor);
  const paidAmountMinor = paymentAmountMinor(value.paidAmountMinor);
  const alreadyRefundedAmountMinor = paymentAmountMinor(value.alreadyRefundedAmountMinor ?? 0, "alreadyRefundedAmountMinor", { allowZero: true });
  paymentContractAssert(
    alreadyRefundedAmountMinor + amountMinor <= paidAmountMinor,
    PAYMENT_ERROR_CODES.REFUND_EXCEEDS_PAID_AMOUNT,
    "cumulative refund exceeds paid amount"
  );
  const status = String(value.status ?? "CREATED").trim().toUpperCase();
  paymentContractAssert(PAYMENT_REFUND_STATUSES.includes(status), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid refund status");
  return deepFreeze({
    schemaVersion: PAYMENT_REFUND_SCHEMA_VERSION,
    refundId: paymentIdentifier(value.refundId, "refundId"),
    orderId: paymentIdentifier(value.orderId, "orderId"),
    providerRefundId: value.providerRefundId ? paymentIdentifier(value.providerRefundId, "providerRefundId") : null,
    amountMinor,
    currency: paymentCurrency(value.currency),
    status,
    reason: String(value.reason ?? "").trim().slice(0, 240),
    createdAt: paymentTimestamp(value.createdAt, "createdAt"),
    completedAt: paymentTimestamp(value.completedAt, "completedAt", { nullable: true })
  });
}

export function createPaymentIdempotencyContract({ scope, identity, purchaseIntent, idempotencyKey } = {}) {
  const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
  paymentContractAssert(PAYMENT_IDEMPOTENCY_SCOPES.includes(scope), PAYMENT_ERROR_CODES.INVALID_REQUEST, "invalid idempotency scope");
  const intent = paymentIdentifier(purchaseIntent, "purchaseIntent");
  return deepFreeze({
    schemaVersion: PAYMENT_IDEMPOTENCY_SCHEMA_VERSION,
    scope,
    userId: verifiedIdentity.userId,
    idempotencyKey: paymentIdentifier(idempotencyKey, "idempotencyKey"),
    purchaseIntent: intent,
    intentHash: crypto.createHash("sha256").update(`${scope}\n${verifiedIdentity.userId}\n${intent}`).digest("hex")
  });
}

export function resolvePaymentIdempotency(existing, candidate) {
  paymentContractAssert(existing?.schemaVersion === PAYMENT_IDEMPOTENCY_SCHEMA_VERSION, PAYMENT_ERROR_CODES.INVALID_REQUEST, "existing idempotency contract required");
  paymentContractAssert(candidate?.schemaVersion === PAYMENT_IDEMPOTENCY_SCHEMA_VERSION, PAYMENT_ERROR_CODES.INVALID_REQUEST, "candidate idempotency contract required");
  const sameKey = existing.scope === candidate.scope && existing.idempotencyKey === candidate.idempotencyKey;
  paymentContractAssert(sameKey, PAYMENT_ERROR_CODES.INVALID_REQUEST, "idempotency records do not share a key");
  paymentContractAssert(existing.userId === candidate.userId, PAYMENT_ERROR_CODES.IDEMPOTENCY_CONFLICT, "idempotency key belongs to another user");
  paymentContractAssert(existing.intentHash === candidate.intentHash, PAYMENT_ERROR_CODES.IDEMPOTENCY_CONFLICT, "idempotency key has a different purchase intent");
  return deepFreeze({ duplicate: true, createNewBusinessOrder: false, contract: existing });
}

export { deepFreeze as freezePaymentContract };
