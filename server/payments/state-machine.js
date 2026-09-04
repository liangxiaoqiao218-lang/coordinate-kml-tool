import { BILLING_EVENT_SCHEMA_VERSION, PAYMENT_ORDER_STATUSES, PAYMENT_STATE_APPLICATION_SCHEMA_VERSION } from "./contracts.js";
import { PAYMENT_ERROR_CODES, PaymentContractError, paymentContractAssert } from "./errors.js";
import {
  assertPaymentOrderContract,
  assertVerifiedPaymentEvent,
  assertVerifiedPaymentIdentity,
  assertVerifiedProviderEvidence,
  freezePaymentContract,
  paymentAmountMinor,
  paymentIdentifier,
  paymentTimestamp
} from "./schemas.js";

const transitions = Object.freeze({
  CREATED: Object.freeze(["PENDING", "CLOSED", "EXPIRED", "FAILED"]),
  PENDING: Object.freeze(["PAID", "CLOSED", "EXPIRED", "FAILED"]),
  PAID: Object.freeze(["PARTIALLY_REFUNDED", "REFUNDED"]),
  FAILED: Object.freeze([]),
  CLOSED: Object.freeze([]),
  EXPIRED: Object.freeze([]),
  PARTIALLY_REFUNDED: Object.freeze(["PARTIALLY_REFUNDED", "REFUNDED"]),
  REFUNDED: Object.freeze([])
});

const terminalStates = new Set(["FAILED", "CLOSED", "EXPIRED", "REFUNDED"]);
const reconciliationProtectedStates = new Set(["PAID", "PARTIALLY_REFUNDED", ...terminalStates]);
const stateAppliedOrders = new WeakSet();
const trustedStateApplications = new WeakSet();

function normalizedState(value) {
  return String(value ?? "").trim().toUpperCase();
}

export function transitionPaymentState(fromValue, toValue) {
  const from = normalizedState(fromValue);
  const to = normalizedState(toValue);
  const known = PAYMENT_ORDER_STATUSES.includes(from) && PAYMENT_ORDER_STATUSES.includes(to);
  const allowed = known && transitions[from].includes(to);
  return Object.freeze({
    allowed,
    from,
    to,
    reason: allowed ? "PAYMENT_STATE_TRANSITION_ALLOWED" : known ? "PAYMENT_STATE_TRANSITION_NOT_ALLOWED" : "PAYMENT_STATE_UNKNOWN",
    terminal: terminalStates.has(to),
    requiresReconciliation: !allowed && reconciliationProtectedStates.has(from)
  });
}

export function assertPaymentStateTransition(from, to) {
  const result = transitionPaymentState(from, to);
  if (!result.allowed) throw new PaymentContractError(PAYMENT_ERROR_CODES.INVALID_STATE_TRANSITION, result.reason, result);
  return result;
}

function assertStateManagedOrder(order) {
  if (!stateAppliedOrders.has(order)) assertPaymentOrderContract(order);
  return order;
}

function assertEvidenceMatchesOrder(evidence, order) {
  paymentContractAssert(evidence.provider === order.provider, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "provider evidence does not match order provider");
  paymentContractAssert(evidence.orderId === order.orderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "provider evidence does not match order");
  paymentContractAssert(evidence.amountMinor === order.amountMinor, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "provider evidence amount mismatch");
  paymentContractAssert(evidence.currency === order.currency, PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "provider evidence currency mismatch");
  paymentContractAssert(!order.providerOrderId || order.providerOrderId === evidence.providerOrderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "provider order identity mismatch");
}

export function applyPaymentStateTransition({ order, to, appliedAt, transitionId, verifiedEvidence = null } = {}) {
  const currentOrder = assertStateManagedOrder(order);
  const transition = assertPaymentStateTransition(currentOrder.status, to);
  let evidence = null;
  if (verifiedEvidence !== null) {
    evidence = assertVerifiedProviderEvidence(verifiedEvidence);
    assertEvidenceMatchesOrder(evidence, currentOrder);
  }
  if (transition.to === "PAID") {
    paymentContractAssert(evidence !== null && evidence.paymentStateEvidence === "PAID", PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED, "PAID application requires trusted paid provider evidence");
  }
  const normalizedAppliedAt = paymentTimestamp(appliedAt, "appliedAt");
  const normalizedTransitionId = paymentIdentifier(transitionId, "transitionId");
  const stateVersion = Number.isSafeInteger(currentOrder.stateVersion) ? currentOrder.stateVersion : 0;
  const nextOrder = freezePaymentContract({
    ...currentOrder,
    status: transition.to,
    stateVersion: stateVersion + 1,
    providerOrderId: evidence?.providerOrderId ?? currentOrder.providerOrderId,
    providerStatus: evidence?.providerStatus ?? currentOrder.providerStatus,
    paidAt: transition.to === "PAID" ? evidence.paidAt : currentOrder.paidAt,
    updatedAt: normalizedAppliedAt
  });
  stateAppliedOrders.add(nextOrder);
  const application = freezePaymentContract({
    schemaVersion: PAYMENT_STATE_APPLICATION_SCHEMA_VERSION,
    transitionId: normalizedTransitionId,
    orderId: currentOrder.orderId,
    from: transition.from,
    to: transition.to,
    allowed: transition.allowed,
    reason: transition.reason,
    appliedAt: normalizedAppliedAt,
    fromStateVersion: stateVersion,
    stateVersion: stateVersion + 1,
    providerEvidenceReference: evidence ? freezePaymentContract({
      provider: evidence.provider,
      providerOrderId: evidence.providerOrderId,
      providerEventId: evidence.providerEventId,
      verifiedAt: evidence.verifiedAt
    }) : null,
    order: nextOrder
  });
  trustedStateApplications.add(application);
  return application;
}

export function assertPaymentStateApplication(value) {
  paymentContractAssert(
    value && typeof value === "object" && trustedStateApplications.has(value),
    PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED,
    "trusted payment state application required"
  );
  return value;
}

export function createPaymentConfirmedBillingEvent({ eventId, identity, order, verifiedEvidence, paymentEvent, stateApplication, createdAt } = {}) {
  const verifiedIdentity = assertVerifiedPaymentIdentity(identity);
  const evidence = assertVerifiedProviderEvidence(verifiedEvidence);
  const event = assertVerifiedPaymentEvent(paymentEvent);
  const application = assertPaymentStateApplication(stateApplication);
  paymentContractAssert(application.allowed === true && application.to === "PAID", PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED, "successful PAID state application required");
  paymentContractAssert(application.order === order && order?.status === "PAID", PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED, "state-applied paid order required");
  paymentContractAssert(order.userId === verifiedIdentity.userId, PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "payment identity does not own order");
  assertEvidenceMatchesOrder(evidence, order);
  paymentContractAssert(event.eventType === "PAYMENT_CONFIRMED", PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "PAYMENT_CONFIRMED event required");
  paymentContractAssert(event.provider === evidence.provider, PAYMENT_ERROR_CODES.PROVIDER_MISMATCH, "payment event provider mismatch");
  paymentContractAssert(event.providerOrderId === evidence.providerOrderId && event.orderId === order.orderId, PAYMENT_ERROR_CODES.ORDER_MISMATCH, "payment event order mismatch");
  paymentContractAssert(event.amountMinor === order.amountMinor, PAYMENT_ERROR_CODES.INVALID_AMOUNT, "payment event amount mismatch");
  paymentContractAssert(event.currency === order.currency, PAYMENT_ERROR_CODES.CURRENCY_MISMATCH, "payment event currency mismatch");
  paymentContractAssert(application.providerEvidenceReference?.providerEventId === evidence.providerEventId, PAYMENT_ERROR_CODES.CONFIRMED_PROVENANCE_INVALID, "state application evidence mismatch");
  return freezePaymentContract({
    schemaVersion: BILLING_EVENT_SCHEMA_VERSION,
    eventId: paymentIdentifier(eventId, "eventId"),
    eventType: "PAYMENT_CONFIRMED",
    orderId: order.orderId,
    userId: order.userId,
    productId: order.productId,
    purchaseType: order.purchaseType,
    grantDefinition: structuredClone(order.productSnapshot.grantDefinition),
    paymentTransitionId: application.transitionId,
    providerEventId: evidence.providerEventId,
    createdAt: paymentTimestamp(createdAt, "createdAt")
  });
}

export function calculateRefundProgress({ paidAmountMinor, refundedAmountMinor = 0, requestedAmountMinor } = {}) {
  const paid = paymentAmountMinor(paidAmountMinor, "paidAmountMinor");
  const refunded = paymentAmountMinor(refundedAmountMinor, "refundedAmountMinor", { allowZero: true });
  const requested = paymentAmountMinor(requestedAmountMinor, "requestedAmountMinor");
  if (refunded + requested > paid) {
    throw new PaymentContractError(PAYMENT_ERROR_CODES.REFUND_EXCEEDS_PAID_AMOUNT, "cumulative refund exceeds paid amount");
  }
  const nextRefundedAmountMinor = refunded + requested;
  const from = refunded === 0 ? "PAID" : "PARTIALLY_REFUNDED";
  const to = nextRefundedAmountMinor === paid ? "REFUNDED" : "PARTIALLY_REFUNDED";
  return Object.freeze({ ...assertPaymentStateTransition(from, to), nextRefundedAmountMinor });
}

export const PAYMENT_STATE_TRANSITIONS = transitions;
