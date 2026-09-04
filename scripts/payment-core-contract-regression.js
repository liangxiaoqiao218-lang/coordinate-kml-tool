import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FEATURE_NAME_MAPPINGS,
  PAYMENT_BILLING_BOUNDARY,
  PAYMENT_ERROR_CODES,
  PAYMENT_IDENTITY_REQUIREMENT,
  PAYMENT_PROVIDER_METHODS,
  PAYMENT_SECURITY_FAIL_MODE,
  PaymentContractError,
  PaymentProviderAdapter,
  applyPaymentStateTransition,
  assertPaymentProviderAdapter,
  calculateRefundProgress,
  createBillingProductContract,
  createNormalizedProviderResult,
  createPaymentConfirmedBillingEvent,
  createPaymentEventContract,
  createPaymentIdempotencyContract,
  createPaymentOrderContract,
  createPaymentRefundContract,
  createProductSnapshot,
  createServerVerifiedPaymentIdentity,
  createVerifiedProviderEvidence,
  resolveFeatureNameMapping,
  resolvePaymentIdempotency,
  resolveProductPurchasePolicy,
  transitionPaymentState,
  validateCreatePaymentRequest,
  validateWebhookEnvelope
} from "../server/payments/index.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const paymentRoot = path.join(root, "server", "payments");
const paymentSources = fs.readdirSync(paymentRoot).filter(name => name.endsWith(".js")).map(name => fs.readFileSync(path.join(paymentRoot, name), "utf8")).join("\n");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const coordinateSources = ["recognition", "coordinate-finalizer", "projection", "spatial"]
  .flatMap(directory => walkJavaScript(path.join(root, "server", directory)))
  .map(file => fs.readFileSync(file, "utf8"))
  .join("\n");

function walkJavaScript(directory) {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const item = path.join(directory, entry.name);
    return entry.isDirectory() ? walkJavaScript(item) : entry.name.endsWith(".js") ? [item] : [];
  });
}

const tests = [];
function test(id, name, run) { tests.push({ id, name, run }); }
function throwsCode(run, code) {
  assert.throws(run, error => error instanceof PaymentContractError && error.code === code);
}

const now = "2026-09-04T00:00:00.000Z";
const identity = createServerVerifiedPaymentIdentity({ userId: "user-001", verified: true, verification: "authenticated_session" });
const professional = createBillingProductContract({
  productId: "professional-period-contract-test",
  productClass: "professional",
  purchaseType: "subscription",
  priceMinor: 9900,
  currency: "CNY",
  grantDefinition: { type: "subscription", planId: "professional", durationPolicy: "product_defined" },
  status: "active",
  version: "contract-test-v1"
});
const quotaProduct = createBillingProductContract({
  productId: "coordinate-quota-contract-test",
  productClass: "quota_package",
  purchaseType: "quota_package",
  priceMinor: 1000,
  currency: "CNY",
  grantDefinition: { type: "quota_increment", canonicalFeature: "coordinate_processing", quantityPolicy: "product_defined" },
  status: "active",
  version: "contract-test-v1"
});

function order(overrides = {}, client = {}) {
  const product = overrides.productSnapshot || professional;
  return createPaymentOrderContract({
    identity: overrides.identity || identity,
    authority: {
      orderId: "order-001",
      provider: "other",
      productId: product.productId,
      purchaseType: product.purchaseType,
      amountMinor: product.priceMinor,
      currency: product.currency,
      idempotencyKey: "create-order-key-001",
      productSnapshot: createProductSnapshot(product),
      createdAt: now,
      ...overrides
    },
    client
  });
}

function confirmedEvent(value = {}) {
  const verifiedEvidence = value.verifiedEvidence || providerEvidence(value.evidence || {});
  return createPaymentEventContract({
    eventId: "event-001",
    eventType: "PAYMENT_CONFIRMED",
    verifiedEvidence,
    payloadHash: "a".repeat(64),
    receivedAt: now,
    ...value
  });
}

function providerEvidence(overrides = {}) {
  return createVerifiedProviderEvidence({
    provider: "other",
    providerOrderId: "provider-order-001",
    providerEventId: "provider-event-001",
    orderId: "order-001",
    amountMinor: 9900,
    currency: "CNY",
    providerStatus: "SUCCESS",
    paymentStateEvidence: "PAID",
    paidAt: now,
    occurredAt: now,
    verifiedAt: now,
    verificationSource: "verified_webhook",
    ...overrides
  });
}

function paidFlow({ sourceOrder = order(), evidence = providerEvidence() } = {}) {
  const pending = applyPaymentStateTransition({ order: sourceOrder, to: "PENDING", appliedAt: now, transitionId: "transition-pending-001" });
  const paid = applyPaymentStateTransition({ order: pending.order, to: "PAID", appliedAt: now, transitionId: "transition-paid-001", verifiedEvidence: evidence });
  const paymentEvent = confirmedEvent({ verifiedEvidence: evidence });
  return { identity, evidence, pending, paid, order: paid.order, paymentEvent };
}

function billingEvent(flow = paidFlow()) {
  return createPaymentConfirmedBillingEvent({
    eventId: "billing-event-001",
    identity: flow.identity,
    order: flow.order,
    verifiedEvidence: flow.evidence,
    paymentEvent: flow.paymentEvent,
    stateApplication: flow.paid,
    createdAt: now
  });
}

test("PAY01-01", "valid order creation contract", () => assert.equal(order().schemaVersion, "payment_order_v1"));
test("PAY01-02", "amountMinor must be integer", () => throwsCode(() => order({ amountMinor: 99.5 }), PAYMENT_ERROR_CODES.INVALID_AMOUNT));
test("PAY01-03", "client cannot set paid status", () => throwsCode(() => order({}, { status: "PAID" }), PAYMENT_ERROR_CODES.INVALID_REQUEST));
test("PAY01-04", "client cannot authoritatively set amount", () => throwsCode(() => order({}, { amountMinor: 1 }), PAYMENT_ERROR_CODES.INVALID_REQUEST));
test("PAY01-05", "client user_id is rejected as authority", () => throwsCode(() => order({}, { user_id: "another-user" }), PAYMENT_ERROR_CODES.INVALID_REQUEST));
test("PAY01-06", "visitorId cannot satisfy payment identity", () => throwsCode(() => order({ identity: { visitorId: "browser-id", userId: "browser-id", verified: false } }), PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID));
test("PAY01-07", "valid state transitions are centralized", () => {
  for (const [from, to] of [["CREATED", "PENDING"], ["PENDING", "PAID"], ["PAID", "REFUNDED"]]) assert.equal(transitionPaymentState(from, to).allowed, true);
});
test("PAY01-08", "invalid backwards transitions are blocked", () => assert.deepEqual(transitionPaymentState("PAID", "PENDING"), {
  allowed: false, from: "PAID", to: "PENDING", reason: "PAYMENT_STATE_TRANSITION_NOT_ALLOWED", terminal: false, requiresReconciliation: true
}));
test("PAY01-09", "paid state cannot regress to failed or pending", () => {
  assert.equal(transitionPaymentState("PAID", "FAILED").allowed, false);
  assert.equal(transitionPaymentState("PAID", "PENDING").allowed, false);
});
test("PAY01-10", "partial refund progression is monotonic", () => {
  const first = calculateRefundProgress({ paidAmountMinor: 9900, requestedAmountMinor: 1000 });
  assert.equal(first.to, "PARTIALLY_REFUNDED");
  assert.equal(calculateRefundProgress({ paidAmountMinor: 9900, refundedAmountMinor: 1000, requestedAmountMinor: 8900 }).to, "REFUNDED");
});
test("PAY01-11", "refund cannot exceed paid amount", () => throwsCode(() => createPaymentRefundContract({
  refundId: "refund-001", orderId: "order-001", amountMinor: 9001, paidAmountMinor: 9900, alreadyRefundedAmountMinor: 900,
  currency: "CNY", status: "CREATED", reason: "contract test", createdAt: now
}), PAYMENT_ERROR_CODES.REFUND_EXCEEDS_PAID_AMOUNT));
test("PAY01-12", "provider-neutral interface shape is frozen", () => {
  assert.deepEqual(PAYMENT_PROVIDER_METHODS, ["createPayment", "queryPayment", "closePayment", "refundPayment", "queryRefund", "verifyAndParseWebhook"]);
  assert.equal(assertPaymentProviderAdapter(new PaymentProviderAdapter("other")).provider, "other");
});
test("PAY01-13", "provider-specific wire fields are absent from core contracts", () => assert.doesNotMatch(paymentSources, /\b(mchid|appid|out_trade_no|trade_state|prepay_id|transaction_id)\b/));
test("PAY01-14", "billing product is not a plan", () => {
  assert.equal(professional.schemaVersion, "billing_product_v1");
  assert.equal(Object.hasOwn(professional, "plan"), false);
});
test("PAY01-15", "Free produces no paid order", () => assert.equal(resolveProductPurchasePolicy("free").paidOrderAllowed, false));
test("PAY01-16", "Professional maps to subscription", () => assert.equal(resolveProductPurchasePolicy("professional").purchaseType, "subscription"));
test("PAY01-17", "quota package maps to a one-time quota grant", () => {
  assert.equal(quotaProduct.purchaseType, "quota_package");
  assert.equal(quotaProduct.grantDefinition.type, "quota_increment");
});
test("PAY01-18", "Enterprise self-checkout is disabled", () => assert.equal(resolveProductPurchasePolicy("enterprise").selfCheckoutEnabled, false));
test("PAY01-19", "payment event contract is deeply immutable", () => {
  const event = confirmedEvent();
  assert.equal(Object.isFrozen(event), true);
  assert.throws(() => { event.providerStatus = "ALTERED"; }, TypeError);
});
test("PAY01-20", "PAYMENT_CONFIRMED is the payment-to-billing boundary", () => {
  const event = billingEvent();
  assert.equal(event.eventType, "PAYMENT_CONFIRMED");
  assert.deepEqual(event.grantDefinition, professional.grantDefinition);
});
test("PAY01-21", "duplicate create idempotency returns the original contract", () => {
  const contract = createPaymentIdempotencyContract({ scope: "create_order", identity, purchaseIntent: "professional-contract-test", idempotencyKey: "same-key" });
  assert.deepEqual(resolvePaymentIdempotency(contract, contract), { duplicate: true, createNewBusinessOrder: false, contract });
});
test("PAY01-22", "cross-user idempotency conflict is rejected", () => {
  const first = createPaymentIdempotencyContract({ scope: "create_order", identity, purchaseIntent: "professional-contract-test", idempotencyKey: "same-key" });
  const secondIdentity = createServerVerifiedPaymentIdentity({ userId: "user-002", verified: true, verification: "authenticated_session" });
  const second = createPaymentIdempotencyContract({ scope: "create_order", identity: secondIdentity, purchaseIntent: "professional-contract-test", idempotencyKey: "same-key" });
  throwsCode(() => resolvePaymentIdempotency(first, second), PAYMENT_ERROR_CODES.IDEMPOTENCY_CONFLICT);
});
test("PAY01-23", "Provider result cannot directly grant entitlement", () => {
  throwsCode(() => createNormalizedProviderResult({
    provider: "other", providerOrderId: "provider-order", providerStatus: "SUCCESS", paymentStateEvidence: "PAID",
    amountMinor: 9900, currency: "CNY", paidAt: now, grantDefinition: { type: "quota_increment" }
  }), PAYMENT_ERROR_CODES.INVALID_REQUEST);
  assert.equal(PAYMENT_BILLING_BOUNDARY.providerCanGrantEntitlement, false);
});
test("PAY01-24", "frontend success has zero authority", () => {
  throwsCode(() => createPaymentConfirmedBillingEvent({ eventId: "billing-event", identity, order: { ...order(), status: "PAID" }, verifiedEvidence: providerEvidence(), paymentEvent: { source: "success_redirect" }, stateApplication: { allowed: true, to: "PAID" }, createdAt: now }), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED);
  assert.equal(PAYMENT_BILLING_BOUNDARY.frontendPaymentSuccessAuthority, false);
});
test("PAY01-25", "Golden and coordinate recognition code has zero payment dependency", () => assert.doesNotMatch(coordinateSources, /payments\//));
test("PAY01-26", "payment failure is isolated from coordinate, KML, and Map code", async () => {
  let coordinateCalls = 0;
  const adapter = new PaymentProviderAdapter("other");
  await assert.rejects(() => adapter.createPayment(), error => error.code === PAYMENT_ERROR_CODES.PROVIDER_UNAVAILABLE);
  assert.equal(coordinateCalls, 0);
  assert.doesNotMatch(paymentSources, /coordinate-finalizer|recognition\/|projection\/|spatial\/|kml/i);
});
test("PAY01-27", "Payment Core performs no database access", () => assert.doesNotMatch(paymentSources, /@supabase|\bsupabase\b|\.from\s*\(|CREATE\s+TABLE|ALTER\s+TABLE/i));
test("PAY01-28", "Payment Core performs no network or provider call", () => assert.doesNotMatch(paymentSources, /\bfetch\s*\(|axios|https?:\/\/|node:https|node:http/));
test("PAY01-29", "Payment Core is not wired into production authority", () => {
  assert.doesNotMatch(serverSource, /server\/payments|api\/payments|api\/billing/);
  assert.equal(PAYMENT_SECURITY_FAIL_MODE, "FAIL_CLOSED");
  assert.equal(PAYMENT_IDENTITY_REQUIREMENT, "SERVER_VERIFIED_USER_ID_ONLY");
});
test("PAY01-30", "pure deterministic regression contracts are stable", () => {
  assert.equal(validateCreatePaymentRequest({ order: order(), description: "contract test" }).order.status, "CREATED");
  assert.equal(validateWebhookEnvelope({ headers: { signature: "contract-only" }, rawBody: "{}" }).rawBody, "{}");
  assert.equal(resolveFeatureNameMapping("convert", "production").canonicalBillingName, "coordinate_processing");
  assert.equal(FEATURE_NAME_MAPPINGS.every(item => item.changesProductionAuthority === false), true);
});

test("PAY01-31", "forged identity marker is rejected", () => throwsCode(() => order({ identity: {
  userId: "forged-user", verified: true, requirement: PAYMENT_IDENTITY_REQUIREMENT, verification: "authenticated_session"
} }), PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID));
test("PAY01-32", "visitorId wrapped in forged verified identity is rejected", () => throwsCode(() => order({ identity: {
  userId: "visitor-001", visitorId: "visitor-001", verified: true, requirement: PAYMENT_IDENTITY_REQUIREMENT, verification: "authenticated_session"
} }), PAYMENT_ERROR_CODES.IDENTITY_PROVENANCE_INVALID));
test("PAY01-33", "plain verified-provider-looking object is rejected", () => throwsCode(() => createPaymentEventContract({
  eventId: "event-forged", eventType: "PAYMENT_CONFIRMED", verifiedEvidence: { ...providerEvidence() }, payloadHash: "b".repeat(64), receivedAt: now
}), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED));
test("PAY01-34", "provider mismatch is rejected", () => {
  const wechatOrder = order({ provider: "wechat" });
  const stripeEvidence = providerEvidence({ provider: "stripe" });
  const pending = applyPaymentStateTransition({ order: wechatOrder, to: "PENDING", appliedAt: now, transitionId: "provider-mismatch-pending" });
  throwsCode(() => applyPaymentStateTransition({ order: pending.order, to: "PAID", appliedAt: now, transitionId: "provider-mismatch-paid", verifiedEvidence: stripeEvidence }), PAYMENT_ERROR_CODES.PROVIDER_MISMATCH);
});
test("PAY01-35", "orderId mismatch is rejected", () => {
  const evidence = providerEvidence({ orderId: "other-order" });
  const pending = applyPaymentStateTransition({ order: order(), to: "PENDING", appliedAt: now, transitionId: "order-mismatch-pending" });
  throwsCode(() => applyPaymentStateTransition({ order: pending.order, to: "PAID", appliedAt: now, transitionId: "order-mismatch-paid", verifiedEvidence: evidence }), PAYMENT_ERROR_CODES.ORDER_MISMATCH);
});
test("PAY01-36", "amount mismatch is rejected", () => {
  const evidence = providerEvidence({ amountMinor: 9901 });
  const pending = applyPaymentStateTransition({ order: order(), to: "PENDING", appliedAt: now, transitionId: "amount-mismatch-pending" });
  throwsCode(() => applyPaymentStateTransition({ order: pending.order, to: "PAID", appliedAt: now, transitionId: "amount-mismatch-paid", verifiedEvidence: evidence }), PAYMENT_ERROR_CODES.INVALID_AMOUNT);
});
test("PAY01-37", "currency mismatch is rejected", () => {
  const evidence = providerEvidence({ currency: "USD" });
  const pending = applyPaymentStateTransition({ order: order(), to: "PENDING", appliedAt: now, transitionId: "currency-mismatch-pending" });
  throwsCode(() => applyPaymentStateTransition({ order: pending.order, to: "PAID", appliedAt: now, transitionId: "currency-mismatch-paid", verifiedEvidence: evidence }), PAYMENT_ERROR_CODES.CURRENCY_MISMATCH);
});
test("PAY01-38", "plain PAID order cannot create PAYMENT_CONFIRMED", () => {
  const flow = paidFlow();
  throwsCode(() => createPaymentConfirmedBillingEvent({ eventId: "plain-paid-order", identity, order: { ...flow.order }, verifiedEvidence: flow.evidence, paymentEvent: flow.paymentEvent, stateApplication: flow.paid, createdAt: now }), PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED);
});
test("PAY01-39", "plain payment event cannot create PAYMENT_CONFIRMED", () => {
  const flow = paidFlow();
  throwsCode(() => createPaymentConfirmedBillingEvent({ eventId: "plain-payment-event", identity, order: flow.order, verifiedEvidence: flow.evidence, paymentEvent: { ...flow.paymentEvent }, stateApplication: flow.paid, createdAt: now }), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED);
});
test("PAY01-40", "successful state-machine PAID application creates PAYMENT_CONFIRMED", () => {
  const flow = paidFlow();
  assert.equal(flow.paid.to, "PAID");
  assert.equal(billingEvent(flow).paymentTransitionId, flow.paid.transitionId);
});
test("PAY01-41", "frontend success objects have zero authority", () => {
  for (const frontend of [{ success: true }, { status: "PAID" }, { paymentSuccess: true, userId: "user-001", amountMinor: 9900 }]) {
    throwsCode(() => applyPaymentStateTransition({ order: frontend, to: "PAID", appliedAt: now, transitionId: "frontend-forgery" }), PAYMENT_ERROR_CODES.INVALID_REQUEST);
    throwsCode(() => createPaymentEventContract({ eventId: "frontend-event", eventType: "PAYMENT_CONFIRMED", verifiedEvidence: frontend, payloadHash: "c".repeat(64), receivedAt: now }), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED);
  }
});
test("PAY01-42", "unverified Provider payload cannot create payment event", () => {
  const unverified = createNormalizedProviderResult({ provider: "other", providerOrderId: "provider-order-001", providerStatus: "SUCCESS", paymentStateEvidence: "PAID", amountMinor: 9900, currency: "CNY", paidAt: now });
  throwsCode(() => createPaymentEventContract({ eventId: "unverified-event", eventType: "PAYMENT_CONFIRMED", verifiedEvidence: unverified, payloadHash: "d".repeat(64), receivedAt: now }), PAYMENT_ERROR_CODES.PROVIDER_EVIDENCE_UNVERIFIED);
});
test("PAY01-43", "verified evidence valid order and valid PAID transition pass", () => {
  const flow = paidFlow();
  assert.equal(flow.order.status, "PAID");
  assert.equal(flow.paymentEvent.schemaVersion, "payment_event_v1");
  assert.equal(billingEvent(flow).schemaVersion, "billing_event_v1");
});
test("PAY01-44", "duplicate parallel state authority implementation is absent", () => {
  const stateSource = fs.readFileSync(path.join(paymentRoot, "state-machine.js"), "utf8");
  assert.equal((stateSource.match(/const transitions\s*=/g) || []).length, 1);
  assert.equal((stateSource.match(/assertPaymentStateTransition\(currentOrder\.status, to\)/g) || []).length, 1);
});
test("PAY01-45", "providerOrderId mismatch is rejected", () => {
  const evidence = providerEvidence();
  const eventInput = { eventId: "provider-order-mismatch", eventType: "PAYMENT_CONFIRMED", verifiedEvidence: evidence, providerOrderId: "wrong-provider-order", payloadHash: "e".repeat(64), receivedAt: now };
  throwsCode(() => createPaymentEventContract(eventInput), PAYMENT_ERROR_CODES.ORDER_MISMATCH);
});
test("PAY01-46", "plain state application receipt cannot authorize PAYMENT_CONFIRMED", () => {
  const flow = paidFlow();
  throwsCode(() => createPaymentConfirmedBillingEvent({ eventId: "plain-state-application", identity, order: flow.order, verifiedEvidence: flow.evidence, paymentEvent: flow.paymentEvent, stateApplication: { ...flow.paid }, createdAt: now }), PAYMENT_ERROR_CODES.STATE_APPLICATION_REQUIRED);
});

assert.equal(tests.length, 46);
let passed = 0;
for (const entry of tests) {
  await entry.run();
  passed += 1;
  console.log(`PASS ${entry.id} ${entry.name}`);
}
console.log(`PAY-01 Payment Core contract regression: ${passed}/${tests.length} PASS`);
console.log("PURE_DETERMINISTIC=true");
console.log("DATABASE_ACCESS=false");
console.log("NETWORK_PROVIDER_CALLS=0");
console.log("PROVIDER_CAN_GRANT_ENTITLEMENT=false");
console.log("FRONTEND_PAYMENT_SUCCESS_AUTHORITY=false");
console.log("forgedIdentityAccepted=false");
console.log("forgedBillingEventAccepted=false");
console.log("providerMismatchAccepted=false");
