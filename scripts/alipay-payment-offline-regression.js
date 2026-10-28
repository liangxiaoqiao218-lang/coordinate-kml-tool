import assert from "node:assert/strict";
import {
  AlipayPaymentAdapter,
  PAYMENT_ERROR_CODES,
  PaymentConfirmationOrchestrator,
  PaymentContractError,
  createBillingProductContract,
  createPaymentOrderContract,
  createProductSnapshot,
  createServerVerifiedPaymentIdentity
} from "../server/payments/index.js";

const now = "2026-10-28T00:00:00.000Z";
const payloadHash = "a".repeat(64);

class MemoryOrderStore {
  constructor(order) {
    this.orders = new Map([[order.orderId, order]]);
    this.locks = new Map();
    this.saveCalls = 0;
    this.withOrderLockCalls = 0;
  }
  async get(orderId) { return this.orders.get(orderId) || null; }
  async save(order) { this.saveCalls += 1; this.orders.set(order.orderId, order); return order; }
  async withOrderLock(orderId, run) {
    this.withOrderLockCalls += 1;
    const previous = this.locks.get(orderId) || Promise.resolve();
    let release;
    const current = new Promise(resolve => { release = resolve; });
    this.locks.set(orderId, previous.then(() => current));
    await previous;
    try { return await run(); } finally { release(); if (this.locks.get(orderId) === current) this.locks.delete(orderId); }
  }
}

class MemoryTransactionStore {
  constructor() { this.bindings = new Map(); this.claimCalls = 0; }
  async claim({ provider, providerTransactionId, orderId }) {
    this.claimCalls += 1;
    const key = `${provider}:${providerTransactionId}`;
    const existing = this.bindings.get(key);
    if (existing) return { claimed: false, orderId: existing };
    this.bindings.set(key, orderId);
    return { claimed: true, orderId };
  }
}

class MemoryEntitlementStore {
  constructor({ fail = false } = {}) { this.receipts = new Map(); this.applyCalls = 0; this.fail = fail; }
  async get(key) { return this.receipts.get(key) || null; }
  async applyOnce({ key }) {
    const existing = this.receipts.get(key);
    if (existing) return existing;
    this.applyCalls += 1;
    if (this.fail) throw new Error("synthetic grant failure");
    const receipt = Object.freeze({ key, status: "APPLIED", grantCount: 1 });
    this.receipts.set(key, receipt);
    return receipt;
  }
  async markFailed({ key }) {
    const existing = this.receipts.get(key);
    if (existing) return existing;
    const receipt = Object.freeze({ key, status: "FAILED", grantCount: 0 });
    this.receipts.set(key, receipt);
    return receipt;
  }
}

function makeRecord(overrides = {}) {
  return {
    appId: "app-001",
    sellerId: "seller-001",
    merchantOrderId: "order-001",
    providerTransactionId: "trade-001",
    providerEventId: "event-001",
    userId: "user-001",
    amountMinor: 1200,
    currency: "CNY",
    status: "TRADE_SUCCESS",
    paidAt: now,
    observedAt: now,
    payloadHash,
    rawReference: "synthetic-reference",
    ...overrides
  };
}

function makeEnvironment({ record = makeRecord(), signatureValid = true, grantFails = false } = {}) {
  const identity = createServerVerifiedPaymentIdentity({ userId: "user-001", verification: "authenticated_session" });
  const product = createBillingProductContract({
    productId: "quota-001",
    productClass: "quota_package",
    purchaseType: "quota_package",
    priceMinor: 1200,
    currency: "CNY",
    grantDefinition: { type: "quota_increment", feature: "paid_tool", quantity: 10 },
    status: "active",
    version: "offline-v1"
  });
  const createOrder = orderId => createPaymentOrderContract({
    identity,
    authority: {
      orderId,
      provider: "alipay",
      productId: product.productId,
      purchaseType: product.purchaseType,
      amountMinor: product.priceMinor,
      currency: product.currency,
      idempotencyKey: `create-${orderId}`,
      productSnapshot: createProductSnapshot(product),
      createdAt: now
    }
  });
  const order = createOrder("order-001");
  const orderStore = new MemoryOrderStore(order);
  const transactionStore = new MemoryTransactionStore();
  const entitlementStore = new MemoryEntitlementStore({ fail: grantFails });
  let queryRecord = record;
  const adapter = new AlipayPaymentAdapter({
    appId: "app-001",
    sellerId: "seller-001",
    currency: "CNY",
    signatureVerifier: async () => signatureValid,
    notificationDecoder: async rawBody => JSON.parse(String(rawBody)),
    queryTransport: async () => queryRecord
  });
  const orchestrator = new PaymentConfirmationOrchestrator({ adapter, orderStore, transactionStore, entitlementStore });
  return {
    identity, orderStore, transactionStore, entitlementStore, orchestrator,
    createOrder,
    setQueryRecord(value) { queryRecord = value; },
    envelope(value = record) { return { headers: { signature: "synthetic" }, rawBody: JSON.stringify(value) }; }
  };
}

function throwsCode(run, code) {
  return assert.rejects(run, error => error instanceof PaymentContractError && error.code === code);
}

const tests = [];
function test(id, name, run) { tests.push({ id, name, run }); }

test("A01", "valid notification creates first PAID and one grant", async () => {
  const env = makeEnvironment();
  const result = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(result.firstPaid, true); assert.equal(result.order.status, "PAID"); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A02", "invalid signature is rejected", async () => {
  const env = makeEnvironment({ signatureValid: false });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.WEBHOOK_INVALID);
});
test("A03", "amount mismatch is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ amountMinor: 1201 }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.INVALID_AMOUNT);
});
test("A04", "application mismatch is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ appId: "other-app" }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.PROVIDER_MISMATCH);
});
test("A05", "payee mismatch is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ sellerId: "other-seller" }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.PROVIDER_MISMATCH);
});
test("A06", "non-CNY is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ currency: "USD" }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.CURRENCY_MISMATCH);
});
test("A07", "user swap is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ userId: "user-002" }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.ORDER_OWNERSHIP_MISMATCH);
});
test("A08", "merchant order swap is rejected", async () => {
  const env = makeEnvironment({ record: makeRecord({ merchantOrderId: "order-002" }) });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.INVALID_REQUEST);
});
test("A09", "provider transaction conflict is rejected", async () => {
  const env = makeEnvironment();
  await env.transactionStore.claim({ provider: "alipay", providerTransactionId: "trade-001", orderId: "order-002" });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.ORDER_MISMATCH);
});
test("A10", "duplicate notification confirms and grants once", async () => {
  const env = makeEnvironment();
  const first = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  const second = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(first.firstPaid, true); assert.equal(second.firstPaid, false); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A11", "notification then query is idempotent", async () => {
  const env = makeEnvironment();
  await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  env.setQueryRecord(makeRecord({ providerEventId: "query-001" }));
  const second = await env.orchestrator.confirmQuery({ identity: env.identity, orderId: "order-001" });
  assert.equal(second.firstPaid, false); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A12", "query then notification is idempotent", async () => {
  const env = makeEnvironment();
  env.setQueryRecord(makeRecord({ providerEventId: "query-001" }));
  const first = await env.orchestrator.confirmQuery({ identity: env.identity, orderId: "order-001" });
  const second = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(first.firstPaid, true); assert.equal(second.firstPaid, false); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A13", "concurrent notification and query produce one first PAID", async () => {
  const env = makeEnvironment();
  env.setQueryRecord(makeRecord({ providerEventId: "query-001" }));
  const results = await Promise.all([
    env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }),
    env.orchestrator.confirmQuery({ identity: env.identity, orderId: "order-001" })
  ]);
  assert.equal(results.filter(item => item.firstPaid).length, 1); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A14", "successful entitlement has one APPLIED receipt", async () => {
  const env = makeEnvironment();
  const result = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(result.entitlementStatus, "APPLIED"); assert.equal(result.entitlementReceipt.grantCount, 1);
});
test("A15", "grant failure preserves PAID and records FAILED", async () => {
  const env = makeEnvironment({ grantFails: true });
  const result = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(result.order.status, "PAID"); assert.equal(result.entitlementStatus, "FAILED");
});
test("A16", "repeat after grant failure does not grant twice or hide PAID", async () => {
  const env = makeEnvironment({ grantFails: true });
  await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  const repeated = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(repeated.order.status, "PAID"); assert.equal(repeated.firstPaid, false); assert.equal(env.entitlementStore.applyCalls, 1);
});
test("A17", "non-success status does not enter PAID or grant", async () => {
  const record = makeRecord({ status: "WAIT_BUYER_PAY", paidAt: null });
  const env = makeEnvironment({ record });
  const result = await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  assert.equal(result.paid, false); assert.equal(result.order.status, "CREATED"); assert.equal(env.entitlementStore.applyCalls, 0);
});
test("A18", "missing critical field fails closed", async () => {
  const record = makeRecord(); delete record.sellerId;
  const env = makeEnvironment({ record });
  await throwsCode(() => env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() }), PAYMENT_ERROR_CODES.INVALID_REQUEST);
});

test("A19", "query for A rejects a same-user response for existing order B before side effects", async () => {
  const env = makeEnvironment();
  const orderA = await env.orderStore.get("order-001");
  const orderB = env.createOrder("order-002");
  env.orderStore.orders.set(orderB.orderId, orderB);
  const beforeA = JSON.stringify(orderA);
  const beforeB = JSON.stringify(orderB);
  env.setQueryRecord(makeRecord({ merchantOrderId: "order-002", providerEventId: "query-order-b" }));

  await throwsCode(
    () => env.orchestrator.confirmQuery({ identity: env.identity, orderId: "order-001" }),
    PAYMENT_ERROR_CODES.ORDER_MISMATCH
  );

  assert.equal(JSON.stringify(await env.orderStore.get("order-001")), beforeA);
  assert.equal(JSON.stringify(await env.orderStore.get("order-002")), beforeB);
  assert.equal((await env.orderStore.get("order-001")).status, "CREATED");
  assert.equal((await env.orderStore.get("order-002")).status, "CREATED");
  assert.equal(env.orderStore.withOrderLockCalls, 0);
  assert.equal(env.orderStore.saveCalls, 0);
  assert.equal(env.transactionStore.claimCalls, 0);
  assert.equal(env.transactionStore.bindings.size, 0);
  assert.equal(env.entitlementStore.applyCalls, 0);
  assert.equal(env.entitlementStore.receipts.size, 0);
});

test("A20", "query rejects a transaction mismatch on an already-bound paid order", async () => {
  const env = makeEnvironment();
  await env.orchestrator.confirmNotification({ identity: env.identity, envelope: env.envelope() });
  const paidOrder = await env.orderStore.get("order-001");
  const beforeOrder = JSON.stringify(paidOrder);
  const beforeSaveCalls = env.orderStore.saveCalls;
  const beforeLockCalls = env.orderStore.withOrderLockCalls;
  const beforeClaimCalls = env.transactionStore.claimCalls;
  const beforeApplyCalls = env.entitlementStore.applyCalls;
  env.setQueryRecord(makeRecord({ providerTransactionId: "trade-002", providerEventId: "query-trade-002" }));

  await throwsCode(
    () => env.orchestrator.confirmQuery({ identity: env.identity, orderId: "order-001" }),
    PAYMENT_ERROR_CODES.ORDER_MISMATCH
  );

  assert.equal(JSON.stringify(await env.orderStore.get("order-001")), beforeOrder);
  assert.equal((await env.orderStore.get("order-001")).status, "PAID");
  assert.equal(env.orderStore.withOrderLockCalls, beforeLockCalls);
  assert.equal(env.orderStore.saveCalls, beforeSaveCalls);
  assert.equal(env.transactionStore.claimCalls, beforeClaimCalls);
  assert.equal(env.entitlementStore.applyCalls, beforeApplyCalls);
});

assert.equal(tests.length, 20);
let passed = 0;
for (const entry of tests) {
  await entry.run();
  passed += 1;
  console.log(`PASS ${entry.id} ${entry.name}`);
}
console.log(`PAY-ALIPAY-01 offline regression: ${passed}/${tests.length} PASS`);
console.log("SYNTHETIC_ONLY=true");
console.log("REAL_PROVIDER_CALLS=0");
console.log("DATABASE_ACCESS=false");
