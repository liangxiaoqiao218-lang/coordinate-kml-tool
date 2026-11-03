import assert from "node:assert/strict";
import { constants, generateKeyPairSync, sign } from "node:crypto";
import { performance } from "node:perf_hooks";
import { AlipayPaymentAdapter } from "../server/payments/alipay-adapter.js";
import { createAlipayRsa2Verifier } from "../server/payments/alipay-rsa2-verifier.js";
import { createAlipayNotificationDecoder } from "../server/payments/alipay-notification-decoder.js";
import { createAlipayNotificationHandler } from "../server/payments/alipay-notification-handler.js";
import { createPaymentPersistence } from "../server/payments/payment-persistence.js";
import { PaymentConfirmationOrchestrator } from "../server/payments/payment-orchestrator.js";
import {
  createServerVerifiedPaymentIdentity, createPaymentOrderContract, assertPaymentOrderContract,
  createTrustedStoredPaymentOrderReader, createVerifiedProviderEvidence, assertVerifiedProviderEvidence
} from "../server/payments/schemas.js";
import { createBillingProductContract, createProductSnapshot } from "../server/payments/product-mapping.js";
import { transitionPaymentState } from "../server/payments/state-machine.js";

const started = performance.now();
const now = "2026-11-03T04:08:52.000Z";
const userId = "11111111-1111-4111-8111-111111111111";
const totals = { verifications: 0, decodes: 0, t1Attempts: 0, t1Commits: 0, firstPaid: 0,
  t2Attempts: 0, t2Commits: 0, grantsApplied: 0, recoveryCalls: 0, realProviderCalls: 0, databaseCalls: 0 };
let keys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const publicPem = keys.publicKey.export({ type: "spki", format: "pem" });
const clone = value => structuredClone(value);
const json = value => JSON.stringify(value);

function originalOrder(orderId = "order-A") {
  const identity = createServerVerifiedPaymentIdentity({ userId, verification: "authenticated_session" });
  const product = createBillingProductContract({ productId: "synthetic-coordinate-quota", productClass: "quota_package",
    purchaseType: "quota_package", priceMinor: 1200, currency: "CNY", version: "03-offline-v1", status: "active",
    grantDefinition: { type: "quota_increment", canonicalFeature: "coordinate_processing", quantity: 10 } });
  const order = createPaymentOrderContract({ identity, authority: { orderId, provider: "alipay", productId: product.productId,
    purchaseType: product.purchaseType, amountMinor: 1200, currency: "CNY", idempotencyKey: `create:${orderId}`,
    productSnapshot: createProductSnapshot(product), createdAt: now } });
  return { ...clone(order), stateVersion: 0, appId: "synthetic-app", sellerId: "synthetic-seller" };
}

function wireFields(overrides = {}) {
  return { charset: "utf-8", sign_type: "RSA2", app_id: "synthetic-app", seller_id: "synthetic-seller",
    out_trade_no: "order-A", trade_no: "trade-A", notify_id: "event-A", total_amount: "12.00",
    trade_status: "TRADE_SUCCESS", gmt_payment: "2026-11-03 12:00:00", buyer_id: "not-the-site-uuid", ...overrides };
}
function body(overrides = {}) {
  const fields = wireFields(overrides);
  const source = Object.entries(fields).filter(([key, value]) => key !== "sign_type" && key !== "sign" && value !== "")
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, value]) => `${key}=${value}`).join("&");
  return new URLSearchParams({ ...fields, sign: sign("RSA-SHA256", Buffer.from(source), {
    key: keys.privateKey, padding: constants.RSA_PKCS1_PADDING
  }).toString("base64") }).toString();
}

// A serialized in-memory fault fixture. Atomic clone/swap models the required
// driver contract; it is not a database, durable lock or cross-process proof.
class SyntheticDriver {
  constructor({ state = null, mapping = true } = {}) {
    this.state = state ? clone(state) : { orders: new Map([["order-A", originalOrder()]]), events: new Map(),
      bindings: new Map(), tasks: new Map(), accountMappings: new Map(mapping ? [[userId, "existing-account-A"]] : []),
      existingBalances: new Map([["existing-account-A", 5]]) };
    this.tail = Promise.resolve();
    this.failT1 = false; this.loseT1Ack = false; this.failT2 = false; this.loseT2Ack = false;
  }
  async atomic(run) {
    const previous = this.tail;
    let release;
    this.tail = new Promise(resolve => { release = resolve; });
    await previous;
    try { return await run(); } finally { release(); }
  }
  async loadOrder(id) { return clone(this.state.orders.get(id) || null); }
  async loadGrantTask(id) { return clone(this.state.tasks.get(id) || null); }
  async confirmPaidOnce(command) {
    totals.t1Attempts++;
    return this.atomic(async () => {
      const next = clone(this.state);
      const current = next.orders.get(command.expectedOrder.orderId);
      assert.ok(current, "original store order missing");
      for (const field of ["orderId", "userId", "amountMinor", "currency", "provider", "appId", "sellerId", "productId"]) {
        assert.equal(current[field], command.expectedOrder[field], `stored ${field} changed`);
      }
      assert.deepEqual(current.productSnapshot, command.expectedOrder.productSnapshot);
      const existingEvent = next.events.get(command.eventKey);
      if (existingEvent && existingEvent.payloadHash !== command.event.payloadHash) {
        throw Object.assign(new Error("event payload conflict"), { code: "PAYMENT_IDEMPOTENCY_CONFLICT" });
      }
      const transaction = `${command.evidence.provider}:${command.evidence.providerOrderId}`;
      const owner = next.bindings.get(transaction);
      if (owner && owner !== current.orderId) throw Object.assign(new Error("cross-order transaction conflict"), { code: "PAYMENT_ORDER_MISMATCH" });
      const firstPaid = current.status !== "PAID";
      if (firstPaid) {
        assert.equal(current.stateVersion, command.expectedOrder.stateVersion);
        assert.equal(command.nextOrder.status, "PAID");
        assert.equal(command.billingEvent.userId, current.userId);
        next.orders.set(current.orderId, clone(command.nextOrder));
        next.tasks.set(current.orderId, { key: current.orderId, orderId: current.orderId, userId: current.userId,
          grantDefinition: clone(current.productSnapshot.grantDefinition), status: "PENDING", version: 0 });
      } else {
        assert.equal(current.providerOrderId, command.evidence.providerOrderId);
        assert.ok(next.tasks.has(current.orderId), "PAID without original task is not grant authority");
      }
      next.events.set(command.eventKey, clone(command.event));
      next.bindings.set(transaction, current.orderId);
      if (this.failT1) throw new Error("synthetic T1 rollback");
      this.state = next; totals.t1Commits++; if (firstPaid) totals.firstPaid++;
      if (this.loseT1Ack) { this.loseT1Ack = false; throw new Error("synthetic lost T1 acknowledgment"); }
      return { committed: true, firstPaid, orderId: current.orderId, providerOrderId: command.evidence.providerOrderId };
    });
  }
  async applyGrantOnce(command) {
    totals.t2Attempts++;
    return this.atomic(async () => {
      const next = clone(this.state);
      const task = next.tasks.get(command.orderId);
      assert.ok(task && next.orders.get(command.orderId).status === "PAID");
      assert.equal(task.userId, command.userId);
      assert.deepEqual(task.grantDefinition, command.grantDefinition);
      if (task.status === "APPLIED") return clone(task);
      assert.equal(task.version, command.expectedTaskVersion);
      const account = next.accountMappings.get(task.userId);
      if (!account) return { key: command.key, status: "PENDING", reason: "MAPPING_REQUIRED" };
      assert.ok(next.existingBalances.has(account), "no guessed balance account");
      next.existingBalances.set(account, next.existingBalances.get(account) + command.grantDefinition.quantity);
      if (this.failT2) throw new Error("synthetic T2 rollback after tentative increment");
      task.status = "APPLIED"; task.version++;
      this.state = next; totals.t2Commits++; totals.grantsApplied++;
      if (this.loseT2Ack) { this.loseT2Ack = false; throw new Error("synthetic lost T2 acknowledgment"); }
      return clone(task);
    });
  }
  async syntheticConsumeOne() {
    return this.atomic(async () => { const next = clone(this.state);
      next.existingBalances.set("existing-account-A", next.existingBalances.get("existing-account-A") - 1); this.state = next; });
  }
  snapshot() { return json({ orders: [...this.state.orders], events: [...this.state.events], bindings: [...this.state.bindings],
    tasks: [...this.state.tasks], mappings: [...this.state.accountMappings], balances: [...this.state.existingBalances] }); }
  balance() { return this.state.existingBalances.get("existing-account-A"); }
}

function environment({ driver = new SyntheticDriver(), configured = true } = {}) {
  const persistence = createPaymentPersistence({ driver: configured ? driver : null });
  const verifier = createAlipayRsa2Verifier({ trustedPublicKey: publicPem });
  const decoder = createAlipayNotificationDecoder({ resolveOrder: id => persistence.loadOrder(id), clock: () => now });
  const adapter = new AlipayPaymentAdapter({ appId: "synthetic-app", sellerId: "synthetic-seller", currency: "CNY",
    signatureVerifier: input => { totals.verifications++; return verifier(input); },
    notificationDecoder: raw => { totals.decodes++; return decoder(raw); },
    queryTransport: async () => { throw new Error("query forbidden in new notification entry"); }
  });
  const orchestrator = new PaymentConfirmationOrchestrator({ adapter, persistence });
  return { driver, persistence, adapter, decoder, orchestrator,
    notify(raw = body()) { return orchestrator.confirmProviderNotification({ envelope: {
      headers: { "content-type": "application/x-www-form-urlencoded; charset=utf-8" }, rawBody: raw } }); },
    async recover() { totals.recoveryCalls++; return orchestrator.recoverPendingGrant({ orderId: "order-A" }); }
  };
}
async function rejectWithoutMutation(env, raw, code) {
  const before = env.driver.snapshot();
  const t1 = totals.t1Attempts; const t2 = totals.t2Attempts;
  await assert.rejects(() => env.notify(raw), error => error.code === code);
  assert.equal(env.driver.snapshot(), before);
  assert.equal(totals.t1Attempts, t1); assert.equal(totals.t2Attempts, t2);
}
function response() {
  return { statusCode: null, contentType: null, text: null,
    status(value) { this.statusCode = value; return this; }, type(value) { this.contentType = value; return this; },
    send(value) { this.text = value; return this; } };
}
const tests = [];
const test = (id, name, run) => tests.push({ id, name, run });
test("N01", "valid notification without cookie persists PAID and grants", async () => {
  const env = environment(); const res = response();
  await createAlipayNotificationHandler({ orchestrator: env.orchestrator })({ method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" }, rawBody: body() }, res);
  assert.equal(res.statusCode, 200); assert.equal(res.text, "success");
  assert.equal(env.driver.state.orders.get("order-A").status, "PAID"); assert.equal(env.driver.balance(), 15);
});
test("N02", "tampered signature has no storage mutation", async () => {
  await rejectWithoutMutation(environment(), body().replace("total_amount=12.00", "total_amount=13.00"), "PAYMENT_WEBHOOK_INVALID");
});
test("N03", "duplicate form field rejected", async () => {
  await rejectWithoutMutation(environment(), body() + "&app%5Fid=synthetic-app", "PAYMENT_WEBHOOK_INVALID");
});
test("N04", "notification cannot supply site identity", async () => {
  await rejectWithoutMutation(environment(), body({ visitorId: userId }), "PAYMENT_ORDER_OWNERSHIP_MISMATCH");
});
test("N05", "unknown original order rejected", async () => {
  await rejectWithoutMutation(environment(), body({ out_trade_no: "missing" }), "PAYMENT_ORDER_MISMATCH");
});
test("N06", "unmapped non-UUID store owner cannot become trusted identity", async () => {
  const env = environment(); env.driver.state.orders.get("order-A").userId = "visitor-A";
  await rejectWithoutMutation(env, body(), "PAYMENT_IDENTITY_PROVENANCE_INVALID");
});
test("N07", "missing balance mapping retains PAID pending without changing balances", async () => {
  const env = environment({ driver: new SyntheticDriver({ mapping: false }) }); const result = await env.notify();
  assert.equal(result.paid, true); assert.equal(result.entitlementStatus, "PENDING");
  assert.equal(result.entitlementReceipt.reason, "MAPPING_REQUIRED"); assert.equal(env.driver.balance(), 5);
});
test("N08", "amount mismatch rejected", async () => {
  await rejectWithoutMutation(environment(), body({ total_amount: "12.01" }), "PAYMENT_INVALID_AMOUNT");
});
test("N09", "application mismatch rejected", async () => {
  await rejectWithoutMutation(environment(), body({ app_id: "other-app" }), "PAYMENT_ORDER_MISMATCH");
});
test("N10", "actual seller mismatch rejected", async () => {
  await rejectWithoutMutation(environment(), body({ seller_id: "other-seller" }), "PAYMENT_ORDER_MISMATCH");
});
test("N11", "non-CNY rejected", async () => {
  await rejectWithoutMutation(environment(), body({ currency: "USD" }), "PAYMENT_CURRENCY_MISMATCH");
});
test("N12", "cross-order transaction conflict rolls back T1", async () => {
  const env = environment(); env.driver.state.bindings.set("alipay:trade-A", "other-order");
  const before = env.driver.snapshot(); const attempts = totals.t1Attempts;
  await assert.rejects(() => env.notify(), error => error.code === "PAYMENT_ORDER_MISMATCH");
  assert.equal(totals.t1Attempts, attempts + 1); assert.equal(env.driver.snapshot(), before);
});
test("N13", "same event replay has one task and one increment", async () => {
  const env = environment(); const raw = body(); await env.notify(raw); const again = await env.notify(raw);
  assert.equal(again.firstPaid, false); assert.equal(env.driver.balance(), 15);
  assert.equal(env.driver.state.tasks.size, 1); assert.equal(env.driver.state.events.size, 1);
});
test("N14", "same event with changed signed payload is a conflict", async () => {
  const env = environment(); await env.notify(); const before = env.driver.snapshot();
  await assert.rejects(() => env.notify(body({ subject: "changed" })), error => error.code === "PAYMENT_IDEMPOTENCY_CONFLICT");
  assert.equal(env.driver.snapshot(), before);
});
test("N15", "different events for same trade do not grant twice", async () => {
  const env = environment(); await env.notify(); const next = await env.notify(body({ notify_id: "event-B" }));
  assert.equal(next.firstPaid, false); assert.equal(env.driver.state.events.size, 2); assert.equal(env.driver.balance(), 15);
});
test("N16", "concurrent confirmations have one first PAID and one increment", async () => {
  const env = environment(); const raw = body(); const results = await Promise.all([env.notify(raw), env.notify(raw)]);
  assert.equal(results.filter(item => item.firstPaid).length, 1); assert.equal(env.driver.balance(), 15);
});
test("N17", "concurrent two-order claim admits only one owner", async () => {
  const env = environment(); env.driver.state.orders.set("order-B", originalOrder("order-B"));
  const results = await Promise.allSettled([env.notify(), env.notify(body({ out_trade_no: "order-B", notify_id: "event-B" }))]);
  assert.equal(results.filter(item => item.status === "fulfilled").length, 1);
  assert.equal(env.driver.state.bindings.size, 1); assert.equal(env.driver.balance(), 15);
});
test("N18", "T1 failure leaves event transaction order and task unchanged", async () => {
  const env = environment(); env.driver.failT1 = true; const before = env.driver.snapshot();
  await assert.rejects(() => env.notify(), /synthetic T1 rollback/); assert.equal(env.driver.snapshot(), before);
});
test("N19", "lost T1 acknowledgment keeps durable-model PAID pending recoverable", async () => {
  const env = environment(); env.driver.loseT1Ack = true;
  await assert.rejects(() => env.notify(), /synthetic lost T1 acknowledgment/);
  assert.equal(env.driver.state.orders.get("order-A").status, "PAID");
  assert.equal(env.driver.state.tasks.get("order-A").status, "PENDING"); assert.equal(env.driver.balance(), 5);
  assert.equal((await env.recover()).entitlementStatus, "APPLIED"); assert.equal(env.driver.balance(), 15);
});
test("N20", "T2 rollback preserves PAID and original balance", async () => {
  const env = environment(); env.driver.failT2 = true; const result = await env.notify();
  assert.equal(result.order.status, "PAID"); assert.equal(result.entitlementStatus, "PENDING");
  assert.equal(env.driver.balance(), 5); assert.equal(env.driver.state.tasks.get("order-A").status, "PENDING");
});
test("N21", "lost T2 acknowledgment recovers applied receipt without second grant", async () => {
  const env = environment(); env.driver.loseT2Ack = true; const result = await env.notify();
  assert.equal(result.entitlementStatus, "PENDING"); assert.equal(env.driver.balance(), 15);
  assert.equal((await env.recover()).entitlementStatus, "APPLIED"); assert.equal(env.driver.balance(), 15);
});
test("N22", "recovery after grant failure and repeated recovery increment once", async () => {
  const env = environment(); env.driver.failT2 = true; await env.notify(); env.driver.failT2 = false;
  assert.equal((await env.recover()).entitlementStatus, "APPLIED");
  assert.equal((await env.recover()).entitlementStatus, "APPLIED"); assert.equal(env.driver.balance(), 15);
});
test("N23", "new reader reconstitutes stored pending grant without old WeakSets", async () => {
  const first = environment(); first.driver.failT2 = true; await first.notify();
  const resumed = environment({ driver: new SyntheticDriver({ state: first.driver.state }) });
  const loaded = await resumed.persistence.loadOrder("order-A"); assertPaymentOrderContract(loaded);
  assert.equal((await resumed.recover()).entitlementStatus, "APPLIED"); assert.equal(resumed.driver.balance(), 15);
});
test("N24", "unconfigured persistence fails closed and handler never acknowledges", async () => {
  const env = environment({ configured: false }); const before = env.driver.snapshot(); const res = response();
  await createAlipayNotificationHandler({ orchestrator: env.orchestrator })({ method: "POST", headers: {}, rawBody: body() }, res);
  assert.equal(res.statusCode, 503); assert.equal(res.text, "failure"); assert.equal(env.driver.snapshot(), before);
});
test("N25", "method raw-body and content-type boundaries reject without writes", async () => {
  const env = environment(); const handler = createAlipayNotificationHandler({ orchestrator: env.orchestrator });
  for (const [request, status] of [[{ method: "GET" }, 405], [{ method: "POST", body: {} }, 400],
    [{ method: "POST", headers: { "content-type": "application/json" }, rawBody: body() }, 503]]) {
    const res = response(); await handler(request, res); assert.equal(res.statusCode, status); assert.equal(res.text, "failure");
  }
  assert.equal(env.driver.state.events.size, 0); assert.equal(env.driver.balance(), 5);
});
test("N26", "legacy notification and query entry compatibility retained", async () => {
  const env = environment(); let order = await env.persistence.loadOrder("order-A"); let grantCalls = 0; let receipt = null;
  const normalized = await env.decoder(body());
  const verifier = createAlipayRsa2Verifier({ trustedPublicKey: publicPem });
  const adapter = new AlipayPaymentAdapter({ appId: "synthetic-app", sellerId: "synthetic-seller", signatureVerifier: verifier,
    notificationDecoder: async () => normalized, queryTransport: async () => normalized });
  const orchestrator = new PaymentConfirmationOrchestrator({ adapter,
    orderStore: { get: async () => order, save: async value => { order = value; }, withOrderLock: async (id, run) => run() },
    transactionStore: { claim: async () => ({ claimed: false, orderId: "order-A" }) },
    entitlementStore: { get: async () => receipt, applyOnce: async () => { grantCalls++; return (receipt = { status: "APPLIED" }); } }
  });
  const identity = createServerVerifiedPaymentIdentity({ userId, verification: "authenticated_session" });
  const first = await orchestrator.confirmNotification({ identity, envelope: {
    headers: { "content-type": "application/x-www-form-urlencoded" }, rawBody: body() } });
  const second = await orchestrator.confirmQuery({ identity, orderId: "order-A" });
  assert.equal(first.firstPaid, true); assert.equal(second.firstPaid, false); assert.equal(grantCalls, 1);
});
test("N27", "legacy provenance rules and constrained reader remain strict", async () => {
  assert.throws(() => assertPaymentOrderContract({ ...originalOrder(), verified: true }), error => error.code === "PAYMENT_INVALID_REQUEST");
  assert.equal(transitionPaymentState("PAID", "PENDING").allowed, false);
  const evidence = createVerifiedProviderEvidence({ provider: "alipay", providerOrderId: "trade-A", providerEventId: "event-A",
    orderId: "order-A", amountMinor: 1200, currency: "CNY", providerStatus: "TRADE_SUCCESS", paymentStateEvidence: "PAID",
    paidAt: now, occurredAt: now, verifiedAt: now, verificationSource: "verified_webhook" });
  assert.throws(() => assertVerifiedProviderEvidence({ ...evidence }), error => error.code === "PAYMENT_PROVIDER_EVIDENCE_UNVERIFIED");
  const reader = createTrustedStoredPaymentOrderReader({ readOrder: async () => ({ ...originalOrder(), amountMinor: 1 }) });
  await assert.rejects(() => reader.load("order-A"), error => error.code === "PAYMENT_INVALID_AMOUNT");
});
test("N28", "concurrent recovery and modeled consumption share existing account", async () => {
  const env = environment(); env.driver.failT2 = true; await env.notify(); env.driver.failT2 = false;
  await Promise.all([env.recover(), env.recover(), env.driver.syntheticConsumeOne()]);
  assert.equal(env.driver.balance(), 14); assert.equal(env.driver.state.tasks.size, 1);
  assert.equal(env.driver.state.tasks.get("order-A").status, "APPLIED");
});

assert.equal(tests.length, 28);
const statuses = [];
let failed = false;
try {
  for (const entry of tests) {
    if (failed) { statuses.push({ id: entry.id, status: "NOT_RUN" }); continue; }
    const scenarioStarted = performance.now();
    try {
      await entry.run();
      assert.ok(performance.now() - scenarioStarted <= 5000, "scenario exceeded 5 seconds");
      assert.ok(performance.now() - started <= 120000, "verification exceeded 120 seconds");
      statuses.push({ id: entry.id, status: "PASS" }); console.log(`PASS ${entry.id} ${entry.name}`);
    } catch (error) {
      failed = true; process.exitCode = 1; statuses.push({ id: entry.id, status: "FAIL" }); console.error(error.stack || String(error));
    }
  }
} finally {
  keys = null;
  console.log(JSON.stringify({ batch: "PAY-ALIPAY-03-A", statuses, passed: statuses.filter(item => item.status === "PASS").length,
    total: tests.length, counts: totals, elapsedMs: performance.now() - started,
    testKeyReferencesReleased: true, privateKeysSavedOrPrinted: false, childProcesses: 0, listeners: 0,
    scope: "synthetic transaction-interface behavior only; no durability or database atomicity proof" }));
}
