import assert from "node:assert/strict";
import { constants, generateKeyPairSync, sign } from "node:crypto";
import { performance } from "node:perf_hooks";
import { AlipayPaymentAdapter } from "../server/payments/alipay-adapter.js";
import { PaymentConfirmationOrchestrator } from "../server/payments/payment-orchestrator.js";
import { createAlipayRsa2Verifier } from "../server/payments/alipay-rsa2-verifier.js";
import { createAlipayNotificationDecoder } from "../server/payments/alipay-notification-decoder.js";
import {
  createPaymentOrderContract, createServerVerifiedPaymentIdentity, createVerifiedProviderEvidence
} from "../server/payments/schemas.js";
import { createBillingProductContract, createProductSnapshot } from "../server/payments/product-mapping.js";

const started = performance.now();
const now = "2026-11-03T03:09:44.000Z";
let keys = generateKeyPairSync("rsa", { modulusLength: 2048 });
let otherKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const publicPem = keys.publicKey.export({ type: "spki", format: "pem" });
const otherPublicPem = otherKeys.publicKey.export({ type: "spki", format: "pem" });
const totals = { signatureVerifications: 0, decodes: 0, queries: 0, saves: 0, claims: 0, grants: 0, rejectedScenarios: 0 };
const tests = [];
const test = (id, name, run) => tests.push({ id, name, run });

function fields(overrides = {}) {
  return {
    charset: "utf-8", sign_type: "RSA2", app_id: "synthetic-app", seller_id: "synthetic-seller",
    out_trade_no: "order-A", trade_no: "trade-A", notify_id: "notify-A", total_amount: "12.00",
    trade_status: "TRADE_SUCCESS", gmt_payment: "2026-11-03 11:00:00",
    buyer_id: "buyer-is-not-site-user", subject: "离线测试 + 中文", ...overrides
  };
}

function signedBody(input = fields()) {
  const source = Object.entries(input).filter(([key, value]) => key !== "sign" && key !== "sign_type" && value !== "")
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    .map(([key, value]) => `${key}=${value}`).join("&");
  const signature = sign("RSA-SHA256", Buffer.from(source, "utf8"), {
    key: keys.privateKey, padding: constants.RSA_PKCS1_PADDING
  }).toString("base64");
  return new URLSearchParams({ ...input, sign: signature }).toString();
}

function environment({ wrongKey = false, boundTrade = null } = {}) {
  const identity = createServerVerifiedPaymentIdentity({ userId: "site-user-A", verification: "authenticated_session" });
  const product = createBillingProductContract({ productId: "synthetic-quota", productClass: "quota_package",
    purchaseType: "quota_package", priceMinor: 1200, currency: "CNY",
    grantDefinition: { type: "quota_increment", quantity: 10 }, version: "rsa2-offline-v1", status: "active" });
  const original = createPaymentOrderContract({ identity, authority: {
    orderId: "order-A", provider: "alipay", productId: product.productId, purchaseType: product.purchaseType,
    amountMinor: 1200, currency: "CNY", idempotencyKey: "synthetic-create-A",
    productSnapshot: createProductSnapshot(product), createdAt: now
  }, providerEvidence: boundTrade ? createVerifiedProviderEvidence({ provider: "alipay", providerOrderId: boundTrade,
    providerEventId: "prior-event", orderId: "order-A", amountMinor: 1200, currency: "CNY",
    providerStatus: "WAIT_BUYER_PAY", paymentStateEvidence: "PENDING", paidAt: null,
    occurredAt: now, verifiedAt: now, verificationSource: "verified_query" }) : {} });
  const orders = new Map([[original.orderId, original]]);
  const bindings = new Map();
  const receipts = new Map();
  const counts = { saves: 0, claims: 0, grants: 0 };
  let lockTail = Promise.resolve();
  const orderStore = {
    async get(id) { return orders.get(id) || null; },
    async save(order) { counts.saves++; totals.saves++; orders.set(order.orderId, order); },
    async withOrderLock(id, run) {
      const previous = lockTail;
      let release;
      lockTail = new Promise(resolve => { release = resolve; });
      await previous;
      try { return await run(); } finally { release(); }
    }
  };
  const transactionStore = { async claim({ providerTransactionId, orderId }) {
    counts.claims++; totals.claims++;
    const existing = bindings.get(providerTransactionId);
    if (existing) return { claimed: false, orderId: existing };
    bindings.set(providerTransactionId, orderId);
    return { claimed: true, orderId };
  } };
  const entitlementStore = {
    async get(id) { return receipts.get(id) || null; },
    async applyOnce({ key }) {
      counts.grants++; totals.grants++;
      if (!receipts.has(key)) receipts.set(key, Object.freeze({ key, status: "APPLIED", count: 1 }));
      return receipts.get(key);
    }
  };
  const verifier = createAlipayRsa2Verifier({ trustedPublicKey: wrongKey ? otherPublicPem : publicPem });
  const decoder = createAlipayNotificationDecoder({ resolveOrder: id => orderStore.get(id), clock: () => now });
  const adapter = new AlipayPaymentAdapter({ appId: "synthetic-app", sellerId: "synthetic-seller", currency: "CNY",
    signatureVerifier: envelope => { totals.signatureVerifications++; return verifier(envelope); },
    notificationDecoder: body => { totals.decodes++; return decoder(body); },
    queryTransport: async () => { totals.queries++; throw new Error("query forbidden in notification-only batch"); }
  });
  const orchestrator = new PaymentConfirmationOrchestrator({ adapter, orderStore, transactionStore, entitlementStore });
  return { identity, original, orders, bindings, receipts, counts,
    confirm(body, context = identity) { return orchestrator.confirmNotification({ identity: context,
      envelope: { headers: { "content-type": "application/x-www-form-urlencoded; charset=utf-8" }, rawBody: body } }); }
  };
}

async function rejected(env, body, code, identity = env.identity) {
  const before = JSON.stringify([...env.orders]);
  await assert.rejects(() => env.confirm(body, identity), error => error.code === code);
  assert.equal(JSON.stringify([...env.orders]), before);
  assert.deepEqual(env.counts, { saves: 0, claims: 0, grants: 0 });
  assert.equal(env.bindings.size, 0);
  assert.equal(env.receipts.size, 0);
  totals.rejectedScenarios++;
}

test("R01", "actual RSA2 and decoding confirm original server order", async () => {
  const env = environment();
  const result = await env.confirm(signedBody());
  assert.equal(result.firstPaid, true);
  assert.equal(result.order.userId, "site-user-A");
  assert.equal(result.order.status, "PAID");
  assert.equal(result.entitlementStatus, "APPLIED");
  assert.deepEqual(env.counts, { saves: 2, claims: 1, grants: 1 });
});
test("R02", "signed duplicate notification grants once", async () => {
  const env = environment();
  const body = signedBody();
  await env.confirm(body);
  const again = await env.confirm(body);
  assert.equal(again.firstPaid, false);
  assert.equal(again.order.status, "PAID");
  assert.equal(env.counts.saves, 2);
  assert.equal(env.counts.claims, 2);
  assert.equal(env.counts.grants, 1);
  assert.equal(env.receipts.size, 1);
});
test("R03", "tampered signed amount rejected with zero mutations", async () => {
  await rejected(environment(), signedBody().replace("total_amount=12.00", "total_amount=13.00"), "PAYMENT_WEBHOOK_INVALID");
});
test("R04", "wrong trusted public key rejected", async () => {
  await rejected(environment({ wrongKey: true }), signedBody(), "PAYMENT_WEBHOOK_INVALID");
});
test("R05", "RSA signature type rejected", async () => {
  await rejected(environment(), signedBody(fields({ sign_type: "RSA" })), "PAYMENT_WEBHOOK_INVALID");
});
test("R06", "duplicate percent-encoded field rejected", async () => {
  await rejected(environment(), signedBody() + "&app%5Fid=synthetic-app", "PAYMENT_WEBHOOK_INVALID");
});
test("R07", "invalid percent encoding rejected", async () => {
  await rejected(environment(), signedBody() + "&extra=%ZZ", "PAYMENT_WEBHOOK_INVALID");
});
test("R08", "invalid UTF-8 encoding rejected", async () => {
  await rejected(environment(), signedBody() + "&extra=%C3%28", "PAYMENT_WEBHOOK_INVALID");
});
test("R09", "invalid base64 signature rejected", async () => {
  const body = new URLSearchParams(signedBody()); body.set("sign", "***");
  await rejected(environment(), body.toString(), "PAYMENT_WEBHOOK_INVALID");
});
test("R10", "valid signature with wrong original-order amount rejected", async () => {
  await rejected(environment(), signedBody(fields({ total_amount: "12.01" })), "PAYMENT_INVALID_AMOUNT");
});
test("R11", "valid signature with wrong application rejected", async () => {
  await rejected(environment(), signedBody(fields({ app_id: "other-app" })), "PAYMENT_PROVIDER_MISMATCH");
});
test("R12", "valid signature with wrong actual seller rejected", async () => {
  await rejected(environment(), signedBody(fields({ seller_id: "other-seller" })), "PAYMENT_PROVIDER_MISMATCH");
});
test("R13", "missing original merchant order rejected", async () => {
  await rejected(environment(), signedBody(fields({ out_trade_no: "unknown-order" })), "PAYMENT_ORDER_MISMATCH");
});
test("R14", "cross-order transaction claim rejects and preserves original binding", async () => {
  const env = environment();
  assert.equal(env.original.providerOrderId, null);
  const otherOrder = createPaymentOrderContract({ identity: env.identity, authority: {
    ...env.original, orderId: "other-order", idempotencyKey: "synthetic-other-order"
  } });
  env.orders.set(otherOrder.orderId, otherOrder);
  env.bindings.set("trade-A", "other-order");
  const beforeOrders = JSON.stringify([...env.orders]);
  const beforeBindings = JSON.stringify([...env.bindings]);
  const beforeReceipts = JSON.stringify([...env.receipts]);

  await assert.rejects(() => env.confirm(signedBody()), error =>
    error.code === "PAYMENT_ORDER_MISMATCH" && error.message === "provider transaction belongs to another order");
  assert.deepEqual(env.counts, { saves: 0, claims: 1, grants: 0 });
  assert.equal(env.bindings.get("trade-A"), "other-order");
  assert.equal(JSON.stringify([...env.orders]), beforeOrders);
  assert.equal(JSON.stringify([...env.bindings]), beforeBindings);
  assert.equal(JSON.stringify([...env.receipts]), beforeReceipts);
  assert.equal(env.orders.get("order-A").status, "CREATED");
  assert.equal(env.orders.get("other-order").status, "CREATED");
  assert.equal(env.receipts.size, 0);
  totals.rejectedScenarios++;
});
test("R15", "signed site-user impersonation field rejected", async () => {
  await rejected(environment(), signedBody(fields({ user_id: "another-site-user" })), "PAYMENT_ORDER_OWNERSHIP_MISMATCH");
});
test("R16", "forged authentication context rejected before confirmation", async () => {
  const env = environment();
  await rejected(env, signedBody(), "PAYMENT_IDENTITY_PROVENANCE_INVALID", { ...env.identity });
});
test("R17", "signed non-CNY currency rejected", async () => {
  await rejected(environment(), signedBody(fields({ currency: "USD" })), "PAYMENT_CURRENCY_MISMATCH");
});
test("R18", "signed fractional minor unit rejected", async () => {
  await rejected(environment(), signedBody(fields({ total_amount: "12.001" })), "PAYMENT_INVALID_AMOUNT");
});
test("R19", "unsupported charset rejected", async () => {
  await rejected(environment(), signedBody(fields({ charset: "gbk" })), "PAYMENT_WEBHOOK_INVALID");
});
test("R20", "missing signed notification identifier rejected", async () => {
  const input = fields(); delete input.notify_id;
  await rejected(environment(), signedBody(input), "PAYMENT_WEBHOOK_INVALID");
});

assert.equal(tests.length, 20);
let passed = 0;
let failed = false;
const statuses = [];
try {
  for (const entry of tests) {
    if (failed) { statuses.push({ id: entry.id, status: "NOT_RUN" }); continue; }
    const scenarioStarted = performance.now();
    try {
      await entry.run();
      assert.ok(performance.now() - scenarioStarted <= 5000, "scenario exceeded 5-second limit");
      assert.ok(performance.now() - started <= 120000, "verification exceeded 120-second limit");
      statuses.push({ id: entry.id, status: "PASS" }); passed++;
      console.log(`PASS ${entry.id} ${entry.name}`);
    } catch (error) {
      failed = true; process.exitCode = 1;
      statuses.push({ id: entry.id, status: "FAIL" });
      console.error(error.stack || String(error));
    }
  }
} finally {
  keys = null; otherKeys = null;
  console.log(JSON.stringify({ batch: "PAY-ALIPAY-02-RSA2-OFFLINE", passed, total: tests.length,
    statuses, counts: totals, elapsedMs: performance.now() - started,
    privateKeyReferencesReleased: true, privateKeysWrittenOrPrinted: false,
    childProcesses: 0, listeners: 0, realProviderCalls: 0, databaseCalls: 0 }));
}
