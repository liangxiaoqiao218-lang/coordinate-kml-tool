import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  INDONESIA_STRUCTURED_B_RC,
  IndonesiaStructuredBRcAdmissionError,
  createIndonesiaStructuredBRcAdmission,
  readIndonesiaStructuredBRcConfig
} from "../server/recognition/indonesia-structured-b-rc-admission.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const migrationSource = await readFile(
  path.join(root, "supabase", "migrations", "20261010141047_indonesia_structured_b_rc_admission.sql"),
  "utf8"
);
const rollbackSource = await readFile(
  path.join(root, "supabase", "rollbacks", "20261010141047_indonesia_structured_b_rc_admission.rollback.sql"),
  "utf8"
);
const sourceImagePath = process.env.INDONESIA_STRUCTURED_B_RC_TEST_IMAGE
  || "D:\\矿业空间实验室\\测试坐标图\\印尼矿地03.jpg";
const sourceImage = await readFile(sourceImagePath);
const requestIds = [
  "11111111-1111-4111-8111-111111111111",
  "22222222-2222-4222-8222-222222222222",
  "33333333-3333-4333-8333-333333333333"
];

const exactEnv = Object.freeze({
  INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "true",
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ENABLED: "true",
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ID: INDONESIA_STRUCTURED_B_RC.runId,
  INDONESIA_STRUCTURED_B_RC_ALLOWED_IMAGE_SHA256: INDONESIA_STRUCTURED_B_RC.imageSha256,
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_WINDOW_SECONDS: "1800",
  INDONESIA_STRUCTURED_B_RC_CLAIM_TTL_SECONDS: "600",
  SUPABASE_URL: `https://${INDONESIA_STRUCTURED_B_RC.rcProjectRef}.supabase.co`,
  RC_SUPABASE_PROJECT_REF: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
  RENDER_SERVICE_NAME: INDONESIA_STRUCTURED_B_RC.rcServiceName,
  ALIYUN_VISION_MODEL: INDONESIA_STRUCTURED_B_RC.model,
  ALIYUN_BASE_URL: `https://${INDONESIA_STRUCTURED_B_RC.providerHostname}/compatible-mode/v1`
});

class FakeAtomicRpcStore {
  constructor({ nowMs = 1_000_000, budgetOpen = true } = {}) {
    this.nowMs = nowMs;
    this.budgetOpen = budgetOpen;
    this.activationAt = null;
    this.deadlineAt = null;
    this.claim = null;
    this.dispatched = 2;
    this.reserved = 1.632;
    this.active = 0;
    this.queue = Promise.resolve();
  }

  rpc(name, params) {
    const operation = this.queue.then(() => this.#execute(name, params));
    this.queue = operation.catch(() => {});
    return operation;
  }

  #execute(name, params) {
    if (params.p_project_ref !== INDONESIA_STRUCTURED_B_RC.rcProjectRef) {
      return { data: { accepted: false, code: "RC_IDENTITY_MISMATCH" }, error: null };
    }
    if (name === "claim_indonesia_structured_b_single_run") {
      if (this.activationAt === null) {
        this.activationAt = this.nowMs;
        this.deadlineAt = this.nowMs + 1_800_000;
      }
      if (this.nowMs > this.deadlineAt) return { data: { accepted: false, code: "RC_RUN_WINDOW_CLOSED" }, error: null };
      if (params.p_activation_only) return { data: { accepted: true, action: "ACTIVATED" }, error: null };
      if (this.claim || !this.budgetOpen) {
        return { data: { accepted: false, code: "RC_CLAIM_ALREADY_CONSUMED_OR_BUDGET_CLOSED" }, error: null };
      }
      this.claim = {
        requestId: params.p_recognition_request_id,
        tokenHash: params.p_token_sha256,
        imageSha256: params.p_image_sha256,
        status: "RESERVED",
        expiresAt: this.nowMs + 600_000
      };
      return { data: { accepted: true, action: "CLAIMED", expires_at: new Date(this.claim.expiresAt).toISOString() }, error: null };
    }
    if (name === "dispatch_indonesia_structured_b_single_run") {
      const bound = this.claim
        && this.claim.status === "RESERVED"
        && this.claim.requestId === params.p_recognition_request_id
        && this.claim.tokenHash === params.p_token_sha256
        && this.claim.imageSha256 === params.p_image_sha256
        && this.nowMs <= this.claim.expiresAt
        && this.nowMs <= this.deadlineAt;
      if (!bound || this.active !== 0 || !this.budgetOpen) {
        return { data: { accepted: false, code: "RC_DISPATCH_CLOSED" }, error: null };
      }
      this.claim.status = "DISPATCHED";
      this.dispatched += 1;
      this.reserved += 0.816;
      this.active = 1;
      return { data: { accepted: true, action: "DISPATCHED" }, error: null };
    }
    if (name === "settle_indonesia_structured_b_single_run") {
      const bound = this.claim
        && this.claim.requestId === params.p_recognition_request_id
        && this.claim.tokenHash === params.p_token_sha256;
      if (!bound) return { data: { accepted: false, code: "RC_SETTLEMENT_IDENTITY_MISMATCH" }, error: null };
      if (params.p_outcome === "FAILED_PRE_PROVIDER" && this.claim.status === "RESERVED") {
        this.claim.status = "FAILED_PRE_PROVIDER";
        return { data: { accepted: true, action: "SETTLED" }, error: null };
      }
      if (this.claim.status !== "DISPATCHED") {
        return { data: { accepted: false, code: "RC_SETTLEMENT_ALREADY_FINAL" }, error: null };
      }
      this.claim.status = params.p_outcome;
      this.active = 0;
      return { data: { accepted: true, action: "SETTLED" }, error: null };
    }
    if (name === "close_indonesia_structured_b_single_run") {
      return { data: { accepted: true, action: "CLOSED" }, error: null };
    }
    return { data: null, error: { code: "UNKNOWN_RPC" } };
  }
}

function expectCode(error, code) {
  return error instanceof IndonesiaStructuredBRcAdmissionError && error.code === code;
}

const cases = [];
async function test(name, action) {
  await action();
  cases.push(name);
}

await test("strict RC environment, database, model and endpoint identity is required", async () => {
  assert.equal(readIndonesiaStructuredBRcConfig(exactEnv).ready, true);
  assert.equal(readIndonesiaStructuredBRcConfig({
    ...exactEnv,
    INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "false",
    INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ENABLED: "false"
  }).rcServiceScoped, true);
  for (const serviceName of ["", "coordinate-kml-tool"]) {
    const guardedMisconfiguration = readIndonesiaStructuredBRcConfig({
      ...exactEnv,
      RENDER_SERVICE_NAME: serviceName
    });
    assert.equal(guardedMisconfiguration.guardRequired, true);
    assert.equal(guardedMisconfiguration.ready, false);
  }
  assert.equal(readIndonesiaStructuredBRcConfig({
    ...exactEnv,
    ALIYUN_BASE_URL: `http://${INDONESIA_STRUCTURED_B_RC.providerHostname}/compatible-mode/v1`
  }).ready, false);
  assert.equal(readIndonesiaStructuredBRcConfig({
    ...exactEnv,
    ALIYUN_BASE_URL: `https://user@${INDONESIA_STRUCTURED_B_RC.providerHostname}/compatible-mode/v1`
  }).ready, false);
  for (const changed of [
    { RENDER_SERVICE_NAME: "coordinate-kml-tool" },
    { SUPABASE_URL: "https://xyiffmpzdtmurmnsibdt.supabase.co" },
    { RC_SUPABASE_PROJECT_REF: "xyiffmpzdtmurmnsibdt" },
    { ALIYUN_BASE_URL: "https://example.invalid/v1" },
    { ALIYUN_VISION_MODEL: "another-model" },
    { INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_WINDOW_SECONDS: "1801" },
    { INDONESIA_STRUCTURED_B_RC_CLAIM_TTL_SECONDS: "601" }
  ]) {
    assert.equal(readIndonesiaStructuredBRcConfig({ ...exactEnv, ...changed }).ready, false);
  }
});

await test("header-only operator auth creates one opaque claim and persists only its hash", async () => {
  const store = new FakeAtomicRpcStore();
  const admission = createIndonesiaStructuredBRcAdmission({
    supabase: store,
    adminPassword: "local-test-admin",
    env: exactEnv,
    randomToken: () => "A".repeat(43)
  });
  await admission.activateOnStartup();
  await assert.rejects(() => admission.claim({
    adminHeader: "wrong",
    runId: INDONESIA_STRUCTURED_B_RC.runId,
    batchId: INDONESIA_STRUCTURED_B_RC.batchId,
    imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256,
    recognitionRequestId: requestIds[0]
  }), error => expectCode(error, "INDONESIA_STRUCTURED_B_RC_ADMIN_REQUIRED"));
  const claim = await admission.claim({
    adminHeader: "local-test-admin",
    runId: INDONESIA_STRUCTURED_B_RC.runId,
    batchId: INDONESIA_STRUCTURED_B_RC.batchId,
    imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256,
    recognitionRequestId: requestIds[0]
  });
  assert.equal(claim.claimToken, "A".repeat(43));
  assert.notEqual(store.claim.tokenHash, claim.claimToken);
  assert.match(store.claim.tokenHash, /^[0-9a-f]{64}$/u);
  assert.equal(JSON.stringify(store).includes(claim.claimToken), false);
});

await test("two concurrent claims have exactly one winner and restart/new UUID cannot reclaim", async () => {
  const store = new FakeAtomicRpcStore();
  const admissionA = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "B".repeat(43)
  });
  const admissionB = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "C".repeat(43)
  });
  await Promise.all([admissionA.activateOnStartup(), admissionB.activateOnStartup()]);
  const attempts = await Promise.allSettled([
    admissionA.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] }),
    admissionB.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[1] })
  ]);
  assert.equal(attempts.filter(result => result.status === "fulfilled").length, 1);
  assert.equal(attempts.filter(result => result.status === "rejected").length, 1);
  const restarted = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "D".repeat(43)
  });
  await restarted.activateOnStartup();
  await assert.rejects(() => restarted.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[2] }));
});

await test("a valid claim is consumed when exact-original-image preflight fails", async () => {
  const store = new FakeAtomicRpcStore();
  const admission = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "E".repeat(43)
  });
  await admission.activateOnStartup();
  const claim = await admission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] });
  await assert.rejects(() => admission.preflightJobOrTerminate({ mode: INDONESIA_STRUCTURED_B_RC.mode, recognitionRequestId: requestIds[0], imageBuffer: Buffer.from("wrong"), authorization: `Bearer ${claim.claimToken}` }));
  assert.equal(store.claim.status, "FAILED_PRE_PROVIDER");
  await assert.rejects(() => admission.dispatch({ mode: INDONESIA_STRUCTURED_B_RC.mode, recognitionRequestId: requestIds[0], imageBuffer: sourceImage, authorization: `Bearer ${claim.claimToken}`, asyncAuthorized: true }));
  assert.equal(store.dispatched, 2);
});

await test("exact original image, request and bearer binding dispatch once and unknown outcome blocks reuse", async () => {
  const store = new FakeAtomicRpcStore();
  const admission = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "J".repeat(43)
  });
  await admission.activateOnStartup();
  const claim = await admission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] });
  const dispatch = await admission.dispatch({
    mode: INDONESIA_STRUCTURED_B_RC.mode,
    recognitionRequestId: requestIds[0],
    imageBuffer: sourceImage,
    authorization: `Bearer ${claim.claimToken}`,
    asyncAuthorized: true
  });
  assert.equal(dispatch.dispatched, true);
  assert.equal(store.dispatched, 3);
  assert.equal(store.reserved, 2.448);
  await admission.settle({ recognitionRequestId: requestIds[0], authorization: `Bearer ${claim.claimToken}`, outcome: "OUTCOME_UNKNOWN", providerCallCount: 1 });
  await assert.rejects(() => admission.dispatch({ mode: INDONESIA_STRUCTURED_B_RC.mode, recognitionRequestId: requestIds[0], imageBuffer: sourceImage, authorization: `Bearer ${claim.claimToken}`, asyncAuthorized: true }));
  assert.equal(store.dispatched, 3);
});

await test("pre-provider failure consumes the only claim but does not reserve a model attempt", async () => {
  const store = new FakeAtomicRpcStore();
  const admission = createIndonesiaStructuredBRcAdmission({
    supabase: store, adminPassword: "admin", env: exactEnv, randomToken: () => "F".repeat(43)
  });
  await admission.activateOnStartup();
  const claim = await admission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] });
  await admission.settle({ recognitionRequestId: requestIds[0], authorization: `Bearer ${claim.claimToken}`, outcome: "FAILED_PRE_PROVIDER", providerCallCount: 0 });
  assert.equal(store.dispatched, 2);
  assert.equal(store.reserved, 1.632);
  await assert.rejects(() => admission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[1] }));
});

await test("window, TTL and conservative batch budget fail closed", async () => {
  const expiredWindowStore = new FakeAtomicRpcStore();
  const windowAdmission = createIndonesiaStructuredBRcAdmission({ supabase: expiredWindowStore, adminPassword: "admin", env: exactEnv, randomToken: () => "G".repeat(43) });
  await windowAdmission.activateOnStartup();
  expiredWindowStore.nowMs += 1_800_001;
  await assert.rejects(() => windowAdmission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] }));

  const ttlStore = new FakeAtomicRpcStore();
  const ttlAdmission = createIndonesiaStructuredBRcAdmission({ supabase: ttlStore, adminPassword: "admin", env: exactEnv, randomToken: () => "H".repeat(43) });
  await ttlAdmission.activateOnStartup();
  const claim = await ttlAdmission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] });
  ttlStore.nowMs += 600_001;
  await assert.rejects(() => ttlAdmission.dispatch({ mode: INDONESIA_STRUCTURED_B_RC.mode, recognitionRequestId: requestIds[0], imageBuffer: sourceImage, authorization: `Bearer ${claim.claimToken}`, asyncAuthorized: true }));

  const closedBudgetStore = new FakeAtomicRpcStore({ budgetOpen: false });
  const budgetAdmission = createIndonesiaStructuredBRcAdmission({ supabase: closedBudgetStore, adminPassword: "admin", env: exactEnv, randomToken: () => "I".repeat(43) });
  await budgetAdmission.activateOnStartup();
  await assert.rejects(() => budgetAdmission.claim({ adminHeader: "admin", runId: INDONESIA_STRUCTURED_B_RC.runId, batchId: INDONESIA_STRUCTURED_B_RC.batchId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256, recognitionRequestId: requestIds[0] }));
});

await test("server integration is async-only, single-original-image and UI-hidden in single-run mode", async () => {
  assert.match(serverSource, /req\.indonesiaStructuredRcAsyncAuthorized = true/u);
  assert.match(serverSource, /INDONESIA_STRUCTURED_B_RC_ASYNC_JOB_REQUIRED/u);
  const rawCaptureOffset = serverSource.indexOf("const originalImageBuffer = Buffer.from(req.file.buffer);");
  const rawPreflightOffset = serverSource.indexOf("await indonesiaStructuredBRcAdmission.preflightJobOrTerminate", rawCaptureOffset);
  const rawForwardOffset = serverSource.indexOf("buffer: indonesiaStructuredRcJobSelected ? originalImageBuffer", rawPreflightOffset);
  assert.ok(rawCaptureOffset >= 0 && rawPreflightOffset > rawCaptureOffset && rawForwardOffset > rawPreflightOffset);
  assert.match(serverSource, /structuredImageItems = indonesiaStructuredRcSelected[\s\S]{0,400}indonesiaStructuredRcOriginalImageBuffer\.toString\("base64"\)/u);
  assert.match(serverSource, /!indonesiaStructuredBRcAdmission\.config\.guardRequired/u);
  assert.match(serverSource, /structuredModeHeader && structuredModeBody && structuredModeHeader !== structuredModeBody/u);
  assert.match(serverSource, /supabase: coordinateUsageGuardedSupabase/u);
  assert.match(serverSource, /recognitionBudget\?\.assertCanStartProvider[\s\S]{0,800}indonesiaStructuredBRcAdmission\.dispatch[\s\S]{0,1200}callAliyunVision\(structuredProviderRequest\)/u);
  assert.doesNotMatch(serverSource, /console\.(?:log|error)\([^\n]*(?:claimToken|authorization)/iu);
});

await test("migration binds persistent RC identity, row locks, private grants and non-overwriting frozen seed", async () => {
  assert.match(migrationSource, /create schema if not exists private/u);
  for (const table of ["indonesia_structured_b_batches", "indonesia_structured_b_single_runs", "indonesia_structured_b_claims"]) {
    assert.match(migrationSource, new RegExp(`create table if not exists private\\.${table}`, "u"));
  }
  for (const rpc of ["claim", "dispatch", "settle", "close"]) {
    assert.match(migrationSource, new RegExp(`function public\\.${rpc}_indonesia_structured_b_single_run`, "u"));
  }
  assert.match(migrationSource, /security definer\s+set search_path = ''/u);
  assert.match(migrationSource, /revoke all on function[\s\S]+from public, anon, authenticated/u);
  assert.match(migrationSource, /grant execute on function[\s\S]+to service_role/u);
  assert.doesNotMatch(migrationSource, /revoke all on all tables in schema private/iu);
  assert.doesNotMatch(migrationSource, /revoke all on schema private/iu);
  assert.equal((migrationSource.match(/revoke all on table private\.indonesia_structured_b_/gu) || []).length, 3);
  assert.match(migrationSource, /project_ref text not null/u);
  assert.match(migrationSource, /v_batch\.project_ref is distinct from p_project_ref/u);
  assert.match(migrationSource, /v_batch\.source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'/u);
  assert.equal((migrationSource.match(/v_now := clock_timestamp\(\);/gu) || []).length, 4);
  assert.doesNotMatch(migrationSource, /v_now timestamptz := statement_timestamp\(\)/u);
  assert.match(migrationSource, /for update/u);
  assert.match(migrationSource, /on conflict \(batch_id\) do nothing/u);
  assert.match(migrationSource, /source_ledger_sha256 is distinct from '0ed002526e26a440f15ef20e82cfc1ee3c05099a3cb3b8bf5688c145d14e6aeb'/u);
  assert.match(rollbackSource, /rollback requires an explicit evidence-retention decision/u);
});

console.log(`Indonesia structured B RC admission delta: ${cases.length}/${cases.length} PASS`);
for (const name of cases) console.log(`PASS ${name}`);
console.log(JSON.stringify({
  status: "PASS",
  providerCalls: 0,
  networkCalls: 0,
  databaseWrites: 0,
  dynamicCases: cases.length,
  postgresConcurrencyProof: "NOT_CLAIMED_BY_FAKE_STORE_TEST",
  authorizedImageSha256Matched: true,
  frozenSuitesRerun: 0
}));
