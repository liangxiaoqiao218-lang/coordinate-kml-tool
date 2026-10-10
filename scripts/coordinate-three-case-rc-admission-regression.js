import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_THREE_CASE_RC,
  CoordinateThreeCaseRcAdmissionError,
  createCoordinateThreeCaseRcAdmission,
  readCoordinateThreeCaseRcConfig
} from "../server/recognition/coordinate-three-case-rc-admission.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const env = Object.freeze({
  COORDINATE_THREE_CASE_RC_ENABLED: "true",
  COORDINATE_THREE_CASE_RC_RUN_ID: COORDINATE_THREE_CASE_RC.runId,
  COORDINATE_THREE_CASE_RC_WINDOW_SECONDS: "1800",
  COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: "600",
  RC_SUPABASE_PROJECT_REF: COORDINATE_THREE_CASE_RC.rcProjectRef,
  RENDER_SERVICE_NAME: COORDINATE_THREE_CASE_RC.rcServiceName,
  SUPABASE_URL: `https://${COORDINATE_THREE_CASE_RC.rcProjectRef}.supabase.co`,
  ALIYUN_VISION_MODEL: COORDINATE_THREE_CASE_RC.model,
  ALIYUN_BASE_URL: "https://dashscope.aliyuncs.com/compatible-mode/v1"
});

class FakeLedger {
  constructor() {
    this.activated = false;
    this.closed = false;
    this.activeDispatches = 0;
    this.totalDispatched = 0;
    this.nonIndonesiaDispatched = 0;
    this.nonIndonesiaReservedCny = 0;
    this.indonesiaHistoricalDispatched = 2;
    this.indonesiaHistoricalReservedCny = 1.632;
    this.claims = new Map();
    this.cases = new Map(Object.keys(COORDINATE_THREE_CASE_RC.cases).map(caseId => [caseId, {
      status: "PENDING", claimCount: 0, dispatchCount: 0, providerCallCount: null,
      outcome: null, costEvidence: "UNKNOWN", estimatedCostCny: null
    }]));
  }

  async rpc(name, args) {
    if (name === "activate_coordinate_three_case_rc_run") {
      if (this.closed) return { data: { accepted: false, code: "CLOSED" } };
      const action = this.activated ? "ALREADY_ACTIVE" : "ACTIVATED";
      this.activated = true;
      return { data: { accepted: true, action } };
    }
    if (name === "claim_coordinate_three_case_rc") {
      const entry = this.cases.get(args.p_case_id);
      if (!this.activated || this.closed || !entry || entry.claimCount !== 0) {
        return { data: { accepted: false, code: "RC_CLAIM_CLOSED_OR_CONSUMED" } };
      }
      entry.claimCount = 1;
      entry.status = "CLAIMED";
      this.claims.set(args.p_recognition_request_id, { ...args, status: "RESERVED" });
      return { data: { accepted: true, action: "CLAIMED", expires_at: new Date(Date.now() + 60_000).toISOString() } };
    }
    if (name === "dispatch_coordinate_three_case_rc") {
      const claim = this.claims.get(args.p_recognition_request_id);
      const entry = this.cases.get(args.p_case_id);
      if (!claim || claim.status !== "RESERVED" || this.activeDispatches !== 0 || entry.dispatchCount !== 0) {
        return { data: { accepted: false, code: "RC_DISPATCH_CLOSED_OR_CONCURRENT" } };
      }
      if (args.p_case_id === "indonesia") {
        if (this.indonesiaHistoricalDispatched >= 6 || this.indonesiaHistoricalReservedCny + 0.816 > 5) {
          return { data: { accepted: false, code: "RC_INDONESIA_HISTORICAL_BUDGET_REJECTED" } };
        }
        this.indonesiaHistoricalDispatched += 1;
        this.indonesiaHistoricalReservedCny += 0.816;
      } else {
        if (this.nonIndonesiaDispatched >= 2 || this.nonIndonesiaReservedCny + 1 > 2) {
          return { data: { accepted: false, code: "RC_NON_INDONESIA_SHARED_BUDGET_REJECTED" } };
        }
        this.nonIndonesiaDispatched += 1;
        this.nonIndonesiaReservedCny += 1;
      }
      claim.status = "DISPATCHED";
      entry.status = "DISPATCHED";
      entry.dispatchCount = 1;
      this.totalDispatched += 1;
      this.activeDispatches += 1;
      return { data: { accepted: true, action: "DISPATCHED" } };
    }
    if (name === "settle_coordinate_three_case_rc") {
      const claim = this.claims.get(args.p_recognition_request_id);
      const entry = this.cases.get(args.p_case_id);
      if (!claim || !entry) return { data: { accepted: false, code: "RC_SETTLEMENT_BINDING_NOT_FOUND" } };
      if (["COMPLETED", "FAILED_PRE_PROVIDER", "FAILED_AFTER_PROVIDER", "OUTCOME_UNKNOWN"].includes(claim.status)) {
        return { data: { accepted: claim.status === args.p_outcome, action: claim.status === args.p_outcome ? "SETTLED" : undefined } };
      }
      if (claim.status === "DISPATCHED") this.activeDispatches -= 1;
      claim.status = args.p_outcome;
      entry.status = args.p_outcome;
      entry.providerCallCount = args.p_provider_call_count;
      entry.outcome = args.p_outcome;
      entry.costEvidence = args.p_cost_evidence;
      entry.estimatedCostCny = args.p_estimated_cost_cny;
      return { data: { accepted: true, action: "SETTLED" } };
    }
    if (name === "close_coordinate_three_case_rc_run") {
      this.closed = true;
      return { data: { accepted: true, action: "CLOSED" } };
    }
    if (name === "read_coordinate_three_case_rc_status") {
      return { data: {
        accepted: true,
        action: "STATUS",
        run_status: this.closed ? "CLOSED" : "ACTIVE",
        run_activated: this.activated,
        run_closed: this.closed,
        active_dispatches: this.activeDispatches,
        total_dispatched: this.totalDispatched,
        indonesia_historical_dispatched: this.indonesiaHistoricalDispatched,
        indonesia_historical_reserved_cny: this.indonesiaHistoricalReservedCny,
        non_indonesia_dispatched: this.nonIndonesiaDispatched,
        non_indonesia_reserved_cny: this.nonIndonesiaReservedCny,
        non_indonesia_estimated_cost_cny: null,
        cases: [...this.cases].map(([caseId, entry]) => ({
          case_id: caseId,
          status: entry.status,
          claim_count: entry.claimCount,
          dispatch_count: entry.dispatchCount,
          provider_call_count: entry.providerCallCount,
          outcome: entry.outcome,
          cost_evidence: entry.costEvidence,
          estimated_cost_cny: entry.estimatedCostCny
        }))
      } };
    }
    return { error: { message: "unexpected rpc" } };
  }
}

const cases = [];
async function test(name, action) {
  try {
    await action();
    cases.push({ name, pass: true });
  } catch (error) {
    cases.push({ name, pass: false, error: error?.message || String(error) });
  }
}

const config = readCoordinateThreeCaseRcConfig(env);
await test("exact RC configuration is ready", () => assert.equal(config.ready, true));
await test("partial RC configuration fails closed", () => assert.equal(readCoordinateThreeCaseRcConfig({
  ...env,
  COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: "601"
}).ready, false));
await test("RC service remains guarded when the run flag is off", () => {
  const disabled = readCoordinateThreeCaseRcConfig({ ...env, COORDINATE_THREE_CASE_RC_ENABLED: "false" });
  assert.equal(disabled.enabled, false);
  assert.equal(disabled.ready, false);
  assert.equal(disabled.guardRequired, true);
});
await test("RC service remains guarded when its database reference is wrong", () => {
  const misconfigured = readCoordinateThreeCaseRcConfig({
    ...env,
    SUPABASE_URL: "https://aaaaaaaaaaaaaaaaaaaa.supabase.co"
  });
  assert.equal(misconfigured.ready, false);
  assert.equal(misconfigured.guardRequired, true);
});
await test("explicit three-case configuration remains guarded on the wrong service", () => {
  const misconfigured = readCoordinateThreeCaseRcConfig({ ...env, RENDER_SERVICE_NAME: "wrong-service" });
  assert.equal(misconfigured.ready, false);
  assert.equal(misconfigured.guardRequired, true);
});
await test("direct OCR and automatic retries remain zero", () => {
  assert.equal(COORDINATE_THREE_CASE_RC.directOcrCalls, 0);
  assert.equal(COORDINATE_THREE_CASE_RC.automaticRetries, 0);
});

const ledger = new FakeLedger();
const hashByBuffer = new Map([
  ["indonesia", COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256],
  ["mgrs", COORDINATE_THREE_CASE_RC.cases.mgrs.imageSha256],
  ["bftm", COORDINATE_THREE_CASE_RC.cases.bftm.imageSha256]
]);
let tokenIndex = 0;
const admission = createCoordinateThreeCaseRcAdmission({
  supabase: ledger,
  adminPassword: "local-test-admin",
  env,
  randomToken: () => `token_${String(++tokenIndex).padStart(40, "0")}`,
  hashImage: buffer => hashByBuffer.get(buffer.toString("utf8")) || ""
});

await test("startup activation is required and succeeds", async () => {
  const result = await admission.activateOnStartup();
  assert.equal(result.activated, true);
});
await test("admin authentication is exact", async () => {
  await assert.rejects(() => admission.claim({ adminHeader: "wrong" }), CoordinateThreeCaseRcAdmissionError);
});

const requestIds = Object.freeze({
  indonesia: "00000000-0000-4000-8000-000000000001",
  mgrs: "00000000-0000-4000-8000-000000000002",
  bftm: "00000000-0000-4000-8000-000000000003"
});

async function claimAndBind(caseId) {
  const definition = COORDINATE_THREE_CASE_RC.cases[caseId];
  const claim = await admission.claim({
    adminHeader: "local-test-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId,
    imageSha256: definition.imageSha256,
    recognitionRequestId: requestIds[caseId]
  });
  const budget = { providerAttemptCount: 0 };
  admission.bindBudget({
    budget,
    caseId,
    productMode: definition.productMode,
    recognitionRequestId: requestIds[caseId],
    imageBuffer: Buffer.from(caseId),
    authorization: `Bearer ${claim.claimToken}`
  });
  return { budget, claim };
}

await test("wrong image fails before dispatch", () => assert.throws(() => admission.preflightJob({
  caseId: "mgrs",
  productMode: "",
  recognitionRequestId: requestIds.mgrs,
  imageBuffer: Buffer.from("not-mgrs"),
  authorization: `Bearer ${"a".repeat(40)}`
}), /COORDINATE_THREE_CASE_RC_JOB_PREFLIGHT_REJECTED/u));

const indonesia = await claimAndBind("indonesia");
await test("wrong model fails before provider dispatch", async () => {
  await assert.rejects(() => admission.dispatchForBudget({ budget: indonesia.budget, modelName: "other-model", maxTokens: 8000, enableThinking: false }), /MODEL_MISMATCH/u);
});
await test("thinking and oversized output are rejected before dispatch", async () => {
  await assert.rejects(() => admission.dispatchForBudget({
    budget: indonesia.budget,
    modelName: COORDINATE_THREE_CASE_RC.model,
    maxTokens: 8001,
    enableThinking: true
  }), /PROVIDER_PARAMETERS_REJECTED/u);
});
await test("Indonesia dispatch consumes one historical slot", async () => {
  await admission.dispatchForBudget({ budget: indonesia.budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false });
  indonesia.budget.providerAttemptCount = 1;
  admission.recordProviderResultForBudget({ budget: indonesia.budget, completionState: "SUCCEEDED", usage: null });
  const settled = await admission.settleForBudget({ budget: indonesia.budget, httpStatus: 200, body: { success: true } });
  assert.equal(settled.providerCallCount, 1);
  assert.equal(settled.costEvidence, "UNKNOWN");
  assert.equal(settled.estimatedCostCny, null);
  assert.equal(ledger.indonesiaHistoricalDispatched, 3);
});
await test("duplicate case claim is rejected", async () => {
  await assert.rejects(() => admission.claim({
    adminHeader: "local-test-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "indonesia",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256,
    recognitionRequestId: "00000000-0000-4000-8000-000000000004"
  }), /RC_CLAIM_CLOSED_OR_CONSUMED/u);
});

const mgrs = await claimAndBind("mgrs");
const bftm = await claimAndBind("bftm");
await test("global concurrency rejects overlapping dispatch", async () => {
  await admission.dispatchForBudget({ budget: mgrs.budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false });
  await assert.rejects(() => admission.dispatchForBudget({ budget: bftm.budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false }), /RC_DISPATCH_CLOSED_OR_CONCURRENT/u);
});
await test("MGRS settlement preserves unknown cost as unknown", async () => {
  mgrs.budget.providerAttemptCount = 1;
  admission.recordProviderResultForBudget({ budget: mgrs.budget, completionState: "FAILED", usage: null });
  const settled = await admission.settleForBudget({ budget: mgrs.budget, httpStatus: 422, body: { success: false } });
  assert.equal(settled.costEvidence, "UNKNOWN");
  assert.equal(settled.estimatedCostCny, null);
});
await test("coerced Provider usage is rejected rather than priced", async () => {
  const strictLedger = new FakeLedger();
  const strictAdmission = createCoordinateThreeCaseRcAdmission({
    supabase: strictLedger,
    adminPassword: "local-test-admin",
    env,
    randomToken: () => `token_${"6".repeat(40)}`,
    hashImage: buffer => hashByBuffer.get(buffer.toString("utf8")) || ""
  });
  await strictAdmission.activateOnStartup();
  const claim = await strictAdmission.claim({
    adminHeader: "local-test-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "mgrs",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.mgrs.imageSha256,
    recognitionRequestId: requestIds.mgrs
  });
  const budget = { providerAttemptCount: 0 };
  strictAdmission.bindBudget({
    budget,
    caseId: "mgrs",
    productMode: "",
    recognitionRequestId: requestIds.mgrs,
    imageBuffer: Buffer.from("mgrs"),
    authorization: `Bearer ${claim.claimToken}`
  });
  await strictAdmission.dispatchForBudget({ budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false });
  budget.providerAttemptCount = 1;
  strictAdmission.recordProviderResultForBudget({
    budget,
    completionState: "SUCCEEDED",
    usage: { prompt_tokens: "1000", completion_tokens: 100, total_tokens: 1100 }
  });
  const settlement = await strictAdmission.settleForBudget({ budget, httpStatus: 200, body: { success: true } });
  assert.equal(settlement.costEvidence, "UNKNOWN");
  assert.equal(settlement.estimatedCostCny, null);
});
await test("BFTM uses the second and final shared CNY reservation", async () => {
  await admission.dispatchForBudget({ budget: bftm.budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false });
  bftm.budget.providerAttemptCount = 1;
  admission.recordProviderResultForBudget({
    budget: bftm.budget,
    completionState: "SUCCEEDED",
    usage: { prompt_tokens: 1000, completion_tokens: 100, total_tokens: 1100 }
  });
  const settled = await admission.settleForBudget({ budget: bftm.budget, httpStatus: 200, body: { success: true } });
  assert.equal(settled.costEvidence, "USAGE_REPORTED");
  assert.equal(ledger.nonIndonesiaDispatched, 2);
  assert.equal(ledger.nonIndonesiaReservedCny, 2);
});
await test("closed run cannot be reopened or claimed", async () => {
  await admission.close({ adminHeader: "local-test-admin" });
  const status = await admission.status({ adminHeader: "local-test-admin" });
  assert.equal(status.runClosed, true);
  assert.equal(status.nonIndonesiaEstimatedCostCny, null);
});

await test("pre-provider failure records NOT_INCURRED rather than zero usage", async () => {
  const isolatedLedger = new FakeLedger();
  const isolated = createCoordinateThreeCaseRcAdmission({
    supabase: isolatedLedger,
    adminPassword: "local-test-admin",
    env,
    randomToken: () => `token_${"4".repeat(40)}`,
    hashImage: buffer => hashByBuffer.get(buffer.toString("utf8")) || ""
  });
  await isolated.activateOnStartup();
  const claim = await isolated.claim({
    adminHeader: "local-test-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "mgrs",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.mgrs.imageSha256,
    recognitionRequestId: requestIds.mgrs
  });
  await isolated.terminatePreProviderClaim({
    caseId: "mgrs",
    productMode: "",
    recognitionRequestId: requestIds.mgrs,
    imageBuffer: Buffer.from("mgrs"),
    authorization: `Bearer ${claim.claimToken}`
  });
  assert.equal(isolatedLedger.cases.get("mgrs").costEvidence, "NOT_INCURRED");
  assert.equal(isolatedLedger.cases.get("mgrs").estimatedCostCny, null);
});

await test("close blocks new work while preserving an inflight unknown dispatch", async () => {
  const isolatedLedger = new FakeLedger();
  const isolated = createCoordinateThreeCaseRcAdmission({
    supabase: isolatedLedger,
    adminPassword: "local-test-admin",
    env,
    randomToken: () => `token_${"5".repeat(40)}`,
    hashImage: buffer => hashByBuffer.get(buffer.toString("utf8")) || ""
  });
  await isolated.activateOnStartup();
  const claim = await isolated.claim({
    adminHeader: "local-test-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "bftm",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.bftm.imageSha256,
    recognitionRequestId: requestIds.bftm
  });
  const budget = { providerAttemptCount: 0 };
  isolated.bindBudget({
    budget,
    caseId: "bftm",
    productMode: "",
    recognitionRequestId: requestIds.bftm,
    imageBuffer: Buffer.from("bftm"),
    authorization: `Bearer ${claim.claimToken}`
  });
  await isolated.dispatchForBudget({ budget, modelName: COORDINATE_THREE_CASE_RC.model, maxTokens: 8000, enableThinking: false });
  await isolated.close({ adminHeader: "local-test-admin" });
  assert.equal(isolatedLedger.closed, true);
  assert.equal(isolatedLedger.activeDispatches, 1);
  assert.equal(isolatedLedger.cases.get("bftm").costEvidence, "UNKNOWN");
});

const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const migrationSource = fs.readFileSync(path.join(root, "supabase/migrations/20261010180527_coordinate_three_case_rc_admission.sql"), "utf8");
const manifestSource = fs.readFileSync(path.join(root, "docs/coordinate-three-case-rc-manifest.json"), "utf8");
const clientSource = fs.readFileSync(path.join(root, "scripts/coordinate-three-case-rc-client.ps1"), "utf8");
await test("server binds dispatch before provider attempt", () => {
  const dispatchIndex = serverSource.indexOf("coordinateThreeCaseRcAdmission.dispatchForBudget");
  const attemptedIndex = serverSource.indexOf("budget?.markProviderAttempted()", dispatchIndex);
  assert.ok(dispatchIndex >= 0 && attemptedIndex > dispatchIndex);
});
await test("direct recognition endpoint fails closed during RC", () => {
  assert.match(serverSource, /\/api\/recognize-coordinates[\s\S]{0,300}coordinateThreeCaseRcAdmission\.config\.guardRequired/u);
});
await test("async job enforces case preflight and forwards case binding", () => {
  assert.match(serverSource, /coordinateThreeCaseRcAdmission\.preflightJob/u);
  assert.match(serverSource, /forwardHeaders\["x-coordinate-rc-case-id"\]/u);
});
await test("closed Run04 admission is not selected by the new Indonesia case", () => {
  assert.match(serverSource, /indonesiaStructuredRcSelected = indonesiaStructuredBRcAdmission\.config\.ready/u);
  assert.match(serverSource, /indonesiaStructuredRcJobSelected = indonesiaStructuredBRcAdmission\.config\.ready/u);
});
await test("database functions use SECURITY DEFINER with empty search_path", () => {
  assert.equal((migrationSource.match(/security definer set search_path = ''/gu) || []).length, 6);
});
await test("database tables use RLS and private storage", () => {
  assert.equal((migrationSource.match(/enable row level security/gu) || []).length, 4);
  assert.doesNotMatch(migrationSource, /create table public\./u);
});
await test("Indonesia dispatch remains atomically bound to the historical cumulative budget", () => {
  assert.match(migrationSource, /select \* into v_legacy from private\.indonesia_structured_b_batches[\s\S]*for update/u);
  assert.match(migrationSource, /update private\.indonesia_structured_b_batches set dispatched_attempts = dispatched_attempts \+ 1/u);
  assert.doesNotMatch(migrationSource, /update private\.indonesia_structured_b_single_runs/u);
});
await test("database RPCs are revoked from public clients", () => {
  assert.equal((migrationSource.match(/revoke all on function/gu) || []).length, 6);
  assert.equal((migrationSource.match(/grant execute on function/gu) || []).length, 6);
});
await test("manifest contains exactly three one-call cases", () => {
  const manifest = JSON.parse(manifestSource);
  assert.deepEqual(manifest.cases.map(entry => entry.caseId), ["indonesia", "mgrs", "bftm"]);
  assert.ok(manifest.cases.every(entry => entry.maxVisionCalls === 1));
  assert.equal(createHash("sha256").update(Buffer.from(manifestSource)).digest("hex"), COORDINATE_THREE_CASE_RC.manifestSha256);
});
await test("trusted client is serial, one-shot and always closes the run", () => {
  assert.match(clientSource, /foreach \(\$definition in \$caseDefinitions\)/u);
  assert.match(clientSource, /automaticRetries = 0/u);
  assert.match(clientSource, /\/api\/admin\/coordinate-products\/three-case-rc\/close/u);
  assert.doesNotMatch(clientSource, /Authorization.*claimToken.*Write|Write.*claimToken/iu);
});

const passed = cases.filter(entry => entry.pass).length;
const failed = cases.filter(entry => !entry.pass);
console.log(JSON.stringify({
  suite: "COORDINATE_THREE_CASE_RC_ADMISSION",
  passed,
  total: cases.length,
  failed,
  providerCalls: 0,
  productionCalls: 0,
  automaticRetries: COORDINATE_THREE_CASE_RC.automaticRetries,
  directOcrCalls: COORDINATE_THREE_CASE_RC.directOcrCalls
}, null, 2));
if (failed.length > 0) process.exitCode = 1;
