import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  RecognitionBudget,
  RECOGNITION_BUDGET_CODE,
  RECOGNITION_BUDGET_PHASE,
  RECOGNITION_DEADLINE_CODE,
  RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE,
  RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE,
  getRecognitionDeadlineContext,
  recognitionDeadlineMiddleware
} from "../server/coordinate-finalizer/recognition-deadline.js";
import { runCancellableOcrJob } from "../server/recognition/cancellable-ocr.js";
import { isFamilyRetryAllowed } from "../server/recognition/family-retry-policy.js";

const tests = [];
const test = (id, name, fn) => tests.push({ id, name, fn });

function makeBudget({ nowMs = 0, deadlineMs = 55_000, reserveMs = 2_500, beginExecution = true } = {}) {
  let current = nowMs;
  const controller = new AbortController();
  const budget = new RecognitionBudget({
    signal: controller.signal,
    startedAt: 0,
    deadlineMs,
    responseReserveMs: reserveMs,
    lowValueFallbackCutoffMs: 45_000,
    now: () => current,
    trace: false,
    requestId: "sr08f-regression"
  });
  if (beginExecution) budget.beginExecutionPhase();
  return { budget, controller, setNow(value) { current = value; } };
}

test("B01", "80s cap is clipped to remaining budget minus reserve", () => {
  const { budget } = makeBudget({ nowMs: 10_000 });
  assert.equal(budget.remainingMs(), 42_500);
  assert.equal(budget.effectiveTimeout(80_000), 40_000);
});

test("B02", "10s remaining never yields an 80s timeout", () => {
  const { budget, setNow } = makeBudget();
  setNow(32_500);
  assert.equal(budget.effectiveTimeout(80_000), 7_500);
});

test("B03", "stage is rejected below minimum usable budget", () => {
  const { budget, setNow } = makeBudget();
  setNow(39_750);
  assert.equal(budget.canStartStage(500), false);
  assert.throws(() => budget.assertCanContinue({ stageName: "family_retry" }), { code: RECOGNITION_BUDGET_CODE });
});

test("B04", "low-value fallback is rejected at 45s cutoff", () => {
  const { budget, setNow } = makeBudget();
  setNow(42_500);
  assert.equal(budget.canStartStage(500, { lowValue: true }), false);
  assert.throws(() => budget.assertCanContinue({ stageName: "local_ocr", lowValue: true }), { code: RECOGNITION_BUDGET_CODE });
});

test("B05", "required stage may use only remaining bounded time", () => {
  const { budget, setNow } = makeBudget();
  setNow(30_000);
  budget.assertCanContinue({ stageName: "verification", minRequiredMs: 500 });
  assert.equal(budget.effectiveTimeout(80_000), 10_000);
});

test("B06", "ingress preflight and recognition execution use separate phase clocks under one hard deadline", () => {
  const { budget, setNow } = makeBudget({ beginExecution: false });
  budget.startIngressUpload();
  setNow(7_000);
  budget.completeIngressUpload();
  const eligibility = budget.stageStarted("usage_eligibility");
  setNow(12_000);
  budget.stageCompleted(eligibility);
  assert.equal(budget.budgetPhase, RECOGNITION_BUDGET_PHASE.INGRESS_PREFLIGHT);
  assert.equal(budget.beginExecutionPhase(), true);
  assert.equal(budget.budgetPhase, RECOGNITION_BUDGET_PHASE.EXECUTION);
  assert.equal(budget.preflightCompletedAt, 12_000);
  setNow(17_000);
  const ledger = budget.toSanitizedLedger();
  assert.equal(ledger.preflightDurationMs, 12_000);
  assert.equal(ledger.executionElapsedMs, 5_000);
  assert.equal(ledger.overallElapsedMs, 17_000);
  assert.equal(budget.deadlineMs, 55_000);
  assert.equal(budget.remainingMs(), 37_500);
});

test("B07", "preflight time does not prematurely trigger the execution fallback cutoff", () => {
  const { budget, setNow } = makeBudget({ beginExecution: false });
  setNow(10_000);
  budget.beginExecutionPhase();
  setNow(45_000);
  assert.doesNotThrow(() => budget.assertCanContinue({ stageName: "local_ocr", lowValue: true }));
  setNow(52_500);
  assert.throws(
    () => budget.assertCanContinue({ stageName: "local_ocr", lowValue: true }),
    error => error.code === RECOGNITION_BUDGET_CODE && error.reason === "soft_fallback_cutoff"
  );
});

test("B08", "Provider admission reserves parsing Finalizer and response time and fails before attempt", () => {
  const { budget, setNow } = makeBudget();
  setNow(39_000);
  assert.throws(
    () => budget.assertCanStartProvider({ stageName: "generic_provider", minRequiredMs: 500 }),
    error => error.code === RECOGNITION_BUDGET_CODE
      && error.reason === "provider_admission_budget_insufficient"
  );
  assert.equal(budget.providerAttempted, false);
  assert.equal(budget.providerAttemptCount, 0);
  assert.equal(budget.events.at(-1).skippedReason, "provider_admission_budget_insufficient");
});

test("B09", "Provider timeout excludes post-Provider and response reserves", () => {
  const { budget, setNow } = makeBudget();
  setNow(10_000);
  assert.equal(budget.effectiveProviderTimeout(80_000), 28_500);
});

test("B10", "preflight elapsed time cannot consume the fixed execution allowance", () => {
  const early = makeBudget({ beginExecution: false });
  early.setNow(1_000);
  early.budget.beginExecutionPhase();
  const late = makeBudget({ beginExecution: false });
  late.setNow(12_000);
  late.budget.beginExecutionPhase();
  assert.equal(early.budget.remainingMs(), 42_500);
  assert.equal(late.budget.remainingMs(), 42_500);
});

test("B10A", "expired preflight cannot mint a fresh execution budget or enter Provider", () => {
  const { budget, setNow } = makeBudget({ beginExecution: false });
  setNow(12_501);
  assert.throws(
    () => budget.beginExecutionPhase(),
    error => error.code === RECOGNITION_BUDGET_CODE
      && error.reason === "insufficient_remaining_budget"
  );
  assert.equal(budget.budgetPhase, RECOGNITION_BUDGET_PHASE.INGRESS_PREFLIGHT);
  assert.equal(budget.executionStartedAt, null);
  assert.equal(budget.providerAttemptCount, 0);
  assert.throws(
    () => budget.assertCanStartProvider({ stageName: "generic_provider" }),
    error => error.code === RECOGNITION_BUDGET_CODE
  );
});

test("B11", "the request-scoped Provider boundary rejects every second call before attempt", () => {
  const { budget } = makeBudget();
  budget.assertCanStartProvider({ stageName: "generic_provider" });
  budget.markProviderAttempted();
  budget.markProviderCompleted({ state: "FAILED" });
  assert.throws(
    () => budget.assertCanStartProvider({ stageName: "wgs84_retry" }),
    error => error.code === RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE
      && error.reason === "provider_attempt_limit_reached"
  );
  assert.equal(budget.providerAttemptCount, 1);
});

test("B12", "Provider failure permits at most one bounded local OCR evidence attempt", () => {
  const { budget } = makeBudget();
  assert.equal(budget.assertCanStartLocalOcr({ stageName: "local_ocr" }), true);
  assert.throws(
    () => budget.assertCanStartLocalOcr({ stageName: "local_ocr_map_layout" }),
    error => error.code === RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE
      && error.reason === "local_ocr_attempt_limit_reached"
  );
  assert.equal(budget.localOcrAttemptCount, 1);
});

test("B13", "fixed acquisition diagnostic exposes only bounded enums buckets and counts", () => {
  const { budget, setNow } = makeBudget();
  budget.setAcquisitionRouteReason("WGS84_SINGLE_POINT_PRIMARY");
  budget.assertCanStartProvider({ stageName: "generic_provider" });
  budget.markProviderAttempted();
  budget.markProviderCompleted({ state: "SUCCEEDED", usageObserved: true });
  setNow(4_000);
  budget.markResponseSent({ httpStatus: 200, responseCode: "OK" });
  const diagnostic = budget.toSanitizedAcquisitionDiagnostic();
  assert.deepEqual(Object.keys(diagnostic).sort(), [
    "localOcrCallCount", "providerCallCount", "routeReason", "schemaVersion",
    "stageNames", "terminalState", "timingBucket"
  ].sort());
  assert.equal(diagnostic.routeReason, "WGS84_SINGLE_POINT_PRIMARY");
  assert.equal(diagnostic.timingBucket, "LE_5S");
  assert.equal(diagnostic.providerCallCount, 1);
  assert.equal(diagnostic.localOcrCallCount, 0);
  assert.equal(diagnostic.terminalState, "COMPLETED");
  assert.doesNotMatch(JSON.stringify(diagnostic).toLowerCase(), /rawtext|coordinate|header|cookie|secret|api_key|error/);
});

test("A01-A06", "hard deadline sends 504 and prevents every later recognition stage", async () => {
  const response = Object.assign(new EventEmitter(), {
    headersSent: false,
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.headersSent = true; this.body = body; this.emit("finish"); return this; }
  });
  let context;
  recognitionDeadlineMiddleware({ deadlineMs: 15 })({}, response, () => {
    context = getRecognitionDeadlineContext();
  });
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(response.statusCode, 504);
  assert.equal(response.body.code, RECOGNITION_DEADLINE_CODE);
  assert.equal(response.body.error, "本次识别未完成，未扣除使用次数。你可以直接重新识别；如仍失败，请向支持人员提供本次请求编号。");
  assert.match(response.body.requestId, /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  assert.equal(response.body.usageConsumed, false);
  assert.equal(response.body.retryAllowed, true);
  assert.equal(context.signal.aborted, true);
  assert.equal(context.budget.toSanitizedAcquisitionDiagnostic().terminalState, "TIMED_OUT");
  const eventCount = context.budget.events.length;
  for (const stageName of ["generic_provider", "local_ocr", "family_retry", "finalizer"]) {
    assert.throws(() => context.budget.assertCanContinue({ stageName }), { code: RECOGNITION_DEADLINE_CODE });
  }
  const rejectedStages = context.budget.events.slice(eventCount);
  assert.equal(rejectedStages.length, 4);
  assert.ok(rejectedStages.every(event => event.result === "aborted"));
  assert.ok(rejectedStages.every(event => event.stageStartElapsedMs === event.stageEndElapsedMs));
  context.budget.markHandlerCompleted();
  assert.ok(context.budget.handlerCompletedAt !== null);
});

test("A07", "response guard proves no stage can start after response", () => {
  const { budget } = makeBudget();
  budget.markResponseSent();
  const eventCount = budget.events.length;
  assert.throws(() => budget.assertCanContinue({ stageName: "parser" }), { code: RECOGNITION_BUDGET_CODE });
  assert.equal(budget.events.length, eventCount + 1);
  assert.equal(budget.events.at(-1).result, "skipped");
  assert.equal(budget.events.at(-1).skippedReason, "response_already_sent");
});

test("F01", "generic timeout without family evidence cannot start Kyrgyz retry", () => {
  assert.equal(isFamilyRetryAllowed({ stageName: "kyrgyz_retry", familyEvidence: false }), false);
});

for (const [id, stageName] of [
  ["F02", "kyrgyz_retry"],
  ["F03", "mgrs_retry"],
  ["F04", "handwritten_retry"],
  ["F05", "cadastral_retry"]
]) {
  test(id, `${stageName} requires explicit matched-family evidence`, () => {
    assert.equal(isFamilyRetryAllowed({ stageName, familyEvidence: true }), true);
    assert.equal(isFamilyRetryAllowed({ stageName, familyEvidence: false }), false);
  });
}

test("F06", "unknown family has no specialized retry authorization", () => {
  assert.equal(isFamilyRetryAllowed({ stageName: "family_retry", familyEvidence: false }), false);
});

test("T01", "OCR completes and worker terminates within budget", async () => {
  let active = 0;
  const result = await runCancellableOcrJob({
    createWorker: async () => {
      active += 1;
      return {
        recognize: async () => ({ data: { text: "ok" } }),
        terminate: async () => { active -= 1; }
      };
    },
    image: Buffer.from("fixture"),
    timeoutMs: 100
  });
  assert.equal(result.data.text, "ok");
  assert.equal(active, 0);
});

test("T02", "OCR stage timeout terminates worker", async () => {
  let terminateCount = 0;
  await assert.rejects(runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => new Promise(() => {}),
      terminate: async () => { terminateCount += 1; }
    }),
    image: Buffer.from("fixture"),
    timeoutMs: 10
  }), { code: RECOGNITION_BUDGET_CODE });
  assert.equal(terminateCount, 1);
});

test("T03-T05", "request abort terminates OCR with no worker left for next request", async () => {
  const controller = new AbortController();
  let active = 0;
  const job = runCancellableOcrJob({
    createWorker: async () => {
      active += 1;
      return {
        recognize: async () => new Promise(() => {}),
        terminate: async () => { active -= 1; }
      };
    },
    image: Buffer.from("fixture"),
    signal: controller.signal,
    timeoutMs: 1_000
  });
  await new Promise(resolve => setTimeout(resolve, 5));
  controller.abort();
  await assert.rejects(job, { code: RECOGNITION_DEADLINE_CODE });
  assert.equal(active, 0);
});

test("TRACE01", "stage trace contains timing only and no recognition payload", () => {
  const { budget } = makeBudget();
  const event = budget.stageStarted("generic_provider", {
    configuredTimeoutMs: 80_000,
    effectiveTimeoutMs: 52_500
  });
  budget.stageCompleted(event);
  for (const forbidden of ["rawText", "coordinates", "fileName", "projectName", "visitorId", "apiKey", "authorization"]) {
    assert.equal(Object.hasOwn(event, forbidden), false);
  }
});

test("A08", "hard deadline during an atomic commit reports unknown instead of uncharged", async () => {
  for (const state of ["PREPARING", "PREPARED", "COMMITTING"]) {
    const response = Object.assign(new EventEmitter(), {
      headersSent: false,
      statusCode: 200,
      body: null,
      headers: {},
      setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
      status(code) { this.statusCode = code; return this; },
      json(body) { this.headersSent = true; this.body = body; this.emit("finish"); return this; }
    });
    recognitionDeadlineMiddleware({ deadlineMs: 15 })({
      get(name) {
        if (String(name).toLowerCase() === "x-recognition-request-id") return "11111111-1111-4111-8111-111111111111";
        return "";
      }
    }, response, () => {
      getRecognitionDeadlineContext().budget.markUsageCommitState(state);
    });
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(response.statusCode, 504, state);
    assert.equal(response.body.code, "USAGE_COMMIT_OUTCOME_UNKNOWN", state);
    assert.equal(response.body.requestId, "11111111-1111-4111-8111-111111111111", state);
    assert.equal(response.body.usageConsumed, null, state);
    assert.equal(response.body.recoveryRequired, true, state);
    assert.equal(response.body.retryAllowed, false, state);
    assert.doesNotMatch(response.body.error, /未扣除使用次数/, state);
  }
});

test("A09", "invalid client request ID is replaced by a server UUID", () => {
  const response = Object.assign(new EventEmitter(), {
    headersSent: false,
    statusCode: 200,
    headers: {},
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.headersSent = true; this.body = body; this.emit("finish"); return this; }
  });
  recognitionDeadlineMiddleware({ deadlineMs: 1000 })({ get: () => "not-a-uuid" }, response, () => {});
  assert.match(response.headers["x-recognition-request-id"], /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  response.emit("close");
});

test("LEDGER01", "strict ledger contains only approved fields and normalizes unknown identity and reasons", () => {
  const { budget } = makeBudget();
  budget.setIngressMetadata({
    runtimeCommit: "private commit text",
    runtimeBranch: "branch with spaces and secret",
    uploadSize: 900_000
  });
  budget.beginExecutionPhase();
  budget.markProviderAttempted();
  budget.markProviderCompleted({ state: "SUCCEEDED", usageObserved: true });
  budget.recordSkippedStage("private_stage_name", "private_error_detail", "failed");
  budget.markResponseSent({ httpStatus: 200, responseCode: "private_response_detail" });
  budget.markHandlerCompleted();
  const ledger = budget.toSanitizedLedger();
  assert.deepEqual(Object.keys(ledger).sort(), [
    "budgetPhase", "executionElapsedMs", "httpStatus", "overallElapsedMs",
    "preflightDurationMs", "providerAttempted", "providerCompletionState",
    "providerCostState", "requestId", "responseCode",
    "runtimeBranch", "runtimeCommit", "schemaVersion", "stages", "uploadSizeBucket", "usageCommitState", "userUsageConsumed"
  ].sort());
  assert.deepEqual(Object.keys(ledger.stages[0]).sort(), [
    "budgetPhase", "durationMs", "reasonCode", "remainingBudgetAtEndMs",
    "remainingBudgetAtStartMs", "result", "stageName"
  ].sort());
  assert.equal(ledger.runtimeCommit, "UNLISTED_RUNTIME_COMMIT");
  assert.equal(ledger.runtimeBranch, "UNLISTED_RUNTIME_BRANCH");
  assert.equal(ledger.uploadSizeBucket, "LE_1_MIB");
  assert.equal(ledger.providerAttempted, true);
  assert.equal(ledger.providerCompletionState, "SUCCEEDED");
  assert.equal(ledger.providerCostState, "USAGE_REPORTED");
  assert.equal(ledger.userUsageConsumed, false);
  assert.equal(ledger.usageCommitState, "NOT_STARTED");
  assert.equal(ledger.responseCode, "UNLISTED_RESPONSE_CODE");
  assert.equal(ledger.stages[0].stageName, "UNLISTED_STAGE");
  assert.equal(ledger.stages[0].reasonCode, "UNLISTED_STAGE_REASON");
  const serialized = JSON.stringify(ledger).toLowerCase();
  for (const forbidden of [
    "rawtext", "coordinate", "filename", "filehash", "prompt", "providerrequest",
    "providerresponse", "visitorid", "userid", "authorization", "cookie", "secret", "api_key"
  ]) assert.equal(serialized.includes(forbidden), false, forbidden);
});

let passed = 0;
for (const entry of tests) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}

console.log(`SR-08F recognition budget regression: ${passed}/${tests.length} PASS`);
