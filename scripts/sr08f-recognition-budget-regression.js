import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  RecognitionBudget,
  RECOGNITION_BUDGET_CODE,
  RECOGNITION_DEADLINE_CODE,
  getRecognitionDeadlineContext,
  recognitionDeadlineMiddleware
} from "../server/coordinate-finalizer/recognition-deadline.js";
import { runCancellableOcrJob } from "../server/recognition/cancellable-ocr.js";
import { isFamilyRetryAllowed } from "../server/recognition/family-retry-policy.js";

const tests = [];
const test = (id, name, fn) => tests.push({ id, name, fn });

function makeBudget({ nowMs = 0, deadlineMs = 55_000, reserveMs = 2_500 } = {}) {
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
  return { budget, controller, setNow(value) { current = value; } };
}

test("B01", "80s cap is clipped to remaining budget minus reserve", () => {
  const { budget } = makeBudget({ nowMs: 15_000 });
  assert.equal(budget.remainingMs(), 40_000);
  assert.equal(budget.effectiveTimeout(80_000), 37_500);
});

test("B02", "10s remaining never yields an 80s timeout", () => {
  const { budget } = makeBudget({ nowMs: 45_000 });
  assert.equal(budget.effectiveTimeout(80_000), 7_500);
});

test("B03", "stage is rejected below minimum usable budget", () => {
  const { budget } = makeBudget({ nowMs: 52_250 });
  assert.equal(budget.canStartStage(500), false);
  assert.throws(() => budget.assertCanContinue({ stageName: "family_retry" }), { code: RECOGNITION_BUDGET_CODE });
});

test("B04", "low-value fallback is rejected at 45s cutoff", () => {
  const { budget } = makeBudget({ nowMs: 45_000 });
  assert.equal(budget.canStartStage(500, { lowValue: true }), false);
  assert.throws(() => budget.assertCanContinue({ stageName: "local_ocr", lowValue: true }), { code: RECOGNITION_BUDGET_CODE });
});

test("B05", "required stage may use only remaining bounded time", () => {
  const { budget } = makeBudget({ nowMs: 40_000 });
  budget.assertCanContinue({ stageName: "verification", minRequiredMs: 500 });
  assert.equal(budget.effectiveTimeout(80_000), 12_500);
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
  assert.equal(context.signal.aborted, true);
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
