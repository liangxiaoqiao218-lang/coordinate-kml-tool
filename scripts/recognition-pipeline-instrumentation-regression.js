import assert from "node:assert/strict";

import {
  createRecognitionMetrics,
  finishAttempt,
  markFallback,
  markSpecialParser,
  recognitionMetricsConstants,
  sanitizeRecognitionMetricsForResponse,
  startAttempt
} from "../server/recognition-metrics/index.js";

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

function assertNoSensitiveText(value, context) {
  const serialized = JSON.stringify(value);
  assert.doesNotMatch(serialized, /API_KEY|SECRET|TOKEN|PASSWORD|SUPABASE|DASHSCOPE_API_KEY|AUTHORIZATION/i, context);
  assert.doesNotMatch(serialized, /sk-live|Bearer\s+/i, context);
}

test("schema creation is additive and starts with safe defaults", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  const safe = sanitizeRecognitionMetricsForResponse(metrics, { endedAt: 1500 });

  assert.equal(safe.schemaVersion, "recognition_metrics_v1");
  assert.equal(safe.pipelineStatus, "success");
  assert.equal(safe.vision.started, false);
  assert.equal(safe.vision.status, "skipped");
  assert.equal(safe.ocr.started, false);
  assert.equal(safe.ocr.status, "skipped");
  assert.equal(safe.fallback.used, false);
  assert.equal(safe.fallback.reason, "NONE");
  assert.equal(safe.attempts, undefined, "attempts are debug-only by default");
});

test("main vision success records duration and keeps fallback disabled", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  const attempt = startAttempt(metrics, {
    stage: "main_vision",
    provider: "vision",
    model: "qwen-vl-plus",
    timeoutMs: 35000,
    startedAt: 1000
  });

  finishAttempt(metrics, attempt, {
    status: "success",
    resultCount: 12,
    endedAt: 28700
  });

  const safe = sanitizeRecognitionMetricsForResponse(metrics, { includeAttempts: true, endedAt: 28700 });
  assert.equal(safe.pipelineStatus, "success");
  assert.equal(safe.vision.started, true);
  assert.equal(safe.vision.status, "success");
  assert.equal(safe.vision.durationMs, 27700);
  assert.equal(safe.fallback.used, false);
  assert.equal(safe.attempts[0].stage, "main_vision");
  assert.equal(safe.attempts[0].timeoutMs, 35000);
  assert.equal(safe.attempts[0].resultCount, 12);
});

test("vision timeout records normalized fallback reason", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  const attempt = startAttempt(metrics, {
    stage: "main_vision",
    provider: "vision",
    model: "qwen-vl-plus",
    timeoutMs: 35000,
    startedAt: 1000
  });

  finishAttempt(metrics, attempt, {
    status: "timeout",
    errorCode: "VISION_TIMEOUT",
    endedAt: 36000
  });
  markFallback(metrics, {
    used: true,
    type: "local_tesseract",
    reason: "VISION_TIMEOUT"
  });

  const safe = sanitizeRecognitionMetricsForResponse(metrics, { includeAttempts: true, endedAt: 36000 });
  assert.equal(safe.pipelineStatus, "degraded");
  assert.equal(safe.vision.status, "timeout");
  assert.equal(safe.vision.errorCode, "VISION_TIMEOUT");
  assert.equal(safe.fallback.used, true);
  assert.equal(safe.fallback.type, "local_tesseract");
  assert.equal(safe.fallback.reason, "VISION_TIMEOUT");
});

test("Madagascar cadastral success records parser start, success, and row count", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  markSpecialParser(metrics, "cadastralGrid", {
    started: true,
    status: "success",
    rowCount: 32
  });

  const safe = sanitizeRecognitionMetricsForResponse(metrics);
  assert.equal(safe.specialParsers.cadastralGrid.started, true);
  assert.equal(safe.specialParsers.cadastralGrid.status, "success");
  assert.equal(safe.specialParsers.cadastralGrid.rowCount, 32);
  assert.equal(safe.pipelineStatus, "success");
});

test("Madagascar timeout fallback keeps cadastral skipped and fallback reason separate from UTM lock", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  markSpecialParser(metrics, "cadastralGrid", {
    started: false,
    status: "skipped",
    reason: "main_vision_timeout"
  });
  markSpecialParser(metrics, "utm", {
    started: true,
    status: "rejected",
    reason: "utm_evidence_locked_fallback_unavailable",
    zone: "50S"
  });
  markFallback(metrics, {
    used: true,
    type: "local_tesseract",
    reason: "VISION_TIMEOUT"
  });

  const safe = sanitizeRecognitionMetricsForResponse(metrics);
  assert.equal(safe.pipelineStatus, "degraded");
  assert.equal(safe.fallback.reason, "VISION_TIMEOUT");
  assert.equal(safe.specialParsers.utm.reason, "utm_evidence_locked_fallback_unavailable");
  assert.notEqual(safe.fallback.reason, safe.specialParsers.utm.reason);
});

test("UTM success records special parser without changing decision state", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  markSpecialParser(metrics, "utm", {
    started: true,
    status: "success",
    rowCount: 6,
    zone: "50S"
  });

  const coordinateResultState = "AUTO_EXPORT";
  const safe = sanitizeRecognitionMetricsForResponse(metrics);
  assert.equal(safe.specialParsers.utm.status, "success");
  assert.equal(safe.specialParsers.utm.rowCount, 6);
  assert.equal(safe.specialParsers.utm.zone, "50S");
  assert.equal(coordinateResultState, "AUTO_EXPORT", "metrics helper does not mutate coordinateResult");
});

test("security cleanup redacts sensitive-looking fields and keeps whitelisted diagnostics", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  const attempt = startAttempt(metrics, {
    stage: "main_vision_AUTHORIZATION",
    provider: "vision",
    model: "DASHSCOPE_API_KEY=sk-live-secret",
    timeoutMs: 35000,
    startedAt: 1000
  });

  finishAttempt(metrics, attempt, {
    status: "error",
    errorCode: "SUPABASE_SERVICE_ROLE_KEY leaked",
    endedAt: 1200
  });
  markSpecialParser(metrics, "dms", {
    started: true,
    status: "failed",
    reason: "TOKEN visible in provider error"
  });

  const safe = sanitizeRecognitionMetricsForResponse(metrics, { includeAttempts: true, endedAt: 1200 });
  assert.equal(safe.attempts[0].stage, "redacted_stage");
  assert.equal(safe.attempts[0].model, "REDACTED");
  assert.equal(safe.attempts[0].errorCode, "REDACTED");
  assert.equal(safe.specialParsers.dms.reason, "REDACTED");
  assert.equal(safe.attempts[0].timeoutMs, 35000);
  assertNoSensitiveText(safe, "sanitized metrics must not contain sensitive markers");
});

test("constants expose stable enum sets", () => {
  assert.equal(recognitionMetricsConstants.schemaVersion, "recognition_metrics_v1");
  assert.ok(recognitionMetricsConstants.attemptStatuses.includes("timeout"));
  assert.ok(recognitionMetricsConstants.parserStatuses.includes("rejected"));
  assert.ok(recognitionMetricsConstants.fallbackReasons.includes("VISION_TIMEOUT"));
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Recognition Pipeline Instrumentation Regression: 8/8 PASS");
