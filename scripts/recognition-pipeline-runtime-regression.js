import assert from "node:assert/strict";
import fs from "node:fs";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";
import {
  createRecognitionMetrics,
  finishAttempt,
  markFallback,
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

function assertNoDecisionChange(payload, expectedState) {
  const finalized = buildFinalizedCoordinateVerificationResponse(payload);
  assert.equal(finalized.coordinateResult.state, expectedState);
  assert.deepEqual(finalized.recognitionMetrics, payload.recognitionMetrics);
}

test("server wires main Vision attempt without changing Vision call semantics", () => {
  const source = fs.readFileSync("server.js", "utf8");
  assert.match(source, /startAttempt\(recognitionMetrics,\s*{\s*stage:\s*"main_vision"/s);
  assert.match(source, /provider:\s*"vision"/);
  assert.match(source, /model:\s*aliyunVisionModel/);
  assert.match(source, /timeoutMs:\s*mainVisionTimeoutMs/);
  assert.match(source, /callAliyunVision\(\{\s*modelName:\s*aliyunVisionModel,\s*prompt,\s*imageItems,\s*temperature:\s*0\.1,\s*timeoutMs:\s*mainVisionTimeoutMs/s);
});

test("server wires local OCR fallback and normalized fallback reason", () => {
  const source = fs.readFileSync("server.js", "utf8");
  assert.match(source, /runInstrumentedLocalOcrFallback/);
  assert.match(source, /runInstrumentedLocalOcrFallback\(req\.file\.buffer,\s*errorMessage,\s*"local_ocr_fallback"\)/);
  assert.match(source, /provider:\s*"ocr"/);
  assert.match(source, /model:\s*"local_tesseract"/);
  assert.match(source, /markFallback\(recognitionMetrics,\s*{\s*used:\s*true,\s*type:\s*"local_tesseract",\s*reason:\s*getRecognitionFallbackReason\(error\)/s);
});

test("Vision success metrics are additive and preserve AUTO_EXPORT decision", () => {
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
    resultCount: 6,
    endedAt: 9000
  });
  const recognitionMetrics = sanitizeRecognitionMetricsForResponse(metrics);

  assertNoDecisionChange({
    coordinateType: "wgs84_geographic_table",
    precisionMode: "wgs84-table-coordinates",
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    kml_ready: true,
    requires_review: false,
    recognitionMetrics,
    coordinateArbitration: {
      coordinateType: "wgs84_geographic_table",
      precisionMode: "wgs84-table-coordinates",
      authority: "validated_wgs84",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      reason: "validated_wgs84_table",
      blockedFallbacks: []
    }
  }, "AUTO_EXPORT");

  assert.equal(recognitionMetrics.vision.status, "success");
  assert.equal(recognitionMetrics.fallback.used, false);
});

test("Vision timeout plus local fallback metrics are additive and preserve BLOCKED_REVIEW decision", () => {
  const metrics = createRecognitionMetrics({ startedAt: 1000 });
  const visionAttempt = startAttempt(metrics, {
    stage: "main_vision",
    provider: "vision",
    model: "qwen-vl-plus",
    timeoutMs: 35000,
    startedAt: 1000
  });
  finishAttempt(metrics, visionAttempt, {
    status: "timeout",
    errorCode: "VISION_TIMEOUT",
    endedAt: 36000
  });
  const ocrAttempt = startAttempt(metrics, {
    stage: "local_ocr_fallback",
    provider: "ocr",
    model: "local_tesseract",
    startedAt: 36000
  });
  finishAttempt(metrics, ocrAttempt, {
    status: "success",
    resultCount: 0,
    endedAt: 44000
  });
  markFallback(metrics, {
    used: true,
    type: "local_tesseract",
    reason: "VISION_TIMEOUT"
  });
  const recognitionMetrics = sanitizeRecognitionMetricsForResponse(metrics);

  assertNoDecisionChange({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    requires_review: true,
    recognitionMetrics,
    coordinateArbitration: {
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y-review",
      authority: "explicit_crs_evidence",
      requires_review: true,
      arbitrationEligible: false,
      confirmationStatus: "blocked",
      qualityGateStatus: "blocked",
      kml_allowed: false,
      kml_ready: false,
      lat_lon_role: "verification_only",
      reason: "utm_transformation_verification_failed",
      blockedFallbacks: []
    }
  }, "BLOCKED_REVIEW");

  assert.equal(recognitionMetrics.vision.status, "timeout");
  assert.equal(recognitionMetrics.ocr.status, "success");
  assert.equal(recognitionMetrics.fallback.used, true);
  assert.equal(recognitionMetrics.fallback.reason, "VISION_TIMEOUT");
});

test("runtime metrics sanitization keeps attempts debug-gated", () => {
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
    resultCount: 4,
    endedAt: 2000
  });

  const normal = sanitizeRecognitionMetricsForResponse(metrics);
  const debug = sanitizeRecognitionMetricsForResponse(metrics, { includeAttempts: true });
  assert.equal(normal.attempts, undefined);
  assert.equal(debug.attempts.length, 1);
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Recognition Pipeline Runtime Regression: 5/5 PASS");
