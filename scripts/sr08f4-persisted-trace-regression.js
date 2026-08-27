import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import {
  RecognitionBudget,
  activateRecognitionDeadlineContext
} from "../server/coordinate-finalizer/recognition-deadline.js";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";
import {
  computeFixtureSetFingerprint,
  computeProductionSourceFingerprint,
  computeReleaseGovernanceFingerprint,
  validateReleaseEvidenceBinding
} from "../release-governance/evidence-binding.js";
import { buildEvidenceCase, writeEvidenceArtifact } from "./coordinate-regression-runner.js";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(__filename), "..");
const results = [];

async function test(id, description, fn) {
  try {
    await fn();
    results.push({ id, status: "PASS" });
    console.log(`PASS ${id} ${description}`);
  } catch (error) {
    results.push({ id, status: "FAIL", error: error.message });
    console.error(`FAIL ${id} ${description}: ${error.stack || error.message}`);
  }
}

function makeBudget({ caseId = "deterministic_case_001", trace = false } = {}) {
  let now = 1_000_000;
  const budget = new RecognitionBudget({
    startedAt: now,
    deadlineMs: 55_000,
    caseId,
    trace,
    now: () => now
  });
  return {
    budget,
    advance(ms) { now += ms; }
  };
}

function buildCompleteTrace() {
  const clock = makeBudget();
  const stageNames = [
    "upload", "pre_route", "generic_provider", "local_ocr", "handwritten_retry",
    "wgs84_retry", "mgrs_retry", "kyrgyz_retry", "cadastral_layout", "cadastral_grid",
    "cote_divoire_retry", "family_retry", "parser", "crs", "geometry", "verification", "finalizer"
  ];
  for (const [index, stageName] of stageNames.entries()) {
    const stage = clock.budget.stageStarted(stageName, {
      attempt: index + 1,
      configuredTimeoutMs: 35_000,
      effectiveTimeoutMs: 20_000
    });
    clock.advance(2);
    clock.budget.stageCompleted(stage);
  }
  clock.budget.markResponseSent({ httpStatus: 200, responseCode: "OK" });
  clock.advance(1);
  clock.budget.markHandlerCompleted();
  return clock.budget.toSanitizedTrace();
}

function deterministicAuthorityResult(traceEnabled) {
  const clock = makeBudget({ caseId: `authority_${traceEnabled ? "on" : "off"}`, trace: traceEnabled });
  const context = {
    budget: clock.budget,
    signal: new AbortController().signal,
    startedAt: 1_000_000,
    deadlineAt: 1_055_000,
    deadlineMs: 55_000
  };
  activateRecognitionDeadlineContext({ recognitionDeadlineContext: context });
  const payload = {
    precisionMode: "wgs84-table-coordinates",
    coordinates: "",
    rawText: ""
  };
  const engine = {
    coordinate_type: "decimal_latlon",
    precision_mode: "wgs84-table-coordinates",
    requires_review: false,
    kml_ready: true,
    groups: [{
      geometry: "polygon",
      requires_review: false,
      validation: { status: "scored" },
      points: [
        { lat: 10, lon: 20 },
        { lat: 10, lon: 21 },
        { lat: 11, lon: 21 },
        { lat: 10, lon: 20 }
      ]
    }]
  };
  const result = buildCoordinateVerificationResponse(payload, engine);
  return {
    decisionState: result.finalizedCoordinateResult.decisionState,
    confirmationStatus: result.finalizedCoordinateResult.confirmationStatus,
    qualityGateStatus: result.finalizedCoordinateResult.qualityGateStatus,
    kmlReady: result.finalizedCoordinateResult.kmlReady,
    geometryHash: result.finalizedCoordinateResult.geometryHash
  };
}

const trace = buildCompleteTrace();

await test("E01", "requestId generated", () => {
  assert.match(trace.requestId, /^recognition_/);
  assert.notEqual(makeBudget().budget.requestId, makeBudget().budget.requestId);
});
await test("E02", "caseId propagated", () => assert.equal(trace.caseId, "deterministic_case_001"));
await test("E03", "caseId requestId mapping persists", () => {
  const evidenceCase = buildEvidenceCase({
    sample: { sample_id: trace.caseId },
    runs: [{ actual: { requestId: trace.requestId, stageTrace: trace }, semantics: {} }]
  });
  assert.equal(evidenceCase.caseId, trace.caseId);
  assert.equal(evidenceCase.attempts[0].requestId, trace.requestId);
  assert.equal(evidenceCase.attempts[0].stageTrace.caseId, trace.caseId);
});
await test("E04", "stage start end duration persisted", () => {
  assert.ok(trace.stages.every(stage => Number.isFinite(stage.stageStartElapsedMs)));
  assert.ok(trace.stages.every(stage => Number.isFinite(stage.stageEndElapsedMs)));
  assert.ok(trace.stages.every(stage => Number.isFinite(stage.durationMs)));
});
await test("E05", "configured and effective timeout persisted", () => {
  const provider = trace.stages.find(stage => stage.stageName === "generic_provider");
  assert.equal(provider.configuredTimeoutMs, 35_000);
  assert.equal(provider.effectiveTimeoutMs, 20_000);
});
await test("E06", "remaining budget persisted", () => {
  assert.ok(trace.stages.every(stage => Number.isFinite(stage.remainingBudgetAtStartMs)));
  assert.ok(trace.stages.every(stage => Number.isFinite(stage.remainingBudgetAtEndMs)));
});
await test("E07", "skipped stage reason persisted", () => {
  const clock = makeBudget();
  clock.advance(46_000);
  assert.throws(() => clock.budget.assertCanContinue({ stageName: "local_ocr", lowValue: true }));
  const skipped = clock.budget.toSanitizedTrace().stages.at(-1);
  assert.equal(skipped.result, "budget_exhausted");
  assert.equal(skipped.skippedReason, "soft_fallback_cutoff");
});
await test("E08", "response timing persisted", () => {
  assert.equal(trace.response.httpStatus, 200);
  assert.equal(trace.response.responseCode, "OK");
  assert.ok(Number.isFinite(trace.response.responseElapsedMs));
});
await test("E09", "handler completion persisted", () => {
  assert.ok(Number.isFinite(trace.handler.handlerCompletedElapsedMs));
  assert.equal(trace.handler.handlerCompletionDeltaMs, 1);
});
await test("E10", "post response stage count calculated", () => {
  assert.equal(trace.postResponseStageCount, 0);
  assert.equal(trace.postDeadlineWorkStatus, "PROVEN_NONE");
});

const [sourceFingerprint, governanceFingerprint, fixtureFingerprint] = await Promise.all([
  computeProductionSourceFingerprint(repoRoot),
  computeReleaseGovernanceFingerprint(repoRoot),
  computeFixtureSetFingerprint(repoRoot)
]);
const binding = await validateReleaseEvidenceBinding({
  repoRoot,
  runtimeIdentity: { runtimeSourceSha256: sourceFingerprint.hash },
  frozenIdentity: {
    productionSourceHash: sourceFingerprint.hash,
    releaseGovernanceHash: governanceFingerprint.hash,
    fixtureSetHash: fixtureFingerprint.hash
  }
});

await test("E11", "source hash correct", () => assert.equal(binding.productionSourceHash, sourceFingerprint.hash));
await test("E12", "governance hash correct", () => assert.equal(binding.releaseGovernanceHash, governanceFingerprint.hash));
await test("E13", "fixture hash correct", () => assert.equal(binding.fixtureSetHash, fixtureFingerprint.hash));
await test("E14", "mismatched hash fail closed", async () => {
  await assert.rejects(validateReleaseEvidenceBinding({
    repoRoot,
    runtimeIdentity: { runtimeSourceSha256: sourceFingerprint.hash },
    frozenIdentity: {
      productionSourceHash: "0".repeat(64),
      releaseGovernanceHash: governanceFingerprint.hash,
      fixtureSetHash: fixtureFingerprint.hash
    }
  }), { code: "EVIDENCE_BINDING_MISMATCH" });
});

const tempRoot = await mkdtemp(path.join(os.tmpdir(), "sr08f4-trace-"));
const priorArtifactPath = process.env.COORDINATE_REGRESSION_ARTIFACT_PATH;
const artifactPath = path.join(tempRoot, "evidence.json");
process.env.COORDINATE_REGRESSION_ARTIFACT_PATH = artifactPath;
const mockResult = {
  sample: { sample_id: trace.caseId },
  runs: [{
    actual: {
      requestId: trace.requestId,
      stageTrace: trace,
      httpStatus: 503,
      durationMs: 52_000,
      responseCode: "RECOGNITION_BUDGET_EXHAUSTED",
      responseReason: "insufficient_remaining_budget",
      finalizerEvaluated: false,
      providerRawEvidenceAvailable: false,
      normalizedEvidenceAvailable: false
    },
    semantics: {}
  }]
};
const mockSummary = {
  safetyStatus: "PASS",
  truthStatus: "NOT_EVALUATED",
  policyStatus: "NOT_EVALUATED",
  reliabilityStatus: "FAIL",
  governanceStatus: "PASS",
  liveCoordinateGoldenStatus: "FAIL",
  truthEvaluatedCount: 0,
  truthNotEvaluatedCount: 1,
  policyEvaluatedCount: 0,
  policyNotEvaluatedCount: 1,
  timeoutCount: 0
};
await writeEvidenceArtifact([mockResult], mockSummary, { approval: { source: "TEST" } }, binding);
const artifactText = await readFile(artifactPath, "utf8");
const artifact = JSON.parse(artifactText);

await test("E15", "raw OCR absent", () => {
  assert.equal(artifactText.includes("rawText"), false);
  assert.equal(artifactText.includes("recognizedText"), false);
});
await test("E16", "coordinate values absent", () => {
  assert.equal(artifactText.includes("10,20"), false);
  assert.equal(/"(?:lat|lon|x|y)"\s*:/.test(artifactText), false);
});
await test("E17", "Geometry absent", () => assert.equal(/"geometry"\s*:/.test(artifactText), false));
await test("E18", "secret leakage zero", () => {
  for (const pattern of ["Authorization", "ALIYUN_API_KEY", "DASHSCOPE_API_KEY", "Bearer ", "apiKey", "cookie"]) {
    assert.equal(artifactText.toLowerCase().includes(pattern.toLowerCase()), false);
  }
});
await test("E19", "trace does not affect Finalizer result", () => {
  const originalLog = console.log;
  console.log = () => {};
  try {
    assert.deepEqual(deterministicAuthorityResult(true), deterministicAuthorityResult(false));
  } finally {
    console.log = originalLog;
  }
});
await test("E20", "trace does not affect KML permission", () => {
  const originalLog = console.log;
  console.log = () => {};
  try {
    assert.equal(deterministicAuthorityResult(true).kmlReady, deterministicAuthorityResult(false).kmlReady);
  } finally {
    console.log = originalLog;
  }
});

await test("PERF01", "trace overhead acceptable", () => {
  const originalLog = console.log;
  console.log = () => {};
  const started = performance.now();
  try {
    const clock = makeBudget({ trace: true });
    for (let index = 0; index < 1_000; index += 1) {
      const stage = clock.budget.stageStarted("parser");
      clock.advance(1);
      clock.budget.stageCompleted(stage);
    }
    clock.budget.toSanitizedTrace();
  } finally {
    console.log = originalLog;
  }
  assert.ok(performance.now() - started < 1_000);
});

if (priorArtifactPath === undefined) delete process.env.COORDINATE_REGRESSION_ARTIFACT_PATH;
else process.env.COORDINATE_REGRESSION_ARTIFACT_PATH = priorArtifactPath;
await rm(tempRoot, { recursive: true, force: true });

const failed = results.filter(result => result.status === "FAIL");
console.log(`\nSR-08F.4 deterministic regression: ${results.length - failed.length}/${results.length} PASS`);
console.log(`TRACE_OVERHEAD_ACCEPTABLE=${failed.some(result => result.id === "PERF01") ? "false" : "true"}`);
console.log(`SECRET_LEAKAGE=${failed.some(result => result.id === "E18") ? "UNKNOWN" : "0"}`);
if (failed.length) process.exitCode = 1;

