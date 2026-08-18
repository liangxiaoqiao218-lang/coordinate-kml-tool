import assert from "node:assert/strict";

import {
  ACQUISITION_PROVENANCE,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  acquisitionCandidateToRunnerInput,
  calculateCandidateCompleteness,
  createAcquisitionBudget,
  createAcquisitionCandidate,
  createAcquisitionResult,
  dedupeAcquisitionCandidates,
  stripSensitiveAcquisitionMetadata,
  validateAcquisitionCandidate,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function assertThrowsInvalidCandidate(value) {
  assert.throws(() => createAcquisitionCandidate(value), /Invalid acquisition candidate/);
}

function makeCandidate(overrides = {}) {
  return createAcquisitionCandidate({
    id: "fixture",
    text: "12.319572, -11.178174",
    sourceType: ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE,
    provenance: ACQUISITION_PROVENANCE.PRIMARY,
    confidence: 0.9,
    timing: { durationMs: 10 },
    ...overrides,
  });
}

test("candidate valid contract", () => {
  const candidate = makeCandidate();
  assert.equal(candidate.schemaVersion, "coordinate_engine_v3_acquisition_candidate_v1");
  assert.equal(candidate.sourceType, "whole_image");
  assert.equal(candidate.provenance, "primary");
});

test("authority fields rejected", () => {
  const validation = validateAcquisitionCandidate({
    text: "x",
    sourceType: "whole_image",
    provenance: "primary",
    recognizerId: "indonesia_utm",
  });
  assert.equal(validation.valid, false);
  assert.deepEqual(validation.authorityFields, ["recognizerId"]);
  assertThrowsInvalidCandidate({
    text: "x",
    sourceType: "whole_image",
    provenance: "primary",
    coordinateType: "indonesia_utm",
  });
});

test("sourceType enum", () => {
  assert.deepEqual(Object.values(ACQUISITION_SOURCE_TYPE), [
    "whole_image",
    "table",
    "text_block",
    "coordinate_block",
    "targeted_region",
  ]);
  assertThrowsInvalidCandidate({ text: "x", sourceType: "indonesia_table", provenance: "primary" });
});

test("provenance enum", () => {
  assert.deepEqual(Object.values(ACQUISITION_PROVENANCE), ["primary", "targeted", "local_fallback"]);
  assertThrowsInvalidCandidate({ text: "x", sourceType: "whole_image", provenance: "verified" });
});

test("confidence bounds", () => {
  assert.equal(makeCandidate({ confidence: 2 }).confidence, 1);
  assert.equal(makeCandidate({ confidence: -1 }).confidence, 0);
  assertThrowsInvalidCandidate({ text: "x", sourceType: "whole_image", provenance: "primary", confidence: "high" });
});

test("timing sanitize", () => {
  const candidate = makeCandidate({ timing: { durationMs: -100 } });
  const result = createAcquisitionResult({
    status: ACQUISITION_STATUS.SUCCESS,
    candidates: [candidate],
    timing: { totalDurationMs: -1, primaryDurationMs: "25", targetedDurationMs: null },
    providerCalls: 1,
  });
  assert.equal(candidate.timing.durationMs, 0);
  assert.equal(result.timing.totalDurationMs, 0);
  assert.equal(result.timing.primaryDurationMs, 25);
});

test("cropRegion sanitize", () => {
  const candidate = makeCandidate({
    cropRegion: { x: -5, y: 10, width: 300, height: 200, coordinateSpace: "pixel", filesystemPath: "C:/secret" },
  });
  assert.deepEqual(candidate.cropRegion, {
    x: 0,
    y: 10,
    width: 300,
    height: 200,
    coordinateSpace: "pixel",
  });
});

test("acquisition SUCCESS", () => {
  const result = createAcquisitionResult({ status: ACQUISITION_STATUS.SUCCESS, candidates: [makeCandidate()], providerCalls: 1 });
  assert.equal(result.status, "SUCCESS");
});

test("acquisition PARTIAL", () => {
  const result = createAcquisitionResult({ status: ACQUISITION_STATUS.PARTIAL, candidates: [makeCandidate()], providerCalls: 1 });
  assert.equal(result.status, "PARTIAL");
});

test("acquisition FAILED", () => {
  const result = createAcquisitionResult({ status: ACQUISITION_STATUS.FAILED, candidates: [], providerCalls: 1 });
  assert.equal(result.status, "FAILED");
});

test("acquisition DEADLINE_EXCEEDED", () => {
  const result = createAcquisitionResult({ status: ACQUISITION_STATUS.DEADLINE_EXCEEDED, candidates: [], providerCalls: 1 });
  assert.equal(result.status, "DEADLINE_EXCEEDED");
});

test("provider limit 2", () => {
  const result = createAcquisitionResult({ status: ACQUISITION_STATUS.SUCCESS, candidates: [makeCandidate()], providerCalls: 2 });
  assert.equal(result.providerCalls, 2);
  assert.throws(() => createAcquisitionResult({ status: ACQUISITION_STATUS.SUCCESS, candidates: [], providerCalls: 3 }), /provider_call_limit_exceeded/);
});

test("third provider rejected", () => {
  const budget = createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 });
  const first = budget.recordProviderCall();
  const second = first.budget.recordProviderCall();
  const third = second.budget.recordProviderCall();
  assert.equal(first.accepted, true);
  assert.equal(second.accepted, true);
  assert.equal(third.accepted, false);
  assert.equal(third.reason, "PROVIDER_CALL_LIMIT_EXCEEDED");
});

test("sensitive fields stripped", () => {
  const candidate = makeCandidate({
    apiKey: "secret",
    rawPrompt: "prompt",
    rawProviderResponse: "raw",
    base64: "AAAA",
    filesystemPath: "C:/hidden/file.png",
    structuredRows: [{ label: "1", value: "ok", credentials: "secret" }],
  });
  assert.equal(Object.hasOwn(candidate, "apiKey"), false);
  assert.equal(Object.hasOwn(candidate, "rawPrompt"), false);
  assert.equal(Object.hasOwn(candidate.structuredRows[0], "credentials"), false);
});

test("candidate to runner input preserves neutral data", () => {
  const candidate = makeCandidate({
    structuredRows: [{ label: "1", latitude: "3.7638", longitude: "16.0320" }],
    headers: ["Longitude", "Latitude"],
    documentCues: ["WGS84"],
  });
  const runnerInput = acquisitionCandidateToRunnerInput(candidate);
  assert.equal(runnerInput.text, "12.319572, -11.178174");
  assert.equal(runnerInput.tableRows.length, 1);
  assert.deepEqual(runnerInput.headers, ["Longitude", "Latitude"]);
  assert.deepEqual(runnerInput.documentCues, ["WGS84"]);
});

test("exact candidate dedupe", () => {
  const a = makeCandidate({ id: "a", text: "12.319572, -11.178174" });
  const b = makeCandidate({ id: "b", text: " 12.319572,   -11.178174 " });
  const deduped = dedupeAcquisitionCandidates([a, b]);
  assert.equal(deduped.candidates.length, 1);
  assert.deepEqual(deduped.duplicateCandidateIds, ["b"]);
});

test("no fuzzy dedupe", () => {
  const a = makeCandidate({ id: "a", text: "12.319572, -11.178174" });
  const b = makeCandidate({ id: "b", text: "12.319573, -11.178174" });
  const deduped = dedupeAcquisitionCandidates([a, b]);
  assert.equal(deduped.candidates.length, 2);
});

test("incomplete structured candidate", () => {
  const candidate = makeCandidate({
    id: "incomplete",
    sourceType: "table",
    structuredRows: [{ label: "1" }, { label: "2" }, { label: "3" }],
    headers: ["No.", "X", "Y"],
    expectedRowCount: 16,
    truncated: true,
  });
  const completeness = calculateCandidateCompleteness(candidate);
  assert.equal(completeness.incompleteStructuredCandidate, true);
  assert.equal(completeness.structuredRowCount, 3);
});

test("complete structured candidate", () => {
  const candidate = makeCandidate({
    sourceType: "table",
    structuredRows: [{ label: "1" }, { label: "2" }],
    headers: ["Longitude", "Latitude"],
    expectedRowCount: 2,
  });
  assert.equal(calculateCandidateCompleteness(candidate).incompleteStructuredCandidate, false);
});

test("metadata strip nested", () => {
  const sanitized = stripSensitiveAcquisitionMetadata({
    provider: "mock",
    rawProviderResponse: "raw",
    nested: { apiKey: "secret", durationMs: 20 },
  });
  assert.equal(sanitized.provider, "mock");
  assert.equal(Object.hasOwn(sanitized, "rawProviderResponse"), false);
  assert.equal(Object.hasOwn(sanitized.nested, "apiKey"), false);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.fn();
    passed += 1;
    console.log(`PASS ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.name}`);
    throw error;
  }
}

console.log(`Coordinate Engine V3 Acquisition Contract Regression: ${passed}/${tests.length} PASS`);
