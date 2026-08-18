import assert from "node:assert/strict";

import {
  ACQUISITION_ADAPTER_STATUS,
  ACQUISITION_PROVENANCE,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  createAcquisitionBudget,
  createAcquisitionCandidate,
  createAcquisitionResult,
  createLatencyBudget,
  createNormalizedCoordinateResult,
  createRecognizerContract,
  RECOGNIZER_PORT_STATUS,
  runAcquisitionCandidatesThroughRunner,
  shouldRequestTargetedAcquisition,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function candidate(id, text, overrides = {}) {
  return createAcquisitionCandidate({
    id,
    text,
    sourceType: ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE,
    provenance: ACQUISITION_PROVENANCE.PRIMARY,
    confidence: 0.8,
    timing: { durationMs: 5 },
    ...overrides,
  });
}

function acquisition(candidates, overrides = {}) {
  return createAcquisitionResult({
    status: ACQUISITION_STATUS.SUCCESS,
    candidates,
    timing: { totalDurationMs: 10, primaryDurationMs: 10, targetedDurationMs: 0 },
    providerCalls: 1,
    ...overrides,
  });
}

function matchedResult(input, {
  recognizerId = "wgs84_decimal",
  coordinateType = "wgs84_decimal",
  longitude = 2,
  latitude = 1,
  warnings = [],
  suspectedPoints = [],
} = {}) {
  const normalized = createNormalizedCoordinateResult({
    coordinateType,
    recognizerId,
    coordinates: [{ label: "1", longitude, latitude }],
    geometryType: "point",
    crs: "EPSG:4326",
    precisionMode: "mock",
    warnings,
    suspectedPoints,
  });
  return Object.freeze({
    schemaVersion: "coordinate_engine_v3_runner_v1",
    status: V3_RUNNER_STATUS.MATCHED,
    recognizerId,
    coordinateType,
    normalized,
    verification: { verified: true },
    technicalKmlReady: normalized.technicalKmlReady,
    warnings: normalized.warnings,
    suspectedPoints: normalized.suspectedPoints,
    candidates: Object.freeze([{ recognizerId, coordinateType, portStatus: "IMPLEMENTED" }]),
    errors: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

async function runWithActual(candidates) {
  return runAcquisitionCandidatesThroughRunner(acquisition(candidates), {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
}

test("single match", async () => {
  const result = await runWithActual([candidate("wgs84", "12.319572, -11.178174")]);
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT);
  assert.equal(result.recognizerId, "wgs84_decimal");
});

test("zero match", async () => {
  const result = await runWithActual([candidate("garbage", "not coordinates")]);
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH);
});

test("candidate ambiguous", async () => {
  const mockA = createRecognizerContract({
    recognizerId: "ambiguous_a",
    coordinateType: "wgs84_decimal",
    portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
    canHandle: () => true,
  });
  const mockB = createRecognizerContract({
    recognizerId: "ambiguous_b",
    coordinateType: "wgs84_decimal",
    portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
    canHandle: () => true,
  });
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([candidate("ambiguous", "anything")]), {
    registry: [mockA, mockB],
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.AMBIGUOUS_RECOGNIZER_MATCH);
});

test("same-owner equivalent merge", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("a", "first"),
    candidate("b", "second"),
  ]), {
    runner: async (input) => matchedResult(input, { recognizerId: "same_owner", coordinateType: "wgs84_decimal" }),
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT);
  assert.deepEqual(result.mergedCandidateIds, ["a", "b"]);
});

test("same-owner conflicting output", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("a", "first"),
    candidate("b", "second"),
  ]), {
    runner: async (input) => matchedResult(input, {
      recognizerId: "same_owner",
      coordinateType: "wgs84_decimal",
      longitude: input.acquisitionCandidateId === "a" ? 2 : 3,
    }),
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT);
  assert.equal(result.conflicts.length, 2);
});

test("cross-type conflict", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("a", "first"),
    candidate("b", "second"),
  ]), {
    runner: async (input) => matchedResult(input, input.acquisitionCandidateId === "a"
      ? { recognizerId: "wgs84_table", coordinateType: "wgs84_table" }
      : { recognizerId: "indonesia_utm", coordinateType: "indonesia_utm" }),
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT);
});

test("exact duplicate candidate does not rerun", async () => {
  let calls = 0;
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("a", "12.319572, -11.178174"),
    candidate("b", " 12.319572,   -11.178174 "),
  ]), {
    runner: async (input) => {
      calls += 1;
      return matchedResult(input);
    },
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT);
  assert.equal(calls, 1);
  assert.equal(result.metrics.candidateCount, 2);
  assert.equal(result.metrics.dedupedCandidateCount, 1);
});

test("highest confidence does not win", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("low", "first", { confidence: 0.1 }),
    candidate("high", "second", { confidence: 0.99 }),
  ]), {
    runner: async (input) => matchedResult(input, {
      longitude: input.acquisitionCandidateId === "low" ? 2 : 9,
    }),
  });
  assert.equal(result.status, ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT);
});

test("WGS84 decimal ownership", async () => {
  const result = await runWithActual([candidate("wgs84_decimal", "12.319572, -11.178174")]);
  assert.equal(result.recognizerId, "wgs84_decimal");
});

test("WGS84 table ownership", async () => {
  const result = await runWithActual([candidate("wgs84_table", "Longitude | Latitude\n16.0320 | 3.7638")]);
  assert.equal(result.recognizerId, "wgs84_table");
});

test("MGRS ownership", async () => {
  const result = await runWithActual([candidate("mgrs", "47RLH 24469 42832")]);
  assert.equal(result.recognizerId, "mgrs");
});

test("generic DMS ownership", async () => {
  const result = await runWithActual([candidate("generic_dms", "11°27'45\"N 08°36'30\"W")]);
  assert.equal(result.recognizerId, "generic_dms");
});

test("Kyrgyz ownership", async () => {
  const result = await runWithActual([candidate("kyrgyz", "№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780")]);
  assert.equal(result.recognizerId, "kyrgyzstan_gauss_kruger");
});

test("Madagascar ownership", async () => {
  const result = await runWithActual([candidate("madagascar", "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280\n2 | 292812,5 | 361562,5 | Ilakaka | 281\n3 | 292812,5 | 362187,5 | Ilakaka | 282")]);
  assert.equal(result.recognizerId, "madagascar_cadastral");
});

test("Côte d'Ivoire ownership", async () => {
  const result = await runWithActual([candidate("cote", "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"")]);
  assert.equal(result.recognizerId, "cote_divoire_dms");
});

test("Indonesia ownership", async () => {
  const result = await runWithActual([candidate("indonesia", "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E")]);
  assert.equal(result.recognizerId, "indonesia_utm");
});

test("standard ambiguity=0", async () => {
  const fixtures = [
    candidate("wgs84_decimal", "12.319572, -11.178174"),
    candidate("wgs84_table", "Longitude | Latitude\n16.0320 | 3.7638"),
    candidate("mgrs", "47RLH 24469 42832"),
    candidate("generic_dms", "11°27'45\"N 08°36'30\"W"),
    candidate("kyrgyz", "№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780"),
    candidate("madagascar", "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280\n2 | 292812,5 | 361562,5 | Ilakaka | 281\n3 | 292812,5 | 362187,5 | Ilakaka | 282"),
    candidate("cote", "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\""),
    candidate("indonesia", "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E"),
  ];
  for (const item of fixtures) {
    const result = await runWithActual([item]);
    assert.notEqual(result.status, ACQUISITION_ADAPTER_STATUS.AMBIGUOUS_RECOGNIZER_MATCH);
  }
});

test("targeted false on clean match", () => {
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([candidate("clean", "x")]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
});

test("targeted false on warning match", () => {
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([candidate("warning", "x")]),
    adapterResult: {
      status: ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT,
      normalized: createNormalizedCoordinateResult({
        coordinateType: "indonesia_utm",
        recognizerId: "indonesia_utm",
        coordinates: [{ label: "1", longitude: 119, latitude: -2 }],
        warnings: [{ code: "UTM_REFERENCE_MISMATCH" }],
      }),
    },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
});

test("targeted false on P13 mismatch", () => {
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([candidate("p13", "x")]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT, mismatchLabels: ["13"] },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
});

test("targeted false on P4 mismatch", () => {
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([candidate("p4", "x")]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT, mismatchLabels: ["4"] },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
});

test("targeted false on garbage NO_MATCH", () => {
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([candidate("garbage", "not coordinates")]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
});

test("targeted true on incomplete structured NO_MATCH", () => {
  const incomplete = candidate("incomplete", "No | X | Y", {
    sourceType: "table",
    structuredRows: [{ label: "1" }, { label: "2" }, { label: "3" }],
    headers: ["No.", "X", "Y"],
    expectedRowCount: 16,
    truncated: true,
  });
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([incomplete]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, true);
  assert.equal(decision.candidateId, "incomplete");
});

test("targeted false if provider limit reached", () => {
  const incomplete = candidate("incomplete", "No | X | Y", {
    sourceType: "table",
    structuredRows: [{ label: "1" }],
    headers: ["No.", "X", "Y"],
    expectedRowCount: 16,
    truncated: true,
  });
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([incomplete], { providerCalls: 2 }),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH },
    budget: createAcquisitionBudget({ providerCalls: 2, startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(decision.targeted, false);
  assert.equal(decision.reason, "PROVIDER_CALL_LIMIT_EXCEEDED");
});

test("targeted false if deadline insufficient", () => {
  const incomplete = candidate("incomplete", "No | X | Y", {
    sourceType: "table",
    structuredRows: [{ label: "1" }],
    headers: ["No.", "X", "Y"],
    expectedRowCount: 16,
    truncated: true,
  });
  const decision = shouldRequestTargetedAcquisition({
    acquisitionResult: acquisition([incomplete]),
    adapterResult: { status: ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH },
    budget: createAcquisitionBudget({ startedAtMs: 0, clock: () => 59950 }),
    minimumMs: 1000,
  });
  assert.equal(decision.targeted, false);
  assert.equal(decision.reason, "ACQUISITION_DEADLINE_INSUFFICIENT");
});

test("metrics", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("matched", "12.319572, -11.178174"),
    candidate("no_match", "not coordinates"),
  ]), {
    runner: async (input) => input.acquisitionCandidateId === "matched"
      ? matchedResult(input)
      : { status: V3_RUNNER_STATUS.NO_MATCH, candidates: [] },
  });
  assert.equal(result.metrics.candidateCount, 2);
  assert.equal(result.metrics.runnerMatchedCount, 1);
  assert.equal(result.metrics.runnerNoMatchCount, 1);
});

test("security summary remains sanitized", async () => {
  const result = await runAcquisitionCandidatesThroughRunner(acquisition([
    candidate("secure", "12.319572, -11.178174", {
      rawProviderResponse: "raw",
      filesystemPath: "C:/secret.png",
    }),
  ]));
  assert.equal(JSON.stringify(result).includes("secret"), false);
  assert.equal(JSON.stringify(result).includes("rawProviderResponse"), false);
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

console.log(`Coordinate Engine V3 Acquisition Adapter Regression: ${passed}/${tests.length} PASS`);
