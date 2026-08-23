import assert from "node:assert/strict";

import {
  ACQUISITION_ADAPTER_STATUS,
  ACQUISITION_PROVENANCE,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  createAcquisitionResult,
  createLatencyBudget,
  createNormalizedCoordinateResult,
  mapV3ProductionResult,
  runCoordinateEngineV3,
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_STATUS,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function run(text) {
  return runCoordinateEngineV3({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
}

async function mappedFromText(text, productionMetadata = {}) {
  const runnerResult = await run(text);
  return mapV3ProductionResult({ runnerResult, productionMetadata });
}

function acquisition(candidates = [], overrides = {}) {
  return createAcquisitionResult({
    status: ACQUISITION_STATUS.SUCCESS,
    candidates,
    timing: { totalDurationMs: 10, primaryDurationMs: 10, targetedDurationMs: 0 },
    providerCalls: 1,
    ...overrides,
  });
}

function candidate(overrides = {}) {
  return {
    id: "candidate_1",
    text: "coordinate evidence",
    sourceType: ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE,
    provenance: ACQUISITION_PROVENANCE.PRIMARY,
    confidence: 0.8,
    timing: { durationMs: 10 },
    ...overrides,
  };
}

function normalized(overrides = {}) {
  return createNormalizedCoordinateResult({
    coordinateType: "wgs84_decimal",
    recognizerId: "wgs84_decimal",
    coordinates: [
      { label: "1", latitude: 12.319572, longitude: -11.178174 },
      { label: "2", latitude: 12.32, longitude: -11.18 },
      { label: "3", latitude: 12.318, longitude: -11.182 },
    ],
    geometryType: "polygon",
    crs: "EPSG:4326",
    precisionMode: "decimal-degree",
    ...overrides,
  });
}

function adapterMatched(value = normalized(), overrides = {}) {
  return Object.freeze({
    status: ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT,
    recognizerId: value.recognizerId,
    coordinateType: value.coordinateType,
    normalized: value,
    ...overrides,
  });
}

function assertSuccess(result, owner) {
  assert.equal(result.status, V3_PRODUCTION_STATUS.SUCCESS);
  assert.equal(result.reasonCode, null);
  assert.equal(result.owner, owner);
  assert.equal(result.productionSupported, true);
  assert.equal(result.technicalKmlReady, true);
  assert.equal(result.technicalKmlBlockReason, null);
}

function assertReview(result, reasonCode) {
  assert.equal(result.status, V3_PRODUCTION_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reasonCode, reasonCode);
  assert.equal(result.productionSupported, false);
}

function assertUnsupported(result, reasonCode) {
  assert.equal(result.status, V3_PRODUCTION_STATUS.UNSUPPORTED);
  assert.equal(result.reasonCode, reasonCode);
  assert.equal(result.productionSupported, false);
  assert.equal(result.technicalKmlReady, false);
}

test("SUCCESS maps Côte d’Ivoire DMS", async () => {
  const result = await mappedFromText(`Project coordinates
Point | Latitude Nord | Longitude Ouest
A | 11°52'11.93" | 08°53'32.66"
B | 11°52'17.21" | 08°53'33.18"
C | 11°52'12.57" | 08°53'54.03"`);
  assertSuccess(result, "cote_divoire_dms");
  assert.equal(result.coordinates.length, 3);
});

test("SUCCESS maps Indonesia UTM", async () => {
  const result = await mappedFromText(`SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y
1 | 778807,293 | 9721476,737
2 | 778981,768 | 9721477,288
3 | 778982,700 | 9721182,351`);
  assertSuccess(result, "indonesia_utm");
  assert.equal(result.crs, "EPSG:32750");
});

test("SUCCESS maps WGS84 decimal", async () => {
  const result = await mappedFromText("12.319572, -11.178174");
  assertSuccess(result, "wgs84_decimal");
});

test("SUCCESS maps generic DMS", async () => {
  const result = await mappedFromText("11°27'45\"N 08°36'30\"W");
  assertSuccess(result, "generic_dms");
});

test("SUCCESS maps grouped DMS", async () => {
  const result = await mappedFromText(`Mining Area 1:
1. 11°52'25.72"N, 08°53'13.39"W
2. 11°52'21.27"N, 08°53'11.78"W
3. 11°52'20.00"N, 08°53'28.00"W
4. 11°52'18.00"N, 08°53'25.00"W

Mining Area Two:
1. 11°52'11.93"N, 08°53'32.66"W
2. 11°52'17.21"N, 08°53'33.18"W
3. 11°52'12.57"N, 08°53'54.03"W
4. 11°52'07.65"N, 08°53'53.56"W`);
  assertSuccess(result, "dms_grouped_coordinates");
  assert.equal(result.coordinates.length, 8);
});

test("REVIEW_REQUIRED maps incomplete extraction", () => {
  const result = mapV3ProductionResult({
    normalized: normalized(),
    acquisitionResult: acquisition([
      candidate({
        structuredRows: [{ point: "1" }, { point: "2" }],
        completeness: { expectedRowCount: 3, structuredRowCount: 2 },
      }),
    ], { status: ACQUISITION_STATUS.PARTIAL }),
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.INCOMPLETE_EXTRACTION);
  assert.equal(result.technicalKmlReady, true);
});

test("REVIEW_REQUIRED maps ambiguous recognizer", () => {
  const result = mapV3ProductionResult({
    runnerResult: {
      status: V3_RUNNER_STATUS.AMBIGUOUS,
      candidates: [
        { recognizerId: "generic_dms" },
        { recognizerId: "cote_divoire_dms" },
      ],
    },
    acquisitionResult: acquisition([candidate()]),
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.AMBIGUOUS_RECOGNIZER);
});

test("REVIEW_REQUIRED maps candidate conflict", () => {
  const value = normalized();
  const result = mapV3ProductionResult({
    adapterResult: adapterMatched(value, {
      status: ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT,
    }),
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.CANDIDATE_CONFLICT);
  assert.equal(result.technicalKmlReady, true);
});

test("REVIEW_REQUIRED maps recognizer not available", () => {
  const result = mapV3ProductionResult({
    runnerResult: { status: V3_RUNNER_STATUS.NO_MATCH },
    acquisitionResult: acquisition([candidate({
      text: "historical structure recovered but no V3 recognizer owns it",
      structuredRows: [{ point: "1" }, { point: "2" }],
    })]),
    productionMetadata: {
      recognizerNotAvailable: true,
      meaningfulEvidence: true,
      availableRows: 2,
      expectedRows: 2,
    },
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE);
});

test("REVIEW_REQUIRED maps experimental path success", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({ recognizerId: "indonesia_utm", coordinateType: "indonesia_utm" }),
    productionMetadata: { experimental: true, inputMode: "table_context_composite" },
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED);
  assert.equal(result.technicalKmlReady, true);
});

test("REVIEW_REQUIRED maps timeout with partial data", () => {
  const result = mapV3ProductionResult({
    acquisitionResult: acquisition([], {
      status: ACQUISITION_STATUS.DEADLINE_EXCEEDED,
      warnings: ["PROVIDER_TIMEOUT"],
    }),
    productionMetadata: {
      providerTimeout: true,
      meaningfulEvidence: true,
      availableRows: 15,
    },
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.PROVIDER_TIMEOUT);
});

test("UNSUPPORTED maps no usable candidate", () => {
  const result = mapV3ProductionResult({
    acquisitionResult: acquisition([], { status: ACQUISITION_STATUS.FAILED }),
  });
  assertUnsupported(result, V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE);
});

test("UNSUPPORTED maps no normalized coordinates", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({ coordinates: [] }),
  });
  assertUnsupported(result, V3_PRODUCTION_REASON_CODE.NO_NORMALIZED_COORDINATES);
});

test("UNSUPPORTED maps invalid coordinate structure", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({ coordinates: [{ label: "bad", latitude: "not-a-number", longitude: 1 }] }),
  });
  assertUnsupported(result, V3_PRODUCTION_REASON_CODE.INVALID_COORDINATE_STRUCTURE);
});

test("UNSUPPORTED maps invalid geometry", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({
      geometryType: "polygon",
      coordinates: [
        { label: "1", latitude: 1, longitude: 2 },
        { label: "2", latitude: 3, longitude: 4 },
      ],
    }),
  });
  assertUnsupported(result, V3_PRODUCTION_REASON_CODE.INVALID_GEOMETRY);
});

test("KML policy preserves review technical readiness", () => {
  const result = mapV3ProductionResult({
    normalized: normalized(),
    productionMetadata: { lowConfidence: true },
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.LOW_CONFIDENCE);
  assert.equal(result.technicalKmlReady, true);
  assert.equal(result.technicalKmlBlockReason, null);
});

test("KML policy keeps uncertainty warning-only", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({
      warnings: [{ code: "CRS_UNCERTAINTY", message: "Coordinate reference system requires review." }],
    }),
    productionMetadata: { ambiguousCoordinateSystem: true },
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.AMBIGUOUS_COORDINATE_SYSTEM);
  assert.equal(result.technicalKmlReady, true);
  assert.equal(result.technicalKmlBlockReason, null);
});

test("KML policy hard-stops unsupported technical failure", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({ coordinates: [] }),
  });
  assertUnsupported(result, V3_PRODUCTION_REASON_CODE.NO_NORMALIZED_COORDINATES);
  assert.equal(result.technicalKmlReady, false);
  assert.equal(result.technicalKmlBlockReason, "NO_COORDINATES");
});

test("review-only recognizer scope cannot be production success", () => {
  const result = mapV3ProductionResult({
    normalized: normalized({
      coordinateType: "mgrs",
      recognizerId: "mgrs",
      precisionMode: "mgrs",
    }),
  });
  assertReview(result, V3_PRODUCTION_REASON_CODE.UNVERIFIED_PRODUCTION_SCOPE);
  assert.equal(result.productionScopeStatus, "REVIEW_ONLY");
  assert.equal(result.technicalKmlReady, true);
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

console.log(`Coordinate Engine V3 Production Result Mapper Regression: ${passed}/${tests.length} PASS`);
