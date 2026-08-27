import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  acceptCoordinateRevision,
  composeAbortSignals,
  consumeFinalizedGeometry,
  createCoordinateRevision,
  createGeometryHash,
  createLegacyFinalizerInput,
  createSpatialExecutionBoundary,
  createSpatialResponseIdentity,
  createV3FinalizerInput,
  finalizeCoordinateResult,
  getRecognitionDeadlineContext,
  getRecognitionHardDeadlineMs,
  incrementCoordinateRevision,
  recognitionDeadlineMiddleware,
  spatialResponseMatchesCurrent
} from "../server/coordinate-finalizer/index.js";
import { FinalizedResultSpatialGeometryAdapter } from "../server/spatial/adapters/finalized-result-adapter.js";

const FIXED_TIME = "2026-08-26T00:00:00.000Z";
const clock = () => FIXED_TIME;
const pointGeometry = Object.freeze({ type: "Point", coordinates: Object.freeze([103.1, 16.5]) });

function candidate(overrides = {}) {
  return {
    resultId: "result-1",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "wgs84_decimal",
    precisionMode: "wgs84-table-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: pointGeometry,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    requiresReview: false,
    kmlReady: true,
    groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
    warnings: [],
    ...overrides
  };
}

const cases = [];
function test(id, name, fn) {
  cases.push({ id, name, fn });
}

test("F01", "valid result is AUTO_EXPORT", () => {
  const result = finalizeCoordinateResult(candidate(), { clock });
  assert.equal(result.schemaVersion, "finalized_coordinate_result_v1");
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.equal(result.reasonCodes.length, 0);
  assert.equal(result.crs.axisOrder, "longitude_latitude");
  assert.ok(result.geometryHash.startsWith("sha256:"));
  assert.equal("rawText" in result, false);
  assert.equal("coordinates" in result, false);
});

test("F02", "pending confirmation requires review", () => {
  const result = finalizeCoordinateResult(candidate({ confirmationStatus: "pending" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED));
});

test("F03", "quality failure blocks", () => {
  const result = finalizeCoordinateResult(candidate({ qualityGateStatus: "failed" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_FAILED));
});

test("F04", "requires review cannot auto export", () => {
  const result = finalizeCoordinateResult(candidate({ requiresReview: true }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
});

test("F05", "invalid CRS blocks", () => {
  const result = finalizeCoordinateResult(candidate({ crs: { id: "EPSG:3857", axisOrder: "longitude_latitude" } }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.CRS_NOT_FINALIZED));
});

test("F06", "invalid geometry blocks", () => {
  const result = finalizeCoordinateResult(candidate({ geometry: { type: "Point", coordinates: [200, 16.5] } }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.GEOMETRY_INVALID));
});

test("F07", "KML not ready blocks", () => {
  const result = finalizeCoordinateResult(candidate({ kmlReady: false }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.KML_NOT_READY));
});

test("F08", "unknown gate state fails closed", () => {
  const result = finalizeCoordinateResult(candidate({ qualityGateStatus: "unknown" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_UNKNOWN));
});

test("R01-R05", "confirmation and edit revision lifecycle", () => {
  const initial = createCoordinateRevision({ resultId: "revision-result", confirmationRequired: true });
  let finalized = finalizeCoordinateResult(candidate({
    resultId: initial.resultId,
    resultRevision: initial.resultRevision,
    currentRevision: initial.resultRevision,
    confirmationStatus: initial.confirmationStatus,
    confirmedRevision: initial.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "REVIEW_REQUIRED");

  const confirmed = acceptCoordinateRevision(initial);
  finalized = finalizeCoordinateResult(candidate({
    resultId: confirmed.resultId,
    resultRevision: confirmed.resultRevision,
    currentRevision: confirmed.resultRevision,
    confirmationStatus: confirmed.confirmationStatus,
    confirmedRevision: confirmed.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "AUTO_EXPORT");

  const edited = incrementCoordinateRevision(confirmed);
  assert.equal(edited.resultRevision, 2);
  assert.equal(edited.confirmedRevision, null);
  finalized = finalizeCoordinateResult(candidate({
    resultId: edited.resultId,
    resultRevision: edited.resultRevision,
    currentRevision: edited.resultRevision,
    confirmationStatus: edited.confirmationStatus,
    confirmedRevision: edited.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "REVIEW_REQUIRED");

  const reconfirmed = acceptCoordinateRevision(edited);
  finalized = finalizeCoordinateResult(candidate({
    resultId: reconfirmed.resultId,
    resultRevision: reconfirmed.resultRevision,
    currentRevision: reconfirmed.resultRevision,
    confirmationStatus: reconfirmed.confirmationStatus,
    confirmedRevision: reconfirmed.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "AUTO_EXPORT");
});

test("R06", "geometry hash is canonical and ignores presentation fields", () => {
  const first = createGeometryHash({ type: "Point", coordinates: [103.1, 16.5] });
  const second = createGeometryHash({ coordinates: [103.1, 16.5], type: "Point" });
  assert.equal(first, second);
});

test("I01", "legacy structured input produces geometry without raw text", () => {
  const input = createLegacyFinalizerInput({
    recognitionResult: { precisionMode: "wgs84-table-coordinates", rawText: "must not be consumed" },
    coordinateEngineV2: {
      coordinate_type: "wgs84_decimal",
      precision_mode: "wgs84-table-coordinates",
      requires_review: false,
      groups: [{ group_id: "group_1", geometry: "point", requires_review: false, kml_ready: true, points: [{ lon: 103.1, lat: 16.5 }] }]
    },
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "legacy-1", resultRevision: 1 }
  });
  const result = finalizeCoordinateResult(input, { clock });
  assert.deepEqual(result.geometry, pointGeometry);
  assert.equal(result.decisionState, "AUTO_EXPORT");
});

test("I02", "V3 remains fail closed without authority decision", () => {
  const input = createV3FinalizerInput({
    coordinateEngineV3: {
      coordinate_type: "wgs84_decimal",
      precision_mode: "decimal",
      requires_review: false,
      groups: [{ group_id: "group_1", geometry: "point", requires_review: false, kml_ready: true, points: [{ lon: 103.1, lat: 16.5 }] }]
    },
    verification: { status: "PASS" },
    productionAuthority: false,
    revision: { resultId: "v3-1", resultRevision: 1 }
  });
  assert.equal(finalizeCoordinateResult(input, { clock }).decisionState, "BLOCKED");
});

test("S01", "fake consumer only receives AUTO_EXPORT geometry", () => {
  let count = 0;
  const accepted = consumeFinalizedGeometry(finalizeCoordinateResult(candidate(), { clock }), geometry => {
    count += 1;
    return geometry.type;
  });
  const blocked = consumeFinalizedGeometry(finalizeCoordinateResult(candidate({ qualityGateStatus: "failed" }), { clock }), () => {
    count += 1;
  });
  assert.equal(accepted.consumed, true);
  assert.equal(accepted.value, "Point");
  assert.equal(blocked.consumed, false);
  assert.equal(count, 1);
});

test("S02", "Spatial adapter consumes finalized result without reparsing", () => {
  const finalized = finalizeCoordinateResult(candidate(), { clock });
  const adapted = new FinalizedResultSpatialGeometryAdapter().adapt(finalized);
  assert.equal(adapted.ok, true);
  assert.equal(adapted.geometry.schemaVersion, "normalized_geometry_v1");
  assert.deepEqual(adapted.geometry.geometry, pointGeometry);
  assert.equal(adapted.geometry.source.geometryHash, finalized.geometryHash);
});

test("S03", "Spatial adapter never upgrades blocked result", () => {
  const finalized = finalizeCoordinateResult(candidate({ confirmationStatus: "pending" }), { clock });
  const adapted = new FinalizedResultSpatialGeometryAdapter().adapt(finalized);
  assert.equal(adapted.ok, false);
  assert.equal(adapted.reasonCode, "CONFIRMATION_REQUIRED");
});

test("A01", "async response identity rejects stale revision and hash", () => {
  const current = createSpatialResponseIdentity(finalizeCoordinateResult(candidate(), { clock }));
  assert.equal(spatialResponseMatchesCurrent(current, current), true);
  assert.equal(spatialResponseMatchesCurrent({ ...current, resultRevision: 2 }, current), false);
  assert.equal(spatialResponseMatchesCurrent({ ...current, geometryHash: "sha256:stale" }, current), false);
});

test("K01", "master flag defaults off and performs zero Spatial work", () => {
  let initialized = 0;
  let requested = 0;
  const boundary = createSpatialExecutionBoundary({
    env: {},
    initializeSpatial() { initialized += 1; },
    requestProvider() { requested += 1; }
  });
  const result = boundary.run({});
  assert.equal(result.status, "disabled");
  assert.equal(initialized, 0);
  assert.equal(requested, 0);
});

test("K02", "master flag can enable an explicitly guarded boundary", () => {
  let initialized = 0;
  const boundary = createSpatialExecutionBoundary({ env: { SPATIAL_RESULT_ENABLED: "true" }, initializeSpatial() { initialized += 1; } });
  assert.equal(boundary.run({}).status, "enabled");
  assert.equal(initialized, 1);
});

test("D01", "deadline configuration is always below 60 seconds", () => {
  assert.equal(getRecognitionHardDeadlineMs({}), 55_000);
  assert.equal(getRecognitionHardDeadlineMs({ RECOGNITION_HARD_DEADLINE_MS: "80000" }), 59_000);
});

test("D02", "request deadline aborts and returns controlled 504", async () => {
  const emitter = new EventEmitter();
  const response = Object.assign(emitter, {
    headersSent: false,
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.headersSent = true; this.body = body; this.emit("finish"); return this; }
  });
  let context;
  recognitionDeadlineMiddleware({ deadlineMs: 15 })({}, response, () => { context = getRecognitionDeadlineContext(); });
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(response.statusCode, 504);
  assert.equal(response.body.code, "RECOGNITION_DEADLINE_EXCEEDED");
  assert.equal(context.signal.aborted, true);
});

test("D03", "request signal cancels composed provider signal", () => {
  const request = new AbortController();
  const provider = new AbortController();
  const combined = composeAbortSignals([request.signal, provider.signal]);
  request.abort();
  assert.equal(combined.signal.aborted, true);
  combined.cleanup();
});

let passed = 0;
for (const entry of cases) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}
console.log(`SR-08 regression: ${passed}/${cases.length} PASS`);
