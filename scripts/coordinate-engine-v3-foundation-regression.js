import assert from "node:assert/strict";

import {
  COORDINATE_ENGINE_V3_DISABLED_REASON,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  createNormalizedCoordinateResult,
  isCoordinateEngineV3Enabled,
  recognizeWithIsolatedRecognizers,
  RECOGNIZER_PORT_STATUS,
  RECOGNIZER_TYPES,
  validateNormalizedCoordinateResult,
  validateRecognizerRegistry,
} from "../server/coordinate-engine-v3/index.js";

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test("v3 disabled by default", () => {
  assert.equal(isCoordinateEngineV3Enabled({}), false);
  assert.equal(isCoordinateEngineV3Enabled({ ENABLE_COORDINATE_ENGINE_V3: "false" }), false);
  assert.equal(isCoordinateEngineV3Enabled({ ENABLE_COORDINATE_ENGINE_V3: "true" }), true);
});

test("registry contains planned recognizers as NOT_PORTED", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.length, RECOGNIZER_TYPES.length);
  assert.deepEqual(registry.map((item) => item.coordinateType), RECOGNIZER_TYPES);
  assert.equal(registry.every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
  assert.equal(validateRecognizerRegistry(registry).valid, true);
});

test("normalized result keeps warning-only KML semantics", () => {
  const result = createNormalizedCoordinateResult({
    coordinateType: "wgs84_decimal",
    recognizerId: "wgs84_decimal_recognizer",
    coordinates: [
      { label: "1", longitude: 10, latitude: 20 },
      { label: "2", longitude: 11, latitude: 21 },
      { label: "3", longitude: 12, latitude: 20 },
    ],
    geometryType: "polygon",
    crs: "EPSG:4326",
    precisionMode: "decimal",
    warnings: [{ code: "REVIEW", message: "Point 2 requires review." }],
  });
  assert.equal(result.technicalKmlReady, true);
  assert.equal(result.technicalKmlBlockReason, null);
  assert.equal(result.warnings.length, 1);
  assert.equal(validateNormalizedCoordinateResult(result).valid, true);
});

test("technical impossibility blocks KML", () => {
  const result = createNormalizedCoordinateResult({
    coordinateType: "wgs84_decimal",
    recognizerId: "wgs84_decimal_recognizer",
    coordinates: [{ label: "1", longitude: 10, latitude: 20 }],
    geometryType: "polygon",
  });
  assert.equal(result.technicalKmlReady, false);
  assert.equal(result.technicalKmlBlockReason, "INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY");
});

test("migration and arbitration fields are rejected as authority", () => {
  const result = {
    ...createNormalizedCoordinateResult({ coordinates: [] }),
    confirmationStatus: "accepted",
    shadowWinner: "verified_utm_transformation",
    migrationStatus: "AUTHORIZED",
    arbitrationProposal: {},
    dryRun: {},
  };
  const validation = validateNormalizedCoordinateResult(result);
  assert.equal(validation.valid, false);
  assert.deepEqual(validation.errors, [
    "confirmation_status_is_not_authority",
    "shadow_winner_is_not_authority",
    "migration_status_is_not_authority",
    "arbitration_proposal_is_not_authority",
    "dry_run_is_not_authority",
  ]);
});

test("latency budget defaults to target 30s and hard 60s", () => {
  const budget = createLatencyBudget({ startedAtMs: 1000, clock: () => 1000 });
  assert.equal(budget.targetMs, 30000);
  assert.equal(budget.hardDeadlineMs, 60000);
  assert.equal(budget.remainingMs(() => 2000), 59000);
  assert.equal(budget.targetExceeded(() => 32001), true);
  assert.equal(budget.deadlineExceeded(() => 61000), true);
});

await testAsync("disabled recognizer system does not handle requests", async () => {
  const result = await recognizeWithIsolatedRecognizers({ image: "fixture" }, { env: {} });
  assert.equal(result.handled, false);
  assert.equal(result.reason, COORDINATE_ENGINE_V3_DISABLED_REASON);
});

console.log("Coordinate Engine V3 Foundation Regression: 7/7 PASS");

