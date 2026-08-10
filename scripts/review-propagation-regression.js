import assert from "node:assert/strict";

import { finalizeCoordinateResponse } from "../server/coordinate-type/response-finalizer.js";

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

function finalizeWithDecision(decision, engine = { requires_review: true }) {
  return finalizeCoordinateResponse({
    coordinateArbitration: {
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      blockedFallbacks: [],
      ...decision
    }
  }, { coordinateEngineV2: engine });
}

function assertBlockedByEngineReview(name, finalized) {
  assert.equal(finalized.requires_review, true, `${name} requires_review`);
  assert.equal(finalized.qualityGateStatus, "blocked", `${name} qualityGateStatus`);
  assert.equal(finalized.kml_ready, false, `${name} kml_ready`);
  assert.equal(finalized.coordinateArbitration.kml_allowed, false, `${name} kml_allowed`);
  assert.match(finalized.coordinateArbitration.reason, /engine_review_required/, `${name} reason`);
  assert.equal(finalized.coordinateResult.state, "BLOCKED_REVIEW", `${name} coordinateResult`);
  assert.equal(finalized.coordinateResult.kml.ready, false, `${name} coordinateResult kml`);
}

test("WGS84 chat engine review propagates to BLOCKED_REVIEW", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "wgs84_chat_coordinates",
    precisionMode: "wgs84-chat-coordinates",
    authority: "chat",
    reason: "wgs84_chat_coordinates"
  });

  assertBlockedByEngineReview("wgs84 chat", finalized);
});

test("WGS84 table engine review propagates to BLOCKED_REVIEW", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "wgs84_geographic_table",
    precisionMode: "wgs84-table-coordinates",
    authority: "validated_wgs84",
    reason: "validated_wgs84_table"
  });

  assertBlockedByEngineReview("wgs84 table", finalized);
});

test("Verified UTM ignores generic engine review", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    authority: "explicit_crs_evidence",
    confirmationStatus: "awaiting_confirmation",
    kml_ready: false,
    reason: "explicit_utm_crs_and_structured_xy"
  });

  assert.equal(finalized.requires_review, false);
  assert.equal(finalized.confirmationStatus, "awaiting_confirmation");
  assert.equal(finalized.qualityGateStatus, "passed");
  assert.equal(finalized.coordinateResult.state, "AUTO_EXPORT");
});

test("MGRS ignores generic engine review", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "mgrs_utm_grid_reference",
    precisionMode: "mgrs-utm-grid-reference",
    authority: "explicit_crs_evidence",
    reason: "mgrs_type_lock"
  });

  assert.equal(finalized.requires_review, false);
  assert.equal(finalized.coordinateResult.state, "AUTO_EXPORT");
});

test("BFTM ignores generic engine review", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "bftm_projected_xy",
    precisionMode: "bftm-projected-x-y",
    authority: "explicit_crs_evidence",
    reason: "bftm_type_lock"
  });

  assert.equal(finalized.requires_review, false);
  assert.equal(finalized.coordinateResult.state, "AUTO_EXPORT");
});

test("Handwritten DMS remains CONFIRM_REQUIRED", () => {
  const finalized = finalizeCoordinateResponse({
    precisionMode: "handwritten-dms-coordinates"
  }, {
    coordinateEngineV2: {
      requires_review: false
    }
  });

  assert.equal(finalized.precisionMode, "handwritten-dms-coordinates");
  assert.equal(finalized.coordinateResult.state, "CONFIRM_REQUIRED");
});

test("Hemisphere ambiguity remains CONFIRM_REQUIRED without engine review", () => {
  const finalized = finalizeWithDecision({
    coordinateType: "wgs84_chat_coordinates",
    precisionMode: "wgs84-chat-coordinates",
    authority: "chat",
    confirmationStatus: "required",
    kml_allowed: false,
    kml_ready: false,
    reason: "hemisphere_ambiguous"
  }, {
    requires_review: false
  });

  assert.equal(finalized.requires_review, false);
  assert.equal(finalized.confirmationStatus, "required");
  assert.equal(finalized.qualityGateStatus, "passed");
  assert.equal(finalized.coordinateResult.state, "CONFIRM_REQUIRED");
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Review Propagation Regression: 7/7 PASS");
