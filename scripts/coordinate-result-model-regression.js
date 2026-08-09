import assert from "node:assert/strict";

import { buildCoordinateResultV1 } from "../server/coordinate-type/coordinate-result.js";
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

function assertState(name, payload, expectedState) {
  const result = buildCoordinateResultV1(payload);
  assert.equal(result.schemaVersion, "coordinate_result_v1", `${name} schema`);
  assert.equal(result.state, expectedState, `${name} state`);
  assert.ok(result.coordinate, `${name} coordinate`);
  assert.ok(result.confidence, `${name} confidence`);
  assert.ok(result.decision, `${name} decision`);
  assert.ok(result.kml, `${name} kml`);
  assert.ok(result.review, `${name} review`);
  assert.ok(result.ui, `${name} ui`);
  return result;
}

test("Indonesia02 verified UTM maps to AUTO_EXPORT without changing legacy fields", () => {
  const finalized = finalizeCoordinateResponse({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    coordinateArbitration: {
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y",
      authority: "explicit_crs_evidence",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "awaiting_confirmation",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: false,
      lat_lon_role: "verification_only",
      reason: "explicit_utm_crs_and_structured_xy",
      blockedFallbacks: []
    }
  });

  assert.equal(finalized.confirmationStatus, "awaiting_confirmation", "legacy confirmation remains unchanged");
  assert.equal(finalized.kml_ready, false, "legacy kml_ready remains unchanged");
  assert.equal(finalized.coordinateResult.state, "AUTO_EXPORT");
  assert.equal(finalized.coordinateResult.coordinate.type, "utm_projected_xy");
  assert.equal(finalized.coordinateResult.coordinate.precision, "utm-projected-x-y");
  assert.equal(finalized.coordinateResult.kml.ready, true);
});

test("Indonesia03 UTM review maps to BLOCKED_REVIEW", () => {
  const result = assertState("Indonesia03", {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    requires_review: true,
    coordinateArbitration: {
      reason: "utm_transformation_verification_failed"
    }
  }, "BLOCKED_REVIEW");

  assert.equal(result.kml.ready, false);
  assert.equal(result.review.required, true);
  assert.equal(result.review.canUserConfirm, false);
});

test("Handwritten DMS maps to CONFIRM_REQUIRED", () => {
  const result = assertState("Handwritten DMS", {
    coordinateType: "dms",
    precisionMode: "handwritten-dms-coordinates",
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "blocked",
    kml_ready: false,
    requires_review: true,
    coordinateArbitration: {
      reason: "handwritten_dms_requires_review"
    }
  }, "CONFIRM_REQUIRED");

  assert.equal(result.review.required, true);
  assert.equal(result.review.canUserConfirm, true);
  assert.equal(result.decision.action, "confirm_result");
});

test("WGS84 swapped lat/lon maps to BLOCKED_REVIEW", () => {
  const result = assertState("WGS84 swapped", {
    coordinateType: "wgs84_geographic_table",
    precisionMode: "wgs84-table-coordinates",
    confirmationStatus: "not_required",
    qualityGateStatus: "blocked",
    kml_ready: false,
    requires_review: true,
    coordinateArbitration: {
      reason: "possible_swapped_lat_lon"
    }
  }, "BLOCKED_REVIEW");

  assert.equal(result.kml.blockedReason, "possible_swapped_lat_lon");
});

test("Normal WGS84 table maps to AUTO_EXPORT", () => {
  const result = assertState("Normal WGS84 table", {
    coordinateType: "wgs84_geographic_table",
    precisionMode: "wgs84-table-coordinates",
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    kml_ready: true,
    requires_review: false,
    coordinateArbitration: {
      reason: "validated_wgs84_table"
    }
  }, "AUTO_EXPORT");

  assert.equal(result.kml.ready, true);
  assert.equal(result.decision.action, "export_kml");
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Coordinate Result Model Regression: 5/5 PASS");
