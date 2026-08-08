import assert from "node:assert/strict";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

const cases = [
  {
    name: "Standard DMS text route",
    payload: {
      precisionMode: "dms-coordinates",
      parserTrace: ["TEXT", "DMS:accepted"]
    },
    expectedType: "dms",
    expectedMode: "dms-coordinates"
  },
  {
    name: "Kyrgyz direct return",
    payload: {
      precisionMode: "kyrgyz-gk-point-x-y",
      parserTrace: ["KYRGYZ_GK:direct"],
      kyrgyzGk: { isKyrgyzGk: true }
    },
    expectedType: "kyrgyz_gk_projected_xy",
    expectedMode: "kyrgyz-gk-point-x-y"
  },
  {
    name: "Mozambique direct return",
    payload: {
      precisionMode: "mozambique-geographic-table",
      parserTrace: ["MOZAMBIQUE_GEOGRAPHIC:direct"],
      mozambiqueGeographicTable: { isMozambiqueGeographicTable: true }
    },
    expectedType: "mozambique_geographic_table",
    expectedMode: "mozambique-geographic-table"
  },
  {
    name: "Handwritten timeout retry",
    payload: {
      precisionMode: "handwritten-dms-coordinates",
      parserTrace: ["OCR", "HANDWRITTEN_DMS:timeout_retry", "HANDWRITTEN_DMS:accepted"]
    },
    expectedType: "dms",
    expectedMode: "handwritten-dms-coordinates",
    expectedReview: true
  },
  {
    name: "WGS84 timeout retry",
    payload: {
      precisionMode: "wgs84-table-coordinates",
      parserTrace: ["OCR", "WGS84_TABLE:timeout_retry", "WGS84_TABLE:accepted"]
    },
    expectedType: "wgs84_geographic_table",
    expectedMode: "wgs84-table-coordinates"
  },
  {
    name: "Local OCR BFTM fallback",
    payload: {
      precisionMode: "bftm-projected-x-y",
      parserTrace: ["OCR", "BFTM:accepted"]
    },
    expectedType: "bftm_projected_xy",
    expectedMode: "bftm-projected-x-y"
  }
];

for (const testCase of cases) {
  const finalized = buildFinalizedCoordinateVerificationResponse(testCase.payload);
  assert.equal(finalized.coordinateType, testCase.expectedType, testCase.name);
  assert.equal(finalized.precisionMode, testCase.expectedMode, testCase.name);
  assert.equal(finalized.requires_review, Boolean(testCase.expectedReview), testCase.name);
  assert.deepEqual(finalized.parserTrace, testCase.payload.parserTrace, `${testCase.name} parserTrace`);
  assert.ok(finalized.coordinateArbitration, `${testCase.name} arbitration`);
  assert.equal(typeof finalized.arbitrationEligible, "boolean", `${testCase.name} arbitrationEligible`);
  assert.equal(typeof finalized.confirmationStatus, "string", `${testCase.name} confirmationStatus`);
  assert.equal(typeof finalized.qualityGateStatus, "string", `${testCase.name} qualityGateStatus`);
  assert.equal(typeof finalized.kml_ready, "boolean", `${testCase.name} kml_ready`);
  console.log(`PASS ${testCase.name}`);
}

const canonicalUtm = buildFinalizedCoordinateVerificationResponse({
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
}, {
  coordinate_type: "projected_xy",
  requires_review: true,
  groups: []
});
assert.equal(canonicalUtm.arbitrationEligible, true);
assert.equal(canonicalUtm.requires_review, false, "generic shadow review must not override authoritative typed UTM");
assert.equal(canonicalUtm.confirmationStatus, "awaiting_confirmation");
assert.equal(canonicalUtm.qualityGateStatus, "passed");
assert.equal(canonicalUtm.kml_ready, false);
console.log("PASS Canonical UTM remains blocked until user confirmation");

console.log(`Coordinate Response Finalizer Regression: ${cases.length + 1}/${cases.length + 1} PASS`);
