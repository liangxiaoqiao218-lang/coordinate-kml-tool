import assert from "node:assert/strict";
import fs from "node:fs";

import {
  PRE_DECISION_EVIDENCE_CONTEXT_SCHEMA_VERSION,
  snapshotPreSuppressionCandidates
} from "../server/coordinate-evidence/index.js";
import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

function pickLegacyFields(response = {}) {
  return {
    coordinateType: response.coordinateType,
    precisionMode: response.precisionMode,
    confirmationStatus: response.confirmationStatus,
    qualityGateStatus: response.qualityGateStatus,
    kml_ready: response.kml_ready,
    coordinateResultState: response.coordinateResult?.state
  };
}

function makeUtmCrsEvidence() {
  return {
    shadowIntent: {
      projection: "utm",
      datum: "WGS84",
      zone: 50,
      hemisphere: "south",
      confidence: "confirmed",
      conflicts: []
    }
  };
}

function makeReviewPayload(context) {
  return {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    requires_review: true,
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
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
      blockedFallbacks: ["dms"]
    },
    _coordinateEvidenceContext: context
  };
}

function buildDebugResponse(context) {
  return buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });
}

test("default response hides sanitized pre-decision context", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "utm_evidence_lock_final_verification_only"
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context));

  assert.equal("debugEvidenceContext" in response, false);
  assert.equal("_coordinateEvidenceContext" in response, false);
  assert.ok(response.coordinateEvidenceSummary);
  assert.equal("coordinateEvidenceCandidates" in response, false);
  assert.equal("shadowEvidenceDecision" in response, false);
});

test("debug response exposes sanitized pre-decision context", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Longitude E Latitude S",
    pointCount: 4,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "utm_evidence_lock_final_verification_only"
  });
  const response = buildDebugResponse(context);

  assert.ok(response.debugEvidenceContext);
  assert.equal(response.debugEvidenceContext.schemaVersion, PRE_DECISION_EVIDENCE_CONTEXT_SCHEMA_VERSION);
  assert.equal(response.debugEvidenceContext.dms.dmsAccepted, true);
  assert.equal(response.debugEvidenceContext.dms.hasExplicitHemisphere, true);
  assert.equal(response.debugEvidenceContext.dms.hasExplicitCoordinateOrder, true);
  assert.equal(response.debugEvidenceContext.dms.sourceHint, "Longitude E Latitude S");
  assert.equal(response.debugEvidenceContext.suppression.utmEvidenceLockApplied, true);
  assert.deepEqual(response.debugEvidenceContext.suppression.suppressedFallbacks, ["DMS"]);
});

test("debug=true and X-Debug-Trace share the existing coordinate evidence debug switch", () => {
  const source = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
  assert.match(source, /function shouldIncludeRecognitionMetricAttempts\(req = \{\}\)/);
  assert.match(source, /req\.query\?\.debug/);
  assert.match(source, /x-debug-trace/i);
  assert.match(source, /function shouldIncludeCoordinateEvidenceDebug\(req = \{\}\) \{\s*return shouldIncludeRecognitionMetricAttempts\(req\);\s*\}/);
  assert.match(source, /includeCoordinateEvidenceDebug:\s*shouldIncludeCoordinateEvidenceDebug\(req\)/);
});

test("debug context exposure uses the existing sanitizer and no raw context spread", () => {
  const source = fs.readFileSync(new URL("../server/verification/index.js", import.meta.url), "utf8");
  assert.match(source, /sanitizePreDecisionEvidenceContext/);
  assert.match(source, /debugEvidenceContext:\s*sanitizePreDecisionEvidenceContext\(options\.coordinateEvidenceContext \|\| \{\}\)/);
  assert.doesNotMatch(source, /debugEvidenceContext:\s*\{\s*\.\.\.options\.coordinateEvidenceContext/);
  assert.doesNotMatch(source, /debugEvidenceContext:\s*options\.coordinateEvidenceContext/);
});

test("debug context sanitizes credential and raw trace markers", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Authorization: Bearer abc.def token:=abc123 password:=pw",
    rawText: "raw OCR should not appear",
    prompt: "secret prompt should not appear",
    modelResponse: "full model response should not appear",
    image: "base64-image-should-not-appear",
    buffer: "buffer should not appear"
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS", "Authorization: Bearer abc.def"],
    reason: "secret:=abc token:=abc"
  });
  const response = buildDebugResponse(context);
  const serialized = JSON.stringify(response.debugEvidenceContext);

  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("token:=abc123"), false);
  assert.equal(serialized.includes("password:=pw"), false);
  assert.equal(serialized.includes("raw OCR should not appear"), false);
  assert.equal(serialized.includes("secret prompt should not appear"), false);
  assert.equal(serialized.includes("full model response should not appear"), false);
  assert.equal(serialized.includes("base64-image-should-not-appear"), false);
  assert.equal(serialized.includes("buffer should not appear"), false);
});

test("debug context exposure does not alter legacy state", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    crsEvidenceShadow: makeUtmCrsEvidence()
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"]
  });
  const normal = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context));
  const debug = buildDebugResponse(context);

  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(normal));
  assert.equal(debug.coordinateEvidenceSummary.affectsLegacyWinner, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsCoordinateResult, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsKml, false);
  assert.equal(debug.shadowEvidenceDecision.affectsLegacyWinner, false);
  assert.equal(debug.shadowEvidenceDecision.affectsCoordinateResult, false);
  assert.equal(debug.shadowEvidenceDecision.affectsKml, false);
});

test("Madagascar debug context exposes cadastral summary", () => {
  const context = snapshotPreSuppressionCandidates({
    cadastralGrid: {
      isCadastralGrid: true,
      rowCount: 32,
      rows: Array.from({ length: 32 }, () => ({}))
    },
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: [],
    reason: "utm_evidence_lock_final_verification_only"
  });
  const response = buildDebugResponse(context);

  assert.equal(response.debugEvidenceContext.cadastral.isCadastralGrid, true);
  assert.equal(response.debugEvidenceContext.cadastral.rowCount, 32);
  assert.equal(response.debugEvidenceContext.utm.explicitUtmEvidenceLock, true);
});

test("Indonesia DMS and UTM lock debug context exposes suppression summary", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Longitude E Latitude S",
    pointCount: 4,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS", "WGS84_CHAT"],
    reason: "utm_evidence_lock_final_verification_only"
  });
  const response = buildDebugResponse(context);

  assert.equal(response.debugEvidenceContext.dms.dmsAccepted, true);
  assert.equal(response.debugEvidenceContext.dms.sourceHint, "Longitude E Latitude S");
  assert.equal(response.debugEvidenceContext.suppression.utmEvidenceLockApplied, true);
  assert.deepEqual(response.debugEvidenceContext.suppression.suppressedFallbacks, ["DMS", "WGS84_CHAT"]);
});

test("Cote d'Ivoire debug context exposes existing header semantic summary when available", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Latitude Nord Longitude Ouest",
    pointCount: 4
  }, {
    utmEvidenceLockApplied: false,
    suppressedFallbacks: []
  });
  const response = buildDebugResponse(context);

  assert.equal(response.debugEvidenceContext.dms.dmsAccepted, true);
  assert.equal(response.debugEvidenceContext.dms.hasExplicitHemisphere, true);
  assert.equal(response.debugEvidenceContext.dms.hasExplicitCoordinateOrder, true);
  assert.equal(response.debugEvidenceContext.dms.sourceHint, "Latitude Nord Longitude Ouest");
});

let passed = 0;
for (const { name, fn } of tests) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
    break;
  }
}

if (process.exitCode !== 1) {
  console.log(`Coordinate Evidence Debug Context Exposure Regression: ${passed}/${tests.length} PASS`);
}
