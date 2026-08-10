import assert from "node:assert/strict";
import fs from "node:fs";

import {
  snapshotPreSuppressionCandidates
} from "../server/coordinate-evidence/index.js";
import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

function findCandidate(candidates = [], evidenceType = "") {
  return candidates.find(candidate => candidate.evidenceType === evidenceType);
}

function pickLegacyFields(response = {}) {
  return {
    coordinateType: response.coordinateType,
    precisionMode: response.precisionMode,
    requires_review: response.requires_review,
    confirmationStatus: response.confirmationStatus,
    qualityGateStatus: response.qualityGateStatus,
    kml_ready: response.kml_ready,
    coordinateResultState: response.coordinateResult?.state
  };
}

function makeUtmReviewPayload(extra = {}) {
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
    ...extra
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

test("server main route wires final pre-suppression context capture", () => {
  const source = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
  assert.match(source, /snapshotPreSuppressionCandidates/);
  assert.match(source, /const preSuppressionEvidenceCandidates = buildPreSuppressionEvidenceSnapshotInput/);
  assert.match(source, /reason:\s*"utm_evidence_lock_final_verification_only"/);
  assert.match(source, /_coordinateEvidenceContext:\s*preDecisionEvidenceContext \|\| undefined/);
});

test("Indonesia03 DMS context survives UTM suppression for shadow diagnostics", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "explicit_hemisphere_marker|coordinate_order_header",
    pointCount: 4,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "utm_evidence_lock_final_verification_only"
  });
  const payload = makeUtmReviewPayload({
    _coordinateEvidenceContext: context
  });

  const normal = buildFinalizedCoordinateVerificationResponse(payload);
  const debug = buildFinalizedCoordinateVerificationResponse(payload, null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(normal));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "dms_geographic"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  assert.equal(debug.shadowEvidenceDecision.differenceFromCurrentWinner, true);
  assert.equal(debug.coordinateEvidenceSummary.affectsLegacyWinner, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsCoordinateResult, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsKml, false);
});

test("Madagascar cadastral context can be preserved beside UTM CRS evidence", () => {
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
  const payload = makeUtmReviewPayload({
    _coordinateEvidenceContext: context
  });
  const debug = buildFinalizedCoordinateVerificationResponse(payload, null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "structured_cadastral_table"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(debug.shadowEvidenceDecision.differenceFromCurrentWinner, true);
});

test("Cote d'Ivoire hemisphere context remains shadow-only when available", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "explicit_hemisphere_marker|coordinate_order_header",
    pointCount: 4
  }, {
    utmEvidenceLockApplied: false,
    suppressedFallbacks: [],
    reason: ""
  });
  const payload = {
    coordinateType: "wgs84_chat_coordinates",
    precisionMode: "wgs84-chat-coordinates",
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    kml_ready: true,
    coordinateArbitration: {
      coordinateType: "wgs84_chat_coordinates",
      precisionMode: "wgs84-chat-coordinates",
      authority: "legacy_wgs84_chat",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      reason: "legacy_wgs84_chat",
      blockedFallbacks: []
    },
    _coordinateEvidenceContext: context
  };
  const debug = buildFinalizedCoordinateVerificationResponse(payload, null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "dms_geographic"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  assert.equal(debug.coordinateType, "wgs84_chat_coordinates");
  assert.equal(debug.precisionMode, "wgs84-chat-coordinates");
});

test("default response hides full pre-decision context and candidates", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"]
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeUtmReviewPayload({
    _coordinateEvidenceContext: context
  }));

  assert.ok(response.coordinateEvidenceSummary);
  assert.equal("coordinateEvidenceCandidates" in response, false);
  assert.equal("shadowEvidenceDecision" in response, false);
  assert.equal("_coordinateEvidenceContext" in response, false);
});

test("debug response sanitizes context-derived diagnostics", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Authorization: Bearer abc.def token:=abc123",
    rawText: "raw OCR should not appear",
    prompt: "secret prompt should not appear",
    modelResponse: "full response should not appear"
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "token:=abc123"
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeUtmReviewPayload({
    _coordinateEvidenceContext: context
  }), null, {
    includeCoordinateEvidenceDebug: true
  });
  const serialized = JSON.stringify(response);

  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should not appear"), false);
  assert.equal(serialized.includes("secret prompt should not appear"), false);
  assert.equal(serialized.includes("full response should not appear"), false);
});

test("verified UTM evidence remains distinct from UTM CRS text under nested context", () => {
  const context = snapshotPreSuppressionCandidates({
    crsEvidenceShadow: makeUtmCrsEvidence(),
    structuredUtmPriority: {
      accepted: true,
      table: {
        rows: [{}, {}, {}, {}]
      },
      transformationVerification: {
        status: "match"
      }
    },
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: false,
    suppressedFallbacks: []
  });
  const payload = {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    requires_review: false,
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    kml_ready: false,
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
    },
    _coordinateEvidenceContext: context
  };
  const debug = buildFinalizedCoordinateVerificationResponse(payload, {
    coordinate_type: "projected_xy",
    requires_review: true,
    groups: []
  }, {
    includeCoordinateEvidenceDebug: true
  });

  assert.equal(debug.coordinateResult.state, "AUTO_EXPORT");
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "verified_utm_transformation"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "verified_utm_transformation");
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
  console.log(`Coordinate Evidence Context Capture Regression: ${passed}/${tests.length} PASS`);
}
