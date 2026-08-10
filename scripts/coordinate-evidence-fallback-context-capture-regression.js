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

function makeFallbackUtmReviewPayload(context) {
  return {
    model: "local-tesseract-fallback+utm-evidence-lock",
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    requires_review: true,
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    crsEvidence: makeUtmCrsEvidence(),
    structuredUtmTable: {
      accepted: false,
      reason: "utm_evidence_locked_fallback_unavailable",
      rowCount: 0,
      transformationVerification: {
        status: "not_available",
        rows: []
      }
    },
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
      reason: "utm_evidence_locked_fallback_unavailable",
      blockedFallbacks: ["dms", "wgs84_chat"]
    },
    _coordinateEvidenceContext: context
  };
}

test("server fallback route captures context before UTM fallback overwrite", () => {
  const source = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
  assert.match(source, /fallbackPreSuppressionEvidenceCandidates = buildPreSuppressionEvidenceSnapshotInput/);
  assert.match(source, /fallbackPreDecisionEvidenceContext = snapshotPreSuppressionCandidates/);
  assert.match(source, /utm_evidence_lock_timeout_fallback_blocked/);
  assert.match(source, /fallback\._coordinateEvidenceContext = fallbackPreDecisionEvidenceContext/);
});

test("timeout fallback UTM lock preserves DMS and UTM context while legacy stays blocked", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "explicit_hemisphere_marker|coordinate_order_header",
    pointCount: 4,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    structuredUtmTable: {
      accepted: false,
      reason: "utm_evidence_locked_fallback_unavailable",
      rowCount: 0,
      transformationVerification: {
        status: "not_available"
      }
    },
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS", "WGS84_CHAT"],
    reason: "utm_evidence_lock_timeout_fallback_blocked"
  });
  const payload = makeFallbackUtmReviewPayload(context);
  const normal = buildFinalizedCoordinateVerificationResponse(payload);
  const debug = buildFinalizedCoordinateVerificationResponse(payload, null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(normal));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "dms_geographic"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.coordinateResult.state, "BLOCKED_REVIEW");
  assert.equal(debug.coordinateEvidenceSummary.affectsLegacyWinner, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsCoordinateResult, false);
  assert.equal(debug.coordinateEvidenceSummary.affectsKml, false);
});

test("Madagascar fallback can preserve cadastral row summary beside UTM context", () => {
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
    suppressedFallbacks: ["CADASTRAL"],
    reason: "utm_evidence_lock_timeout_fallback_blocked"
  });
  const debug = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "structured_cadastral_table"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(findCandidate(debug.coordinateEvidenceCandidates, "structured_cadastral_table").coordinateSummary.pointCount, 32);
  assert.equal(debug.coordinateResult.state, "BLOCKED_REVIEW");
});

test("DMS suppressed by UTM lock remains visible to shadow diagnostics", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsGroupedAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "dms_grouped|explicit_hemisphere_marker",
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "utm_evidence_lock_timeout_fallback_blocked"
  });
  const debug = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "dms_geographic"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  assert.equal(debug.precisionMode, "utm-projected-x-y-review");
});

test("default response does not expose fallback internal context or candidates", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"]
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(context));

  assert.ok(response.coordinateEvidenceSummary);
  assert.equal("coordinateEvidenceCandidates" in response, false);
  assert.equal("shadowEvidenceDecision" in response, false);
  assert.equal("_coordinateEvidenceContext" in response, false);
});

test("fallback context diagnostics sanitize raw OCR, prompt, model response, and credentials", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    sourceHint: "Authorization: Bearer abc.def token:=abc123",
    rawText: "raw OCR should not appear",
    prompt: "secret prompt should not appear",
    modelResponse: "full model response should not appear",
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"],
    reason: "password:=abc123"
  });
  const debug = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });
  const serialized = JSON.stringify(debug);

  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should not appear"), false);
  assert.equal(serialized.includes("secret prompt should not appear"), false);
  assert.equal(serialized.includes("full model response should not appear"), false);
  assert.equal(serialized.includes("password:=abc123"), false);
});

test("context presence does not change fallback legacy fields", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    crsEvidenceShadow: makeUtmCrsEvidence(),
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"]
  });
  const withoutContext = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(undefined));
  const withContext = buildFinalizedCoordinateVerificationResponse(makeFallbackUtmReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.deepEqual(pickLegacyFields(withContext), pickLegacyFields(withoutContext));
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
  console.log(`Coordinate Evidence Fallback Context Capture Regression: ${passed}/${tests.length} PASS`);
}
