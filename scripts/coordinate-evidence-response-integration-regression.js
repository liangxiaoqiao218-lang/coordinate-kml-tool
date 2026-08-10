import assert from "node:assert/strict";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

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
    coordinateResultState: response.coordinateResult?.state,
    coordinateArbitration: response.coordinateArbitration
  };
}

function assertLegacyUnchanged(name, payload, engine = {}) {
  const normal = buildFinalizedCoordinateVerificationResponse(payload, engine);
  const debug = buildFinalizedCoordinateVerificationResponse(payload, engine, {
    includeCoordinateEvidenceDebug: true
  });

  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(normal), `${name} legacy fields`);
  assert.equal(debug.coordinateEvidenceSummary.affectsLegacyWinner, false, `${name} affectsLegacyWinner`);
  assert.equal(debug.coordinateEvidenceSummary.affectsCoordinateResult, false, `${name} affectsCoordinateResult`);
  assert.equal(debug.coordinateEvidenceSummary.affectsKml, false, `${name} affectsKml`);
  assert.equal(debug.shadowEvidenceDecision.affectsLegacyWinner, false, `${name} shadow affectsLegacyWinner`);
  assert.equal(debug.shadowEvidenceDecision.affectsCoordinateResult, false, `${name} shadow affectsCoordinateResult`);
  assert.equal(debug.shadowEvidenceDecision.affectsKml, false, `${name} shadow affectsKml`);

  return { normal, debug };
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("default response exposes summary only", () => {
  const response = buildFinalizedCoordinateVerificationResponse({
    coordinateType: "dms",
    precisionMode: "dms-coordinates",
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    coordinateArbitration: {
      coordinateType: "dms",
      precisionMode: "dms-coordinates",
      authority: "explicit_dms",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      reason: "explicit_dms_with_hemisphere",
      blockedFallbacks: []
    }
  });

  assert.ok(response.coordinateEvidenceSummary);
  assert.equal(response.coordinateEvidenceSummary.available, true);
  assert.equal("coordinateEvidenceCandidates" in response, false);
  assert.equal("shadowEvidenceDecision" in response, false);
});

test("debug response exposes sanitized candidates and shadow decision", () => {
  const response = buildFinalizedCoordinateVerificationResponse({
    coordinateType: "dms",
    precisionMode: "dms-coordinates",
    _coordinateEvidenceContext: {
      dmsAccepted: true,
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      reason: "Longitude Ouest token:=abc Authorization: Bearer abc.def",
      rawText: "raw OCR should not be exposed",
      prompt: "secret prompt should not be exposed"
    },
    coordinateArbitration: {
      coordinateType: "dms",
      precisionMode: "dms-coordinates",
      authority: "explicit_dms",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      reason: "explicit_dms_with_hemisphere",
      blockedFallbacks: []
    }
  }, null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(Array.isArray(response.coordinateEvidenceCandidates));
  assert.ok(response.shadowEvidenceDecision);
  assert.equal(response.coordinateEvidenceCandidates.length, 1);
  assert.equal(response.coordinateEvidenceCandidates[0].evidenceType, "dms_geographic");
  assert.equal(response.shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  const serialized = JSON.stringify(response);
  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should not be exposed"), false);
  assert.equal(serialized.includes("secret prompt should not be exposed"), false);
  assert.equal(serialized.includes("_coordinateEvidenceContext"), false);
});

test("Madagascar shadow ranks structured cadastral above UTM CRS while legacy stays unchanged", () => {
  const payload = {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    requires_review: true,
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    cadastralGrid: {
      isCadastralGrid: true,
      rows: [{}, {}, {}, {}],
      rowCount: 4
    },
    crsEvidence: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
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
      reason: "utm_transformation_verification_failed",
      blockedFallbacks: []
    }
  };

  const { debug } = assertLegacyUnchanged("Madagascar", payload);
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "structured_cadastral_table"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(debug.shadowEvidenceDecision.differenceFromCurrentWinner, true);
});

test("Indonesia03 shadow ranks DMS above UTM CRS while legacy stays unchanged", () => {
  const payload = {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    requires_review: true,
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    crsEvidence: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    },
    _coordinateEvidenceContext: {
      dmsAccepted: true,
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      pointCount: 4,
      reason: "Longitude E Latitude S"
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
      reason: "utm_transformation_verification_failed",
      blockedFallbacks: []
    }
  };

  const { debug } = assertLegacyUnchanged("Indonesia03", payload);
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "dms_geographic"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  assert.equal(debug.shadowEvidenceDecision.differenceFromCurrentWinner, true);
});

test("Verified UTM keeps AUTO_EXPORT legacy decision while exposing verified transformation evidence", () => {
  const payload = {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    requires_review: false,
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    kml_ready: false,
    crsEvidence: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    },
    structuredUtmTable: {
      accepted: true,
      rowCount: 4,
      transformationVerification: {
        status: "match"
      }
    },
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
  };

  const { debug } = assertLegacyUnchanged("Verified UTM", payload, {
    coordinate_type: "projected_xy",
    requires_review: true,
    groups: []
  });
  assert.equal(debug.coordinateResult.state, "AUTO_EXPORT");
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "verified_utm_transformation"));
  assert.ok(findCandidate(debug.coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(debug.shadowEvidenceDecision.winnerEvidenceType, "verified_utm_transformation");
});

test("Cote d'Ivoire explicit DMS evidence is exposed without requiring generic decimal producer", () => {
  const response = buildFinalizedCoordinateVerificationResponse({
    precisionMode: "cote-divoire-geographic-dms-table",
    _coordinateEvidenceContext: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      sourceHint: "Latitude Nord Longitude Ouest"
    },
    coordinateArbitration: {
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      authority: "coordinate_engine_v2",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary",
      reason: "cote_divoire_geographic_dms_table",
      blockedFallbacks: []
    }
  }, {
    coordinate_type: "cote_divoire_geographic_dms_table",
    precision_mode: "cote-divoire-geographic-dms-table",
    requires_review: false,
    groups: [{
      geometry: "polygon",
      points: [{}, {}, {}, {}]
    }]
  }, {
    includeCoordinateEvidenceDebug: true
  });

  assert.ok(findCandidate(response.coordinateEvidenceCandidates, "explicit_geographic_dms"));
  assert.equal(response.shadowEvidenceDecision.winnerEvidenceType, "explicit_geographic_dms");
  assert.equal(findCandidate(response.coordinateEvidenceCandidates, "generic_decimal"), undefined);
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
  console.log(`Coordinate Evidence Response Integration Regression: ${passed}/${tests.length} PASS`);
}
