import assert from "node:assert/strict";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

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

function buildDebugResponse(payload = {}, engine = null) {
  const normal = buildFinalizedCoordinateVerificationResponse(payload, engine);
  const debug = buildFinalizedCoordinateVerificationResponse(payload, engine, {
    includeCoordinateEvidenceDebug: true
  });
  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(normal));
  return { normal, debug };
}

function baseArbitration(overrides = {}) {
  return {
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
    reason: "regression_fixture",
    blockedFallbacks: [],
    ...overrides
  };
}

function assertDryRunSafety(debug = {}) {
  assert.equal(debug.evidenceArbitrationDryRun.safety.migrationEnabled, false);
  assert.equal(debug.evidenceArbitrationDryRun.safety.affectsLegacyWinner, false);
  assert.equal(debug.evidenceArbitrationDryRun.safety.affectsCoordinateResult, false);
  assert.equal(debug.evidenceArbitrationDryRun.safety.affectsKml, false);
  assert.equal(debug.evidenceArbitrationProposal.safety.affectsLegacyWinner, false);
  assert.equal(debug.evidenceArbitrationProposal.safety.affectsCoordinateResult, false);
  assert.equal(debug.evidenceArbitrationProposal.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("default response does not expose arbitration proposal or dry-run diff", () => {
  const response = buildFinalizedCoordinateVerificationResponse({
    coordinateType: "dms",
    precisionMode: "dms-coordinates",
    _coordinateEvidenceContext: {
      dmsAccepted: true,
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true
    },
    coordinateArbitration: baseArbitration({
      coordinateType: "dms",
      precisionMode: "dms-coordinates",
      authority: "explicit_dms",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: true,
      lat_lon_role: "primary"
    })
  });

  assert.equal("evidenceArbitrationProposal" in response, false);
  assert.equal("evidenceArbitrationDryRun" in response, false);
  assert.equal("debugEvidenceContext" in response, false);
});

test("Madagascar debug response exposes review-required dry-run diff", () => {
  const { debug } = buildDebugResponse({
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
    coordinateArbitration: baseArbitration()
  });

  assert.equal(debug.evidenceArbitrationProposal.schemaVersion, "evidence_arbitration_proposal_v1");
  assert.equal(debug.evidenceArbitrationDryRun.schemaVersion, "evidence_arbitration_dry_run_diff_v1");
  assert.equal(debug.evidenceArbitrationDryRun.classification, "REVIEW_REQUIRED");
  assert.equal(debug.evidenceArbitrationDryRun.proposal.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(debug.evidenceArbitrationDryRun.diff.wouldChangeLegacy, true);
  assert.equal(debug.evidenceArbitrationDryRun.diff.wouldChangeCoordinateType, true);
  assert.equal(debug.evidenceArbitrationDryRun.diff.wouldChangeKml, false);
  assert.deepEqual(debug.debugEvidenceContext.evidenceArbitrationDryRun, debug.evidenceArbitrationDryRun);
  assertDryRunSafety(debug);
});

test("Cote d'Ivoire debug response exposes agreement dry-run diff", () => {
  const { debug } = buildDebugResponse({
    precisionMode: "cote-divoire-geographic-dms-table",
    _coordinateEvidenceContext: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      sourceHint: "Latitude Nord Longitude Ouest"
    },
    coordinateArbitration: baseArbitration({
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
      reason: "cote_divoire_geographic_dms_table"
    })
  }, {
    coordinate_type: "cote_divoire_geographic_dms_table",
    precision_mode: "cote-divoire-geographic-dms-table",
    requires_review: false,
    groups: [{
      geometry: "polygon",
      points: [{}, {}, {}, {}]
    }]
  });

  assert.equal(debug.evidenceArbitrationDryRun.classification, "AGREEMENT");
  assert.equal(debug.evidenceArbitrationDryRun.proposal.winnerEvidenceType, "explicit_geographic_dms");
  assert.equal(debug.evidenceArbitrationDryRun.diff.wouldChangeLegacy, false);
  assertDryRunSafety(debug);
});

test("Verified transformation debug response keeps KML dry-run independent", () => {
  const { debug } = buildDebugResponse({
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
    coordinateArbitration: baseArbitration({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y",
      requires_review: false,
      arbitrationEligible: true,
      confirmationStatus: "awaiting_confirmation",
      qualityGateStatus: "passed",
      kml_allowed: true,
      kml_ready: false,
      reason: "explicit_utm_crs_and_structured_xy"
    })
  }, {
    coordinate_type: "projected_xy",
    requires_review: true,
    groups: []
  });

  assert.equal(debug.evidenceArbitrationDryRun.proposal.winnerEvidenceType, "verified_utm_transformation");
  assert.equal(debug.evidenceArbitrationDryRun.diff.wouldChangeKml, false);
  assert.equal(debug.evidenceArbitrationDryRun.safety.migrationEnabled, false);
  assertDryRunSafety(debug);
});

test("runtime dry-run output is sanitized", () => {
  const { debug } = buildDebugResponse({
    coordinateType: "dms",
    precisionMode: "dms-coordinates",
    _coordinateEvidenceContext: {
      dmsAccepted: true,
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      reason: "token:=abc Authorization: Bearer abc.def",
      rawText: "raw OCR should not be exposed",
      prompt: "secret prompt should not be exposed",
      modelResponse: "model response should not be exposed"
    },
    coordinateArbitration: baseArbitration({
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
      reason: "secret:=value"
    })
  });

  const serialized = JSON.stringify({
    evidenceArbitrationProposal: debug.evidenceArbitrationProposal,
    evidenceArbitrationDryRun: debug.evidenceArbitrationDryRun,
    debugEvidenceContext: debug.debugEvidenceContext
  });
  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should not be exposed"), false);
  assert.equal(serialized.includes("secret prompt should not be exposed"), false);
  assert.equal(serialized.includes("model response should not be exposed"), false);
  assert.equal(serialized.includes("secret:=value"), false);
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
  console.log(`Evidence Arbitration Runtime Observation Regression: ${passed}/${tests.length} PASS`);
}
