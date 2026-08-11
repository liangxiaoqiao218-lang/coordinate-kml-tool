import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION,
  EVIDENCE_ARBITRATION_DRY_RUN_DIFF_SCHEMA_VERSION,
  buildEvidenceArbitrationDryRunDiff,
  buildEvidenceArbitrationProposal,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "dry_run_regression",
    coordinateSource: "dry_run_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "dry_run_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "dry_run_regression_default"
    },
    attributes: {
      geometryValid: true
    },
    coordinateSummary: {
      pointCount: 4,
      geometryType: "polygon",
      groupCount: 1
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED,
    canGenerateKml: false,
    reason: "dry_run_regression_default",
    ...overrides
  });
}

function structuredCadastralCandidate() {
  return baseCandidate({
    evidenceType: "structured_cadastral_table",
    sourceParser: "cadastral_grid_parser",
    coordinateSource: "num_xv_yv_table",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "structured_cadastral_table"
    },
    confidence: {
      level: "high",
      reason: "valid_num_xv_yv_rows"
    },
    attributes: {
      hasStructuredTable: true,
      geometryValid: true
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "structured_cadastral_table"
  });
}

function explicitGeographicDmsCandidate() {
  return baseCandidate({
    evidenceType: "explicit_geographic_dms",
    sourceParser: "cote_divoire_dms_parser",
    coordinateSource: "explicit_lat_lon",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_semantic_evidence"
    },
    confidence: {
      level: "high",
      reason: "explicit_hemisphere_and_order"
    },
    attributes: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      geometryValid: true
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
    reason: "explicit_dms_with_hemisphere"
  });
}

function verifiedUtmCandidate() {
  return baseCandidate({
    evidenceType: "verified_utm_transformation",
    sourceParser: "structured_utm_table",
    coordinateSource: "structured_projected_rows",
    authority: {
      level: 4,
      category: AUTHORITY_CATEGORY.VERIFIED_TRANSFORMATION,
      reason: "verified_utm_transformation"
    },
    confidence: {
      level: "high",
      reason: "projection_transform_matches"
    },
    attributes: {
      transformVerified: true,
      geometryValid: true
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
    reason: "verified_utm_transformation"
  });
}

function utmCrsTextCandidate() {
  return baseCandidate({
    evidenceType: "utm_crs_text",
    sourceParser: "crs_evidence",
    coordinateSource: "map_frame_crs_label",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_text"
    },
    confidence: {
      level: "high",
      reason: "clear_utm_wgs84_zone"
    },
    attributes: {
      crsEvidence: true,
      geometryValid: true
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "utm_crs_text"
  });
}

function legacySnapshot(overrides = {}) {
  return {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    coordinateResult: {
      state: "BLOCKED_REVIEW"
    },
    kml_ready: false,
    ...overrides
  };
}

function proposalFor({ candidates, legacy = legacySnapshot(), shadowDecision, extra = {} }) {
  const decision = shadowDecision || rankCoordinateEvidenceCandidates(candidates, legacy);
  return buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision,
    ...extra
  });
}

function dryRunFor(input) {
  return buildEvidenceArbitrationDryRunDiff({
    proposal: proposalFor(input)
  });
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Madagascar dry-run reports review-required coordinate interpretation change", () => {
  const diff = dryRunFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ]
  });

  assert.equal(diff.schemaVersion, EVIDENCE_ARBITRATION_DRY_RUN_DIFF_SCHEMA_VERSION);
  assert.equal(diff.classification, EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.REVIEW_REQUIRED);
  assert.equal(diff.proposal.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(diff.diff.wouldChangeLegacy, true);
  assert.equal(diff.diff.wouldChangeCoordinateType, true);
  assert.equal(diff.diff.wouldChangePrecisionMode, true);
  assert.equal(diff.diff.wouldChangeKml, false);
  assert.equal(diff.safety.migrationEnabled, false);
  assert.equal(diff.safety.affectsLegacyWinner, false);
});

test("Cote d'Ivoire dry-run reports agreement", () => {
  const diff = dryRunFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy: legacySnapshot({
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      coordinateResult: {
        state: "AUTO_EXPORT"
      },
      kml_ready: true
    })
  });

  assert.equal(diff.classification, EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.AGREEMENT);
  assert.equal(diff.diff.wouldChangeLegacy, false);
  assert.equal(diff.diff.wouldChangeCoordinateType, false);
  assert.equal(diff.diff.wouldChangePrecisionMode, false);
});

test("Verified transformation remains KML independent in dry-run", () => {
  const diff = dryRunFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy: legacySnapshot({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y",
      confirmationStatus: "awaiting_confirmation",
      qualityGateStatus: "passed",
      coordinateResult: {
        state: "AUTO_EXPORT"
      },
      kml_ready: false
    })
  });

  assert.equal(diff.classification, EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.AGREEMENT);
  assert.equal(diff.proposal.winnerEvidenceType, "verified_utm_transformation");
  assert.equal(diff.diff.wouldChangeKml, false);
  assert.equal(diff.safety.affectsKml, false);
});

test("Indonesia pending policy is blocked", () => {
  const candidates = [
    verifiedUtmCandidate(),
    utmCrsTextCandidate()
  ];
  const proposal = buildEvidenceArbitrationProposal({
    category: "indonesia_dms_vs_utm",
    fixture: {
      fixtureStatus: "pending_real_fixture"
    },
    legacySnapshot: legacySnapshot(),
    candidates,
    shadowDecision: rankCoordinateEvidenceCandidates(candidates, legacySnapshot())
  });
  const diff = buildEvidenceArbitrationDryRunDiff({ proposal });

  assert.equal(diff.classification, EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.BLOCKED);
  assert.equal(diff.proposal.classification, "BLOCKED_PENDING_POLICY");
  assert.ok(diff.proposal.blockReasons.includes("pending_fixture_policy"));
});

test("No proposal remains no proposal", () => {
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacySnapshot(),
    candidates: [utmCrsTextCandidate()],
    shadowDecision: {}
  });
  const diff = buildEvidenceArbitrationDryRunDiff({ proposal });

  assert.equal(diff.classification, EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.NO_PROPOSAL);
  assert.equal(diff.diff.wouldChangeLegacy, false);
});

test("Dry-run does not mutate proposal or legacy inputs", () => {
  const proposal = proposalFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ]
  });
  const before = JSON.stringify(proposal);

  buildEvidenceArbitrationDryRunDiff({ proposal });

  assert.equal(JSON.stringify(proposal), before);
});

test("Dry-run sanitizes secret-like strings", () => {
  const proposal = {
    schemaVersion: "evidence_arbitration_proposal_v1",
    enabled: true,
    mode: "dry_run",
    flags: {
      dryRun: true,
      reviewOnly: true,
      migration: false,
      kmlGate: false
    },
    shadowWinner: {
      evidenceType: "explicit_geographic_dms",
      authorityLevel: 5,
      confidence: "high",
      reason: "secret:=value"
    },
    legacySnapshot: legacySnapshot({
      coordinateType: "cote_divoire_geographic_dms_table"
    }),
    proposal: {
      classification: "AGREEMENT",
      wouldChangeLegacy: false,
      proposedCoordinateType: "cote_divoire_geographic_dms_table",
      proposedPrecisionMode: "cote-divoire-geographic-dms-table",
      recommendedAction: "token:=abc",
      blockReasons: ["authorization:=bearer abc"]
    },
    safety: {
      rollbackSafe: true,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }
  };
  const diff = buildEvidenceArbitrationDryRunDiff({ proposal });
  const serialized = JSON.stringify(diff);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
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
  console.log(`Evidence Arbitration Dry-run Regression: ${passed}/${tests.length} PASS`);
}
