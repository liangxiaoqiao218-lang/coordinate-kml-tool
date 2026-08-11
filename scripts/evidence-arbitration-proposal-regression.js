import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION,
  EVIDENCE_ARBITRATION_PROPOSAL_MODE,
  buildEvidenceArbitrationProposal,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "proposal_regression",
    coordinateSource: "proposal_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "proposal_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "proposal_regression_default"
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
    reason: "proposal_regression_default",
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

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Madagascar creates review-required proposal without mutating legacy", () => {
  const proposal = proposalFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ]
  });

  assert.equal(proposal.schemaVersion, "evidence_arbitration_proposal_v1");
  assert.equal(proposal.mode, EVIDENCE_ARBITRATION_PROPOSAL_MODE.DRY_RUN);
  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.REVIEW_REQUIRED);
  assert.equal(proposal.shadowWinner.evidenceType, "structured_cadastral_table");
  assert.equal(proposal.proposal.wouldChangeLegacy, true);
  assert.equal(proposal.proposal.proposedCoordinateType, "madagascar_cadastral_grid");
  assert.ok(proposal.proposal.blockReasons.includes("manual_review_required"));
  assert.equal(proposal.safety.affectsLegacyWinner, false);
  assert.equal(proposal.safety.affectsCoordinateResult, false);
  assert.equal(proposal.safety.affectsKml, false);
});

test("Cote d'Ivoire is agreement when legacy already matches explicit DMS interpretation", () => {
  const legacy = legacySnapshot({
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    kml_ready: true
  });
  const proposal = proposalFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy
  });

  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.AGREEMENT);
  assert.equal(proposal.proposal.wouldChangeLegacy, false);
  assert.equal(proposal.shadowWinner.evidenceType, "explicit_geographic_dms");
  assert.equal(proposal.legacySnapshot.coordinateType, "cote_divoire_geographic_dms_table");
});

test("Verified transformation proposal remains dry-run and KML independent", () => {
  const legacy = legacySnapshot({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    kml_ready: false
  });
  const proposal = proposalFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy
  });

  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.AGREEMENT);
  assert.equal(proposal.proposal.proposedCoordinateType, "utm_projected_xy");
  assert.equal(proposal.flags.kmlGate, false);
  assert.equal(proposal.safety.affectsKml, false);
});

test("Indonesia pending policy blocks proposal", () => {
  const proposal = buildEvidenceArbitrationProposal({
    category: "indonesia_dms_vs_utm",
    fixture: {
      fixtureStatus: "pending_real_fixture"
    },
    legacySnapshot: legacySnapshot(),
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    shadowDecision: rankCoordinateEvidenceCandidates([
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ], legacySnapshot())
  });

  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_PENDING_POLICY);
  assert.equal(proposal.proposal.wouldChangeLegacy, false);
  assert.ok(proposal.proposal.blockReasons.includes("pending_fixture_policy"));
});

test("Missing shadow decision returns no proposal", () => {
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacySnapshot(),
    candidates: [utmCrsTextCandidate()],
    shadowDecision: {}
  });

  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.NO_PROPOSAL);
  assert.equal(proposal.proposal.wouldChangeLegacy, false);
  assert.ok(proposal.proposal.blockReasons.includes("shadow_candidate_unavailable"));
});

test("KML gate classifies unsafe winner without enabling migration", () => {
  const proposal = proposalFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    extra: {
      flags: {
        dryRun: true,
        reviewOnly: true,
        migration: false,
        kmlGate: true
      }
    }
  });

  assert.equal(proposal.proposal.classification, EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_KML_GATE);
  assert.ok(proposal.proposal.blockReasons.includes("kml_safety_gate_blocked"));
  assert.equal(proposal.flags.migration, false);
  assert.equal(proposal.safety.affectsLegacyWinner, false);
});

test("Sanitizes secret-like strings in proposal summaries", () => {
  const candidate = baseCandidate({
    evidenceType: "unknown_evidence",
    reason: "token:=abc should not appear",
    authority: {
      level: 1,
      category: AUTHORITY_CATEGORY.CONTEXT_HINT,
      reason: "api_key:=secret"
    }
  });
  const proposal = proposalFor({
    candidates: [candidate],
    shadowDecision: {
      winnerEvidenceType: "unknown_evidence",
      winnerAuthority: {
        level: 1,
        category: AUTHORITY_CATEGORY.CONTEXT_HINT,
        reason: "authorization:=bearer abc"
      },
      reason: "secret:=value",
      differenceFromCurrentWinner: true,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }
  });

  assert.equal(JSON.stringify(proposal).includes("secret:=value"), false);
  assert.equal(JSON.stringify(proposal).includes("authorization:=bearer"), false);
  assert.equal(JSON.stringify(proposal).includes("api_key:=secret"), false);
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
  console.log(`Evidence Arbitration Proposal Regression: ${passed}/${tests.length} PASS`);
}
