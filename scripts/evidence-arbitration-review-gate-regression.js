import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION,
  EVIDENCE_ARBITRATION_REVIEW_GATE_SCHEMA_VERSION,
  EVIDENCE_ARBITRATION_REVIEW_STATUS,
  buildEvidenceArbitrationDryRunDiff,
  buildEvidenceArbitrationMigrationSafety,
  buildEvidenceArbitrationProposal,
  buildEvidenceArbitrationReviewGate,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "review_gate_regression",
    coordinateSource: "review_gate_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "review_gate_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "review_gate_regression_default"
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
    reason: "review_gate_regression_default",
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
    coordinateResultState: "BLOCKED_REVIEW",
    kml_ready: false,
    kmlReady: false,
    ...overrides
  };
}

function dryRunFor({ candidates, legacy = legacySnapshot(), extraProposal = {} }) {
  const decision = rankCoordinateEvidenceCandidates(candidates, legacy);
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision,
    ...extraProposal
  });
  return buildEvidenceArbitrationDryRunDiff({ proposal });
}

function safetyFor({ candidates, legacy = legacySnapshot(), flags = {}, extraProposal = {}, category, fixture }) {
  return buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({ candidates, legacy, extraProposal }),
    flags,
    category,
    fixture
  });
}

function assertNoEffects(gate) {
  assert.equal(gate.effects.affectsLegacyWinner, false);
  assert.equal(gate.effects.affectsCoordinateResult, false);
  assert.equal(gate.effects.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Madagascar requires pending review and keeps KML unsafe", () => {
  const safety = safetyFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy: legacySnapshot()
  });

  assert.equal(gate.schemaVersion, EVIDENCE_ARBITRATION_REVIEW_GATE_SCHEMA_VERSION);
  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.PENDING_REVIEW);
  assert.equal(gate.reviewStatus, EVIDENCE_ARBITRATION_REVIEW_STATUS.PENDING);
  assert.equal(gate.reviewerRequired, true);
  assert.equal(gate.reviewSubject.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(gate.safetyChecks.kmlSafe, false);
  assertNoEffects(gate);
});

test("Cote d'Ivoire is a review candidate and not auto applied", () => {
  const legacy = legacySnapshot({
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    coordinateResultState: "AUTO_EXPORT",
    kml_ready: true,
    kmlReady: true
  });
  const safety = safetyFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy
  });

  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.PENDING_REVIEW);
  assert.equal(gate.reviewSubject.winnerEvidenceType, "explicit_geographic_dms");
  assert.equal(gate.safetyChecks.migrationSafetyPassed, true);
  assertNoEffects(gate);
});

test("Verified transformation can be approved as candidate without executing migration", () => {
  const legacy = legacySnapshot({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    coordinateResultState: "AUTO_EXPORT",
    kml_ready: false,
    kmlReady: false
  });
  const safety = safetyFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy,
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1",
      approvedAt: "2026-08-12T00:00:00.000Z",
      reason: "verified transformation reviewed"
    }
  });

  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.APPROVED_FOR_LIMITED_MIGRATION);
  assert.equal(gate.reviewStatus, EVIDENCE_ARBITRATION_REVIEW_STATUS.APPROVED);
  assert.equal(gate.reviewSubject.winnerEvidenceType, "verified_utm_transformation");
  assert.equal(gate.decision.approvedBy, "reviewer_1");
  assertNoEffects(gate);
});

test("Indonesia pending policy blocks review gate", () => {
  const safety = safetyFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    extraProposal: {
      category: "indonesia_dms_vs_utm",
      fixture: {
        fixtureStatus: "pending_real_fixture"
      }
    },
    category: "indonesia_dms_vs_utm",
    fixture: {
      fixtureStatus: "pending_real_fixture"
    },
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety
  });

  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED);
  assert.equal(gate.reviewStatus, EVIDENCE_ARBITRATION_REVIEW_STATUS.BLOCKED);
  assert.equal(gate.safetyChecks.pendingPolicyBlocked, true);
  assertNoEffects(gate);
});

test("Rejected review keeps legacy unaffected", () => {
  const safety = safetyFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    review: {
      status: "REJECTED",
      reason: "reviewer rejected legal grid proposal"
    }
  });

  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.REJECTED);
  assert.equal(gate.reviewStatus, EVIDENCE_ARBITRATION_REVIEW_STATUS.REJECTED);
  assert.equal(gate.decision.reason, "reviewer rejected legal grid proposal");
  assertNoEffects(gate);
});

test("Rollback review state is explicit and rollback-safe", () => {
  const safety = safetyFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    review: {
      status: "ROLLED_BACK",
      reason: "rollback flag disabled migration"
    }
  });

  assert.equal(gate.classification, EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.ROLLED_BACK);
  assert.equal(gate.reviewStatus, EVIDENCE_ARBITRATION_REVIEW_STATUS.ROLLED_BACK);
  assert.equal(gate.safetyChecks.rollbackSafe, true);
  assertNoEffects(gate);
});

test("Review gate output sanitizes secret-like strings", () => {
  const gate = buildEvidenceArbitrationReviewGate({
    migrationSafety: {
      schemaVersion: "evidence_arbitration_migration_safety_v1",
      category: "secret:=value",
      dryRun: {
        wouldChangeLegacy: true,
        wouldChangeCoordinateType: true,
        wouldChangePrecisionMode: false,
        wouldChangeKml: false
      },
      proposal: {
        classification: "REVIEW_REQUIRED",
        winnerEvidenceType: "authorization:=bearer abc",
        winnerAuthority: 5,
        proposedCoordinateType: "utm_projected_xy",
        proposedPrecisionMode: "utm-projected-x-y"
      },
      eligibility: {
        classification: "MANUAL_REVIEW_REQUIRED",
        migrationEligible: false,
        reviewOnlyEligible: true,
        manualReviewRequired: true,
        blockReasons: ["token:=abc"]
      },
      rollback: {
        rollbackSafe: true
      },
      safety: {
        migrationApplied: false,
        affectsLegacyWinner: false,
        affectsCoordinateResult: false,
        affectsKml: false
      }
    },
    review: {
      status: "PENDING",
      reason: "prompt:=secret"
    }
  });
  const serialized = JSON.stringify(gate);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("prompt:=secret"), false);
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
  console.log(`Evidence Arbitration Review Gate Regression: ${passed}/${tests.length} PASS`);
}
