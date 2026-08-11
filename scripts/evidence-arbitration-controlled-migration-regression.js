import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_SCHEMA_VERSION,
  buildEvidenceArbitrationControlledMigration,
  buildEvidenceArbitrationDryRunDiff,
  buildEvidenceArbitrationLimitedMigrationCandidate,
  buildEvidenceArbitrationMigrationSafety,
  buildEvidenceArbitrationProposal,
  buildEvidenceArbitrationReviewGate,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "controlled_migration_regression",
    coordinateSource: "controlled_migration_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_regression_default"
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
    reason: "controlled_migration_regression_default",
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

function limitedCandidateFor({
  candidates,
  legacy = legacySnapshot(),
  flags = {},
  review = {},
  extraProposal = {},
  category,
  fixture,
  limitedFlags
}) {
  const decision = rankCoordinateEvidenceCandidates(candidates, legacy);
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision,
    ...extraProposal
  });
  const dryRun = buildEvidenceArbitrationDryRunDiff({ proposal });
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun,
    flags,
    category,
    fixture
  });
  const reviewGate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy,
    review
  });
  return buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: limitedFlags || flags
  });
}

function assertNoEffects(controlled) {
  assert.equal(controlled.safety.migrationApplied, false);
  assert.equal(controlled.safety.productionWinnerChanged, false);
  assert.equal(controlled.safety.affectsLegacyWinner, false);
  assert.equal(controlled.safety.affectsCoordinateResult, false);
  assert.equal(controlled.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Verified transformation approved candidate becomes controlled migration eligible", () => {
  const flags = {
    migration: true,
    kmlGate: false,
    autoDecision: false
  };
  const candidate = limitedCandidateFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy: legacySnapshot({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y",
      coordinateResultState: "AUTO_EXPORT",
      kmlReady: false
    }),
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: flags,
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags
  });

  assert.equal(controlled.schemaVersion, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_SCHEMA_VERSION);
  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE);
  assert.equal(controlled.candidate.winnerEvidenceType, "verified_utm_transformation");
  assert.equal(controlled.adapter.validated, true);
  assert.equal(controlled.adapter.updatePlan.wouldUpdateCoordinateResult, true);
  assert.equal(controlled.adapter.updatePlan.wouldUpdateKml, false);
  assert.equal(controlled.adapter.updatePlan.productionApplyEnabled, false);
  assertNoEffects(controlled);
});

test("KML gate remains independent when disabled", () => {
  const candidate = limitedCandidateFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE);
  assert.equal(controlled.adapter.updatePlan.wouldUpdateKml, false);
  assert.equal(controlled.adapter.updatePlan.kmlGateEnabled, false);
  assertNoEffects(controlled);
});

test("Madagascar review-only candidate is blocked from controlled migration", () => {
  const candidate = limitedCandidateFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(controlled.adapter.validated, false);
  assert.ok(controlled.adapter.blockReasons.includes("review_only_category"));
  assertNoEffects(controlled);
});

test("Cote d'Ivoire remains review-only and not automatic", () => {
  const candidate = limitedCandidateFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy: legacySnapshot({
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      coordinateResultState: "AUTO_EXPORT",
      kmlReady: true
    }),
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_2"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(controlled.candidate.winnerEvidenceType, "explicit_geographic_dms");
  assert.equal(controlled.candidate.autoDecisionEligible, false);
  assertNoEffects(controlled);
});

test("Indonesia pending policy blocks controlled migration", () => {
  const candidate = limitedCandidateFor({
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
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.BLOCKED);
  assert.ok(controlled.adapter.blockReasons.includes("pending_fixture_policy"));
  assertNoEffects(controlled);
});

test("Rollback state is preserved and fail-closed", () => {
  const candidate = limitedCandidateFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    review: {
      status: "ROLLED_BACK",
      reason: "rollback flag disabled"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ROLLED_BACK);
  assert.equal(controlled.rollback.rollbackSafe, true);
  assert.equal(controlled.rollback.restoresCoordinateResult, true);
  assert.equal(controlled.safety.failClosed, true);
  assertNoEffects(controlled);
});

test("Adapter fails closed when migration flag is disabled", () => {
  const candidate = limitedCandidateFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: candidate,
    flags: {
      migration: false,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(controlled.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.BLOCKED);
  assert.equal(controlled.adapter.validated, false);
  assert.ok(controlled.adapter.blockReasons.includes("migration_flag_disabled"));
  assertNoEffects(controlled);
});

test("Adapter output sanitizes secret-like strings", () => {
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: {
      schemaVersion: "evidence_arbitration_limited_migration_candidate_v1",
      classification: "LIMITED_MIGRATION_CANDIDATE",
      flags: {
        migration: true,
        kmlGate: true,
        autoDecision: true
      },
      review: {
        classification: "APPROVED_FOR_LIMITED_MIGRATION",
        reviewStatus: "APPROVED",
        approvedBy: "token:=abc",
        approvedAt: "2026-08-12T00:00:00.000Z"
      },
      candidate: {
        category: "verified_transformation",
        winnerEvidenceType: "secret:=value",
        authority: 4,
        proposedCoordinateType: "utm_projected_xy",
        proposedPrecisionMode: "utm-projected-x-y",
        limitedMigrationEligible: true,
        kmlEligible: true,
        autoDecisionEligible: false,
        blockReasons: ["authorization:=bearer abc"]
      },
      legacy: {
        coordinateType: "utm_projected_xy",
        precisionMode: "utm-projected-x-y",
        coordinateResultState: "AUTO_EXPORT",
        kmlReady: false
      },
      eligibility: {
        reviewApproved: true,
        limitedMigrationAllowedByCategory: true,
        rollbackSafe: true,
        migrationSafetyPassed: true
      },
      rollback: {
        rollbackSafe: true
      },
      safety: {
        migrationApplied: false,
        productionWinnerChanged: false,
        affectsLegacyWinner: false,
        affectsCoordinateResult: false,
        affectsKml: false
      }
    },
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const serialized = JSON.stringify(controlled);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(serialized.includes("token:=abc"), false);
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
  console.log(`Evidence Arbitration Controlled Migration Regression: ${passed}/${tests.length} PASS`);
}
