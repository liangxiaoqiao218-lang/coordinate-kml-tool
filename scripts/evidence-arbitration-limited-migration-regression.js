import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION,
  EVIDENCE_ARBITRATION_LIMITED_MIGRATION_SCHEMA_VERSION,
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
    sourceParser: "limited_migration_regression",
    coordinateSource: "limited_migration_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "limited_migration_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "limited_migration_regression_default"
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
    reason: "limited_migration_regression_default",
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

function reviewGateFor({
  candidates,
  legacy = legacySnapshot(),
  flags = {},
  review = {},
  extraProposal = {},
  category,
  fixture
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
  return buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy,
    review
  });
}

function assertNoEffects(candidate) {
  assert.equal(candidate.safety.migrationApplied, false);
  assert.equal(candidate.safety.productionWinnerChanged, false);
  assert.equal(candidate.safety.affectsLegacyWinner, false);
  assert.equal(candidate.safety.affectsCoordinateResult, false);
  assert.equal(candidate.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Verified transformation approved review becomes first limited migration candidate", () => {
  const legacy = legacySnapshot({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    coordinateResultState: "AUTO_EXPORT",
    kml_ready: false,
    kmlReady: false
  });
  const reviewGate = reviewGateFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1",
      approvedAt: "2026-08-12T00:00:00.000Z",
      reason: "verified transformation approved for limited candidate"
    }
  });
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(candidate.schemaVersion, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_SCHEMA_VERSION);
  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.LIMITED_MIGRATION_CANDIDATE);
  assert.equal(candidate.candidate.winnerEvidenceType, "verified_utm_transformation");
  assert.equal(candidate.candidate.category, "verified_transformation");
  assert.equal(candidate.candidate.limitedMigrationEligible, true);
  assert.equal(candidate.candidate.kmlEligible, true);
  assert.equal(candidate.candidate.autoDecisionEligible, false);
  assertNoEffects(candidate);
});

test("Verified transformation remains non-applied when KML gate is disabled", () => {
  const reviewGate = reviewGateFor({
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
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.LIMITED_MIGRATION_CANDIDATE);
  assert.equal(candidate.candidate.limitedMigrationEligible, true);
  assert.equal(candidate.candidate.kmlEligible, false);
  assert.ok(candidate.candidate.blockReasons.includes("kml_gate_disabled"));
  assertNoEffects(candidate);
});

test("Madagascar remains review-only after approval", () => {
  const reviewGate = reviewGateFor({
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
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(candidate.candidate.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(candidate.candidate.limitedMigrationEligible, false);
  assert.ok(candidate.candidate.blockReasons.includes("structured_legal_coordinate_review_only"));
  assertNoEffects(candidate);
});

test("Cote d'Ivoire remains review-only candidate and not auto migration", () => {
  const legacy = legacySnapshot({
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    coordinateResultState: "AUTO_EXPORT",
    kml_ready: true,
    kmlReady: true
  });
  const reviewGate = reviewGateFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy,
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
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(candidate.candidate.winnerEvidenceType, "explicit_geographic_dms");
  assert.equal(candidate.candidate.autoDecisionEligible, false);
  assert.ok(candidate.candidate.blockReasons.includes("explicit_geographic_semantic_more_observation_required"));
  assertNoEffects(candidate);
});

test("Indonesia pending policy remains blocked", () => {
  const reviewGate = reviewGateFor({
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
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED);
  assert.equal(candidate.candidate.limitedMigrationEligible, false);
  assert.ok(candidate.candidate.blockReasons.includes("pending_fixture_policy"));
  assertNoEffects(candidate);
});

test("Rejected review blocks limited migration and leaves legacy untouched", () => {
  const reviewGate = reviewGateFor({
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
      status: "REJECTED",
      reason: "review rejected verified transformation"
    }
  });
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REJECTED);
  assert.equal(candidate.candidate.limitedMigrationEligible, false);
  assertNoEffects(candidate);
});

test("Rollback state is preserved and rollback-safe", () => {
  const reviewGate = reviewGateFor({
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
      reason: "rollback flag disabled migration"
    }
  });
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.ROLLED_BACK);
  assert.equal(candidate.rollback.rollbackSafe, true);
  assert.equal(candidate.rollback.restoresLegacyArbitration, true);
  assert.equal(candidate.rollback.restoresCoordinateResult, true);
  assert.equal(candidate.rollback.restoresKmlBehavior, true);
  assertNoEffects(candidate);
});

test("Pending review is not a limited migration candidate", () => {
  const reviewGate = reviewGateFor({
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
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(candidate.classification, EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.PENDING_REVIEW);
  assert.equal(candidate.candidate.limitedMigrationEligible, false);
  assert.ok(candidate.candidate.blockReasons.includes("review_approval_required"));
  assertNoEffects(candidate);
});

test("Limited migration candidate sanitizes secret-like strings", () => {
  const candidate = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate: {
      schemaVersion: "evidence_arbitration_review_gate_v1",
      classification: "APPROVED_FOR_LIMITED_MIGRATION",
      reviewStatus: "APPROVED",
      reviewerRequired: false,
      reviewSubject: {
        category: "secret:=value",
        winnerEvidenceType: "authorization:=bearer abc",
        authority: 4
      },
      legacy: {
        coordinateType: "utm_projected_xy",
        precisionMode: "utm-projected-x-y",
        coordinateResultState: "AUTO_EXPORT",
        kmlReady: false
      },
      proposal: {
        proposedCoordinateType: "utm_projected_xy",
        proposedPrecisionMode: "utm-projected-x-y",
        reason: "prompt:=secret"
      },
      safetyChecks: {
        rollbackSafe: true,
        kmlSafe: true,
        pendingPolicyBlocked: false,
        migrationSafetyPassed: true
      },
      decision: {
        approvedBy: "token:=abc",
        approvedAt: "2026-08-12T00:00:00.000Z"
      },
      effects: {
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
  const serialized = JSON.stringify(candidate);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(serialized.includes("prompt:=secret"), false);
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
  console.log(`Evidence Arbitration Limited Migration Regression: ${passed}/${tests.length} PASS`);
}
