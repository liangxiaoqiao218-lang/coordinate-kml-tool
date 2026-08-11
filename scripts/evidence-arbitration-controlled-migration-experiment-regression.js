import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_SCHEMA_VERSION,
  buildEvidenceArbitrationControlledMigration,
  buildEvidenceArbitrationControlledMigrationExperiment,
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
    sourceParser: "controlled_migration_experiment_regression",
    coordinateSource: "controlled_migration_experiment_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_experiment_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_experiment_regression_default"
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
    reason: "controlled_migration_experiment_regression_default",
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

function controlledFor({
  candidates,
  legacy = legacySnapshot(),
  safetyFlags = {},
  limitedFlags = {},
  controlledFlags = {},
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
    flags: safetyFlags,
    category,
    fixture
  });
  const reviewGate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy,
    review
  });
  const limited = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags: limitedFlags
  });
  return buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: limited,
    flags: controlledFlags
  });
}

function assertNoEffects(experiment) {
  assert.equal(experiment.safety.experimentApplied, false);
  assert.equal(experiment.safety.productionWriteEnabled, false);
  assert.equal(experiment.safety.coordinateResultProductionWrite, false);
  assert.equal(experiment.safety.kmlWriteEnabled, false);
  assert.equal(experiment.safety.autoDecisionEnabled, false);
  assert.equal(experiment.safety.affectsLegacyWinner, false);
  assert.equal(experiment.safety.affectsCoordinateResult, false);
  assert.equal(experiment.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Verified transformation is ready for controlled experiment with KML and auto decision disabled", () => {
  const flags = {
    migration: true,
    kmlGate: false,
    autoDecision: false
  };
  const controlled = controlledFor({
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
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: flags,
    controlledFlags: flags,
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1",
      approvedAt: "2026-08-12T00:00:00.000Z"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags
  });

  assert.equal(experiment.schemaVersion, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_SCHEMA_VERSION);
  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.READY_FOR_EXPERIMENT);
  assert.equal(experiment.experiment.ready, true);
  assert.equal(experiment.experiment.category, "verified_transformation");
  assert.equal(experiment.adapter.wouldUpdateCoordinateResult, true);
  assert.equal(experiment.adapter.wouldUpdateKml, false);
  assert.equal(experiment.experiment.scope.kmlDisabled, true);
  assert.equal(experiment.experiment.scope.autoDecisionDisabled, true);
  assertNoEffects(experiment);
});

test("KML gate enabled blocks experiment", () => {
  const controlled = controlledFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.BLOCKED);
  assert.ok(experiment.experiment.blockReasons.includes("kml_gate_must_remain_disabled"));
  assertNoEffects(experiment);
});

test("Auto decision enabled blocks experiment", () => {
  const controlled = controlledFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: true
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.BLOCKED);
  assert.ok(experiment.experiment.blockReasons.includes("auto_decision_must_remain_disabled"));
  assertNoEffects(experiment);
});

test("Missing review approval blocks experiment", () => {
  const controlled = controlledFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.PENDING_REVIEW);
  assert.ok(experiment.experiment.blockReasons.includes("controlled_adapter_not_eligible"));
  assertNoEffects(experiment);
});

test("Madagascar remains excluded from experiment", () => {
  const controlled = controlledFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(experiment.experiment.scope.excludesMadagascar, true);
  assertNoEffects(experiment);
});

test("Cote d'Ivoire remains excluded from first experiment", () => {
  const controlled = controlledFor({
    candidates: [explicitGeographicDmsCandidate()],
    legacy: legacySnapshot({
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      coordinateResultState: "AUTO_EXPORT",
      kmlReady: true
    }),
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_2"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.REVIEW_ONLY);
  assert.equal(experiment.experiment.scope.excludesCoteDivoire, true);
  assertNoEffects(experiment);
});

test("Indonesia pending policy remains blocked", () => {
  const controlled = controlledFor({
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
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.BLOCKED);
  assert.ok(experiment.experiment.blockReasons.includes("controlled_adapter_not_eligible"));
  assertNoEffects(experiment);
});

test("Rollback state is preserved and not ready for experiment", () => {
  const controlled = controlledFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    },
    limitedFlags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    },
    controlledFlags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    },
    review: {
      status: "ROLLED_BACK",
      reason: "rollback flag disabled"
    }
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(experiment.classification, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.ROLLED_BACK);
  assert.equal(experiment.rollback.rollbackSafe, true);
  assert.equal(experiment.rollback.restoresCoordinateResult, true);
  assertNoEffects(experiment);
});

test("Experiment output sanitizes secret-like strings", () => {
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: {
      schemaVersion: "evidence_arbitration_controlled_migration_v1",
      classification: "ELIGIBLE",
      flags: {
        migration: true,
        kmlGate: false,
        autoDecision: false
      },
      candidate: {
        category: "verified_transformation",
        winnerEvidenceType: "secret:=value",
        authority: 4,
        limitedMigrationEligible: true,
        kmlEligible: false,
        autoDecisionEligible: false
      },
      review: {
        classification: "APPROVED_FOR_LIMITED_MIGRATION",
        reviewStatus: "APPROVED",
        approvedBy: "token:=abc",
        approvedAt: "2026-08-12T00:00:00.000Z"
      },
      adapter: {
        validated: true,
        blockReasons: ["authorization:=bearer abc"],
        updatePlan: {
          wouldUpdateCoordinateResult: true,
          proposedCoordinateType: "utm_projected_xy",
          proposedPrecisionMode: "utm-projected-x-y",
          wouldUpdateKml: false,
          productionApplyEnabled: false
        }
      },
      rollback: {
        rollbackSafe: true
      },
      safety: {
        migrationApplied: false,
        affectsCoordinateResult: false,
        affectsKml: false
      }
    },
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    experimentId: "prompt:=secret"
  });
  const serialized = JSON.stringify(experiment);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
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
  console.log(`Evidence Arbitration Controlled Migration Experiment Regression: ${passed}/${tests.length} PASS`);
}
