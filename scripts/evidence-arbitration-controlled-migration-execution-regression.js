import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_SCHEMA_VERSION,
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE,
  buildEvidenceArbitrationControlledMigration,
  buildEvidenceArbitrationControlledMigrationExecution,
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
    sourceParser: "controlled_migration_execution_regression",
    coordinateSource: "controlled_migration_execution_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_execution_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_execution_regression_default"
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
    reason: "controlled_migration_execution_regression_default",
    ...overrides
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
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "structured_cadastral_table"
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

function experimentFor({
  candidates = [
    verifiedUtmCandidate(),
    utmCrsTextCandidate()
  ],
  legacy = legacySnapshot({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    coordinateResultState: "AUTO_EXPORT",
    kmlReady: false
  }),
  safetyFlags = {
    migration: true,
    kmlGate: true,
    autoDecision: true
  },
  runtimeFlags = {
    migration: true,
    kmlGate: false,
    autoDecision: false
  },
  review = {
    status: "APPROVED",
    approvedBy: "reviewer_1",
    approvedAt: "2026-08-12T00:00:00.000Z"
  },
  extraProposal = {},
  category,
  fixture
} = {}) {
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
    flags: runtimeFlags
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: limited,
    flags: runtimeFlags
  });
  return buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: runtimeFlags
  });
}

function assertNoEffects(execution) {
  assert.equal(execution.safety.productionMigrationExecuted, false);
  assert.equal(execution.safety.coordinateResultProductionWrite, false);
  assert.equal(execution.safety.kmlChanged, false);
  assert.equal(execution.safety.frontendChanged, false);
  assert.equal(execution.safety.autoDecisionEnabled, false);
  assert.equal(execution.safety.globalRolloutEnabled, false);
  assert.equal(execution.safety.affectsLegacyWinner, false);
  assert.equal(execution.safety.affectsCoordinateResult, false);
  assert.equal(execution.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Ready experiment with no observations is READY and non-executing", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(execution.schemaVersion, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_SCHEMA_VERSION);
  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.READY);
  assert.equal(execution.experiment.category, "verified_transformation");
  assert.equal(execution.experiment.winnerEvidenceType, "verified_utm_transformation");
  assertNoEffects(execution);
});

test("Three successful observations classify as SUCCESS", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    observation: {
      observationCount: 3,
      successCount: 3,
      failureCount: 0
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.SUCCESS);
  assert.equal(execution.observation.observationCount, 3);
  assert.equal(execution.observation.failureReasons.length, 0);
  assertNoEffects(execution);
});

test("Partial observations classify as PARTIAL", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    observation: {
      observationCount: 3,
      successCount: 2,
      failureCount: 0
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.PARTIAL);
  assertNoEffects(execution);
});

test("KML impact fails closed", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    observation: {
      observationCount: 1,
      successCount: 0,
      failureCount: 1,
      kmlImpactDetected: true
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.FAIL);
  assert.ok(execution.observation.failureReasons.includes("kml_impact_detected"));
  assert.equal(execution.safety.failClosed, true);
  assertNoEffects(execution);
});

test("Auto decision detection fails closed", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    observation: {
      observationCount: 1,
      autoDecisionDetected: true
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.FAIL);
  assert.ok(execution.observation.failureReasons.includes("auto_decision_detected"));
  assertNoEffects(execution);
});

test("Non-ready Madagascar experiment is blocked", () => {
  const experiment = experimentFor({
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ],
    safetyFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    },
    runtimeFlags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment,
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.BLOCKED);
  assert.ok(execution.observation.failureReasons.includes("experiment_not_ready"));
  assertNoEffects(execution);
});

test("Rollback trigger classifies as ROLLED_BACK", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: experimentFor(),
    flags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    },
    observation: {
      rollbackTriggered: true
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.ROLLED_BACK);
  assert.equal(execution.rollback.rollbackTriggered, true);
  assert.equal(execution.rollback.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
  assert.equal(execution.rollback.rollbackValue, false);
  assertNoEffects(execution);
});

test("Experiment with KML gate enabled is blocked", () => {
  const experiment = experimentFor({
    runtimeFlags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: false
    }
  });

  assert.equal(execution.state, EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.BLOCKED);
  assert.ok(execution.observation.failureReasons.includes("experiment_not_ready"));
  assert.ok(execution.observation.failureReasons.includes("kml_gate_must_remain_disabled"));
  assertNoEffects(execution);
});

test("Execution output sanitizes secret-like strings", () => {
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment: {
      schemaVersion: "evidence_arbitration_controlled_migration_experiment_v1",
      classification: "READY_FOR_EXPERIMENT",
      experiment: {
        experimentId: "prompt:=secret",
        category: "verified_transformation",
        winnerEvidenceType: "secret:=value",
        ready: true
      },
      flags: {
        migration: true,
        kmlGate: false,
        autoDecision: false
      },
      approval: {
        reviewStatus: "APPROVED",
        approvedBy: "token:=abc",
        approvedAt: "2026-08-12T00:00:00.000Z"
      },
      adapter: {
        classification: "ELIGIBLE",
        validated: true,
        wouldUpdateCoordinateResult: true,
        wouldUpdateKml: false,
        productionApplyEnabled: false
      },
      rollback: {
        rollbackSafe: true
      },
      safety: {
        experimentApplied: false,
        productionWriteEnabled: false,
        coordinateResultProductionWrite: false,
        kmlWriteEnabled: false,
        autoDecisionEnabled: false,
        affectsLegacyWinner: false,
        affectsCoordinateResult: false,
        affectsKml: false
      }
    },
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });
  const serialized = JSON.stringify(execution);

  assert.equal(serialized.includes("prompt:=secret"), false);
  assert.equal(serialized.includes("secret:=value"), false);
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
  console.log(`Evidence Arbitration Controlled Migration Execution Regression: ${passed}/${tests.length} PASS`);
}
