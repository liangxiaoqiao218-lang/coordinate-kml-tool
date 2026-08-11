import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  buildControlledMigrationExperimentPackage,
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
    sourceParser: "controlled_migration_package_regression",
    coordinateSource: "controlled_migration_package_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_package_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_package_regression_default"
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
    reason: "controlled_migration_package_regression_default",
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
    precisionMode: "utm-projected-x-y",
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    coordinateResult: {
      state: "AUTO_EXPORT"
    },
    coordinateResultState: "AUTO_EXPORT",
    kml_ready: false,
    kmlReady: false,
    ...overrides
  };
}

function executionFor({
  candidates = [
    verifiedUtmCandidate(),
    utmCrsTextCandidate()
  ],
  legacy = legacySnapshot(),
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
  }
} = {}) {
  const decision = rankCoordinateEvidenceCandidates(candidates, legacy);
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision
  });
  const dryRun = buildEvidenceArbitrationDryRunDiff({ proposal });
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun,
    flags: safetyFlags
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
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags: runtimeFlags
  });
  return buildEvidenceArbitrationControlledMigrationExecution({
    experiment,
    flags: runtimeFlags
  });
}

function validApproval(overrides = {}) {
  return {
    status: "APPROVED",
    approvedBy: "reviewer_1",
    approvedAt: "2026-08-12T00:00:00.000Z",
    reason: "verified transformation controlled experiment",
    scopeAcknowledged: true,
    rollbackAcknowledged: true,
    kmlDisabledAcknowledged: true,
    autoDecisionDisabledAcknowledged: true,
    ...overrides
  };
}

function validFlags(overrides = {}) {
  return {
    dryRun: true,
    reviewOnly: true,
    migration: true,
    kmlGate: false,
    autoDecision: false,
    ...overrides
  };
}

function validRollback(overrides = {}) {
  return {
    rehearsalStatus: "PASS",
    ...overrides
  };
}

function assertNoEffects(pkg) {
  assert.equal(pkg.safety.packageOnly, true);
  assert.equal(pkg.safety.experimentExecuted, false);
  assert.equal(pkg.safety.productionMigrationExecuted, false);
  assert.equal(pkg.safety.coordinateResultChanged, false);
  assert.equal(pkg.safety.kmlChanged, false);
  assert.equal(pkg.safety.frontendChanged, false);
  assert.equal(pkg.safety.autoDecisionEnabled, false);
  assert.equal(pkg.safety.affectsLegacyWinner, false);
  assert.equal(pkg.safety.affectsCoordinateResult, false);
  assert.equal(pkg.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Complete verified transformation package is ready for experiment", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    approval: validApproval(),
    flags: validFlags(),
    rollback: validRollback()
  });

  assert.equal(pkg.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_SCHEMA_VERSION);
  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_EXPERIMENT);
  assert.equal(pkg.category, "verified_transformation");
  assert.equal(pkg.preconditions.satisfied, true);
  assert.equal(pkg.scope.kmlMigrationAllowed, false);
  assert.equal(pkg.scope.autoDecisionAllowed, false);
  assert.equal(pkg.rollback.rehearsalStatus, "PASS");
  assert.ok(pkg.reportTemplate.sections.includes("Observation Summary"));
  assertNoEffects(pkg);
});

test("Missing approval stays ready for review but not experiment", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    flags: validFlags(),
    rollback: validRollback()
  });

  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_REVIEW);
  assert.ok(pkg.preconditions.issues.includes("approval_not_approved"));
  assert.ok(pkg.preconditions.issues.includes("approved_by_missing"));
  assertNoEffects(pkg);
});

test("KML or auto-decision flags block package", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    approval: validApproval(),
    flags: validFlags({
      kmlGate: true,
      autoDecision: true
    }),
    rollback: validRollback()
  });

  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.BLOCKED);
  assert.ok(pkg.preconditions.issues.includes("kml_gate_must_remain_disabled"));
  assert.ok(pkg.preconditions.issues.includes("auto_decision_must_remain_disabled"));
  assertNoEffects(pkg);
});

test("Rollback rehearsal must pass before experiment", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    approval: validApproval(),
    flags: validFlags(),
    rollback: validRollback({
      rehearsalStatus: "PENDING"
    })
  });

  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.BLOCKED);
  assert.ok(pkg.preconditions.issues.includes("rollback_rehearsal_required"));
  assertNoEffects(pkg);
});

test("Rejected approval rejects package", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    approval: validApproval({
      status: "REJECTED"
    }),
    flags: validFlags(),
    rollback: validRollback()
  });

  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.REJECTED);
  assertNoEffects(pkg);
});

test("Madagascar package is blocked by non-verified category", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor({
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
    }),
    approval: validApproval(),
    flags: validFlags(),
    rollback: validRollback()
  });

  assert.equal(pkg.status, CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.BLOCKED);
  assert.ok(pkg.preconditions.issues.includes("execution_state_must_be_ready"));
  assert.ok(pkg.preconditions.issues.includes("experiment_category_must_be_verified_transformation"));
  assertNoEffects(pkg);
});

test("Package security excludes sensitive payload markers", () => {
  const pkg = buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    experimentId: "prompt:=secret",
    approval: validApproval({
      approvedBy: "token:=abc",
      reason: "authorization:=bearer abc"
    }),
    flags: validFlags(),
    rollback: validRollback()
  });
  const serialized = JSON.stringify(pkg);

  assert.equal(serialized.includes("prompt:=secret"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(pkg.security.rawOcrAllowed, false);
  assert.equal(pkg.security.modelResponseAllowed, false);
  assert.equal(pkg.security.coordinateRowsAllowed, false);
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
  console.log(`Controlled Migration Experiment Package Regression: ${passed}/${tests.length} PASS`);
}
