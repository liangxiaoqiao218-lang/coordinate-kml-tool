import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  buildControlledMigrationExperimentActivationPreflight,
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
    sourceParser: "controlled_migration_activation_regression",
    coordinateSource: "controlled_migration_activation_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_activation_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_activation_regression_default"
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
    reason: "controlled_migration_activation_regression_default",
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

function legacySnapshot() {
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
    kmlReady: false
  };
}

function executionFor() {
  const candidates = [
    verifiedUtmCandidate(),
    utmCrsTextCandidate()
  ];
  const legacy = legacySnapshot();
  const decision = rankCoordinateEvidenceCandidates(candidates, legacy);
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision
  });
  const dryRun = buildEvidenceArbitrationDryRunDiff({ proposal });
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });
  const reviewGate = buildEvidenceArbitrationReviewGate({
    migrationSafety: safety,
    legacy,
    review: {
      status: "APPROVED",
      approvedBy: "reviewer_1",
      approvedAt: "2026-08-12T00:00:00.000Z"
    }
  });
  const flags = {
    migration: true,
    kmlGate: false,
    autoDecision: false
  };
  const limited = buildEvidenceArbitrationLimitedMigrationCandidate({
    reviewGate,
    flags
  });
  const controlled = buildEvidenceArbitrationControlledMigration({
    limitedMigrationCandidate: limited,
    flags
  });
  const experiment = buildEvidenceArbitrationControlledMigrationExperiment({
    controlledMigration: controlled,
    flags
  });
  return buildEvidenceArbitrationControlledMigrationExecution({
    experiment,
    flags
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

function packageFor(overrides = {}) {
  return buildControlledMigrationExperimentPackage({
    execution: executionFor(),
    approval: validApproval(overrides.approval || {}),
    flags: validFlags(overrides.flags || {}),
    rollback: {
      rehearsalStatus: "PASS",
      ...(overrides.rollback || {})
    },
    ...(overrides.packageInput || {})
  });
}

function assertNoEffects(preflight) {
  assert.equal(preflight.safety.preflightOnly, true);
  assert.equal(preflight.safety.experimentExecuted, false);
  assert.equal(preflight.safety.productionMigrationExecuted, false);
  assert.equal(preflight.safety.coordinateResultChanged, false);
  assert.equal(preflight.safety.kmlChanged, false);
  assert.equal(preflight.safety.frontendChanged, false);
  assert.equal(preflight.safety.autoDecisionEnabled, false);
  assert.equal(preflight.safety.affectsLegacyWinner, false);
  assert.equal(preflight.safety.affectsCoordinateResult, false);
  assert.equal(preflight.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Complete package passes activation preflight", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: packageFor()
  });

  assert.equal(preflight.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_SCHEMA_VERSION);
  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE);
  assert.equal(preflight.activation.canStartExperiment, true);
  assert.equal(preflight.approval.complete, true);
  assert.equal(preflight.flags.complete, true);
  assert.equal(preflight.rollback.ready, true);
  assert.equal(preflight.scope.locked, true);
  assert.equal(preflight.executionReadiness.packagePreconditionsSatisfied, true);
  assertNoEffects(preflight);
});

test("Incomplete approval remains ready for review", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: packageFor({
      approval: {
        status: "PENDING",
        approvedBy: null,
        approvedAt: null,
        reason: ""
      }
    })
  });

  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_FOR_REVIEW);
  assert.equal(preflight.approval.complete, false);
  assert.ok(preflight.approval.issues.includes("approval_not_approved"));
  assertNoEffects(preflight);
});

test("Flag conflicts block activation", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: packageFor({
      flags: {
        kmlGate: true,
        autoDecision: true
      }
    })
  });

  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.BLOCKED);
  assert.ok(preflight.flags.issues.includes("kml_gate_must_remain_disabled"));
  assert.ok(preflight.flags.issues.includes("auto_decision_must_remain_disabled"));
  assertNoEffects(preflight);
});

test("Rollback rehearsal missing blocks activation", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: packageFor({
      rollback: {
        rehearsalStatus: "PENDING"
      }
    })
  });

  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.BLOCKED);
  assert.equal(preflight.rollback.ready, false);
  assert.ok(preflight.rollback.issues.includes("rollback_rehearsal_required"));
  assertNoEffects(preflight);
});

test("Scope leakage blocks activation", () => {
  const pkg = {
    ...packageFor(),
    category: "explicit_geographic_semantic",
    scope: {
      includedEvidenceTypes: ["explicit_geographic_dms"],
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      frontendChangeAllowed: false,
      productionRolloutAllowed: false
    }
  };
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: pkg
  });

  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.BLOCKED);
  assert.equal(preflight.scope.locked, false);
  assert.ok(preflight.scope.issues.includes("category_must_be_verified_transformation"));
  assert.ok(preflight.scope.issues.includes("verified_utm_transformation_scope_required"));
  assertNoEffects(preflight);
});

test("Rejected package remains rejected", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: packageFor({
      approval: {
        status: "REJECTED"
      }
    })
  });

  assert.equal(preflight.status, CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.REJECTED);
  assertNoEffects(preflight);
});

test("Activation preflight sanitizes secret-like strings", () => {
  const preflight = buildControlledMigrationExperimentActivationPreflight({
    package: {
      ...packageFor(),
      experimentId: "prompt:=secret",
      approval: {
        ...packageFor().approval,
        approvedBy: "token:=abc",
        reason: "authorization:=bearer abc"
      }
    }
  });
  const serialized = JSON.stringify(preflight);

  assert.equal(serialized.includes("prompt:=secret"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(preflight.security.rawOcrAllowed, false);
  assert.equal(preflight.security.modelResponseAllowed, false);
  assert.equal(preflight.security.coordinateRowsAllowed, false);
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
  console.log(`Controlled Migration Experiment Activation Preflight Regression: ${passed}/${tests.length} PASS`);
}
