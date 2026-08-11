import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  buildControlledMigrationExperimentActivationPreflight,
  buildControlledMigrationExperimentExecutionProposal,
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
    sourceParser: "controlled_migration_execution_proposal_regression",
    coordinateSource: "controlled_migration_execution_proposal_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "controlled_migration_execution_proposal_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "controlled_migration_execution_proposal_regression_default"
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
    reason: "controlled_migration_execution_proposal_regression_default",
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

function activationFor({
  candidates = [
    verifiedUtmCandidate(),
    utmCrsTextCandidate()
  ],
  approval = {},
  flags = {},
  rollback = {},
  runtimeFlags = {
    migration: true,
    kmlGate: false,
    autoDecision: false
  }
} = {}) {
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
  const execution = buildEvidenceArbitrationControlledMigrationExecution({
    experiment,
    flags: runtimeFlags
  });
  const pkg = buildControlledMigrationExperimentPackage({
    execution,
    approval: {
      status: "APPROVED",
      approvedBy: "reviewer_1",
      approvedAt: "2026-08-12T00:00:00.000Z",
      reason: "verified transformation controlled experiment",
      scopeAcknowledged: true,
      rollbackAcknowledged: true,
      kmlDisabledAcknowledged: true,
      autoDecisionDisabledAcknowledged: true,
      ...approval
    },
    flags: {
      dryRun: true,
      reviewOnly: true,
      migration: true,
      kmlGate: false,
      autoDecision: false,
      ...flags
    },
    rollback: {
      rehearsalStatus: "PASS",
      ...rollback
    }
  });
  return buildControlledMigrationExperimentActivationPreflight({ package: pkg });
}

function assertNoEffects(proposal) {
  assert.equal(proposal.safety.proposalOnly, true);
  assert.equal(proposal.safety.experimentExecuted, false);
  assert.equal(proposal.safety.productionMigrationExecuted, false);
  assert.equal(proposal.safety.coordinateResultChanged, false);
  assert.equal(proposal.safety.kmlChanged, false);
  assert.equal(proposal.safety.frontendChanged, false);
  assert.equal(proposal.safety.autoDecisionEnabled, false);
  assert.equal(proposal.safety.affectsLegacyWinner, false);
  assert.equal(proposal.safety.affectsCoordinateResult, false);
  assert.equal(proposal.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Ready activation creates START_READY execution proposal", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor()
  });

  assert.equal(proposal.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_SCHEMA_VERSION);
  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY);
  assert.equal(proposal.decision, "start_controlled_experiment");
  assert.equal(proposal.startProcedure.startAllowed, true);
  assert.equal(proposal.scope.verifiedTransformationOnly, true);
  assertNoEffects(proposal);
});

test("Three successful observations classify SUCCESS", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor(),
    observation: {
      observationCount: 3,
      successCount: 3,
      failureCount: 0
    }
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.SUCCESS);
  assert.equal(proposal.decision, "archive_success_do_not_expand_scope");
  assertNoEffects(proposal);
});

test("Partial observations request extended observation", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor(),
    observation: {
      observationCount: 3,
      successCount: 2,
      failureCount: 0
    }
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.PARTIAL);
  assert.equal(proposal.decision, "extend_observation_window");
  assertNoEffects(proposal);
});

test("KML impact recommends rollback", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor(),
    observation: {
      observationCount: 1,
      failureCount: 1,
      kmlImpactDetected: true
    }
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLBACK_REQUIRED);
  assert.equal(proposal.rollback.recommended, true);
  assert.ok(proposal.observation.failureReasons.includes("kml_impact_detected"));
  assertNoEffects(proposal);
});

test("Rollback trigger classifies ROLLED_BACK", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor(),
    observation: {
      rollbackTriggered: true
    }
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLED_BACK);
  assert.equal(proposal.rollback.triggered, true);
  assert.equal(proposal.decision, "archive_rollback_event");
  assertNoEffects(proposal);
});

test("Non-ready activation blocks execution proposal", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor({
      approval: {
        status: "PENDING",
        approvedBy: null,
        approvedAt: null,
        reason: ""
      }
    })
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.BLOCKED);
  assert.equal(proposal.decision, "do_not_start");
  assert.ok(proposal.observation.failureReasons.includes("activation_not_ready"));
  assertNoEffects(proposal);
});

test("Scope leakage is blocked", () => {
  const proposal = buildControlledMigrationExperimentExecutionProposal({
    activation: activationFor({
      candidates: [
        structuredCadastralCandidate(),
        utmCrsTextCandidate()
      ]
    })
  });

  assert.equal(proposal.state, CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.BLOCKED);
  assert.equal(proposal.scope.verifiedTransformationOnly, false);
  assertNoEffects(proposal);
});

test("Execution proposal sanitizes secret-like strings", () => {
  const activation = {
    ...activationFor(),
    experimentId: "prompt:=secret",
    approval: {
      complete: true,
      issues: [],
      approvedBy: "token:=abc",
      approvedAt: "2026-08-12T00:00:00.000Z"
    },
    executionReadiness: {
      packageStatus: "READY_FOR_EXPERIMENT",
      packagePreconditionsSatisfied: true,
      issues: ["authorization:=bearer abc"]
    }
  };
  const proposal = buildControlledMigrationExperimentExecutionProposal({ activation });
  const serialized = JSON.stringify(proposal);

  assert.equal(serialized.includes("prompt:=secret"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(proposal.security.rawOcrAllowed, false);
  assert.equal(proposal.security.modelResponseAllowed, false);
  assert.equal(proposal.security.coordinateRowsAllowed, false);
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
  console.log(`Controlled Migration Experiment Execution Proposal Regression: ${passed}/${tests.length} PASS`);
}
