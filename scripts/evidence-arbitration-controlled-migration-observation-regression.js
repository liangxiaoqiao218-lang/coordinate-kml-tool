#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION,
  CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_SCHEMA_VERSION,
  buildControlledMigrationExperimentObservation,
  buildControlledMigrationExperimentSession,
  buildControlledMigrationExperimentStart
} from "../server/coordinate-evidence/index.js";

function baseAuthorization(overrides = {}) {
  const authorization = {
    schemaVersion: "controlled_migration_experiment_authorization_v1",
    experimentId: "verified_transformation_controlled_migration_v1",
    status: "AUTHORIZED",
    authorization: {
      authorizedBy: "operator_1",
      authorizedAt: "2026-08-12T00:00:00.000Z",
      reason: "verified transformation controlled experiment approved"
    },
    scope: {
      category: "verified_transformation",
      winnerEvidenceType: "verified_utm_transformation",
      verifiedTransformationOnly: true,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      productionRolloutAllowed: false,
      issues: []
    },
    flags: {
      dryRun: true,
      reviewOnly: true,
      migration: true,
      kmlGate: false,
      autoDecision: false,
      issues: []
    },
    preconditions: {
      executionProposalStartReady: true,
      packageReady: true,
      activationReady: true,
      rollbackReady: true,
      flagsLocked: true,
      scopeLocked: true,
      issues: []
    },
    startPermission: {
      canStartExperiment: true,
      canStartProductionMigration: false,
      canStartKmlMigration: false,
      canStartAutoDecision: false
    },
    readiness: {
      ready: true,
      issues: []
    }
  };

  return {
    ...authorization,
    ...overrides,
    authorization: {
      ...authorization.authorization,
      ...(overrides.authorization || {})
    },
    scope: {
      ...authorization.scope,
      ...(overrides.scope || {})
    },
    flags: {
      ...authorization.flags,
      ...(overrides.flags || {})
    },
    preconditions: {
      ...authorization.preconditions,
      ...(overrides.preconditions || {})
    },
    startPermission: {
      ...authorization.startPermission,
      ...(overrides.startPermission || {})
    },
    readiness: {
      ...authorization.readiness,
      ...(overrides.readiness || {})
    }
  };
}

function buildRunningSession(overrides = {}) {
  const preparedSession = buildControlledMigrationExperimentSession({
    authorization: baseAuthorization(),
    commit: "e185120ac8f7479971061d0f308191c3ebd7d91c",
    preparedAt: "2026-08-12T00:10:00.000Z",
    snapshots: {
      legacy: {
        coordinateType: "utm_projected_xy",
        precisionMode: "verified_transform",
        coordinateResultState: "READY",
        kmlReady: false
      },
      proposal: {
        classification: "AGREEMENT",
        winnerEvidenceType: "verified_utm_transformation",
        category: "verified_transformation"
      },
      safety: {
        classification: "PASSED",
        rollbackSafe: true
      },
      adapter: {
        classification: "VALIDATED",
        validated: true
      }
    }
  });

  return {
    ...buildControlledMigrationExperimentStart({
      session: preparedSession,
      startTime: "2026-08-12T00:15:00.000Z",
      runningSessionId: "verified_transformation_controlled_migration_v1:session_001"
    }),
    ...overrides
  };
}

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-observation-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const successObservation = buildControlledMigrationExperimentObservation({
  runningSession: buildRunningSession(),
  runId: "run_003",
  timestamp: "2026-08-12T00:30:00.000Z",
  metrics: {
    minimumRuns: 3,
    observationCount: 3,
    passCount: 3,
    failCount: 0,
    candidateStableCount: 3,
    adapterStableCount: 3,
    legacyIsolationViolations: 0,
    kmlImpactCount: 0,
    autoDecisionLeakCount: 0,
    unexpectedDiffCount: 0,
    rollbackReadyCount: 3
  }
});

assert.equal(successObservation.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_SCHEMA_VERSION);
assert.equal(successObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PASS);
assert.equal(successObservation.window.successThresholdMet, true);
assert.equal(successObservation.window.needsMoreObservation, false);
assert.equal(successObservation.metrics.candidateStability, true);
assert.equal(successObservation.metrics.adapterStability, true);
assert.equal(successObservation.metrics.rollbackReady, true);
assert.equal(successObservation.effects.affectsLegacyWinner, false);
assert.equal(successObservation.effects.affectsCoordinateResult, false);
assert.equal(successObservation.effects.affectsKml, false);

const partialObservation = buildControlledMigrationExperimentObservation({
  runningSession: buildRunningSession(),
  runId: "run_001",
  metrics: {
    minimumRuns: 3,
    observationCount: 1,
    passCount: 1,
    failCount: 0,
    candidateStableCount: 1,
    adapterStableCount: 1,
    rollbackReadyCount: 1
  }
});

assert.equal(partialObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PARTIAL);
assert.equal(partialObservation.window.needsMoreObservation, true);
assert.equal(partialObservation.safety.passed, true);

const failureObservation = buildControlledMigrationExperimentObservation({
  runningSession: buildRunningSession(),
  runId: "run_fail",
  kmlImpactDetected: true,
  coordinateResultWriteDetected: true,
  metrics: {
    minimumRuns: 3,
    observationCount: 2,
    passCount: 1,
    failCount: 1,
    kmlImpactCount: 1,
    rollbackReadyCount: 2
  }
});

assert.equal(failureObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL);
assert.equal(failureObservation.metrics.failureReasons.includes("kml_impact_detected"), true);
assert.equal(failureObservation.metrics.failureReasons.includes("coordinate_result_write_detected"), true);
assert.equal(failureObservation.safetyBoundary.failClosed, true);

const rollbackObservation = buildControlledMigrationExperimentObservation({
  runningSession: buildRunningSession(),
  runId: "run_rollback",
  metrics: {
    observationCount: 2,
    passCount: 1,
    rollbackTriggered: true,
    legacyRestored: true,
    observationPreserved: true
  }
});

assert.equal(rollbackObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.ROLLED_BACK);
assert.equal(rollbackObservation.rollback.triggered, true);
assert.equal(rollbackObservation.rollback.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(rollbackObservation.rollback.rollbackValue, false);
assert.equal(rollbackObservation.rollback.legacyRestored, true);
assert.equal(rollbackObservation.rollback.observationPreserved, true);

const scopeLeakObservation = buildControlledMigrationExperimentObservation({
  runningSession: buildRunningSession({
    category: "structured_legal_coordinate",
    winnerEvidenceType: "structured_cadastral_table"
  }),
  scopeLeakageDetected: true,
  metrics: {
    observationCount: 1,
    passCount: 0,
    failCount: 1
  }
});

assert.equal(scopeLeakObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL);
assert.equal(scopeLeakObservation.metrics.failureReasons.includes("category_must_be_verified_transformation"), true);
assert.equal(scopeLeakObservation.metrics.failureReasons.includes("winner_must_be_verified_utm_transformation"), true);
assert.equal(scopeLeakObservation.metrics.failureReasons.includes("scope_leakage_detected"), true);

const isolationLeakObservation = buildControlledMigrationExperimentObservation({
  runningSession: {
    ...buildRunningSession(),
    effects: {
      affectsLegacyWinner: true,
      affectsCoordinateResult: true,
      affectsKml: true
    }
  },
  legacyIsolationViolation: true,
  autoDecisionDetected: true,
  metrics: {
    observationCount: 1,
    passCount: 0,
    failCount: 1,
    legacyIsolationViolations: 1,
    autoDecisionLeakCount: 1
  }
});

assert.equal(isolationLeakObservation.classification, CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL);
assert.equal(isolationLeakObservation.metrics.failureReasons.includes("legacy_winner_effect_detected"), true);
assert.equal(isolationLeakObservation.metrics.failureReasons.includes("coordinate_result_effect_detected"), true);
assert.equal(isolationLeakObservation.metrics.failureReasons.includes("kml_effect_detected"), true);
assert.equal(isolationLeakObservation.metrics.failureReasons.includes("auto_decision_detected"), true);

const securityObservation = buildControlledMigrationExperimentObservation({
  runningSession: {
    ...buildRunningSession(),
    sessionId: "prompt: sk-secret-observation-token",
    authorizationSnapshot: {
      authorizedBy: "bearer sk-secret-observation-token",
      authorizedAt: "2026-08-12T00:00:00.000Z",
      status: "AUTHORIZED"
    },
    snapshots: {
      proposal: {
        winnerEvidenceType: "model_response: coordinate rows"
      },
      adapter: {
        classification: "authorization: secret"
      }
    }
  },
  runId: "prompt: coordinate rows",
  proposal: {
    classification: "prompt: sk-secret-observation-token",
    winnerEvidenceType: "model_response: coordinate rows"
  },
  dryRun: {
    classification: "authorization: secret"
  },
  metrics: {
    observationCount: 1,
    passCount: 1
  }
});

assertNoSensitiveSerialization(securityObservation);
assert.equal(securityObservation.security.sanitizedOnly, true);
assert.equal(securityObservation.security.rawOcrAllowed, false);
assert.equal(securityObservation.security.promptAllowed, false);
assert.equal(securityObservation.security.modelResponseAllowed, false);
assert.equal(securityObservation.security.credentialsAllowed, false);
assert.equal(securityObservation.security.imageDataAllowed, false);
assert.equal(securityObservation.security.coordinateRowsAllowed, false);

assert.equal(successObservation.safetyBoundary.actualExperimentExecution, false);
assert.equal(successObservation.safetyBoundary.productionMigrationExecuted, false);
assert.equal(successObservation.safetyBoundary.coordinateResultChanged, false);
assert.equal(successObservation.safetyBoundary.kmlChanged, false);
assert.equal(successObservation.safetyBoundary.frontendChanged, false);
assert.equal(successObservation.safetyBoundary.autoDecisionEnabled, false);
assert.equal(successObservation.safetyBoundary.globalRolloutEnabled, false);

console.log("Controlled Migration Experiment Observation Regression: 7/7 PASS");
