#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION,
  CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_SCHEMA_VERSION,
  buildControlledMigrationExperimentObservation,
  buildControlledMigrationExperimentReview,
  buildControlledMigrationExperimentSession,
  buildControlledMigrationExperimentStart
} from "../server/coordinate-evidence/index.js";

function baseAuthorization() {
  return {
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
}

function buildRunningSession(overrides = {}) {
  const preparedSession = buildControlledMigrationExperimentSession({
    authorization: baseAuthorization(),
    commit: "0f51c7d246d099e2c145ee15146ca8475aebd58c",
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

function buildObservation(metrics = {}, overrides = {}) {
  return buildControlledMigrationExperimentObservation({
    runningSession: buildRunningSession(overrides.runningSession || {}),
    runId: overrides.runId || "run_001",
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
      rollbackReadyCount: 3,
      ...metrics
    },
    ...overrides
  });
}

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-review-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const successReview = buildControlledMigrationExperimentReview({
  reviewedAt: "2026-08-12T00:45:00.000Z",
  observations: [
    buildObservation({}, { runId: "run_001" }),
    buildObservation({}, { runId: "run_002" }),
    buildObservation({}, { runId: "run_003" })
  ]
});

assert.equal(successReview.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_SCHEMA_VERSION);
assert.equal(successReview.decision, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.SUCCESS);
assert.equal(successReview.metrics.passCount, 3);
assert.equal(successReview.decisionRecord.productionMigrationAllowed, false);
assert.equal(successReview.decisionRecord.kmlMigrationAllowed, false);
assert.equal(successReview.decisionRecord.autoDecisionAllowed, false);
assert.equal(successReview.nextStep.prepareNextReview, true);
assert.equal(successReview.nextStep.expandScope, false);

const partialReview = buildControlledMigrationExperimentReview({
  observations: [
    buildObservation({
      observationCount: 1,
      passCount: 1,
      candidateStableCount: 1,
      adapterStableCount: 1,
      rollbackReadyCount: 1
    }, { runId: "run_001" })
  ]
});

assert.equal(partialReview.decision, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.PARTIAL);
assert.equal(partialReview.nextStep.extendObservation, true);
assert.equal(partialReview.decisionRecord.recommendation, "extend_observation_window_same_scope");

const failReview = buildControlledMigrationExperimentReview({
  observations: [
    buildObservation({
      observationCount: 2,
      passCount: 1,
      failCount: 1,
      kmlImpactCount: 1
    }, {
      runId: "run_fail",
      kmlImpactDetected: true,
      coordinateResultWriteDetected: true
    })
  ]
});

assert.equal(failReview.decision, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL);
assert.equal(failReview.nextStep.freezeExperiment, true);
assert.equal(failReview.safetyBoundary.failClosed, true);
assert.equal(failReview.metrics.failureReasons.includes("kml_impact_detected"), true);
assert.equal(failReview.metrics.failureReasons.includes("coordinate_result_write_detected"), true);

const rollbackReview = buildControlledMigrationExperimentReview({
  observations: [
    buildObservation({
      observationCount: 2,
      passCount: 1,
      rollbackTriggered: true,
      legacyRestored: true,
      observationPreserved: true
    }, { runId: "run_rollback" })
  ]
});

assert.equal(rollbackReview.decision, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK);
assert.equal(rollbackReview.nextStep.archiveRollback, true);
assert.equal(rollbackReview.rollbackReview.triggered, true);
assert.equal(rollbackReview.rollbackReview.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(rollbackReview.rollbackReview.rollbackValue, false);
assert.equal(rollbackReview.rollbackReview.legacyRestored, true);
assert.equal(rollbackReview.rollbackReview.observationPreserved, true);

const scopeLeakReview = buildControlledMigrationExperimentReview({
  observations: [
    {
      ...buildObservation({
        observationCount: 1,
        passCount: 0,
        failCount: 1
      }, {
        runId: "run_scope",
        scopeLeakageDetected: true
      }),
      category: "structured_legal_coordinate",
      winnerEvidenceType: "structured_cadastral_table"
    }
  ]
});

assert.equal(scopeLeakReview.decision, CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL);
assert.equal(scopeLeakReview.scope.scopeLeakageDetected, true);
assert.equal(scopeLeakReview.scope.verifiedTransformationOnly, false);

const securityReview = buildControlledMigrationExperimentReview({
  reviewId: "prompt: sk-secret-review-token",
  observations: [
    buildObservation({}, {
      runId: "model_response: coordinate rows",
      proposal: {
        classification: "authorization: secret",
        winnerEvidenceType: "prompt: sk-secret-review-token"
      },
      runningSession: {
        sessionId: "prompt: coordinate rows",
        authorizationSnapshot: {
          status: "AUTHORIZED",
          authorizedBy: "bearer sk-secret-review-token"
        }
      }
    })
  ]
});

assertNoSensitiveSerialization(securityReview);
assert.equal(securityReview.security.sanitizedOnly, true);
assert.equal(securityReview.security.rawOcrAllowed, false);
assert.equal(securityReview.security.promptAllowed, false);
assert.equal(securityReview.security.modelResponseAllowed, false);
assert.equal(securityReview.security.credentialsAllowed, false);
assert.equal(securityReview.security.imageDataAllowed, false);
assert.equal(securityReview.security.coordinateRowsAllowed, false);

assert.equal(successReview.effects.affectsLegacyWinner, false);
assert.equal(successReview.effects.affectsCoordinateResult, false);
assert.equal(successReview.effects.affectsKml, false);
assert.equal(successReview.safetyBoundary.experimentExecuted, false);
assert.equal(successReview.safetyBoundary.productionMigrationExecuted, false);
assert.equal(successReview.safetyBoundary.coordinateResultChanged, false);
assert.equal(successReview.safetyBoundary.kmlChanged, false);
assert.equal(successReview.safetyBoundary.frontendChanged, false);
assert.equal(successReview.safetyBoundary.autoDecisionEnabled, false);
assert.equal(successReview.safetyBoundary.globalRolloutEnabled, false);

console.log("Controlled Migration Experiment Review Regression: 6/6 PASS");
