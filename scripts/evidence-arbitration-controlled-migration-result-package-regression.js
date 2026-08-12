#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION,
  CONTROLLED_MIGRATION_EXPERIMENT_RESULT_PACKAGE_SCHEMA_VERSION,
  buildControlledMigrationExperimentObservation,
  buildControlledMigrationExperimentResultPackage,
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
    commit: "c4543c6e9a5b5531e351ee767fb21e1eab38ddb6",
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

function buildReview(observations) {
  return buildControlledMigrationExperimentReview({
    reviewedAt: "2026-08-12T00:45:00.000Z",
    observations
  });
}

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-result-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const successPackage = buildControlledMigrationExperimentResultPackage({
  review: buildReview([
    buildObservation({}, { runId: "run_001" }),
    buildObservation({}, { runId: "run_002" }),
    buildObservation({}, { runId: "run_003" })
  ]),
  commit: "c4543c6e9a5b5531e351ee767fb21e1eab38ddb6",
  generatedAt: "2026-08-12T01:00:00.000Z"
});

assert.equal(successPackage.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_RESULT_PACKAGE_SCHEMA_VERSION);
assert.equal(successPackage.finalClassification, CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.SUCCESS);
assert.equal(successPackage.observationSummary.runCount, 3);
assert.equal(successPackage.observationSummary.passCount, 3);
assert.equal(successPackage.reviewSummary.decision, "SUCCESS");
assert.equal(successPackage.approvals.productionMigrationApproved, false);
assert.equal(successPackage.approvals.kmlMigrationApproved, false);
assert.equal(successPackage.approvals.autoDecisionApproved, false);
assert.equal(successPackage.finalAction, "archive_result_prepare_next_review_only");

const partialPackage = buildControlledMigrationExperimentResultPackage({
  review: buildReview([
    buildObservation({
      observationCount: 1,
      passCount: 1,
      candidateStableCount: 1,
      adapterStableCount: 1,
      rollbackReadyCount: 1
    }, { runId: "run_001" })
  ])
});

assert.equal(partialPackage.finalClassification, CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.PARTIAL);
assert.equal(partialPackage.finalAction, "extend_observation_same_scope");
assert.equal(partialPackage.scope.verifiedTransformationOnly, true);
assert.equal(partialPackage.reviewSummary.productionMigrationAllowed, false);

const failPackage = buildControlledMigrationExperimentResultPackage({
  review: buildReview([
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
  ])
});

assert.equal(failPackage.finalClassification, CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.FAIL);
assert.equal(failPackage.finalAction, "freeze_experiment_root_cause_review");
assert.equal(failPackage.reviewSummary.blockReasons.includes("kml_impact_detected"), true);
assert.equal(failPackage.reviewSummary.blockReasons.includes("coordinate_result_write_detected"), true);

const rollbackPackage = buildControlledMigrationExperimentResultPackage({
  review: buildReview([
    buildObservation({
      observationCount: 2,
      passCount: 1,
      rollbackTriggered: true,
      legacyRestored: true,
      observationPreserved: true
    }, { runId: "run_rollback" })
  ])
});

assert.equal(rollbackPackage.finalClassification, CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.ROLLED_BACK);
assert.equal(rollbackPackage.rollbackSummary.triggered, true);
assert.equal(rollbackPackage.rollbackSummary.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(rollbackPackage.rollbackSummary.rollbackValue, false);
assert.equal(rollbackPackage.rollbackSummary.legacyRestored, true);
assert.equal(rollbackPackage.rollbackSummary.observationPreserved, true);

const securityPackage = buildControlledMigrationExperimentResultPackage({
  resultPackageId: "prompt: sk-secret-result-token",
  review: {
    ...buildReview([
      buildObservation({}, {
        runId: "model_response: coordinate rows",
        proposal: {
          classification: "authorization: secret",
          winnerEvidenceType: "prompt: sk-secret-result-token"
        },
        dryRun: {
          classification: "model_response: coordinate rows"
        }
      })
    ]),
    reviewId: "bearer sk-secret-result-token"
  }
});

assertNoSensitiveSerialization(securityPackage);
assert.equal(securityPackage.security.sanitizedOnly, true);
assert.equal(securityPackage.security.rawOcrAllowed, false);
assert.equal(securityPackage.security.promptAllowed, false);
assert.equal(securityPackage.security.modelResponseAllowed, false);
assert.equal(securityPackage.security.credentialsAllowed, false);
assert.equal(securityPackage.security.imageDataAllowed, false);
assert.equal(securityPackage.security.coordinateRowsAllowed, false);

assert.equal(successPackage.effects.affectsLegacyWinner, false);
assert.equal(successPackage.effects.affectsCoordinateResult, false);
assert.equal(successPackage.effects.affectsKml, false);
assert.equal(successPackage.safetyBoundary.experimentExecuted, false);
assert.equal(successPackage.safetyBoundary.productionMigrationExecuted, false);
assert.equal(successPackage.safetyBoundary.coordinateResultChanged, false);
assert.equal(successPackage.safetyBoundary.kmlChanged, false);
assert.equal(successPackage.safetyBoundary.frontendChanged, false);
assert.equal(successPackage.safetyBoundary.autoDecisionEnabled, false);
assert.equal(successPackage.safetyBoundary.globalRolloutEnabled, false);
assert.equal(successPackage.safetyBoundary.productionMigrationApproved, false);
assert.equal(successPackage.safetyBoundary.kmlMigrationApproved, false);
assert.equal(successPackage.safetyBoundary.autoDecisionApproved, false);

console.log("Controlled Migration Experiment Result Package Regression: 5/5 PASS");
