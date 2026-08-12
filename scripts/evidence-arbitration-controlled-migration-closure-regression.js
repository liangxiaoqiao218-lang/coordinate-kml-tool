#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE,
  buildControlledMigrationExperimentClosure,
  buildControlledMigrationExperimentResultPackage
} from "../server/coordinate-evidence/index.js";

function resultPackage(finalClassification, overrides = {}) {
  return {
    schemaVersion: "controlled_migration_experiment_result_package_v1",
    experimentId: "verified_transformation_controlled_migration_v1",
    sessionId: "verified_transformation_controlled_migration_v1:session_001",
    resultPackageId: `verified_transformation_controlled_migration_v1:${finalClassification.toLowerCase()}_result_package`,
    commit: "30ca76da3b13f346c8606a652866b5fa811fc091",
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    observationSummary: {
      runCount: finalClassification === "SUCCESS" ? 3 : 1,
      passCount: finalClassification === "SUCCESS" ? 3 : 0,
      partialCount: finalClassification === "PARTIAL" ? 1 : 0,
      failCount: finalClassification === "FAIL" ? 1 : 0,
      rollbackCount: finalClassification === "ROLLED_BACK" ? 1 : 0,
      legacyIsolationViolations: 0,
      kmlImpactCount: 0,
      autoDecisionLeakCount: 0,
      unexpectedDiffCount: 0
    },
    reviewSummary: {
      decision: finalClassification,
      recommendation: "archive_result",
      blockReasons: [],
      scopeLeakageDetected: false,
      productionMigrationAllowed: false,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false
    },
    rollbackSummary: {
      triggered: finalClassification === "ROLLED_BACK",
      reason: finalClassification === "ROLLED_BACK" ? "rollback_triggered" : null,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      legacyRestored: finalClassification === "ROLLED_BACK",
      coordinateResultRestored: finalClassification === "ROLLED_BACK",
      kmlBehaviorRestored: finalClassification === "ROLLED_BACK",
      observationPreserved: true
    },
    finalClassification,
    approvals: {
      productionMigrationApproved: false,
      kmlMigrationApproved: false,
      autoDecisionApproved: false,
      scopeExpansionApproved: false
    },
    scope: {
      verifiedTransformationOnly: true,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      scopeLeakageDetected: false
    },
    effects: {
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    },
    security: {
      sanitizedOnly: true,
      rawOcrAllowed: false,
      promptAllowed: false,
      modelResponseAllowed: false,
      credentialsAllowed: false,
      imageDataAllowed: false,
      coordinateRowsAllowed: false
    },
    ...overrides
  };
}

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-closure-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const successClosure = buildControlledMigrationExperimentClosure({
  resultPackage: resultPackage("SUCCESS"),
  closedAt: "2026-08-12T01:15:00.000Z",
  closedBy: "operator_1"
});

assert.equal(successClosure.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_SCHEMA_VERSION);
assert.equal(successClosure.closureState, CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.SUCCESS_CLOSED);
assert.equal(successClosure.finalClassification, "SUCCESS");
assert.equal(successClosure.approvals.productionMigrationApproved, false);
assert.equal(successClosure.approvals.kmlMigrationApproved, false);
assert.equal(successClosure.approvals.autoDecisionApproved, false);
assert.equal(successClosure.remainingBlockers.includes("requires_next_phase_review"), true);
assert.equal(successClosure.remainingBlockers.includes("requires_limited_scope_decision"), true);

const partialClosure = buildControlledMigrationExperimentClosure({
  resultPackage: resultPackage("PARTIAL")
});

assert.equal(partialClosure.closureState, CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.PARTIAL_CLOSED);
assert.equal(partialClosure.remainingBlockers.includes("requires_extended_observation"), true);
assert.equal(partialClosure.approvals.scopeExpansionApproved, false);

const failedClosure = buildControlledMigrationExperimentClosure({
  resultPackage: resultPackage("FAIL")
});

assert.equal(failedClosure.closureState, CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.FAILED_CLOSED);
assert.equal(failedClosure.remainingBlockers.includes("requires_root_cause_review"), true);
assert.equal(failedClosure.lifecycle.closureComplete, true);

const rolledBackClosure = buildControlledMigrationExperimentClosure({
  resultPackage: resultPackage("ROLLED_BACK")
});

assert.equal(rolledBackClosure.closureState, CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.ROLLED_BACK_CLOSED);
assert.equal(rolledBackClosure.remainingBlockers.includes("requires_rollback_audit"), true);
assert.equal(rolledBackClosure.rollbackArchive.triggered, true);
assert.equal(rolledBackClosure.rollbackArchive.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(rolledBackClosure.rollbackArchive.rollbackValue, false);
assert.equal(rolledBackClosure.rollbackArchive.legacyRestored, true);
assert.equal(rolledBackClosure.rollbackArchive.observationPreserved, true);

const scopeClosure = buildControlledMigrationExperimentClosure({
  resultPackage: resultPackage("FAIL", {
    scope: {
      verifiedTransformationOnly: false,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      scopeLeakageDetected: true
    }
  })
});

assert.equal(scopeClosure.scope.verifiedTransformationOnly, false);
assert.equal(scopeClosure.scope.scopeLeakageDetected, true);
assert.equal(scopeClosure.remainingBlockers.includes("scope_leakage_detected"), true);

const securityClosure = buildControlledMigrationExperimentClosure({
  resultPackage: buildControlledMigrationExperimentResultPackage({
    resultPackageId: "prompt: sk-secret-closure-token",
    review: {
      schemaVersion: "controlled_migration_experiment_review_v1",
      experimentId: "verified_transformation_controlled_migration_v1",
      reviewId: "bearer sk-secret-closure-token",
      decision: "SUCCESS",
      category: "verified_transformation",
      winnerEvidenceType: "verified_utm_transformation",
      inputs: {
        observationCount: 3,
        classifications: ["PASS", "PASS", "PASS"]
      },
      metrics: {
        minimumRuns: 3,
        passCount: 3,
        partialCount: 0,
        failCount: 0,
        failureReasons: ["model_response: coordinate rows"]
      },
      decisionRecord: {
        recommendation: "prompt: sk-secret-closure-token"
      },
      rollbackReview: {
        triggered: false,
        observationPreserved: true
      },
      scope: {
        verifiedTransformationOnly: true,
        scopeLeakageDetected: false
      }
    }
  }),
  closedBy: "authorization: secret",
  closureReason: "prompt: coordinate rows"
});

assertNoSensitiveSerialization(securityClosure);
assert.equal(securityClosure.security.sanitizedOnly, true);
assert.equal(securityClosure.security.rawOcrAllowed, false);
assert.equal(securityClosure.security.promptAllowed, false);
assert.equal(securityClosure.security.modelResponseAllowed, false);
assert.equal(securityClosure.security.credentialsAllowed, false);
assert.equal(securityClosure.security.imageDataAllowed, false);
assert.equal(securityClosure.security.coordinateRowsAllowed, false);

assert.equal(successClosure.effects.affectsLegacyWinner, false);
assert.equal(successClosure.effects.affectsCoordinateResult, false);
assert.equal(successClosure.effects.affectsKml, false);
assert.equal(successClosure.safetyBoundary.experimentExecuted, false);
assert.equal(successClosure.safetyBoundary.productionMigrationExecuted, false);
assert.equal(successClosure.safetyBoundary.coordinateResultChanged, false);
assert.equal(successClosure.safetyBoundary.kmlChanged, false);
assert.equal(successClosure.safetyBoundary.frontendChanged, false);
assert.equal(successClosure.safetyBoundary.autoDecisionEnabled, false);
assert.equal(successClosure.safetyBoundary.globalRolloutEnabled, false);
assert.equal(successClosure.safetyBoundary.productionMigrationApproved, false);
assert.equal(successClosure.safetyBoundary.kmlMigrationApproved, false);
assert.equal(successClosure.safetyBoundary.autoDecisionApproved, false);

console.log("Controlled Migration Experiment Closure Regression: 6/6 PASS");
