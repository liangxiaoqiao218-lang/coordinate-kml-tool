#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE,
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
    acknowledgements: {
      rollbackAcknowledged: true,
      kmlDisabledAcknowledged: true,
      autoDecisionDisabledAcknowledged: true,
      scopeAcknowledged: true,
      notProductionMigrationAcknowledged: true
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
    revocation: {
      revoked: false,
      revokeRequiresRollback: false,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      restoresLegacy: true,
      preservesObservation: true
    },
    readiness: {
      ready: true,
      issues: []
    },
    safety: {
      authorizationOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: false
    },
    security: {
      sanitizedOnly: true,
      rawOcrAllowed: false,
      promptAllowed: false,
      modelResponseAllowed: false,
      credentialsAllowed: false,
      imageDataAllowed: false,
      coordinateRowsAllowed: false
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

function buildPreparedSession(authorization = baseAuthorization(), overrides = {}) {
  return buildControlledMigrationExperimentSession({
    authorization,
    commit: "e232d432fc1e41c0abf7977b46021b6f26b40de8",
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
    },
    ...overrides
  });
}

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-start-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const startSuccess = buildControlledMigrationExperimentStart({
  session: buildPreparedSession(),
  startTime: "2026-08-12T00:15:00.000Z",
  runningSessionId: "verified_transformation_controlled_migration_v1:session_001"
});

assert.equal(startSuccess.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_SCHEMA_VERSION);
assert.equal(startSuccess.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.OBSERVING);
assert.equal(startSuccess.start.recordCreated, true);
assert.equal(startSuccess.start.startAllowed, true);
assert.equal(startSuccess.start.startExecutionPerformed, false);
assert.equal(startSuccess.observationEntry.entered, true);
assert.equal(startSuccess.observationEntry.firstObservationStatus, "PENDING");
assert.equal(startSuccess.category, "verified_transformation");
assert.equal(startSuccess.winnerEvidenceType, "verified_utm_transformation");
assert.equal(startSuccess.flagsSnapshot.kmlGate, false);
assert.equal(startSuccess.flagsSnapshot.autoDecision, false);
assert.equal(startSuccess.safety.actualExperimentStarted, false);
assert.equal(startSuccess.safety.experimentExecuted, false);
assert.equal(startSuccess.safety.productionMigrationExecuted, false);

const missingAuthorization = buildControlledMigrationExperimentStart({
  session: buildPreparedSession(baseAuthorization({
    status: "PENDING",
    startPermission: {
      canStartExperiment: false
    }
  }))
});

assert.equal(missingAuthorization.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.BLOCKED);
assert.equal(missingAuthorization.preconditions.issues.includes("session_not_prepared"), true);
assert.equal(missingAuthorization.preconditions.issues.includes("authorization_not_authorized"), true);
assert.equal(missingAuthorization.start.startAllowed, false);
assert.equal(missingAuthorization.safety.failClosed, true);

const sessionNotPrepared = buildControlledMigrationExperimentStart({
  session: {
    ...buildPreparedSession(),
    status: "BLOCKED",
    startPreparation: {
      canStart: false,
      blockReasons: ["manual_block"]
    },
    environment: {
      valid: false,
      issues: ["manual_block"]
    }
  }
});

assert.equal(sessionNotPrepared.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.BLOCKED);
assert.equal(sessionNotPrepared.preconditions.issues.includes("session_not_prepared"), true);
assert.equal(sessionNotPrepared.preconditions.issues.includes("manual_block"), true);

const flagConflict = buildControlledMigrationExperimentStart({
  session: buildPreparedSession(),
  flags: {
    dryRun: true,
    reviewOnly: true,
    migration: true,
    kmlGate: true,
    autoDecision: true
  }
});

assert.equal(flagConflict.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.BLOCKED);
assert.equal(flagConflict.preconditions.issues.includes("kml_gate_must_remain_disabled"), true);
assert.equal(flagConflict.preconditions.issues.includes("auto_decision_must_remain_disabled"), true);

const rollbackTriggered = buildControlledMigrationExperimentStart({
  session: buildPreparedSession(),
  rollbackTriggered: true
});

assert.equal(rollbackTriggered.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.ROLLED_BACK);
assert.equal(rollbackTriggered.abort.rollbackRecommended, true);
assert.equal(rollbackTriggered.abort.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(rollbackTriggered.abort.rollbackValue, false);

const scopeLeakage = buildControlledMigrationExperimentStart({
  session: {
    ...buildPreparedSession(),
    category: "structured_legal_coordinate",
    winnerEvidenceType: "structured_cadastral_table"
  }
});

assert.equal(scopeLeakage.state, CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.BLOCKED);
assert.equal(scopeLeakage.preconditions.issues.includes("category_must_be_verified_transformation"), true);
assert.equal(scopeLeakage.preconditions.issues.includes("winner_must_be_verified_utm_transformation"), true);

assert.equal(startSuccess.effects.affectsLegacyWinner, false);
assert.equal(startSuccess.effects.affectsCoordinateResult, false);
assert.equal(startSuccess.effects.affectsKml, false);
assert.equal(startSuccess.safety.coordinateResultChanged, false);
assert.equal(startSuccess.safety.kmlChanged, false);
assert.equal(startSuccess.safety.frontendChanged, false);
assert.equal(startSuccess.safety.autoDecisionEnabled, false);
assert.equal(startSuccess.safety.globalRolloutEnabled, false);

const securityStart = buildControlledMigrationExperimentStart({
  runningSessionId: "prompt: sk-secret-start-token",
  session: {
    ...buildPreparedSession(baseAuthorization({
      authorization: {
        authorizedBy: "bearer sk-secret-start-token",
        reason: "model_response: coordinate rows"
      }
    })),
    sessionId: "prompt: coordinate rows",
    snapshots: {
      legacy: {
        coordinateType: "authorization: secret"
      },
      proposal: {
        winnerEvidenceType: "model_response: coordinate rows"
      }
    }
  }
});

assertNoSensitiveSerialization(securityStart);
assert.equal(securityStart.security.sanitizedOnly, true);
assert.equal(securityStart.security.rawOcrAllowed, false);
assert.equal(securityStart.security.promptAllowed, false);
assert.equal(securityStart.security.modelResponseAllowed, false);
assert.equal(securityStart.security.credentialsAllowed, false);
assert.equal(securityStart.security.imageDataAllowed, false);
assert.equal(securityStart.security.coordinateRowsAllowed, false);

console.log("Controlled Migration Experiment Start Regression: 8/8 PASS");
