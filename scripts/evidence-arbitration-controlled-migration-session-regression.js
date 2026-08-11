#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  CONTROLLED_MIGRATION_EXPERIMENT_SESSION_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS,
  buildControlledMigrationExperimentSession
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

function assertNoSensitiveSerialization(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("sk-secret-session-token"), false);
  assert.equal(serialized.includes("prompt:"), false);
  assert.equal(serialized.includes("model_response:"), false);
  assert.equal(serialized.includes("coordinate rows"), false);
}

const readySession = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization(),
  commit: "d8efd2385242b72f02565b5fca8762809c8f6a89",
  preparedAt: "2026-08-12T00:05:00.000Z",
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

assert.equal(readySession.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_SCHEMA_VERSION);
assert.equal(readySession.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED);
assert.equal(readySession.environment.valid, true);
assert.equal(readySession.startPreparation.canStart, true);
assert.equal(readySession.category, "verified_transformation");
assert.equal(readySession.winnerEvidenceType, "verified_utm_transformation");
assert.equal(readySession.safety.preparationOnly, true);
assert.equal(readySession.safety.experimentStarted, false);
assert.equal(readySession.safety.experimentExecuted, false);
assert.equal(readySession.safety.productionMigrationExecuted, false);
assert.equal(readySession.safety.coordinateResultChanged, false);
assert.equal(readySession.safety.kmlChanged, false);
assert.equal(readySession.safety.autoDecisionEnabled, false);
assert.equal(readySession.safety.affectsLegacyWinner, false);
assert.equal(readySession.safety.affectsCoordinateResult, false);
assert.equal(readySession.safety.affectsKml, false);

const missingAuthorization = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization({
    status: "PENDING",
    startPermission: {
      canStartExperiment: false
    }
  })
});

assert.equal(missingAuthorization.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED);
assert.equal(missingAuthorization.environment.valid, false);
assert.equal(missingAuthorization.environment.issues.includes("authorization_not_authorized"), true);
assert.equal(missingAuthorization.environment.issues.includes("start_permission_missing"), true);

const flagConflict = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization({
    flags: {
      kmlGate: true,
      autoDecision: true
    }
  })
});

assert.equal(flagConflict.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED);
assert.equal(flagConflict.environment.issues.includes("kml_gate_must_remain_disabled"), true);
assert.equal(flagConflict.environment.issues.includes("auto_decision_must_remain_disabled"), true);

const rollbackMissing = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization({
    preconditions: {
      rollbackReady: false
    }
  })
});

assert.equal(rollbackMissing.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED);
assert.equal(rollbackMissing.environment.issues.includes("rollback_not_ready"), true);
assert.equal(rollbackMissing.abort.blocked, true);
assert.equal(rollbackMissing.safety.failClosed, true);

const scopeLeakage = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization({
    scope: {
      verifiedTransformationOnly: false,
      productionRolloutAllowed: true
    }
  })
});

assert.equal(scopeLeakage.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED);
assert.equal(scopeLeakage.environment.issues.includes("scope_must_be_verified_transformation_only"), true);
assert.equal(scopeLeakage.environment.issues.includes("production_rollout_not_allowed"), true);

const snapshotSession = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization(),
  snapshots: {
    legacy: {
      coordinateType: "utm_projected_xy",
      precisionMode: "verified_transform",
      coordinateResultState: "CONFIRMED",
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

assert.equal(snapshotSession.snapshots.legacy.coordinateType, "utm_projected_xy");
assert.equal(snapshotSession.snapshots.proposal.winnerEvidenceType, "verified_utm_transformation");
assert.equal(snapshotSession.snapshots.safety.rollbackSafe, true);
assert.equal(snapshotSession.snapshots.adapter.validated, true);
assert.deepEqual(snapshotSession.flagsSnapshot, {
  dryRun: true,
  reviewOnly: true,
  migration: true,
  kmlGate: false,
  autoDecision: false
});

const revokedSession = buildControlledMigrationExperimentSession({
  authorization: baseAuthorization({
    status: "REVOKED",
    startPermission: {
      canStartExperiment: false
    }
  })
});

assert.equal(revokedSession.status, CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.ABORTED);
assert.equal(revokedSession.abort.aborted, true);
assert.equal(revokedSession.abort.rollbackRecommended, true);
assert.equal(revokedSession.abort.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
assert.equal(revokedSession.abort.rollbackValue, false);

const securitySession = buildControlledMigrationExperimentSession({
  experimentId: "prompt: sk-secret-session-token",
  sessionId: "model_response: coordinate rows should not appear",
  authorization: baseAuthorization({
    authorization: {
      authorizedBy: "bearer sk-secret-session-token",
      reason: "prompt: model_response: coordinate rows"
    }
  }),
  snapshots: {
    legacy: {
      coordinateType: "authorization: secret"
    }
  }
});

assertNoSensitiveSerialization(securitySession);
assert.equal(securitySession.security.sanitizedOnly, true);
assert.equal(securitySession.security.rawOcrAllowed, false);
assert.equal(securitySession.security.promptAllowed, false);
assert.equal(securitySession.security.modelResponseAllowed, false);
assert.equal(securitySession.security.credentialsAllowed, false);
assert.equal(securitySession.security.imageDataAllowed, false);
assert.equal(securitySession.security.coordinateRowsAllowed, false);

console.log("Controlled Migration Experiment Session Regression: 8/8 PASS");
