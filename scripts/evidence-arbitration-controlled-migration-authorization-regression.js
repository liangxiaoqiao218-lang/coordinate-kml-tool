import assert from "node:assert/strict";
import {
  CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_SCHEMA_VERSION,
  CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS,
  buildControlledMigrationExperimentAuthorization
} from "../server/coordinate-evidence/index.js";

function startReadyProposal(overrides = {}) {
  return {
    schemaVersion: "controlled_migration_experiment_execution_proposal_v1",
    experimentId: "verified_transformation_controlled_migration_v1",
    state: "START_READY",
    decision: "start_controlled_experiment",
    activation: {
      status: "READY_TO_ACTIVATE",
      ready: true,
      canStartExperiment: true
    },
    flags: {
      dryRun: true,
      reviewOnly: true,
      migration: true,
      kmlGate: false,
      autoDecision: false
    },
    startProcedure: {
      packageReady: true,
      approvalComplete: true,
      flagsComplete: true,
      rollbackReady: true,
      scopeLocked: true,
      executionReady: true,
      startAllowed: true
    },
    observation: {
      failureReasons: []
    },
    rollback: {
      recommended: false,
      triggered: false,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    },
    scope: {
      verifiedTransformationOnly: true,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      productionRolloutAllowed: false
    },
    safety: {
      proposalOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    },
    security: {
      sanitizedOnly: true
    },
    ...overrides
  };
}

function authorization(overrides = {}) {
  return {
    status: "AUTHORIZED",
    authorizedBy: "operator_1",
    authorizedAt: "2026-08-12T00:00:00.000Z",
    reason: "verified transformation controlled experiment",
    rollbackAcknowledged: true,
    kmlDisabledAcknowledged: true,
    autoDecisionDisabledAcknowledged: true,
    scopeAcknowledged: true,
    notProductionMigrationAcknowledged: true,
    ...overrides
  };
}

function assertNoEffects(result) {
  assert.equal(result.safety.authorizationOnly, true);
  assert.equal(result.safety.experimentExecuted, false);
  assert.equal(result.safety.productionMigrationExecuted, false);
  assert.equal(result.safety.coordinateResultChanged, false);
  assert.equal(result.safety.kmlChanged, false);
  assert.equal(result.safety.frontendChanged, false);
  assert.equal(result.safety.autoDecisionEnabled, false);
  assert.equal(result.safety.affectsLegacyWinner, false);
  assert.equal(result.safety.affectsCoordinateResult, false);
  assert.equal(result.safety.affectsKml, false);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Pending authorization cannot start experiment", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization({ status: "PENDING" })
  });

  assert.equal(result.schemaVersion, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_SCHEMA_VERSION);
  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.PENDING);
  assert.equal(result.startPermission.canStartExperiment, false);
  assertNoEffects(result);
});

test("Approved authorization can start controlled experiment only", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization()
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED);
  assert.equal(result.readiness.ready, true);
  assert.equal(result.startPermission.canStartExperiment, true);
  assert.equal(result.startPermission.canStartProductionMigration, false);
  assert.equal(result.startPermission.canStartKmlMigration, false);
  assert.equal(result.startPermission.canStartAutoDecision, false);
  assertNoEffects(result);
});

test("Missing authorization fields block approval", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization({
      authorizedBy: null,
      authorizedAt: null,
      reason: ""
    })
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.BLOCKED);
  assert.ok(result.authorization.issues.includes("authorized_by_missing"));
  assert.ok(result.authorization.issues.includes("authorized_at_missing"));
  assert.ok(result.authorization.issues.includes("authorization_reason_missing"));
  assertNoEffects(result);
});

test("Flag conflict blocks authorization", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal({
      flags: {
        dryRun: true,
        reviewOnly: true,
        migration: true,
        kmlGate: true,
        autoDecision: true
      }
    }),
    authorization: authorization()
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.BLOCKED);
  assert.ok(result.flags.issues.includes("kml_gate_must_remain_disabled"));
  assert.ok(result.flags.issues.includes("auto_decision_must_remain_disabled"));
  assertNoEffects(result);
});

test("Rollback acknowledgement missing blocks authorization", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization({
      rollbackAcknowledged: false
    })
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.BLOCKED);
  assert.ok(result.authorization.issues.includes("rollback_acknowledgement_missing"));
  assertNoEffects(result);
});

test("Scope leakage blocks authorization", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal({
      scope: {
        verifiedTransformationOnly: false,
        kmlMigrationAllowed: false,
        autoDecisionAllowed: false,
        productionRolloutAllowed: false
      }
    }),
    authorization: authorization()
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.BLOCKED);
  assert.ok(result.scope.issues.includes("scope_must_be_verified_transformation_only"));
  assertNoEffects(result);
});

test("Revoked authorization requires rollback metadata", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization({
      status: "REVOKED"
    })
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED);
  assert.equal(result.revocation.revoked, true);
  assert.equal(result.revocation.revokeRequiresRollback, true);
  assert.equal(result.revocation.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
  assertNoEffects(result);
});

test("Rejected authorization cannot start", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal(),
    authorization: authorization({
      status: "REJECTED"
    })
  });

  assert.equal(result.status, CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REJECTED);
  assert.equal(result.startPermission.canStartExperiment, false);
  assertNoEffects(result);
});

test("Authorization output sanitizes secret-like strings", () => {
  const result = buildControlledMigrationExperimentAuthorization({
    executionProposal: startReadyProposal({
      experimentId: "prompt:=secret",
      observation: {
        failureReasons: ["authorization:=bearer abc"]
      }
    }),
    authorization: authorization({
      authorizedBy: "token:=abc",
      reason: "secret:=value"
    })
  });
  const serialized = JSON.stringify(result);

  assert.equal(serialized.includes("prompt:=secret"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
  assert.equal(serialized.includes("token:=abc"), false);
  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(result.security.rawOcrAllowed, false);
  assert.equal(result.security.modelResponseAllowed, false);
  assert.equal(result.security.coordinateRowsAllowed, false);
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
  console.log(`Controlled Migration Experiment Authorization Regression: ${passed}/${tests.length} PASS`);
}
