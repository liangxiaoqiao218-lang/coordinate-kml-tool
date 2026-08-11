import {
  CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS,
  buildControlledMigrationExperimentActivationPreflight
} from "./controlled-migration-activation.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_SCHEMA_VERSION =
  "controlled_migration_experiment_execution_proposal_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE = Object.freeze({
  START_READY: "START_READY",
  OBSERVE: "OBSERVE",
  SUCCESS: "SUCCESS",
  PARTIAL: "PARTIAL",
  FAIL: "FAIL",
  ROLLBACK_REQUIRED: "ROLLBACK_REQUIRED",
  ROLLED_BACK: "ROLLED_BACK",
  BLOCKED: "BLOCKED"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=]|prompt\s*[:=]|model[_-]?response\s*[:=])/ig;

function cleanString(value, fallback = "") {
  const cleaned = String(value ?? fallback)
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .trim();
  return cleaned || fallback;
}

function nullableString(value) {
  const cleaned = cleanString(value);
  return cleaned || null;
}

function normalizeBoolean(value, fallback = false) {
  return value === true || value === false ? value : fallback;
}

function normalizeActivation(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_activation_preflight_v1") {
    return input;
  }
  return buildControlledMigrationExperimentActivationPreflight(
    input.activation
    || input.controlledMigrationExperimentActivationPreflight
    || input.response?.controlledMigrationExperimentActivationPreflight
    || input
  );
}

function normalizeObservation(value = {}) {
  return Object.freeze({
    observationCount: Number.isFinite(Number(value.observationCount)) ? Number(value.observationCount) : 0,
    successCount: Number.isFinite(Number(value.successCount)) ? Number(value.successCount) : 0,
    failureCount: Number.isFinite(Number(value.failureCount)) ? Number(value.failureCount) : 0,
    legacyIsolationViolations: Number.isFinite(Number(value.legacyIsolationViolations)) ? Number(value.legacyIsolationViolations) : 0,
    kmlImpactDetected: value.kmlImpactDetected === true,
    autoDecisionDetected: value.autoDecisionDetected === true,
    unexpectedDiffDetected: value.unexpectedDiffDetected === true,
    rollbackTriggered: value.rollbackTriggered === true
  });
}

function buildFailureReasons(activation = {}, observation = {}) {
  const reasons = [];
  if (activation.status !== CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE) {
    reasons.push("activation_not_ready");
  }
  for (const issue of activation.executionReadiness?.issues || []) {
    reasons.push(issue);
  }
  if (activation.flags?.kmlGate === true) reasons.push("kml_gate_must_remain_disabled");
  if (activation.flags?.autoDecision === true) reasons.push("auto_decision_must_remain_disabled");
  if (activation.safety?.experimentExecuted === true) reasons.push("experiment_already_executed");
  if (activation.safety?.productionMigrationExecuted === true) reasons.push("production_migration_detected");
  if (activation.safety?.coordinateResultChanged === true) reasons.push("coordinate_result_change_detected");
  if (activation.safety?.kmlChanged === true) reasons.push("kml_change_detected");
  if (activation.safety?.frontendChanged === true) reasons.push("frontend_change_detected");
  if (observation.legacyIsolationViolations > 0) reasons.push("legacy_isolation_violation");
  if (observation.kmlImpactDetected) reasons.push("kml_impact_detected");
  if (observation.autoDecisionDetected) reasons.push("auto_decision_detected");
  if (observation.unexpectedDiffDetected) reasons.push("unexpected_diff_detected");
  if (observation.failureCount > 0) reasons.push("observation_failure_detected");
  return Object.freeze([...new Set(reasons.map(reason => cleanString(reason)).filter(Boolean))]);
}

function classifyProposal({ activation, observation, failureReasons }) {
  if (observation.rollbackTriggered) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLED_BACK;
  }
  if (failureReasons.some(reason => [
    "kml_impact_detected",
    "auto_decision_detected",
    "legacy_isolation_violation",
    "unexpected_diff_detected",
    "coordinate_result_change_detected",
    "kml_change_detected",
    "production_migration_detected",
    "observation_failure_detected"
  ].includes(reason))) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLBACK_REQUIRED;
  }
  if (activation.status !== CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.BLOCKED;
  }
  if (observation.observationCount >= 3 && observation.successCount >= observation.observationCount) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.SUCCESS;
  }
  if (observation.successCount > 0) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.PARTIAL;
  }
  if (observation.observationCount > 0) {
    return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.OBSERVE;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY;
}

function decisionForState(state) {
  switch (state) {
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY:
      return "start_controlled_experiment";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.OBSERVE:
      return "continue_observation";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.SUCCESS:
      return "archive_success_do_not_expand_scope";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.PARTIAL:
      return "extend_observation_window";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLBACK_REQUIRED:
      return "trigger_single_flag_rollback";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLED_BACK:
      return "archive_rollback_event";
    case CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.FAIL:
      return "freeze_experiment";
    default:
      return "do_not_start";
  }
}

export function buildControlledMigrationExperimentExecutionProposal(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const activation = normalizeActivation(
    input.activation
    || source.controlledMigrationExperimentActivationPreflight
    || source.activation
    || source
  );
  const observation = normalizeObservation(input.observation || source.observation || {});
  const failureReasons = buildFailureReasons(activation, observation);
  const state = classifyProposal({ activation, observation, failureReasons });

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_SCHEMA_VERSION,
    experimentId: nullableString(activation.experimentId),
    state,
    decision: decisionForState(state),
    activation: Object.freeze({
      status: nullableString(activation.status),
      ready: activation.activation?.ready === true,
      canStartExperiment: activation.activation?.canStartExperiment === true
    }),
    startProcedure: Object.freeze({
      packageReady: activation.executionReadiness?.packageStatus === "READY_FOR_EXPERIMENT",
      approvalComplete: activation.approval?.complete === true,
      flagsComplete: activation.flags?.complete === true,
      rollbackReady: activation.rollback?.ready === true,
      scopeLocked: activation.scope?.locked === true,
      executionReady: activation.executionReadiness?.packagePreconditionsSatisfied === true,
      startAllowed: state === CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY
    }),
    steps: Object.freeze({
      start: "record activation snapshot and keep execution non-mutating",
      validate: "verify package, approval, flags, rollback, scope, and activation",
      observe: "collect sanitized observation counters and effects summary",
      decide: "classify SUCCESS, PARTIAL, FAIL, or ROLLED_BACK without scope expansion"
    }),
    observation: Object.freeze({
      minimumRuns: 3,
      observationCount: observation.observationCount,
      successCount: observation.successCount,
      failureCount: observation.failureCount,
      legacyIsolationViolations: observation.legacyIsolationViolations,
      kmlImpactDetected: observation.kmlImpactDetected,
      autoDecisionDetected: observation.autoDecisionDetected,
      unexpectedDiffDetected: observation.unexpectedDiffDetected,
      failureReasons
    }),
    rollback: Object.freeze({
      recommended: state === CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLBACK_REQUIRED,
      triggered: observation.rollbackTriggered || state === CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLED_BACK,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    report: Object.freeze({
      title: "Controlled Migration Experiment Execution Proposal",
      sections: Object.freeze([
        "Experiment Identity",
        "Activation",
        "Start Procedure",
        "Observation Window",
        "Rollback",
        "Decision"
      ]),
      sanitizedOnly: true
    }),
    scope: Object.freeze({
      verifiedTransformationOnly: activation.scope?.verifiedTransformationOnly === true,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      productionRolloutAllowed: false
    }),
    safety: Object.freeze({
      proposalOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: [
        CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.BLOCKED,
        CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLBACK_REQUIRED,
        CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.ROLLED_BACK,
        CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.FAIL
      ].includes(state)
    }),
    security: Object.freeze({
      sanitizedOnly: true,
      rawOcrAllowed: false,
      promptAllowed: false,
      modelResponseAllowed: false,
      credentialsAllowed: false,
      imageDataAllowed: false,
      coordinateRowsAllowed: false
    })
  });
}
