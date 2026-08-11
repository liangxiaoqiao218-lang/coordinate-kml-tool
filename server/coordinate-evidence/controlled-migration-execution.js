import {
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION,
  buildEvidenceArbitrationControlledMigrationExperiment
} from "./controlled-migration-experiment.js";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_SCHEMA_VERSION =
  "evidence_arbitration_controlled_migration_execution_v1";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE = Object.freeze({
  READY: "READY",
  OBSERVING: "OBSERVING",
  SUCCESS: "SUCCESS",
  PARTIAL: "PARTIAL",
  FAIL: "FAIL",
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

function normalizeExperiment(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_controlled_migration_experiment_v1") {
    return input;
  }
  return buildEvidenceArbitrationControlledMigrationExperiment(
    input.experiment
    || input.evidenceArbitrationControlledMigrationExperiment
    || input.response?.evidenceArbitrationControlledMigrationExperiment
    || input
  );
}

function normalizeFlags(value = {}) {
  return Object.freeze({
    migration: normalizeBoolean(value.migration, false),
    kmlGate: normalizeBoolean(value.kmlGate, false),
    autoDecision: normalizeBoolean(value.autoDecision, false),
    reviewOnly: normalizeBoolean(value.reviewOnly, true),
    dryRun: normalizeBoolean(value.dryRun, true)
  });
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

function buildFailureReasons(experiment = {}, flags = {}, observation = {}) {
  const reasons = [];
  if (experiment.classification !== EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.READY_FOR_EXPERIMENT) {
    reasons.push("experiment_not_ready");
  }
  if (flags.migration !== true) reasons.push("migration_flag_disabled");
  if (flags.kmlGate === true) reasons.push("kml_gate_must_remain_disabled");
  if (flags.autoDecision === true) reasons.push("auto_decision_must_remain_disabled");
  if (experiment.safety?.productionWriteEnabled === true) reasons.push("production_write_not_allowed");
  if (experiment.safety?.coordinateResultProductionWrite === true) reasons.push("coordinate_result_production_write_detected");
  if (experiment.safety?.kmlWriteEnabled === true) reasons.push("kml_write_detected");
  if (experiment.safety?.affectsLegacyWinner === true) reasons.push("legacy_winner_mutation_detected");
  if (experiment.safety?.affectsCoordinateResult === true) reasons.push("coordinate_result_mutation_detected");
  if (experiment.safety?.affectsKml === true) reasons.push("kml_mutation_detected");
  if (experiment.rollback?.rollbackSafe !== true) reasons.push("rollback_not_safe");
  if (observation.legacyIsolationViolations > 0) reasons.push("legacy_isolation_violation");
  if (observation.kmlImpactDetected) reasons.push("kml_impact_detected");
  if (observation.autoDecisionDetected) reasons.push("auto_decision_detected");
  if (observation.unexpectedDiffDetected) reasons.push("unexpected_diff_detected");
  if (observation.failureCount > 0) reasons.push("observation_failure_detected");
  return Object.freeze([...new Set(reasons.map(reason => cleanString(reason)).filter(Boolean))]);
}

function classifyExecution({ experiment, flags, observation, failureReasons }) {
  if (observation.rollbackTriggered || experiment.classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.ROLLED_BACK;
  }
  if (experiment.classification !== EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.READY_FOR_EXPERIMENT) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.BLOCKED;
  }
  if (failureReasons.length > 0) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.FAIL;
  }
  if (observation.observationCount === 0) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.READY;
  }
  if (observation.successCount >= observation.observationCount && observation.observationCount >= 3) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.SUCCESS;
  }
  if (observation.successCount > 0) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.PARTIAL;
  }
  if (flags.migration === true) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.OBSERVING;
  }
  return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.READY;
}

export function buildEvidenceArbitrationControlledMigrationExecution(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const experiment = normalizeExperiment(
    input.experiment
    || source.evidenceArbitrationControlledMigrationExperiment
    || source.experiment
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || experiment.flags || {});
  const observation = normalizeObservation(input.observation || source.observation || {});
  const failureReasons = buildFailureReasons(experiment, flags, observation);
  const state = classifyExecution({
    experiment,
    flags,
    observation,
    failureReasons
  });

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_SCHEMA_VERSION,
    enabled: true,
    mode: "controlled_migration_experiment_execution",
    state,
    experiment: Object.freeze({
      experimentId: nullableString(experiment.experiment?.experimentId),
      classification: nullableString(experiment.classification),
      category: nullableString(experiment.experiment?.category),
      winnerEvidenceType: nullableString(experiment.experiment?.winnerEvidenceType),
      ready: experiment.experiment?.ready === true
    }),
    flags,
    observation: Object.freeze({
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
      rollbackTriggered: observation.rollbackTriggered || state === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.ROLLED_BACK,
      rollbackSafe: experiment.rollback?.rollbackSafe === true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    observability: Object.freeze({
      recordExecutionState: true,
      recordObservationSummary: true,
      recordRollbackMetadata: true,
      sanitizedOnly: true
    }),
    safety: Object.freeze({
      productionMigrationExecuted: false,
      coordinateResultProductionWrite: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      globalRolloutEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: [
        EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.FAIL,
        EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.BLOCKED,
        EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.ROLLED_BACK
      ].includes(state)
    })
  });
}
