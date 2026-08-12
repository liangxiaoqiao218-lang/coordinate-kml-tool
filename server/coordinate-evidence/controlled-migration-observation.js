import {
  CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE,
  buildControlledMigrationExperimentStart
} from "./controlled-migration-start.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_SCHEMA_VERSION =
  "controlled_migration_experiment_observation_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION = Object.freeze({
  PASS: "PASS",
  PARTIAL: "PARTIAL",
  FAIL: "FAIL",
  ROLLED_BACK: "ROLLED_BACK"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=]|prompt\s*[:=]|model[_-]?response\s*[:=]|raw\s*ocr|image\/base64|coordinate\s+rows?)/ig;

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

function normalizeCount(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 0 ? numeric : fallback;
}

function normalizeRunningSession(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_running_session_v1") {
    return input;
  }
  return buildControlledMigrationExperimentStart(
    input.runningSession
    || input.controlledMigrationExperimentRunningSession
    || input.response?.controlledMigrationExperimentRunningSession
    || input
  );
}

function normalizeMetrics(value = {}) {
  return Object.freeze({
    minimumRuns: normalizeCount(value.minimumRuns, 3) || 3,
    observationCount: normalizeCount(value.observationCount),
    passCount: normalizeCount(value.passCount),
    failCount: normalizeCount(value.failCount),
    candidateStableCount: normalizeCount(value.candidateStableCount),
    adapterStableCount: normalizeCount(value.adapterStableCount),
    legacyIsolationViolations: normalizeCount(value.legacyIsolationViolations),
    kmlImpactCount: normalizeCount(value.kmlImpactCount),
    autoDecisionLeakCount: normalizeCount(value.autoDecisionLeakCount),
    unexpectedDiffCount: normalizeCount(value.unexpectedDiffCount),
    rollbackReadyCount: normalizeCount(value.rollbackReadyCount),
    rollbackTriggered: value.rollbackTriggered === true,
    legacyRestored: value.legacyRestored === true,
    observationPreserved: value.observationPreserved !== false
  });
}

function buildFailureReasons({ input = {}, runningSession = {}, metrics = {} }) {
  const issues = [];
  const runningState = runningSession.state;
  if (![CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.OBSERVING,
    CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.STARTED].includes(runningState)) {
    issues.push("running_session_not_observing");
  }
  if (runningSession.category !== "verified_transformation") issues.push("category_must_be_verified_transformation");
  if (runningSession.winnerEvidenceType !== "verified_utm_transformation") {
    issues.push("winner_must_be_verified_utm_transformation");
  }
  if (runningSession.flagsSnapshot?.kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (runningSession.flagsSnapshot?.autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  if (runningSession.effects?.affectsLegacyWinner === true) issues.push("legacy_winner_effect_detected");
  if (runningSession.effects?.affectsCoordinateResult === true) issues.push("coordinate_result_effect_detected");
  if (runningSession.effects?.affectsKml === true) issues.push("kml_effect_detected");
  if (runningSession.safety?.coordinateResultChanged === true) issues.push("coordinate_result_change_detected");
  if (runningSession.safety?.kmlChanged === true) issues.push("kml_change_detected");
  if (runningSession.safety?.autoDecisionEnabled === true) issues.push("auto_decision_detected");
  if (runningSession.safety?.globalRolloutEnabled === true) issues.push("global_rollout_detected");
  if (input.candidateStable === false) issues.push("candidate_instability_detected");
  if (input.adapterStable === false) issues.push("adapter_instability_detected");
  if (input.adapterValidated === false) issues.push("adapter_validation_failed");
  if (input.legacyIsolationViolation === true) issues.push("legacy_isolation_violation");
  if (input.kmlImpactDetected === true) issues.push("kml_impact_detected");
  if (input.autoDecisionDetected === true) issues.push("auto_decision_detected");
  if (input.unexpectedDiffDetected === true) issues.push("unexpected_diff_detected");
  if (input.coordinateResultWriteDetected === true) issues.push("coordinate_result_write_detected");
  if (input.scopeLeakageDetected === true) issues.push("scope_leakage_detected");
  if (metrics.failCount > 0) issues.push("observation_failure_count_nonzero");
  if (metrics.legacyIsolationViolations > 0) issues.push("legacy_isolation_violation");
  if (metrics.kmlImpactCount > 0) issues.push("kml_impact_detected");
  if (metrics.autoDecisionLeakCount > 0) issues.push("auto_decision_detected");
  if (metrics.unexpectedDiffCount > 0) issues.push("unexpected_diff_detected");
  return Object.freeze([...new Set(issues.map(issue => cleanString(issue)).filter(Boolean))]);
}

function classifyObservation({ runningSession = {}, metrics = {}, failureReasons = [] }) {
  if (
    metrics.rollbackTriggered
    || runningSession.state === CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.ROLLED_BACK
  ) {
    return CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.ROLLED_BACK;
  }
  if (failureReasons.length > 0) {
    return CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL;
  }
  if (
    metrics.observationCount >= metrics.minimumRuns
    && metrics.passCount >= metrics.minimumRuns
    && metrics.candidateStableCount >= metrics.minimumRuns
    && metrics.adapterStableCount >= metrics.minimumRuns
    && metrics.rollbackReadyCount >= metrics.minimumRuns
  ) {
    return CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PASS;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PARTIAL;
}

function buildProposalSummary(input = {}, runningSession = {}) {
  const proposal = input.proposal || runningSession.snapshots?.proposal || {};
  return Object.freeze({
    classification: nullableString(proposal.classification),
    winnerEvidenceType: nullableString(proposal.winnerEvidenceType || runningSession.winnerEvidenceType),
    wouldChangeLegacy: normalizeBoolean(proposal.wouldChangeLegacy)
  });
}

function buildDryRunSummary(input = {}) {
  const dryRun = input.dryRun || {};
  return Object.freeze({
    classification: nullableString(dryRun.classification),
    wouldChangeCoordinateType: normalizeBoolean(dryRun.wouldChangeCoordinateType),
    wouldChangePrecisionMode: normalizeBoolean(dryRun.wouldChangePrecisionMode),
    wouldChangeKml: normalizeBoolean(dryRun.wouldChangeKml)
  });
}

export function buildControlledMigrationExperimentObservation(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const runningSession = normalizeRunningSession(
    input.runningSession
    || source.controlledMigrationExperimentRunningSession
    || source.runningSession
    || source
  );
  const metrics = normalizeMetrics(input.metrics || source.metrics || {});
  const failureReasons = buildFailureReasons({ input: source, runningSession, metrics });
  const classification = classifyObservation({ runningSession, metrics, failureReasons });
  const rolledBack = classification === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.ROLLED_BACK;
  const failed = classification === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL;

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_SCHEMA_VERSION,
    experimentId: nullableString(runningSession.experimentId || source.experimentId),
    sessionId: nullableString(runningSession.sessionId || source.sessionId),
    runId: nullableString(source.runId || "observation_run"),
    timestamp: nullableString(source.timestamp),
    commit: nullableString(source.commit || runningSession.commit),
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    proposal: buildProposalSummary(source, runningSession),
    dryRun: buildDryRunSummary(source),
    safety: Object.freeze({
      passed: !failed && !rolledBack,
      rollbackSafe: runningSession.rollbackSnapshot?.restoresLegacyArbitration === true,
      blockReasons: failureReasons
    }),
    review: Object.freeze({
      status: nullableString(runningSession.authorizationSnapshot?.status),
      reviewerRequired: true,
      authorizedBy: nullableString(runningSession.authorizationSnapshot?.authorizedBy),
      authorizedAt: nullableString(runningSession.authorizationSnapshot?.authorizedAt)
    }),
    adapter: Object.freeze({
      validated: source.adapterValidated !== false && runningSession.snapshots?.adapter?.validated !== false,
      classification: nullableString(source.adapter?.classification || runningSession.snapshots?.adapter?.classification)
    }),
    execution: Object.freeze({
      state: nullableString(runningSession.state),
      observationIndex: normalizeCount(source.observationIndex, metrics.observationCount),
      minimumRuns: metrics.minimumRuns
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }),
    metrics: Object.freeze({
      candidateStability: metrics.candidateStableCount >= Math.min(metrics.observationCount, metrics.minimumRuns),
      adapterStability: metrics.adapterStableCount >= Math.min(metrics.observationCount, metrics.minimumRuns),
      legacyIsolationViolations: metrics.legacyIsolationViolations,
      kmlImpactCount: metrics.kmlImpactCount,
      autoDecisionLeakCount: metrics.autoDecisionLeakCount,
      unexpectedDiffCount: metrics.unexpectedDiffCount,
      rollbackReady: metrics.rollbackReadyCount >= Math.min(metrics.observationCount, metrics.minimumRuns),
      failureReasons
    }),
    window: Object.freeze({
      minimumRuns: metrics.minimumRuns,
      observationCount: metrics.observationCount,
      passCount: metrics.passCount,
      failCount: metrics.failCount,
      successThresholdMet: classification === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PASS,
      needsMoreObservation: classification === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PARTIAL
    }),
    rollback: Object.freeze({
      triggered: metrics.rollbackTriggered || rolledBack,
      status: rolledBack ? "ROLLED_BACK" : "READY",
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      legacyRestored: metrics.legacyRestored,
      coordinateResultRestored: metrics.legacyRestored,
      kmlBehaviorRestored: metrics.legacyRestored,
      observationPreserved: metrics.observationPreserved
    }),
    classification,
    safetyBoundary: Object.freeze({
      observationOnly: true,
      actualExperimentExecution: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      globalRolloutEnabled: false,
      failClosed: failed || rolledBack
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
