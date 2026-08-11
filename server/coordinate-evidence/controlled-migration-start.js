import {
  CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS,
  buildControlledMigrationExperimentSession
} from "./controlled-migration-session.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_SCHEMA_VERSION =
  "controlled_migration_experiment_running_session_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE = Object.freeze({
  STARTED: "STARTED",
  OBSERVING: "OBSERVING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  ROLLED_BACK: "ROLLED_BACK",
  BLOCKED: "BLOCKED"
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

function normalizeSession(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_session_v1") {
    return input;
  }
  return buildControlledMigrationExperimentSession(
    input.session
    || input.controlledMigrationExperimentSession
    || input.response?.controlledMigrationExperimentSession
    || input
  );
}

function normalizeFlags(value = {}) {
  return Object.freeze({
    dryRun: normalizeBoolean(value.dryRun, true),
    reviewOnly: normalizeBoolean(value.reviewOnly, true),
    migration: normalizeBoolean(value.migration, true),
    kmlGate: normalizeBoolean(value.kmlGate, false),
    autoDecision: normalizeBoolean(value.autoDecision, false)
  });
}

function buildPreconditionIssues(session = {}, flags = {}) {
  const issues = [];
  if (session.status !== CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED) {
    issues.push("session_not_prepared");
  }
  if (session.authorization?.status !== "AUTHORIZED") issues.push("authorization_not_authorized");
  if (session.startPreparation?.canStart !== true) issues.push("start_preparation_not_ready");
  if (session.environment?.valid !== true) issues.push("environment_not_valid");
  if (session.rollbackSnapshot?.rehearsalStatus !== "PASS") issues.push("rollback_rehearsal_not_passed");
  if (session.rollbackSnapshot?.rollbackValue !== false) issues.push("rollback_value_must_disable_migration");
  if (session.category !== "verified_transformation") issues.push("category_must_be_verified_transformation");
  if (session.winnerEvidenceType !== "verified_utm_transformation") {
    issues.push("winner_must_be_verified_utm_transformation");
  }
  if (flags.dryRun !== true) issues.push("dry_run_flag_required");
  if (flags.reviewOnly !== true) issues.push("review_only_flag_required");
  if (flags.migration !== true) issues.push("migration_flag_required_for_controlled_experiment");
  if (flags.kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (flags.autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  for (const reason of session.startPreparation?.blockReasons || []) issues.push(reason);
  for (const issue of session.environment?.issues || []) issues.push(issue);
  return Object.freeze([...new Set(issues.map(issue => cleanString(issue)).filter(Boolean))]);
}

function buildAbortIssues(input = {}, session = {}) {
  const issues = [];
  if (input.rollbackTriggered === true) issues.push("rollback_triggered");
  if (input.abortRequested === true) issues.push("abort_requested");
  if (session.status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.ABORTED) {
    issues.push("session_aborted");
  }
  if (session.abort?.rollbackRecommended === true) issues.push("rollback_recommended_by_session");
  if (input.legacyIsolationViolation === true) issues.push("legacy_isolation_violation");
  if (input.kmlImpactDetected === true) issues.push("kml_impact_detected");
  if (input.autoDecisionDetected === true) issues.push("auto_decision_detected");
  if (input.coordinateResultWriteDetected === true) issues.push("coordinate_result_write_detected");
  return Object.freeze([...new Set(issues.map(issue => cleanString(issue)).filter(Boolean))]);
}

function classifyRunningSession({ preconditionIssues, abortIssues, requestedState }) {
  if (abortIssues.some(issue => [
    "rollback_triggered",
    "session_aborted",
    "rollback_recommended_by_session"
  ].includes(issue))) {
    return CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.ROLLED_BACK;
  }
  if (abortIssues.length > 0) {
    return CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.FAILED;
  }
  if (preconditionIssues.length > 0) {
    return CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.BLOCKED;
  }
  if (requestedState === CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.STARTED) {
    return CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.STARTED;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.OBSERVING;
}

function buildSnapshot(session = {}) {
  const snapshots = session.snapshots || {};
  return Object.freeze({
    legacy: Object.freeze({
      coordinateType: nullableString(snapshots.legacy?.coordinateType),
      precisionMode: nullableString(snapshots.legacy?.precisionMode),
      coordinateResultState: nullableString(snapshots.legacy?.coordinateResultState),
      kmlReady: normalizeBoolean(snapshots.legacy?.kmlReady)
    }),
    proposal: Object.freeze({
      classification: nullableString(snapshots.proposal?.classification),
      winnerEvidenceType: nullableString(snapshots.proposal?.winnerEvidenceType),
      category: nullableString(snapshots.proposal?.category)
    }),
    safety: Object.freeze({
      classification: nullableString(snapshots.safety?.classification),
      rollbackSafe: snapshots.safety?.rollbackSafe !== false
    }),
    review: Object.freeze({
      status: nullableString(snapshots.review?.status || session.authorization?.status),
      authorizedBy: nullableString(snapshots.review?.authorizedBy || session.authorization?.authorizedBy),
      authorizedAt: nullableString(snapshots.review?.authorizedAt || session.authorization?.authorizedAt)
    }),
    adapter: Object.freeze({
      classification: nullableString(snapshots.adapter?.classification),
      validated: normalizeBoolean(snapshots.adapter?.validated, true)
    })
  });
}

export function buildControlledMigrationExperimentStart(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const session = normalizeSession(
    input.session
    || source.controlledMigrationExperimentSession
    || source.session
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || session.flagsSnapshot || {});
  const preconditionIssues = buildPreconditionIssues(session, flags);
  const abortIssues = buildAbortIssues(input, session);
  const requestedState = cleanString(input.requestedState || source.requestedState || "");
  const state = classifyRunningSession({ preconditionIssues, abortIssues, requestedState });
  const experimentId = nullableString(session.experimentId || input.experimentId || "verified_transformation_controlled_migration_v1");
  const preparedSessionId = nullableString(session.sessionId || input.sessionId || `${experimentId}:session_prepared`);
  const runningSessionId = nullableString(input.runningSessionId || source.runningSessionId || `${preparedSessionId}:running`);
  const startAllowed = [
    CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.STARTED,
    CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.OBSERVING
  ].includes(state);
  const observationEntered = state === CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.OBSERVING;

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_SCHEMA_VERSION,
    experimentId,
    sessionId: runningSessionId,
    preparedSessionId,
    state,
    startTime: nullableString(input.startTime || source.startTime),
    commit: nullableString(input.commit || source.commit || session.commit),
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    flagsSnapshot: flags,
    authorizationSnapshot: Object.freeze({
      status: nullableString(session.authorization?.status),
      authorizedBy: nullableString(session.authorization?.authorizedBy),
      authorizedAt: nullableString(session.authorization?.authorizedAt),
      scope: "verified_transformation"
    }),
    rollbackSnapshot: Object.freeze({
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      rehearsalStatus: nullableString(session.rollbackSnapshot?.rehearsalStatus || "PASS"),
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    preconditions: Object.freeze({
      valid: preconditionIssues.length === 0,
      issues: preconditionIssues,
      packageReady: session.environment?.issues?.includes("package_not_ready_for_experiment") !== true,
      activationReady: session.environment?.issues?.includes("activation_not_ready") !== true,
      authorizationReady: session.authorization?.status === "AUTHORIZED",
      executionProposalStartReady: session.environment?.issues?.includes("execution_proposal_not_start_ready") !== true,
      sessionPrepared: session.status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED,
      rollbackReady: session.rollbackSnapshot?.rehearsalStatus === "PASS"
    }),
    start: Object.freeze({
      recordCreated: startAllowed,
      startAllowed,
      startExecutionPerformed: false,
      productionMigrationPerformed: false,
      enteredState: state,
      blockReasons: preconditionIssues,
      abortReasons: abortIssues
    }),
    observationEntry: Object.freeze({
      entered: observationEntered,
      state: observationEntered ? "OBSERVING" : state,
      initialSnapshotCaptured: observationEntered,
      baselineCaptured: observationEntered,
      firstObservationRequired: observationEntered,
      firstObservationRecorded: false,
      firstObservationStatus: observationEntered ? "PENDING" : "NOT_STARTED"
    }),
    snapshots: buildSnapshot(session),
    abort: Object.freeze({
      aborted: [
        CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.FAILED,
        CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.ROLLED_BACK
      ].includes(state),
      rollbackRecommended: [
        CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.FAILED,
        CONTROLLED_MIGRATION_EXPERIMENT_RUNNING_SESSION_STATE.ROLLED_BACK
      ].includes(state),
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      reasons: abortIssues
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }),
    safety: Object.freeze({
      startRecordOnly: true,
      actualExperimentStarted: false,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      globalRolloutEnabled: false,
      failClosed: !startAllowed
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
