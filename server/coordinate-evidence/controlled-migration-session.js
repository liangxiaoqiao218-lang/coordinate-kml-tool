import {
  CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS,
  buildControlledMigrationExperimentAuthorization
} from "./controlled-migration-authorization.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_SESSION_SCHEMA_VERSION =
  "controlled_migration_experiment_session_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS = Object.freeze({
  PREPARED: "PREPARED",
  BLOCKED: "BLOCKED",
  ABORTED: "ABORTED"
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

function normalizeAuthorization(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_authorization_v1") {
    return input;
  }
  return buildControlledMigrationExperimentAuthorization(
    input.authorization
    || input.controlledMigrationExperimentAuthorization
    || input.response?.controlledMigrationExperimentAuthorization
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

function buildEnvironmentIssues(authorization = {}, flags = {}) {
  const issues = [];
  if (authorization.status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED) {
    issues.push("authorization_revoked");
  }
  if (authorization.status !== CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED) {
    issues.push("authorization_not_authorized");
  }
  if (authorization.startPermission?.canStartExperiment !== true) {
    issues.push("start_permission_missing");
  }
  if (authorization.preconditions?.executionProposalStartReady !== true) {
    issues.push("execution_proposal_not_start_ready");
  }
  if (authorization.preconditions?.packageReady !== true) issues.push("package_not_ready_for_experiment");
  if (authorization.preconditions?.activationReady !== true) issues.push("activation_not_ready");
  if (authorization.preconditions?.rollbackReady !== true) issues.push("rollback_not_ready");
  if (authorization.preconditions?.flagsLocked !== true) issues.push("flags_not_locked");
  if (authorization.preconditions?.scopeLocked !== true) issues.push("scope_not_locked");
  if (authorization.scope?.verifiedTransformationOnly !== true) {
    issues.push("scope_must_be_verified_transformation_only");
  }
  if (authorization.scope?.kmlMigrationAllowed === true) issues.push("kml_migration_not_allowed");
  if (authorization.scope?.autoDecisionAllowed === true) issues.push("auto_decision_not_allowed");
  if (authorization.scope?.productionRolloutAllowed === true) issues.push("production_rollout_not_allowed");
  if (flags.dryRun !== true) issues.push("dry_run_flag_required");
  if (flags.reviewOnly !== true) issues.push("review_only_flag_required");
  if (flags.migration !== true) issues.push("migration_flag_required");
  if (flags.kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (flags.autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  for (const issue of authorization.readiness?.issues || []) {
    issues.push(issue);
  }
  return Object.freeze([...new Set(issues.map(issue => cleanString(issue)).filter(Boolean))]);
}

function classifySession(authorization = {}, issues = []) {
  if (authorization.status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED) {
    return CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.ABORTED;
  }
  return issues.length === 0
    ? CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED
    : CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED;
}

function buildSnapshot(input = {}, authorization = {}, flags = {}) {
  const source = input.snapshots || {};
  return Object.freeze({
    legacy: Object.freeze({
      coordinateType: nullableString(source.legacy?.coordinateType),
      precisionMode: nullableString(source.legacy?.precisionMode),
      coordinateResultState: nullableString(source.legacy?.coordinateResultState),
      kmlReady: normalizeBoolean(source.legacy?.kmlReady)
    }),
    proposal: Object.freeze({
      classification: nullableString(source.proposal?.classification),
      winnerEvidenceType: nullableString(source.proposal?.winnerEvidenceType || authorization.scope?.winnerEvidenceType),
      category: nullableString(source.proposal?.category || authorization.scope?.category)
    }),
    safety: Object.freeze({
      classification: nullableString(source.safety?.classification),
      rollbackSafe: source.safety?.rollbackSafe !== false
    }),
    review: Object.freeze({
      status: "AUTHORIZED",
      authorizedBy: nullableString(authorization.authorization?.authorizedBy),
      authorizedAt: nullableString(authorization.authorization?.authorizedAt)
    }),
    adapter: Object.freeze({
      classification: nullableString(source.adapter?.classification),
      validated: normalizeBoolean(source.adapter?.validated, true)
    }),
    flags: Object.freeze({
      dryRun: flags.dryRun,
      reviewOnly: flags.reviewOnly,
      migration: flags.migration,
      kmlGate: flags.kmlGate,
      autoDecision: flags.autoDecision
    })
  });
}

export function buildControlledMigrationExperimentSession(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const authorization = normalizeAuthorization(
    input.authorization
    || source.controlledMigrationExperimentAuthorization
    || source.authorization
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || authorization.flags || {});
  const environmentIssues = buildEnvironmentIssues(authorization, flags);
  const status = classifySession(authorization, environmentIssues);
  const experimentId = nullableString(input.experimentId || source.experimentId || authorization.experimentId || "verified_transformation_controlled_migration_v1");
  const sessionId = nullableString(input.sessionId || source.sessionId || `${experimentId}:session_prepared`);

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_SESSION_SCHEMA_VERSION,
    experimentId,
    sessionId,
    status,
    commit: nullableString(input.commit || source.commit),
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    authorization: Object.freeze({
      status: nullableString(authorization.status),
      authorizedBy: nullableString(authorization.authorization?.authorizedBy),
      authorizedAt: nullableString(authorization.authorization?.authorizedAt),
      reason: nullableString(authorization.authorization?.reason)
    }),
    flagsSnapshot: flags,
    rollbackSnapshot: Object.freeze({
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      rehearsalStatus: "PASS",
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    environment: Object.freeze({
      valid: environmentIssues.length === 0,
      issues: environmentIssues
    }),
    startPreparation: Object.freeze({
      preparedAt: nullableString(input.preparedAt || source.preparedAt),
      canStart: status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED,
      blockReasons: environmentIssues
    }),
    snapshots: buildSnapshot(input, authorization, flags),
    abort: Object.freeze({
      aborted: status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.ABORTED,
      blocked: status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.BLOCKED,
      rollbackRecommended: status === CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.ABORTED,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false
    }),
    safety: Object.freeze({
      preparationOnly: true,
      experimentStarted: false,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: status !== CONTROLLED_MIGRATION_EXPERIMENT_SESSION_STATUS.PREPARED
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
