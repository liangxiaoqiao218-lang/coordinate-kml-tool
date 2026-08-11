import {
  CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE,
  buildControlledMigrationExperimentExecutionProposal
} from "./controlled-migration-execution-proposal.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_SCHEMA_VERSION =
  "controlled_migration_experiment_authorization_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS = Object.freeze({
  PENDING: "PENDING",
  AUTHORIZED: "AUTHORIZED",
  REJECTED: "REJECTED",
  REVOKED: "REVOKED",
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

function normalizeExecutionProposal(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_execution_proposal_v1") {
    return input;
  }
  return buildControlledMigrationExperimentExecutionProposal(
    input.executionProposal
    || input.controlledMigrationExperimentExecutionProposal
    || input.response?.controlledMigrationExperimentExecutionProposal
    || input
  );
}

function normalizeRequest(value = {}) {
  const status = cleanString(value.status || "PENDING").toUpperCase();
  return Object.freeze({
    status: ["PENDING", "AUTHORIZED", "REJECTED", "REVOKED"].includes(status) ? status : "PENDING",
    authorizedBy: nullableString(value.authorizedBy),
    authorizedAt: nullableString(value.authorizedAt),
    reason: nullableString(value.reason),
    rollbackAcknowledged: value.rollbackAcknowledged === true,
    kmlDisabledAcknowledged: value.kmlDisabledAcknowledged === true,
    autoDecisionDisabledAcknowledged: value.autoDecisionDisabledAcknowledged === true,
    scopeAcknowledged: value.scopeAcknowledged === true,
    notProductionMigrationAcknowledged: value.notProductionMigrationAcknowledged === true
  });
}

function validateAuthorizationRequest(request = {}) {
  const issues = [];
  if (request.status !== "AUTHORIZED") issues.push("authorization_not_authorized");
  if (!request.authorizedBy) issues.push("authorized_by_missing");
  if (!request.authorizedAt) issues.push("authorized_at_missing");
  if (!request.reason) issues.push("authorization_reason_missing");
  if (request.rollbackAcknowledged !== true) issues.push("rollback_acknowledgement_missing");
  if (request.kmlDisabledAcknowledged !== true) issues.push("kml_disabled_acknowledgement_missing");
  if (request.autoDecisionDisabledAcknowledged !== true) issues.push("auto_decision_disabled_acknowledgement_missing");
  if (request.scopeAcknowledged !== true) issues.push("scope_acknowledgement_missing");
  if (request.notProductionMigrationAcknowledged !== true) issues.push("not_production_migration_acknowledgement_missing");
  return Object.freeze(issues);
}

function validatePreconditions(proposal = {}) {
  const issues = [];
  if (proposal.state !== CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY) {
    issues.push("execution_proposal_not_start_ready");
  }
  if (proposal.startProcedure?.packageReady !== true) issues.push("package_not_ready_for_experiment");
  if (proposal.activation?.status !== "READY_TO_ACTIVATE") issues.push("activation_not_ready_to_activate");
  if (proposal.startProcedure?.approvalComplete !== true) issues.push("approval_not_complete");
  if (proposal.startProcedure?.rollbackReady !== true) issues.push("rollback_not_ready");
  if (proposal.startProcedure?.flagsComplete !== true) issues.push("flags_not_complete");
  if (proposal.startProcedure?.scopeLocked !== true) issues.push("scope_not_locked");
  if (proposal.startProcedure?.executionReady !== true) issues.push("execution_not_ready");
  if (proposal.scope?.verifiedTransformationOnly !== true) issues.push("verified_transformation_scope_required");
  return Object.freeze(issues);
}

function validateFlags(proposal = {}) {
  const issues = [];
  const activationFlags = proposal.activation?.flags || proposal.flags || {};
  const explicitFlags = proposal.flags || activationFlags || {};
  const dryRun = explicitFlags.dryRun ?? true;
  const reviewOnly = explicitFlags.reviewOnly ?? true;
  const migration = explicitFlags.migration ?? true;
  const kmlGate = explicitFlags.kmlGate ?? false;
  const autoDecision = explicitFlags.autoDecision ?? false;
  if (dryRun !== true) issues.push("dry_run_flag_required");
  if (reviewOnly !== true) issues.push("review_only_flag_required");
  if (migration !== true) issues.push("migration_flag_required_for_experiment");
  if (kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  return Object.freeze(issues);
}

function validateScope(proposal = {}) {
  const issues = [];
  if (proposal.scope?.verifiedTransformationOnly !== true) issues.push("scope_must_be_verified_transformation_only");
  if (proposal.scope?.kmlMigrationAllowed === true) issues.push("kml_migration_not_allowed");
  if (proposal.scope?.autoDecisionAllowed === true) issues.push("auto_decision_not_allowed");
  if (proposal.scope?.productionRolloutAllowed === true) issues.push("production_rollout_not_allowed");
  return Object.freeze(issues);
}

function classifyAuthorization({ request, issues }) {
  if (request.status === "REVOKED") return CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED;
  if (request.status === "REJECTED") return CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REJECTED;
  if (request.status === "PENDING") return CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.PENDING;
  if (issues.length > 0) return CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.BLOCKED;
  return CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED;
}

export function buildControlledMigrationExperimentAuthorization(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const executionProposal = normalizeExecutionProposal(
    input.executionProposal
    || source.controlledMigrationExperimentExecutionProposal
    || source.executionProposal
    || source
  );
  const request = normalizeRequest(input.authorization || source.authorization || {});
  const authorizationIssues = validateAuthorizationRequest(request);
  const preconditionIssues = validatePreconditions(executionProposal);
  const flagIssues = validateFlags(executionProposal);
  const scopeIssues = validateScope(executionProposal);
  const allIssues = Object.freeze([...new Set([
    ...authorizationIssues,
    ...preconditionIssues,
    ...flagIssues,
    ...scopeIssues,
    ...(executionProposal.observation?.failureReasons || [])
  ].map(issue => cleanString(issue)).filter(Boolean))]);
  const status = classifyAuthorization({ request, issues: allIssues });

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_SCHEMA_VERSION,
    experimentId: nullableString(executionProposal.experimentId),
    status,
    authorization: Object.freeze({
      authorizedBy: request.status === "AUTHORIZED" ? request.authorizedBy : null,
      authorizedAt: request.status === "AUTHORIZED" ? request.authorizedAt : null,
      reason: nullableString(request.reason),
      requestedStatus: request.status,
      issues: authorizationIssues
    }),
    scope: Object.freeze({
      category: "verified_transformation",
      winnerEvidenceType: "verified_utm_transformation",
      verifiedTransformationOnly: executionProposal.scope?.verifiedTransformationOnly === true,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      productionRolloutAllowed: false,
      issues: scopeIssues
    }),
    flags: Object.freeze({
      dryRun: true,
      reviewOnly: true,
      migration: true,
      kmlGate: false,
      autoDecision: false,
      issues: flagIssues
    }),
    acknowledgements: Object.freeze({
      rollbackAcknowledged: request.rollbackAcknowledged,
      kmlDisabledAcknowledged: request.kmlDisabledAcknowledged,
      autoDecisionDisabledAcknowledged: request.autoDecisionDisabledAcknowledged,
      scopeAcknowledged: request.scopeAcknowledged,
      notProductionMigrationAcknowledged: request.notProductionMigrationAcknowledged
    }),
    preconditions: Object.freeze({
      executionProposalStartReady: executionProposal.state === CONTROLLED_MIGRATION_EXPERIMENT_EXECUTION_PROPOSAL_STATE.START_READY,
      packageReady: executionProposal.startProcedure?.packageReady === true,
      activationReady: executionProposal.activation?.status === "READY_TO_ACTIVATE",
      rollbackReady: executionProposal.startProcedure?.rollbackReady === true,
      flagsLocked: flagIssues.length === 0,
      scopeLocked: scopeIssues.length === 0,
      issues: preconditionIssues
    }),
    startPermission: Object.freeze({
      canStartExperiment: status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED,
      canStartProductionMigration: false,
      canStartKmlMigration: false,
      canStartAutoDecision: false
    }),
    revocation: Object.freeze({
      revoked: status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED,
      revokeRequiresRollback: status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.REVOKED,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      restoresLegacy: true,
      preservesObservation: true
    }),
    readiness: Object.freeze({
      issues: allIssues,
      ready: status === CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED
    }),
    safety: Object.freeze({
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
      failClosed: status !== CONTROLLED_MIGRATION_EXPERIMENT_AUTHORIZATION_STATUS.AUTHORIZED
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
