import {
  CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS,
  buildControlledMigrationExperimentPackage
} from "./controlled-migration-package.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_SCHEMA_VERSION =
  "controlled_migration_experiment_activation_preflight_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS = Object.freeze({
  READY_TO_ACTIVATE: "READY_TO_ACTIVATE",
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  BLOCKED: "BLOCKED",
  REJECTED: "REJECTED"
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

function normalizePackage(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_package_v1") {
    return input;
  }
  return buildControlledMigrationExperimentPackage(
    input.package
    || input.controlledMigrationExperimentPackage
    || input.response?.controlledMigrationExperimentPackage
    || input
  );
}

function validateApproval(pkg = {}) {
  const approval = pkg.approval || {};
  const issues = [];
  if (approval.status !== "APPROVED") issues.push("approval_not_approved");
  if (!approval.approvedBy) issues.push("approved_by_missing");
  if (!approval.approvedAt) issues.push("approved_at_missing");
  if (!approval.reason) issues.push("approval_reason_missing");
  if (approval.scopeAcknowledged !== true) issues.push("scope_acknowledgement_missing");
  if (approval.rollbackAcknowledged !== true) issues.push("rollback_acknowledgement_missing");
  if (approval.kmlDisabledAcknowledged !== true) issues.push("kml_disabled_acknowledgement_missing");
  if (approval.autoDecisionDisabledAcknowledged !== true) issues.push("auto_decision_disabled_acknowledgement_missing");
  return Object.freeze(issues);
}

function validateFlags(pkg = {}) {
  const flags = pkg.flags || {};
  const issues = [];
  if (flags.dryRun !== true) issues.push("dry_run_flag_required");
  if (flags.reviewOnly !== true) issues.push("review_only_flag_required");
  if (flags.migration !== true) issues.push("migration_flag_required");
  if (flags.kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (flags.autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  return Object.freeze(issues);
}

function validateRollback(pkg = {}) {
  const rollback = pkg.rollback || {};
  const issues = [];
  if (rollback.singleFlagRollback !== true) issues.push("single_flag_rollback_required");
  if (rollback.trigger !== "ENABLE_EVIDENCE_ARBITRATION_MIGRATION=false") {
    issues.push("rollback_trigger_must_disable_migration_flag");
  }
  if (rollback.restoresLegacyArbitration !== true) issues.push("legacy_arbitration_restore_required");
  if (rollback.restoresCoordinateResult !== true) issues.push("coordinate_result_restore_required");
  if (rollback.restoresKmlBehavior !== true) issues.push("kml_behavior_restore_required");
  if (rollback.preservesObservation !== true) issues.push("observation_preservation_required");
  if (rollback.requiresGitRevert === true) issues.push("git_revert_not_allowed_for_rollback");
  if (rollback.rehearsalStatus !== "PASS") issues.push("rollback_rehearsal_required");
  return Object.freeze(issues);
}

function validateScope(pkg = {}) {
  const scope = pkg.scope || {};
  const issues = [];
  const included = Array.isArray(scope.includedEvidenceTypes) ? scope.includedEvidenceTypes : [];
  if (pkg.category !== "verified_transformation") issues.push("category_must_be_verified_transformation");
  if (!included.includes("verified_utm_transformation")) issues.push("verified_utm_transformation_scope_required");
  if (scope.kmlMigrationAllowed === true) issues.push("kml_migration_not_allowed");
  if (scope.autoDecisionAllowed === true) issues.push("auto_decision_not_allowed");
  if (scope.frontendChangeAllowed === true) issues.push("frontend_change_not_allowed");
  if (scope.productionRolloutAllowed === true) issues.push("production_rollout_not_allowed");
  return Object.freeze(issues);
}

function classifyActivation(pkg = {}, issues = []) {
  if (pkg.status === CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.REJECTED) {
    return CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.REJECTED;
  }
  if (pkg.status === CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_REVIEW) {
    return CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_FOR_REVIEW;
  }
  if (
    pkg.status !== CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_EXPERIMENT
    || issues.length > 0
  ) {
    return CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.BLOCKED;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE;
}

export function buildControlledMigrationExperimentActivationPreflight(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const pkg = normalizePackage(
    input.package
    || source.controlledMigrationExperimentPackage
    || source.package
    || source
  );
  const approvalIssues = validateApproval(pkg);
  const flagIssues = validateFlags(pkg);
  const rollbackIssues = validateRollback(pkg);
  const scopeIssues = validateScope(pkg);
  const packageIssues = Array.isArray(pkg.preconditions?.issues)
    ? pkg.preconditions.issues.map(issue => cleanString(issue)).filter(Boolean)
    : [];
  const allIssues = Object.freeze([...new Set([
    ...approvalIssues,
    ...flagIssues,
    ...rollbackIssues,
    ...scopeIssues,
    ...packageIssues
  ])]);
  const status = classifyActivation(pkg, allIssues);

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_SCHEMA_VERSION,
    experimentId: nullableString(pkg.experimentId),
    status,
    activation: Object.freeze({
      ready: status === CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE,
      canStartExperiment: status === CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE,
      experimentExecutionStarted: false,
      productionMigrationEnabled: false
    }),
    approval: Object.freeze({
      complete: approvalIssues.length === 0,
      issues: approvalIssues,
      approvedBy: nullableString(pkg.approval?.approvedBy),
      approvedAt: nullableString(pkg.approval?.approvedAt)
    }),
    flags: Object.freeze({
      complete: flagIssues.length === 0,
      issues: flagIssues,
      dryRun: normalizeBoolean(pkg.flags?.dryRun),
      reviewOnly: normalizeBoolean(pkg.flags?.reviewOnly),
      migration: normalizeBoolean(pkg.flags?.migration),
      kmlGate: normalizeBoolean(pkg.flags?.kmlGate),
      autoDecision: normalizeBoolean(pkg.flags?.autoDecision)
    }),
    rollback: Object.freeze({
      ready: rollbackIssues.length === 0,
      issues: rollbackIssues,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      rehearsalStatus: nullableString(pkg.rollback?.rehearsalStatus)
    }),
    scope: Object.freeze({
      locked: scopeIssues.length === 0,
      issues: scopeIssues,
      verifiedTransformationOnly: pkg.category === "verified_transformation",
      kmlDisabled: pkg.scope?.kmlMigrationAllowed !== true,
      autoDecisionDisabled: pkg.scope?.autoDecisionAllowed !== true
    }),
    executionReadiness: Object.freeze({
      packageStatus: nullableString(pkg.status),
      packagePreconditionsSatisfied: pkg.preconditions?.satisfied === true,
      issues: allIssues
    }),
    safety: Object.freeze({
      preflightOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: status !== CONTROLLED_MIGRATION_EXPERIMENT_ACTIVATION_STATUS.READY_TO_ACTIVATE
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
