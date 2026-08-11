import {
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE,
  buildEvidenceArbitrationControlledMigrationExecution
} from "./controlled-migration-execution.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_SCHEMA_VERSION =
  "controlled_migration_experiment_package_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS = Object.freeze({
  PROPOSED: "PROPOSED",
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  READY_FOR_EXPERIMENT: "READY_FOR_EXPERIMENT",
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

function normalizeExecution(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_controlled_migration_execution_v1") {
    return input;
  }
  return buildEvidenceArbitrationControlledMigrationExecution(
    input.execution
    || input.evidenceArbitrationControlledMigrationExecution
    || input.response?.evidenceArbitrationControlledMigrationExecution
    || input
  );
}

function normalizeFlags(value = {}) {
  return Object.freeze({
    dryRun: normalizeBoolean(value.dryRun, true),
    reviewOnly: normalizeBoolean(value.reviewOnly, true),
    migration: normalizeBoolean(value.migration, false),
    kmlGate: normalizeBoolean(value.kmlGate, false),
    autoDecision: normalizeBoolean(value.autoDecision, false)
  });
}

function normalizeApproval(value = {}) {
  const status = cleanString(value.status || value.reviewStatus || "PENDING").toUpperCase();
  return Object.freeze({
    required: true,
    status: ["PENDING", "APPROVED", "REJECTED", "REVOKED"].includes(status) ? status : "PENDING",
    approvedBy: nullableString(value.approvedBy),
    approvedAt: nullableString(value.approvedAt),
    reason: nullableString(value.reason),
    scopeAcknowledged: value.scopeAcknowledged === true,
    rollbackAcknowledged: value.rollbackAcknowledged === true,
    kmlDisabledAcknowledged: value.kmlDisabledAcknowledged === true,
    autoDecisionDisabledAcknowledged: value.autoDecisionDisabledAcknowledged === true
  });
}

function validateApproval(approval = {}) {
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

function validateFlags(flags = {}) {
  const issues = [];
  if (flags.dryRun !== true) issues.push("dry_run_flag_required");
  if (flags.reviewOnly !== true) issues.push("review_only_flag_required");
  if (flags.migration !== true) issues.push("migration_flag_required_for_experiment");
  if (flags.kmlGate === true) issues.push("kml_gate_must_remain_disabled");
  if (flags.autoDecision === true) issues.push("auto_decision_must_remain_disabled");
  return Object.freeze(issues);
}

function normalizeRollback(value = {}, execution = {}) {
  return Object.freeze({
    trigger: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION=false",
    singleFlagRollback: true,
    restoresLegacyArbitration: execution.rollback?.restoresLegacyArbitration !== false,
    restoresCoordinateResult: execution.rollback?.restoresCoordinateResult !== false,
    restoresKmlBehavior: execution.rollback?.restoresKmlBehavior !== false,
    preservesObservation: execution.rollback?.preservesObservation !== false,
    requiresGitRevert: false,
    rehearsalRequired: true,
    rehearsalStatus: cleanString(value.rehearsalStatus || "PENDING").toUpperCase()
  });
}

function validateRollback(rollback = {}) {
  const issues = [];
  if (rollback.singleFlagRollback !== true) issues.push("single_flag_rollback_required");
  if (rollback.restoresLegacyArbitration !== true) issues.push("legacy_arbitration_restore_required");
  if (rollback.restoresCoordinateResult !== true) issues.push("coordinate_result_restore_required");
  if (rollback.restoresKmlBehavior !== true) issues.push("kml_behavior_restore_required");
  if (rollback.preservesObservation !== true) issues.push("observation_preservation_required");
  if (rollback.requiresGitRevert === true) issues.push("git_revert_not_allowed_for_rollback");
  if (rollback.rehearsalStatus !== "PASS") issues.push("rollback_rehearsal_required");
  return Object.freeze(issues);
}

function buildObservationPlan(value = {}) {
  return Object.freeze({
    required: true,
    minimumRuns: Number.isFinite(Number(value.minimumRuns)) ? Number(value.minimumRuns) : 3,
    records: Object.freeze([
      "proposal",
      "dryRunDiff",
      "migrationSafety",
      "reviewGate",
      "limitedMigrationCandidate",
      "controlledMigrationAdapter",
      "experimentHarness",
      "executionHarness",
      "effects",
      "rollback"
    ]),
    metrics: Object.freeze({
      candidateStability: true,
      adapterStability: true,
      legacyIsolation: true,
      kmlImpact: true,
      autoDecisionLeak: true,
      rollbackReadiness: true
    })
  });
}

function buildCriteria() {
  return Object.freeze({
    success: Object.freeze([
      "winnerEvidenceType=verified_utm_transformation",
      "reviewStatus=APPROVED",
      "migrationSafetyPassed=true",
      "adapter.validated=true",
      "execution.state=SUCCESS",
      "kmlChanged=false",
      "autoDecisionEnabled=false",
      "affectsLegacyWinner=false",
      "affectsCoordinateResult=false",
      "affectsKml=false"
    ]),
    failure: Object.freeze([
      "wrong_category_included",
      "review_missing_or_revoked",
      "safety_failed",
      "adapter_failed",
      "experiment_not_ready",
      "unexpected_diff",
      "coordinate_result_production_write_detected",
      "kml_impact_detected",
      "auto_decision_detected",
      "legacy_isolation_violation",
      "rollback_unsafe"
    ])
  });
}

function buildReportTemplate() {
  return Object.freeze({
    title: "Controlled Migration Experiment Report",
    sections: Object.freeze([
      "Experiment Identity",
      "Approval",
      "Flags",
      "Rollback",
      "Observation Summary",
      "Chain Status",
      "Classification",
      "Decision"
    ]),
    classifications: Object.freeze(["SUCCESS", "PARTIAL", "FAIL", "ROLLED_BACK"])
  });
}

function buildPreconditionIssues({ execution, flags, approvalIssues, flagIssues, rollbackIssues }) {
  const issues = [
    ...approvalIssues,
    ...flagIssues,
    ...rollbackIssues
  ];
  if (execution.state !== EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXECUTION_STATE.READY) {
    issues.push("execution_state_must_be_ready");
  }
  if (execution.experiment?.category !== "verified_transformation") {
    issues.push("experiment_category_must_be_verified_transformation");
  }
  if (execution.experiment?.winnerEvidenceType !== "verified_utm_transformation") {
    issues.push("winner_must_be_verified_utm_transformation");
  }
  if (execution.safety?.productionMigrationExecuted === true) issues.push("production_migration_already_executed");
  if (execution.safety?.coordinateResultProductionWrite === true) issues.push("coordinate_result_production_write_detected");
  if (execution.safety?.kmlChanged === true) issues.push("kml_change_detected");
  if (execution.safety?.autoDecisionEnabled === true || flags.autoDecision === true) issues.push("auto_decision_not_allowed");
  return Object.freeze([...new Set(issues.map(issue => cleanString(issue)).filter(Boolean))]);
}

function classifyPackage({ approval, preconditionIssues }) {
  if (approval.status === "REJECTED" || approval.status === "REVOKED") {
    return CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.REJECTED;
  }
  if (preconditionIssues.length > 0) {
    return approval.status === "APPROVED"
      ? CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.BLOCKED
      : CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_REVIEW;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_STATUS.READY_FOR_EXPERIMENT;
}

export function buildControlledMigrationExperimentPackage(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const execution = normalizeExecution(
    input.execution
    || source.evidenceArbitrationControlledMigrationExecution
    || source.execution
    || source
  );
  const approval = normalizeApproval(input.approval || source.approval || execution.approval || {});
  const flags = normalizeFlags(input.flags || source.flags || execution.flags || {});
  const rollback = normalizeRollback(input.rollback || source.rollback || {}, execution);
  const approvalIssues = validateApproval(approval);
  const flagIssues = validateFlags(flags);
  const rollbackIssues = validateRollback(rollback);
  const preconditionIssues = buildPreconditionIssues({
    execution,
    flags,
    approvalIssues,
    flagIssues,
    rollbackIssues
  });
  const status = classifyPackage({ approval, preconditionIssues });

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_PACKAGE_SCHEMA_VERSION,
    experimentId: nullableString(input.experimentId || source.experimentId || execution.experiment?.experimentId || "verified_transformation_controlled_migration_v1"),
    status,
    category: nullableString(execution.experiment?.category),
    scope: Object.freeze({
      includedEvidenceTypes: Object.freeze(["verified_utm_transformation"]),
      excludedCategories: Object.freeze([
        "structured_legal_coordinate",
        "explicit_geographic_semantic",
        "indonesia_dms_vs_utm"
      ]),
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false,
      frontendChangeAllowed: false,
      productionRolloutAllowed: false
    }),
    preconditions: Object.freeze({
      satisfied: preconditionIssues.length === 0,
      issues: preconditionIssues
    }),
    approval,
    flags,
    rollback,
    observation: buildObservationPlan(input.observationPlan || source.observationPlan || {}),
    criteria: buildCriteria(),
    security: Object.freeze({
      sanitizedOnly: true,
      rawOcrAllowed: false,
      promptAllowed: false,
      modelResponseAllowed: false,
      credentialsAllowed: false,
      imageDataAllowed: false,
      coordinateRowsAllowed: false,
      fullGeometryArraysAllowed: false
    }),
    reportTemplate: buildReportTemplate(),
    safety: Object.freeze({
      packageOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
