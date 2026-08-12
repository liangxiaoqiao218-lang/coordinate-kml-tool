import {
  CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION,
  buildControlledMigrationExperimentResultPackage
} from "./controlled-migration-result-package.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_SCHEMA_VERSION =
  "controlled_migration_experiment_closure_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE = Object.freeze({
  SUCCESS_CLOSED: "SUCCESS_CLOSED",
  PARTIAL_CLOSED: "PARTIAL_CLOSED",
  FAILED_CLOSED: "FAILED_CLOSED",
  ROLLED_BACK_CLOSED: "ROLLED_BACK_CLOSED"
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

function normalizeResultPackage(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_result_package_v1") {
    return input;
  }
  return buildControlledMigrationExperimentResultPackage(
    input.resultPackage
    || input.controlledMigrationExperimentResultPackage
    || input.response?.controlledMigrationExperimentResultPackage
    || input
  );
}

function classifyClosure(finalClassification) {
  switch (finalClassification) {
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.SUCCESS:
      return CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.SUCCESS_CLOSED;
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.PARTIAL:
      return CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.PARTIAL_CLOSED;
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.ROLLED_BACK:
      return CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.ROLLED_BACK_CLOSED;
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.FAIL:
    default:
      return CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.FAILED_CLOSED;
  }
}

function reasonForClosure(closureState) {
  switch (closureState) {
    case CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.SUCCESS_CLOSED:
      return "experiment_result_success_archived_no_migration_approval";
    case CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.PARTIAL_CLOSED:
      return "experiment_partial_closed_extend_observation_required";
    case CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.ROLLED_BACK_CLOSED:
      return "experiment_rollback_closed_observation_preserved";
    case CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.FAILED_CLOSED:
    default:
      return "experiment_failed_closed_root_cause_review_required";
  }
}

function buildRemainingBlockers(resultPackage = {}, closureState) {
  const blockers = [
    "production_migration_not_approved",
    "kml_gate_not_approved",
    "auto_decision_not_approved",
    "scope_expansion_not_approved",
    "full_rollout_not_approved"
  ];
  if (closureState === CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.SUCCESS_CLOSED) {
    blockers.push("requires_next_phase_review", "requires_limited_scope_decision");
  }
  if (closureState === CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.PARTIAL_CLOSED) {
    blockers.push("requires_extended_observation");
  }
  if (closureState === CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.FAILED_CLOSED) {
    blockers.push("requires_root_cause_review");
  }
  if (closureState === CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_STATE.ROLLED_BACK_CLOSED) {
    blockers.push("requires_rollback_audit");
  }
  if (resultPackage.scope?.scopeLeakageDetected === true) blockers.push("scope_leakage_detected");
  return Object.freeze([...new Set(blockers.map(blocker => cleanString(blocker)).filter(Boolean))]);
}

function buildArchiveMetadata(resultPackage = {}, input = {}) {
  return Object.freeze({
    resultPackageRef: nullableString(resultPackage.resultPackageId || input.resultPackageRef),
    reviewRef: nullableString(input.reviewRef || "controlled_migration_experiment_review_v1"),
    observationSummaryRef: nullableString(input.observationSummaryRef || "controlled_migration_experiment_observation_v1"),
    rollbackRef: nullableString(input.rollbackRef || "ENABLE_EVIDENCE_ARBITRATION_MIGRATION=false"),
    commit: nullableString(input.commit || resultPackage.commit)
  });
}

export function buildControlledMigrationExperimentClosure(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const resultPackage = normalizeResultPackage(
    input.resultPackage
    || source.controlledMigrationExperimentResultPackage
    || source.resultPackage
    || source
  );
  const closureState = classifyClosure(resultPackage.finalClassification);
  const closureReason = nullableString(source.closureReason || reasonForClosure(closureState));

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_CLOSURE_SCHEMA_VERSION,
    experimentId: nullableString(resultPackage.experimentId || source.experimentId),
    resultPackageId: nullableString(resultPackage.resultPackageId || source.resultPackageId),
    finalClassification: nullableString(resultPackage.finalClassification),
    closureState,
    closedAt: nullableString(source.closedAt),
    closedBy: nullableString(source.closedBy),
    closureReason,
    scope: Object.freeze({
      category: "verified_transformation",
      winnerEvidenceType: "verified_utm_transformation",
      verifiedTransformationOnly: resultPackage.scope?.verifiedTransformationOnly !== false,
      scopeExpansionApproved: false,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      scopeLeakageDetected: resultPackage.scope?.scopeLeakageDetected === true
    }),
    approvals: Object.freeze({
      productionMigrationApproved: false,
      kmlMigrationApproved: false,
      autoDecisionApproved: false,
      scopeExpansionApproved: false
    }),
    archive: buildArchiveMetadata(resultPackage, source),
    remainingBlockers: buildRemainingBlockers(resultPackage, closureState),
    lifecycle: Object.freeze({
      closureComplete: true,
      experimentStarted: false,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      nextPhaseRequired: true
    }),
    rollbackArchive: Object.freeze({
      triggered: resultPackage.rollbackSummary?.triggered === true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      rollbackValue: false,
      legacyRestored: resultPackage.rollbackSummary?.legacyRestored === true,
      coordinateResultRestored: resultPackage.rollbackSummary?.coordinateResultRestored === true,
      kmlBehaviorRestored: resultPackage.rollbackSummary?.kmlBehaviorRestored === true,
      observationPreserved: resultPackage.rollbackSummary?.observationPreserved !== false
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }),
    safetyBoundary: Object.freeze({
      closureOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      globalRolloutEnabled: false,
      productionMigrationApproved: false,
      kmlMigrationApproved: false,
      autoDecisionApproved: false
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
