import {
  EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION,
  buildEvidenceArbitrationControlledMigration
} from "./controlled-migration.js";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_SCHEMA_VERSION =
  "evidence_arbitration_controlled_migration_experiment_v1";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION = Object.freeze({
  READY_FOR_EXPERIMENT: "READY_FOR_EXPERIMENT",
  BLOCKED: "BLOCKED",
  REVIEW_ONLY: "REVIEW_ONLY",
  PENDING_REVIEW: "PENDING_REVIEW",
  ROLLED_BACK: "ROLLED_BACK",
  FAILED_CLOSED: "FAILED_CLOSED"
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

function normalizeFlags(value = {}) {
  return Object.freeze({
    migration: normalizeBoolean(value.migration, false),
    kmlGate: normalizeBoolean(value.kmlGate, false),
    autoDecision: normalizeBoolean(value.autoDecision, false),
    reviewOnly: normalizeBoolean(value.reviewOnly, true),
    dryRun: normalizeBoolean(value.dryRun, true)
  });
}

function normalizeControlledMigration(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_controlled_migration_v1") {
    return input;
  }
  return buildEvidenceArbitrationControlledMigration(
    input.controlledMigration
    || input.evidenceArbitrationControlledMigration
    || input.response?.evidenceArbitrationControlledMigration
    || input
  );
}

function experimentChecks(controlledMigration = {}, flags = {}) {
  const reasons = [];
  const candidate = controlledMigration.candidate || {};
  const adapter = controlledMigration.adapter || {};
  const updatePlan = adapter.updatePlan || {};

  if (controlledMigration.classification !== EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE) {
    reasons.push("controlled_adapter_not_eligible");
  }
  if (candidate.category !== "verified_transformation") {
    reasons.push("experiment_category_not_allowlisted");
  }
  if (candidate.winnerEvidenceType !== "verified_utm_transformation") {
    reasons.push("winner_evidence_not_verified_transformation");
  }
  if (controlledMigration.review?.reviewStatus !== "APPROVED") {
    reasons.push("review_approval_required");
  }
  if (flags.migration !== true) {
    reasons.push("migration_flag_disabled");
  }
  if (flags.kmlGate === true) {
    reasons.push("kml_gate_must_remain_disabled");
  }
  if (flags.autoDecision === true) {
    reasons.push("auto_decision_must_remain_disabled");
  }
  if (adapter.validated !== true) {
    reasons.push("adapter_validation_failed");
  }
  if (updatePlan.wouldUpdateCoordinateResult !== true) {
    reasons.push("coordinate_result_update_plan_missing");
  }
  if (updatePlan.wouldUpdateKml === true) {
    reasons.push("kml_update_not_allowed");
  }
  if (updatePlan.productionApplyEnabled === true) {
    reasons.push("production_apply_not_allowed");
  }
  if (controlledMigration.rollback?.rollbackSafe !== true) {
    reasons.push("rollback_not_safe");
  }
  if (controlledMigration.safety?.migrationApplied === true) {
    reasons.push("migration_already_applied");
  }
  if (controlledMigration.safety?.affectsCoordinateResult === true) {
    reasons.push("coordinate_result_mutation_detected");
  }
  if (controlledMigration.safety?.affectsKml === true) {
    reasons.push("kml_mutation_detected");
  }

  return Object.freeze([...new Set(reasons.map(reason => cleanString(reason)).filter(Boolean))]);
}

function classifyExperiment(controlledMigration = {}, blockReasons = []) {
  if (controlledMigration.classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.ROLLED_BACK;
  }
  if (controlledMigration.classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.REVIEW_ONLY) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.REVIEW_ONLY;
  }
  if (controlledMigration.classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.PENDING_REVIEW) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.PENDING_REVIEW;
  }
  if (controlledMigration.classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.FAILED_CLOSED) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.FAILED_CLOSED;
  }
  if (blockReasons.length > 0) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.BLOCKED;
  }
  return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.READY_FOR_EXPERIMENT;
}

export function buildEvidenceArbitrationControlledMigrationExperiment(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const controlledMigration = normalizeControlledMigration(
    input.controlledMigration
    || source.evidenceArbitrationControlledMigration
    || source.controlledMigration
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || controlledMigration.flags || {});
  const blockReasons = experimentChecks(controlledMigration, flags);
  const classification = classifyExperiment(controlledMigration, blockReasons);
  const ready = classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_CLASSIFICATION.READY_FOR_EXPERIMENT;

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_EXPERIMENT_SCHEMA_VERSION,
    enabled: true,
    mode: "controlled_migration_experiment",
    classification,
    experiment: Object.freeze({
      experimentId: nullableString(input.experimentId || source.experimentId || "verified_transformation_controlled_migration_v1"),
      category: nullableString(controlledMigration.candidate?.category),
      winnerEvidenceType: nullableString(controlledMigration.candidate?.winnerEvidenceType),
      ready,
      scope: Object.freeze({
        verifiedTransformationOnly: true,
        excludesMadagascar: true,
        excludesCoteDivoire: true,
        excludesIndonesia: true,
        kmlDisabled: flags.kmlGate !== true,
        autoDecisionDisabled: flags.autoDecision !== true
      }),
      blockReasons
    }),
    flags,
    approval: Object.freeze({
      reviewStatus: nullableString(controlledMigration.review?.reviewStatus),
      approvedBy: nullableString(controlledMigration.review?.approvedBy),
      approvedAt: nullableString(controlledMigration.review?.approvedAt)
    }),
    adapter: Object.freeze({
      classification: nullableString(controlledMigration.classification),
      validated: controlledMigration.adapter?.validated === true,
      wouldUpdateCoordinateResult: controlledMigration.adapter?.updatePlan?.wouldUpdateCoordinateResult === true,
      wouldUpdateKml: controlledMigration.adapter?.updatePlan?.wouldUpdateKml === true,
      productionApplyEnabled: controlledMigration.adapter?.updatePlan?.productionApplyEnabled === true
    }),
    rollback: Object.freeze({
      rollbackSafe: controlledMigration.rollback?.rollbackSafe === true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    observability: Object.freeze({
      recordExperiment: true,
      recordAdapterDecision: true,
      recordRollbackTest: true,
      sanitizedOnly: true
    }),
    safety: Object.freeze({
      experimentApplied: false,
      productionWriteEnabled: false,
      coordinateResultProductionWrite: false,
      kmlWriteEnabled: false,
      autoDecisionEnabled: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: !ready
    })
  });
}
