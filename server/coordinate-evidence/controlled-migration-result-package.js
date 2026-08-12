import {
  CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION,
  buildControlledMigrationExperimentReview
} from "./controlled-migration-review.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_RESULT_PACKAGE_SCHEMA_VERSION =
  "controlled_migration_experiment_result_package_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION = Object.freeze({
  SUCCESS: "SUCCESS",
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

function normalizeCount(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 0 ? numeric : fallback;
}

function normalizeReview(input = {}) {
  if (input?.schemaVersion === "controlled_migration_experiment_review_v1") {
    return input;
  }
  return buildControlledMigrationExperimentReview(
    input.review
    || input.controlledMigrationExperimentReview
    || input.response?.controlledMigrationExperimentReview
    || input
  );
}

function normalizeClassification(value) {
  const classification = cleanString(value || CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.PARTIAL).toUpperCase();
  if (Object.values(CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION).includes(classification)) {
    return classification;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.PARTIAL;
}

function buildObservationSummary(review = {}) {
  const classifications = Array.isArray(review.inputs?.classifications)
    ? review.inputs.classifications
    : [];
  return Object.freeze({
    runCount: normalizeCount(review.inputs?.observationCount),
    passCount: normalizeCount(review.metrics?.passCount),
    partialCount: normalizeCount(review.metrics?.partialCount),
    failCount: normalizeCount(review.metrics?.failCount),
    rollbackCount: classifications.filter(value => value === "ROLLED_BACK").length,
    legacyIsolationViolations: normalizeCount(review.metrics?.legacyIsolationViolations),
    kmlImpactCount: normalizeCount(review.metrics?.kmlImpactCount),
    autoDecisionLeakCount: normalizeCount(review.metrics?.autoDecisionLeakCount),
    unexpectedDiffCount: normalizeCount(review.metrics?.unexpectedDiffCount)
  });
}

function buildReviewSummary(review = {}, finalClassification) {
  return Object.freeze({
    decision: normalizeClassification(review.decision || finalClassification),
    recommendation: nullableString(review.decisionRecord?.recommendation || review.nextStep?.action),
    blockReasons: Object.freeze((review.metrics?.failureReasons || []).map(reason => cleanString(reason)).filter(Boolean)),
    scopeLeakageDetected: review.scope?.scopeLeakageDetected === true,
    productionMigrationAllowed: false,
    kmlMigrationAllowed: false,
    autoDecisionAllowed: false
  });
}

function buildRollbackSummary(review = {}, finalClassification) {
  const rollback = review.rollbackReview || {};
  const rolledBack = finalClassification === CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.ROLLED_BACK;
  return Object.freeze({
    triggered: rollback.triggered === true || rolledBack,
    reason: nullableString(rollback.triggerReason || review.metrics?.failureReasons?.[0]),
    rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
    rollbackValue: false,
    legacyRestored: rollback.legacyRestored === true,
    coordinateResultRestored: rollback.coordinateResultRestored === true,
    kmlBehaviorRestored: rollback.kmlBehaviorRestored === true,
    observationPreserved: rollback.observationPreserved !== false
  });
}

function actionForClassification(finalClassification) {
  switch (finalClassification) {
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.SUCCESS:
      return "archive_result_prepare_next_review_only";
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.PARTIAL:
      return "extend_observation_same_scope";
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.ROLLED_BACK:
      return "archive_rollback_history";
    case CONTROLLED_MIGRATION_EXPERIMENT_RESULT_CLASSIFICATION.FAIL:
    default:
      return "freeze_experiment_root_cause_review";
  }
}

export function buildControlledMigrationExperimentResultPackage(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const review = normalizeReview(
    input.review
    || source.controlledMigrationExperimentReview
    || source.review
    || source
  );
  const finalClassification = normalizeClassification(review.decision);
  const experimentId = nullableString(source.experimentId || review.experimentId || "verified_transformation_controlled_migration_v1");

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_RESULT_PACKAGE_SCHEMA_VERSION,
    experimentId,
    sessionId: nullableString(source.sessionId || review.sessionId),
    resultPackageId: nullableString(source.resultPackageId || `${experimentId}:result_package`),
    commit: nullableString(source.commit),
    generatedAt: nullableString(source.generatedAt),
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    observationSummary: buildObservationSummary(review),
    reviewSummary: buildReviewSummary(review, finalClassification),
    rollbackSummary: buildRollbackSummary(review, finalClassification),
    finalClassification,
    finalAction: actionForClassification(finalClassification),
    approvals: Object.freeze({
      productionMigrationApproved: false,
      kmlMigrationApproved: false,
      autoDecisionApproved: false,
      scopeExpansionApproved: false
    }),
    scope: Object.freeze({
      verifiedTransformationOnly: review.scope?.verifiedTransformationOnly !== false,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      scopeLeakageDetected: review.scope?.scopeLeakageDetected === true
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }),
    safetyBoundary: Object.freeze({
      resultPackageOnly: true,
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
    reportTemplate: Object.freeze({
      title: "Controlled Migration Experiment Result Package",
      sections: Object.freeze([
        "Experiment Identity",
        "Observation Summary",
        "Review Summary",
        "Rollback Summary",
        "Final Classification",
        "Boundary Confirmation"
      ]),
      sanitizedOnly: true
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
