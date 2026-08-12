import {
  CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION,
  buildControlledMigrationExperimentObservation
} from "./controlled-migration-observation.js";

export const CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_SCHEMA_VERSION =
  "controlled_migration_experiment_review_v1";

export const CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION = Object.freeze({
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

function normalizeObservation(value = {}) {
  if (value?.schemaVersion === "controlled_migration_experiment_observation_v1") {
    return value;
  }
  return buildControlledMigrationExperimentObservation(value);
}

function normalizeObservations(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const observations = input.observations || source.observations || source.observationRecords;
  if (Array.isArray(observations)) {
    return Object.freeze(observations.map(normalizeObservation));
  }
  const single = input.observation || source.observation || source.controlledMigrationExperimentObservation;
  return Object.freeze(single ? [normalizeObservation(single)] : []);
}

function collectUnique(values = []) {
  return Object.freeze([...new Set(values.map(value => cleanString(value)).filter(Boolean))]);
}

function aggregateObservations(observations = []) {
  const classifications = observations.map(observation => observation.classification);
  const failureReasons = collectUnique(observations.flatMap(observation => observation.metrics?.failureReasons || []));
  const scopeCategories = collectUnique(observations.map(observation => observation.category));
  const winnerEvidenceTypes = collectUnique(observations.map(observation => observation.winnerEvidenceType));
  const rollbackTriggered = observations.some(observation => observation.rollback?.triggered === true);
  const rolledBack = classifications.includes(CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.ROLLED_BACK);
  const failCount = classifications.filter(value => value === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.FAIL).length;
  const passCount = classifications.filter(value => value === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PASS).length;
  const partialCount = classifications.filter(value => value === CONTROLLED_MIGRATION_EXPERIMENT_OBSERVATION_CLASSIFICATION.PARTIAL).length;
  const minimumRuns = observations.reduce((max, observation) => Math.max(max, normalizeCount(observation.window?.minimumRuns, 3)), 3);
  const legacyIsolationViolations = observations.reduce((sum, observation) => sum + normalizeCount(observation.metrics?.legacyIsolationViolations), 0);
  const kmlImpactCount = observations.reduce((sum, observation) => sum + normalizeCount(observation.metrics?.kmlImpactCount), 0);
  const autoDecisionLeakCount = observations.reduce((sum, observation) => sum + normalizeCount(observation.metrics?.autoDecisionLeakCount), 0);
  const unexpectedDiffCount = observations.reduce((sum, observation) => sum + normalizeCount(observation.metrics?.unexpectedDiffCount), 0);
  const effectsLeakDetected = observations.some(observation => (
    observation.effects?.affectsLegacyWinner === true
    || observation.effects?.affectsCoordinateResult === true
    || observation.effects?.affectsKml === true
    || observation.safetyBoundary?.coordinateResultChanged === true
    || observation.safetyBoundary?.kmlChanged === true
    || observation.safetyBoundary?.autoDecisionEnabled === true
    || observation.safetyBoundary?.globalRolloutEnabled === true
  ));

  return Object.freeze({
    observationCount: observations.length,
    minimumRuns,
    passCount,
    partialCount,
    failCount,
    rollbackTriggered: rollbackTriggered || rolledBack,
    rolledBack,
    failureReasons,
    scopeCategories,
    winnerEvidenceTypes,
    legacyIsolationViolations,
    kmlImpactCount,
    autoDecisionLeakCount,
    unexpectedDiffCount,
    effectsLeakDetected
  });
}

function hasScopeLeakage(aggregate = {}) {
  return (
    aggregate.scopeCategories.some(category => category !== "verified_transformation")
    || aggregate.winnerEvidenceTypes.some(type => type !== "verified_utm_transformation")
  );
}

function classifyReview(aggregate = {}) {
  if (aggregate.rollbackTriggered || aggregate.rolledBack) {
    return CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK;
  }
  if (
    aggregate.failCount > 0
    || aggregate.legacyIsolationViolations > 0
    || aggregate.kmlImpactCount > 0
    || aggregate.autoDecisionLeakCount > 0
    || aggregate.unexpectedDiffCount > 0
    || aggregate.effectsLeakDetected
    || hasScopeLeakage(aggregate)
  ) {
    return CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL;
  }
  if (
    aggregate.observationCount >= aggregate.minimumRuns
    && aggregate.passCount >= aggregate.minimumRuns
    && aggregate.partialCount === 0
  ) {
    return CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.SUCCESS;
  }
  return CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.PARTIAL;
}

function recommendationForDecision(decision) {
  switch (decision) {
    case CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.SUCCESS:
      return "archive_success_prepare_next_review_do_not_expand_scope";
    case CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.PARTIAL:
      return "extend_observation_window_same_scope";
    case CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK:
      return "archive_rollback_event_preserve_observations";
    case CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL:
    default:
      return "freeze_experiment_and_start_root_cause_review";
  }
}

function buildRollbackReview(observations = [], decision) {
  const rollbackObservation = observations.find(observation => observation.rollback?.triggered === true);
  const rolledBack = decision === CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK;
  return Object.freeze({
    triggered: rolledBack,
    status: rolledBack ? "ROLLED_BACK" : "READY",
    triggerReason: nullableString(rollbackObservation?.metrics?.failureReasons?.[0] || null),
    rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
    rollbackValue: false,
    legacyRestored: rollbackObservation?.rollback?.legacyRestored === true,
    coordinateResultRestored: rollbackObservation?.rollback?.coordinateResultRestored === true,
    kmlBehaviorRestored: rollbackObservation?.rollback?.kmlBehaviorRestored === true,
    observationPreserved: rollbackObservation?.rollback?.observationPreserved !== false
  });
}

export function buildControlledMigrationExperimentReview(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const observations = normalizeObservations(source);
  const aggregate = aggregateObservations(observations);
  const decision = classifyReview(aggregate);
  const experimentId = nullableString(source.experimentId || observations[0]?.experimentId || "verified_transformation_controlled_migration_v1");
  const reviewId = nullableString(source.reviewId || `${experimentId}:review`);

  return Object.freeze({
    schemaVersion: CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_SCHEMA_VERSION,
    experimentId,
    reviewId,
    decision,
    reviewedAt: nullableString(source.reviewedAt),
    category: "verified_transformation",
    winnerEvidenceType: "verified_utm_transformation",
    inputs: Object.freeze({
      observationCount: aggregate.observationCount,
      classifications: Object.freeze(observations.map(observation => observation.classification)),
      executionStates: Object.freeze(observations.map(observation => nullableString(observation.execution?.state)))
    }),
    metrics: Object.freeze({
      minimumRuns: aggregate.minimumRuns,
      passCount: aggregate.passCount,
      partialCount: aggregate.partialCount,
      failCount: aggregate.failCount,
      legacyIsolationViolations: aggregate.legacyIsolationViolations,
      kmlImpactCount: aggregate.kmlImpactCount,
      autoDecisionLeakCount: aggregate.autoDecisionLeakCount,
      unexpectedDiffCount: aggregate.unexpectedDiffCount,
      failureReasons: aggregate.failureReasons
    }),
    decisionRecord: Object.freeze({
      classification: decision,
      recommendation: recommendationForDecision(decision),
      automaticExpansionAllowed: false,
      productionMigrationAllowed: false,
      kmlMigrationAllowed: false,
      autoDecisionAllowed: false
    }),
    nextStep: Object.freeze({
      action: recommendationForDecision(decision),
      prepareNextReview: decision === CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.SUCCESS,
      extendObservation: decision === CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.PARTIAL,
      freezeExperiment: decision === CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL,
      archiveRollback: decision === CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK,
      expandScope: false
    }),
    rollbackReview: buildRollbackReview(observations, decision),
    scope: Object.freeze({
      verifiedTransformationOnly: !hasScopeLeakage(aggregate),
      observedCategories: aggregate.scopeCategories,
      observedWinnerEvidenceTypes: aggregate.winnerEvidenceTypes,
      excludesMadagascar: true,
      excludesCoteDivoire: true,
      excludesIndonesia: true,
      scopeLeakageDetected: hasScopeLeakage(aggregate)
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    }),
    safetyBoundary: Object.freeze({
      reviewOnly: true,
      experimentExecuted: false,
      productionMigrationExecuted: false,
      coordinateResultChanged: false,
      kmlChanged: false,
      frontendChanged: false,
      autoDecisionEnabled: false,
      globalRolloutEnabled: false,
      failClosed: [
        CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.FAIL,
        CONTROLLED_MIGRATION_EXPERIMENT_REVIEW_DECISION.ROLLED_BACK
      ].includes(decision)
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
