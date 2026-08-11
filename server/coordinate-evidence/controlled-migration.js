import {
  EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION,
  buildEvidenceArbitrationLimitedMigrationCandidate
} from "./limited-migration.js";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_SCHEMA_VERSION =
  "evidence_arbitration_controlled_migration_v1";

export const EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
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

function normalizeLimitedMigrationCandidate(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_limited_migration_candidate_v1") {
    return input;
  }
  return buildEvidenceArbitrationLimitedMigrationCandidate(
    input.limitedMigrationCandidate
    || input.evidenceArbitrationLimitedMigrationCandidate
    || input.response?.evidenceArbitrationLimitedMigrationCandidate
    || input
  );
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

function adapterChecks(candidate = {}, flags = {}) {
  const reasons = [];
  const limitedMigrationEligible = candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.LIMITED_MIGRATION_CANDIDATE
    && candidate.candidate?.limitedMigrationEligible === true;

  if (flags.migration !== true) reasons.push("migration_flag_disabled");
  if (flags.autoDecision === true) reasons.push("auto_decision_not_allowed");
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY) {
    reasons.push("review_only_category");
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.PENDING_REVIEW) {
    reasons.push("review_approval_required");
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED) {
    reasons.push(...(candidate.candidate?.blockReasons || ["limited_migration_blocked"]));
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REJECTED) {
    reasons.push("review_rejected");
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.NO_CANDIDATE) {
    reasons.push("candidate_unavailable");
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.ROLLED_BACK) {
    reasons.push("rollback_active");
  }
  if (!limitedMigrationEligible) reasons.push("limited_migration_candidate_not_eligible");
  if (candidate.rollback?.rollbackSafe !== true) reasons.push("rollback_not_safe");
  if (candidate.eligibility?.migrationSafetyPassed !== true) reasons.push("migration_safety_not_passed");
  if (!candidate.candidate?.proposedCoordinateType) reasons.push("proposed_coordinate_type_missing");
  if (!candidate.candidate?.proposedPrecisionMode) reasons.push("proposed_precision_mode_missing");

  return Object.freeze([...new Set(reasons.map(reason => cleanString(reason)).filter(Boolean))]);
}

function classifyControlledMigration(candidate = {}, flags = {}, blockReasons = []) {
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ROLLED_BACK;
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.REVIEW_ONLY;
  }
  if (candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.PENDING_REVIEW) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.PENDING_REVIEW;
  }
  if (
    candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED
    || candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REJECTED
    || candidate.classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.NO_CANDIDATE
  ) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.BLOCKED;
  }
  if (flags.migration !== true || blockReasons.length > 0) {
    return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.BLOCKED;
  }
  return EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE;
}

function buildProductionUpdatePlan(candidate = {}, flags = {}, classification = "") {
  const eligible = classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE;
  return Object.freeze({
    wouldUpdateCoordinateResult: eligible,
    proposedCoordinateType: nullableString(candidate.candidate?.proposedCoordinateType),
    proposedPrecisionMode: nullableString(candidate.candidate?.proposedPrecisionMode),
    wouldUpdateConfirmationStatus: false,
    wouldUpdateQualityGateStatus: false,
    wouldUpdateKml: Boolean(eligible && flags.kmlGate === true && candidate.candidate?.kmlEligible === true),
    kmlGateEnabled: flags.kmlGate === true,
    autoDecisionEnabled: false,
    productionApplyEnabled: false
  });
}

export function buildEvidenceArbitrationControlledMigration(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const limitedMigrationCandidate = normalizeLimitedMigrationCandidate(
    input.limitedMigrationCandidate
    || source.evidenceArbitrationLimitedMigrationCandidate
    || source.limitedMigrationCandidate
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || limitedMigrationCandidate.flags || {});
  const blockReasons = adapterChecks(limitedMigrationCandidate, flags);
  const classification = classifyControlledMigration(limitedMigrationCandidate, flags, blockReasons);
  const updatePlan = buildProductionUpdatePlan(limitedMigrationCandidate, flags, classification);

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_SCHEMA_VERSION,
    enabled: true,
    mode: "controlled_migration_adapter",
    classification,
    flags,
    candidate: Object.freeze({
      schemaVersion: nullableString(limitedMigrationCandidate.schemaVersion),
      classification: nullableString(limitedMigrationCandidate.classification),
      category: nullableString(limitedMigrationCandidate.candidate?.category),
      winnerEvidenceType: nullableString(limitedMigrationCandidate.candidate?.winnerEvidenceType),
      authority: limitedMigrationCandidate.candidate?.authority ?? null,
      limitedMigrationEligible: limitedMigrationCandidate.candidate?.limitedMigrationEligible === true,
      kmlEligible: limitedMigrationCandidate.candidate?.kmlEligible === true,
      autoDecisionEligible: false
    }),
    review: Object.freeze({
      classification: nullableString(limitedMigrationCandidate.review?.classification),
      reviewStatus: nullableString(limitedMigrationCandidate.review?.reviewStatus),
      approvedBy: nullableString(limitedMigrationCandidate.review?.approvedBy),
      approvedAt: nullableString(limitedMigrationCandidate.review?.approvedAt)
    }),
    legacy: Object.freeze({
      coordinateType: nullableString(limitedMigrationCandidate.legacy?.coordinateType),
      precisionMode: nullableString(limitedMigrationCandidate.legacy?.precisionMode),
      coordinateResultState: nullableString(limitedMigrationCandidate.legacy?.coordinateResultState),
      kmlReady: normalizeBoolean(limitedMigrationCandidate.legacy?.kmlReady)
    }),
    adapter: Object.freeze({
      validated: classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE,
      blockReasons,
      updatePlan
    }),
    rollback: Object.freeze({
      rollbackSafe: limitedMigrationCandidate.rollback?.rollbackSafe === true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    observability: Object.freeze({
      recordMigrationAttempt: true,
      recordCandidate: true,
      recordAdapterDecision: true,
      recordRollbackEvent: classification === EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ROLLED_BACK,
      sanitizedOnly: true
    }),
    safety: Object.freeze({
      migrationApplied: false,
      productionWinnerChanged: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false,
      failClosed: classification !== EVIDENCE_ARBITRATION_CONTROLLED_MIGRATION_CLASSIFICATION.ELIGIBLE
    })
  });
}
