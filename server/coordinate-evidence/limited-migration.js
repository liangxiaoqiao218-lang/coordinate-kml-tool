import {
  EVIDENCE_ARBITRATION_MIGRATION_CATEGORY
} from "./migration-safety.js";
import {
  EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION,
  EVIDENCE_ARBITRATION_REVIEW_STATUS,
  buildEvidenceArbitrationReviewGate
} from "./review-gate.js";

export const EVIDENCE_ARBITRATION_LIMITED_MIGRATION_SCHEMA_VERSION =
  "evidence_arbitration_limited_migration_candidate_v1";

export const EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION = Object.freeze({
  LIMITED_MIGRATION_CANDIDATE: "LIMITED_MIGRATION_CANDIDATE",
  REVIEW_ONLY: "REVIEW_ONLY",
  PENDING_REVIEW: "PENDING_REVIEW",
  BLOCKED: "BLOCKED",
  REJECTED: "REJECTED",
  ROLLED_BACK: "ROLLED_BACK",
  NO_CANDIDATE: "NO_CANDIDATE"
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

function normalizeReviewGate(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_review_gate_v1") {
    return input;
  }
  return buildEvidenceArbitrationReviewGate(
    input.reviewGate
    || input.evidenceArbitrationReviewGate
    || input.response?.evidenceArbitrationReviewGate
    || input
  );
}

function inferCategory(reviewGate = {}) {
  const explicit = cleanString(reviewGate.reviewSubject?.category);
  if (explicit) return explicit;
  switch (reviewGate.reviewSubject?.winnerEvidenceType) {
    case "structured_cadastral_table":
      return EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE;
    case "explicit_geographic_dms":
      return EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC;
    case "verified_utm_transformation":
      return EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.VERIFIED_TRANSFORMATION;
    default:
      return EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.UNKNOWN;
  }
}

function buildCategoryPolicy(category = "", winnerEvidenceType = "") {
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.VERIFIED_TRANSFORMATION
    || winnerEvidenceType === "verified_utm_transformation"
  ) {
    return Object.freeze({
      limitedMigrationAllowed: true,
      reviewOnly: false,
      kmlEligibleAfterGate: true,
      autoDecisionAllowed: false,
      reason: "verified_transformation_first_limited_candidate"
    });
  }
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE
    || winnerEvidenceType === "structured_cadastral_table"
  ) {
    return Object.freeze({
      limitedMigrationAllowed: false,
      reviewOnly: true,
      kmlEligibleAfterGate: false,
      autoDecisionAllowed: false,
      reason: "structured_legal_coordinate_review_only"
    });
  }
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC
    || winnerEvidenceType === "explicit_geographic_dms"
  ) {
    return Object.freeze({
      limitedMigrationAllowed: false,
      reviewOnly: true,
      kmlEligibleAfterGate: false,
      autoDecisionAllowed: false,
      reason: "explicit_geographic_semantic_more_observation_required"
    });
  }
  if (category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.PENDING_FIXTURE) {
    return Object.freeze({
      limitedMigrationAllowed: false,
      reviewOnly: false,
      kmlEligibleAfterGate: false,
      autoDecisionAllowed: false,
      reason: "pending_fixture_policy"
    });
  }
  return Object.freeze({
    limitedMigrationAllowed: false,
    reviewOnly: false,
    kmlEligibleAfterGate: false,
    autoDecisionAllowed: false,
    reason: "unknown_category"
  });
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

function reviewApproved(reviewGate = {}) {
  return reviewGate.classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.APPROVED_FOR_LIMITED_MIGRATION
    && reviewGate.reviewStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.APPROVED;
}

function classifyLimitedMigration({
  reviewGate,
  categoryPolicy,
  approved,
  pendingPolicy
}) {
  if (reviewGate.classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.ROLLED_BACK;
  }
  if (reviewGate.classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.REJECTED) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REJECTED;
  }
  if (pendingPolicy || reviewGate.classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED;
  }
  if (!reviewGate.reviewSubject?.winnerEvidenceType) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.NO_CANDIDATE;
  }
  if (!approved) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.PENDING_REVIEW;
  }
  if (categoryPolicy.limitedMigrationAllowed === true) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.LIMITED_MIGRATION_CANDIDATE;
  }
  if (categoryPolicy.reviewOnly === true) {
    return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY;
  }
  return EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED;
}

function buildBlockReasons({
  classification,
  reviewGate,
  categoryPolicy,
  flags,
  approved,
  pendingPolicy
}) {
  const reasons = [];
  if (pendingPolicy) reasons.push("pending_fixture_policy");
  if (!reviewGate.reviewSubject?.winnerEvidenceType) reasons.push("proposal_candidate_unavailable");
  if (!approved) reasons.push("review_approval_required");
  if (classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.REVIEW_ONLY) {
    reasons.push(categoryPolicy.reason);
  }
  if (classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.BLOCKED) {
    reasons.push(categoryPolicy.reason);
  }
  if (flags.migration !== true) reasons.push("migration_flag_disabled");
  if (flags.kmlGate !== true) reasons.push("kml_gate_disabled");
  if (flags.autoDecision === true) reasons.push("auto_decision_not_allowed");
  if (reviewGate.safetyChecks?.rollbackSafe !== true) reasons.push("rollback_not_safe");
  if (reviewGate.safetyChecks?.migrationSafetyPassed !== true) reasons.push("migration_safety_not_passed");
  if (reviewGate.safetyChecks?.pendingPolicyBlocked === true) reasons.push("pending_fixture_policy");
  if (reviewGate.safetyChecks?.kmlSafe !== true) reasons.push("kml_not_safe");
  return Object.freeze([...new Set(reasons.map(reason => cleanString(reason)).filter(Boolean))]);
}

function limitedMigrationEligible({
  classification,
  flags,
  reviewGate,
  categoryPolicy,
  approved
}) {
  return Boolean(
    classification === EVIDENCE_ARBITRATION_LIMITED_MIGRATION_CLASSIFICATION.LIMITED_MIGRATION_CANDIDATE
    && approved
    && flags.migration === true
    && categoryPolicy.limitedMigrationAllowed === true
    && reviewGate.safetyChecks?.rollbackSafe === true
    && reviewGate.safetyChecks?.migrationSafetyPassed === true
  );
}

export function buildEvidenceArbitrationLimitedMigrationCandidate(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const reviewGate = normalizeReviewGate(
    input.reviewGate
    || source.evidenceArbitrationReviewGate
    || source.reviewGate
    || source
  );
  const flags = normalizeFlags(input.flags || source.flags || {});
  const category = inferCategory(reviewGate);
  const categoryPolicy = buildCategoryPolicy(category, reviewGate.reviewSubject?.winnerEvidenceType);
  const approved = reviewApproved(reviewGate);
  const pendingPolicy = reviewGate.safetyChecks?.pendingPolicyBlocked === true
    || category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.PENDING_FIXTURE;
  const classification = classifyLimitedMigration({
    reviewGate,
    categoryPolicy,
    approved,
    pendingPolicy
  });
  const blockReasons = buildBlockReasons({
    classification,
    reviewGate,
    categoryPolicy,
    flags,
    approved,
    pendingPolicy
  });
  const migrationEligible = limitedMigrationEligible({
    classification,
    flags,
    reviewGate,
    categoryPolicy,
    approved
  });

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_LIMITED_MIGRATION_SCHEMA_VERSION,
    enabled: true,
    mode: "limited_migration_candidate",
    classification,
    flags,
    review: Object.freeze({
      schemaVersion: nullableString(reviewGate.schemaVersion),
      classification: nullableString(reviewGate.classification),
      reviewStatus: nullableString(reviewGate.reviewStatus),
      reviewerRequired: reviewGate.reviewerRequired === true,
      approvedBy: nullableString(reviewGate.decision?.approvedBy),
      approvedAt: nullableString(reviewGate.decision?.approvedAt)
    }),
    candidate: Object.freeze({
      category: cleanString(category, EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.UNKNOWN),
      winnerEvidenceType: nullableString(reviewGate.reviewSubject?.winnerEvidenceType),
      authority: reviewGate.reviewSubject?.authority ?? null,
      proposedCoordinateType: nullableString(reviewGate.proposal?.proposedCoordinateType),
      proposedPrecisionMode: nullableString(reviewGate.proposal?.proposedPrecisionMode),
      limitedMigrationEligible: migrationEligible,
      kmlEligible: Boolean(migrationEligible && flags.kmlGate === true && categoryPolicy.kmlEligibleAfterGate === true),
      autoDecisionEligible: false,
      blockReasons
    }),
    legacy: Object.freeze({
      coordinateType: nullableString(reviewGate.legacy?.coordinateType),
      precisionMode: nullableString(reviewGate.legacy?.precisionMode),
      coordinateResultState: nullableString(reviewGate.legacy?.coordinateResultState),
      kmlReady: normalizeBoolean(reviewGate.legacy?.kmlReady)
    }),
    eligibility: Object.freeze({
      reviewApproved: approved,
      limitedMigrationAllowedByCategory: categoryPolicy.limitedMigrationAllowed === true,
      reviewOnly: categoryPolicy.reviewOnly === true,
      rollbackSafe: reviewGate.safetyChecks?.rollbackSafe === true,
      migrationSafetyPassed: reviewGate.safetyChecks?.migrationSafetyPassed === true,
      kmlGateRequired: categoryPolicy.kmlEligibleAfterGate === true,
      kmlGateEnabled: flags.kmlGate === true,
      autoDecisionAllowed: false,
      blockReasons
    }),
    rollback: Object.freeze({
      rollbackSafe: reviewGate.safetyChecks?.rollbackSafe === true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesObservation: true
    }),
    safety: Object.freeze({
      migrationApplied: false,
      productionWinnerChanged: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
