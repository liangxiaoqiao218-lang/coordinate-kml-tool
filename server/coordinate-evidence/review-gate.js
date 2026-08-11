import {
  EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION,
  buildEvidenceArbitrationMigrationSafety
} from "./migration-safety.js";

export const EVIDENCE_ARBITRATION_REVIEW_GATE_SCHEMA_VERSION =
  "evidence_arbitration_review_gate_v1";

export const EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION = Object.freeze({
  PENDING_REVIEW: "PENDING_REVIEW",
  APPROVED_FOR_LIMITED_MIGRATION: "APPROVED_FOR_LIMITED_MIGRATION",
  REJECTED: "REJECTED",
  BLOCKED: "BLOCKED",
  ROLLED_BACK: "ROLLED_BACK"
});

export const EVIDENCE_ARBITRATION_REVIEW_STATUS = Object.freeze({
  PENDING: "PENDING",
  APPROVED: "APPROVED",
  REJECTED: "REJECTED",
  BLOCKED: "BLOCKED",
  ROLLED_BACK: "ROLLED_BACK"
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

function normalizeReviewStatus(value) {
  const status = cleanString(value, EVIDENCE_ARBITRATION_REVIEW_STATUS.PENDING).toUpperCase();
  return Object.values(EVIDENCE_ARBITRATION_REVIEW_STATUS).includes(status)
    ? status
    : EVIDENCE_ARBITRATION_REVIEW_STATUS.PENDING;
}

function normalizeMigrationSafety(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_migration_safety_v1") {
    return input;
  }
  return buildEvidenceArbitrationMigrationSafety(input);
}

function isSafetyBlocked(safety = {}) {
  return [
    EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_PENDING_POLICY,
    EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.NO_PROPOSAL,
    EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.AUTO_DECISION_BLOCKED
  ].includes(safety.eligibility?.classification);
}

function isKmlSafe(safety = {}, input = {}) {
  if (input.kmlSafe === true || input.kmlSafe === false) return input.kmlSafe;
  return safety.eligibility?.kmlGateEligible === true
    && safety.dryRun?.wouldChangeKml !== true;
}

function reviewRequired(safety = {}, kmlSafe = false) {
  const eligibility = safety.eligibility || {};
  const dryRun = safety.dryRun || {};
  return Boolean(
    eligibility.manualReviewRequired === true
    || dryRun.wouldChangeLegacy === true
    || dryRun.wouldChangeCoordinateType === true
    || dryRun.wouldChangePrecisionMode === true
    || dryRun.wouldChangeKml === true
    || kmlSafe !== true
    || eligibility.classification === EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED
  );
}

function classifyReviewGate({ safety, reviewStatus, kmlSafe }) {
  if (reviewStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.ROLLED_BACK;
  }
  if (reviewStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.REJECTED) {
    return EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.REJECTED;
  }
  if (isSafetyBlocked(safety)) {
    return EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED;
  }
  if (reviewStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.APPROVED) {
    return EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.APPROVED_FOR_LIMITED_MIGRATION;
  }
  if (reviewStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.BLOCKED) {
    return EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED;
  }
  return reviewRequired(safety, kmlSafe)
    ? EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.PENDING_REVIEW
    : EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.PENDING_REVIEW;
}

function effectiveReviewStatus(classification, requestedStatus) {
  if (classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED) {
    return EVIDENCE_ARBITRATION_REVIEW_STATUS.BLOCKED;
  }
  if (classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.ROLLED_BACK) {
    return EVIDENCE_ARBITRATION_REVIEW_STATUS.ROLLED_BACK;
  }
  if (classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.REJECTED) {
    return EVIDENCE_ARBITRATION_REVIEW_STATUS.REJECTED;
  }
  if (classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.APPROVED_FOR_LIMITED_MIGRATION) {
    return EVIDENCE_ARBITRATION_REVIEW_STATUS.APPROVED;
  }
  return requestedStatus === EVIDENCE_ARBITRATION_REVIEW_STATUS.PENDING
    ? requestedStatus
    : EVIDENCE_ARBITRATION_REVIEW_STATUS.PENDING;
}

function buildProposalId(input = {}, safety = {}) {
  return cleanString(
    input.proposalId
    || [
      safety.category,
      safety.proposal?.winnerEvidenceType,
      safety.proposal?.proposedCoordinateType,
      safety.proposal?.proposedPrecisionMode
    ].filter(Boolean).join(":"),
    "proposal_unavailable"
  );
}

export function buildEvidenceArbitrationReviewGate(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const safety = normalizeMigrationSafety(
    input.migrationSafety
    || source.evidenceArbitrationMigrationSafety
    || source.migrationSafety
    || source
  );
  const requestedStatus = normalizeReviewStatus(
    input.review?.status
    || source.review?.status
    || input.reviewStatus
    || source.reviewStatus
  );
  const kmlSafe = isKmlSafe(safety, input.safetyChecks || source.safetyChecks || {});
  const classification = classifyReviewGate({
    safety,
    reviewStatus: requestedStatus,
    kmlSafe
  });
  const reviewStatus = effectiveReviewStatus(classification, requestedStatus);
  const reviewerRequired = classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.PENDING_REVIEW
    || classification === EVIDENCE_ARBITRATION_REVIEW_GATE_CLASSIFICATION.BLOCKED
    || safety.eligibility?.manualReviewRequired === true
    || safety.dryRun?.wouldChangeLegacy === true;

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_REVIEW_GATE_SCHEMA_VERSION,
    proposalId: buildProposalId(input, safety),
    mode: "review_only",
    classification,
    reviewerRequired,
    reviewStatus,
    reviewSubject: Object.freeze({
      category: nullableString(safety.category),
      winnerEvidenceType: nullableString(safety.proposal?.winnerEvidenceType),
      authority: safety.proposal?.winnerAuthority ?? null,
      wouldChangeLegacy: safety.dryRun?.wouldChangeLegacy === true,
      wouldChangeCoordinateType: safety.dryRun?.wouldChangeCoordinateType === true,
      wouldChangePrecisionMode: safety.dryRun?.wouldChangePrecisionMode === true,
      wouldChangeKml: safety.dryRun?.wouldChangeKml === true
    }),
    legacy: Object.freeze({
      coordinateType: nullableString(input.legacy?.coordinateType || source.legacy?.coordinateType),
      precisionMode: nullableString(input.legacy?.precisionMode || source.legacy?.precisionMode),
      coordinateResultState: nullableString(input.legacy?.coordinateResultState || source.legacy?.coordinateResultState),
      kmlReady: normalizeBoolean(input.legacy?.kmlReady ?? source.legacy?.kmlReady)
    }),
    proposal: Object.freeze({
      proposedCoordinateType: nullableString(safety.proposal?.proposedCoordinateType),
      proposedPrecisionMode: nullableString(safety.proposal?.proposedPrecisionMode),
      reason: nullableString(
        input.decision?.reason
        || input.review?.reason
        || source.decision?.reason
        || safety.eligibility?.blockReasons?.[0]
      )
    }),
    safetyChecks: Object.freeze({
      rollbackSafe: safety.rollback?.rollbackSafe === true,
      kmlSafe,
      pendingPolicyBlocked: safety.eligibility?.classification === EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_PENDING_POLICY,
      migrationSafetyPassed: safety.eligibility?.migrationEligible === true || safety.eligibility?.reviewOnlyEligible === true
    }),
    decision: Object.freeze({
      approvedBy: nullableString(input.review?.approvedBy || source.review?.approvedBy),
      approvedAt: nullableString(input.review?.approvedAt || source.review?.approvedAt),
      reason: nullableString(input.review?.reason || source.review?.reason || input.decisionReason || source.decisionReason)
    }),
    effects: Object.freeze({
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
