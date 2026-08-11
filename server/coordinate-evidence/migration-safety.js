import {
  EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION,
  buildEvidenceArbitrationDryRunDiff
} from "./arbitration-dry-run.js";

export const EVIDENCE_ARBITRATION_MIGRATION_SAFETY_SCHEMA_VERSION =
  "evidence_arbitration_migration_safety_v1";

export const EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION = Object.freeze({
  MIGRATION_DISABLED: "MIGRATION_DISABLED",
  NO_PROPOSAL: "NO_PROPOSAL",
  BLOCKED_PENDING_POLICY: "BLOCKED_PENDING_POLICY",
  MANUAL_REVIEW_REQUIRED: "MANUAL_REVIEW_REQUIRED",
  BLOCKED_KML_GATE: "BLOCKED_KML_GATE",
  AUTO_DECISION_BLOCKED: "AUTO_DECISION_BLOCKED",
  REVIEW_ONLY_ELIGIBLE: "REVIEW_ONLY_ELIGIBLE",
  LIMITED_MIGRATION_ELIGIBLE: "LIMITED_MIGRATION_ELIGIBLE",
  AGREEMENT_NO_CHANGE: "AGREEMENT_NO_CHANGE"
});

export const EVIDENCE_ARBITRATION_MIGRATION_CATEGORY = Object.freeze({
  STRUCTURED_LEGAL_COORDINATE: "structured_legal_coordinate",
  EXPLICIT_GEOGRAPHIC_SEMANTIC: "explicit_geographic_semantic",
  VERIFIED_TRANSFORMATION: "verified_transformation",
  PENDING_FIXTURE: "pending_fixture",
  UNKNOWN: "unknown"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

const PENDING_PATTERN = /pending|indonesia.*dms.*utm|dms.*utm.*indonesia/i;

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

function normalizeDryRun(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_dry_run_diff_v1") {
    return input;
  }
  return buildEvidenceArbitrationDryRunDiff(input);
}

function inferCategory(input = {}, dryRun = {}) {
  const explicit = cleanString(input.category || input.policy?.category);
  if (explicit) return explicit;
  const fixtureStatus = cleanString(input.fixture?.fixtureStatus || input.fixtureStatus);
  if (PENDING_PATTERN.test(fixtureStatus)) {
    return EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.PENDING_FIXTURE;
  }
  switch (dryRun.proposal?.winnerEvidenceType) {
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

function isPendingPolicy(input = {}, category = "", dryRun = {}) {
  const values = [
    category,
    input.fixture?.fixtureStatus,
    input.fixtureStatus,
    dryRun.proposal?.classification,
    ...(dryRun.proposal?.blockReasons || [])
  ].filter(Boolean).join(" ");
  return input.pendingPolicy === true || PENDING_PATTERN.test(values);
}

function buildCategoryPolicy(category = "", winnerEvidenceType = "") {
  if (category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.PENDING_FIXTURE) {
    return Object.freeze({
      migrationAllowed: false,
      reviewOnlyAllowed: false,
      autoDecisionAllowed: false,
      kmlMayBeEligible: false,
      requiresManualReview: true,
      reason: "pending_fixture_policy"
    });
  }
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE
    || winnerEvidenceType === "structured_cadastral_table"
  ) {
    return Object.freeze({
      migrationAllowed: false,
      reviewOnlyAllowed: true,
      autoDecisionAllowed: false,
      kmlMayBeEligible: false,
      requiresManualReview: true,
      reason: "structured_legal_coordinate_review_only"
    });
  }
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC
    || winnerEvidenceType === "explicit_geographic_dms"
  ) {
    return Object.freeze({
      migrationAllowed: true,
      reviewOnlyAllowed: true,
      autoDecisionAllowed: false,
      kmlMayBeEligible: true,
      requiresManualReview: true,
      reason: "explicit_geographic_semantic_review_first"
    });
  }
  if (
    category === EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.VERIFIED_TRANSFORMATION
    || winnerEvidenceType === "verified_utm_transformation"
  ) {
    return Object.freeze({
      migrationAllowed: true,
      reviewOnlyAllowed: true,
      autoDecisionAllowed: true,
      kmlMayBeEligible: true,
      requiresManualReview: false,
      reason: "verified_transformation_limited_migration_candidate"
    });
  }
  return Object.freeze({
    migrationAllowed: false,
    reviewOnlyAllowed: false,
    autoDecisionAllowed: false,
    kmlMayBeEligible: false,
    requiresManualReview: true,
    reason: "unknown_category"
  });
}

function hasReason(reasons = [], reason = "") {
  return reasons.includes(reason);
}

function classifySafety({
  flags,
  dryRun,
  categoryPolicy,
  pendingPolicy,
  manualReviewCompleted
}) {
  if (pendingPolicy) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_PENDING_POLICY;
  }
  if (dryRun.classification === EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.NO_PROPOSAL) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.NO_PROPOSAL;
  }
  if (flags.migration !== true) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MIGRATION_DISABLED;
  }
  if (!categoryPolicy.migrationAllowed && categoryPolicy.reviewOnlyAllowed) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED;
  }
  if (!categoryPolicy.migrationAllowed) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.AUTO_DECISION_BLOCKED;
  }
  if (categoryPolicy.requiresManualReview && manualReviewCompleted !== true) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED;
  }
  if (
    categoryPolicy.kmlMayBeEligible
    && dryRun.diff?.wouldChangeKml === true
    && flags.kmlGate !== true
  ) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_KML_GATE;
  }
  if (flags.autoDecision === true && categoryPolicy.autoDecisionAllowed) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.LIMITED_MIGRATION_ELIGIBLE;
  }
  if (dryRun.classification === EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.AGREEMENT) {
    return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.AGREEMENT_NO_CHANGE;
  }
  return EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.REVIEW_ONLY_ELIGIBLE;
}

function buildBlockReasons({
  classification,
  flags,
  dryRun,
  categoryPolicy,
  pendingPolicy,
  manualReviewCompleted
}) {
  const reasons = [];
  if (pendingPolicy) reasons.push("pending_fixture_policy");
  if (classification === EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.NO_PROPOSAL) {
    reasons.push("shadow_candidate_unavailable");
  }
  if (flags.migration !== true) reasons.push("migration_flag_disabled");
  if (flags.kmlGate !== true) reasons.push("kml_gate_disabled");
  if (flags.autoDecision !== true) reasons.push("auto_decision_disabled");
  if (!categoryPolicy.migrationAllowed) reasons.push(categoryPolicy.reason);
  if (categoryPolicy.requiresManualReview && manualReviewCompleted !== true) {
    reasons.push("manual_review_required");
  }
  for (const reason of dryRun.proposal?.blockReasons || []) {
    reasons.push(cleanString(reason));
  }
  if (
    classification === EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_KML_GATE
    || hasReason(dryRun.proposal?.blockReasons || [], "kml_safety_gate_blocked")
  ) {
    reasons.push("kml_safety_gate_blocked");
  }
  return Object.freeze([...new Set(reasons.filter(Boolean))]);
}

export function buildEvidenceArbitrationMigrationSafety(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const dryRun = normalizeDryRun(input.dryRun || source.evidenceArbitrationDryRun || source.dryRun || source);
  const flags = normalizeFlags(input.flags || source.flags || {});
  const category = inferCategory(input, dryRun);
  const pendingPolicy = isPendingPolicy(input, category, dryRun);
  const categoryPolicy = buildCategoryPolicy(category, dryRun.proposal?.winnerEvidenceType);
  const manualReviewCompleted = input.manualReview?.completed === true || source.manualReview?.completed === true;
  const classification = classifySafety({
    flags,
    dryRun,
    categoryPolicy,
    pendingPolicy,
    manualReviewCompleted
  });
  const blockReasons = buildBlockReasons({
    classification,
    flags,
    dryRun,
    categoryPolicy,
    pendingPolicy,
    manualReviewCompleted
  });
  const migrationEligible = Boolean(
    flags.migration === true
    && !pendingPolicy
    && categoryPolicy.migrationAllowed
    && classification !== EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.NO_PROPOSAL
    && classification !== EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_KML_GATE
    && classification !== EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED
  );

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_MIGRATION_SAFETY_SCHEMA_VERSION,
    enabled: true,
    mode: "migration_safety_gate",
    flags,
    category: cleanString(category, EVIDENCE_ARBITRATION_MIGRATION_CATEGORY.UNKNOWN),
    dryRun: Object.freeze({
      schemaVersion: nullableString(dryRun.schemaVersion),
      classification: nullableString(dryRun.classification),
      wouldChangeLegacy: dryRun.diff?.wouldChangeLegacy === true,
      wouldChangeCoordinateType: dryRun.diff?.wouldChangeCoordinateType === true,
      wouldChangePrecisionMode: dryRun.diff?.wouldChangePrecisionMode === true,
      wouldChangeKml: dryRun.diff?.wouldChangeKml === true
    }),
    proposal: Object.freeze({
      classification: nullableString(dryRun.proposal?.classification),
      winnerEvidenceType: nullableString(dryRun.proposal?.winnerEvidenceType),
      winnerAuthority: dryRun.proposal?.winnerAuthority ?? null,
      proposedCoordinateType: nullableString(dryRun.proposal?.proposedCoordinateType),
      proposedPrecisionMode: nullableString(dryRun.proposal?.proposedPrecisionMode)
    }),
    eligibility: Object.freeze({
      classification,
      migrationEligible,
      reviewOnlyEligible: categoryPolicy.reviewOnlyAllowed === true && !pendingPolicy,
      autoDecisionEligible: Boolean(migrationEligible && flags.autoDecision === true && categoryPolicy.autoDecisionAllowed),
      kmlGateEligible: Boolean(migrationEligible && flags.kmlGate === true && categoryPolicy.kmlMayBeEligible),
      manualReviewRequired: categoryPolicy.requiresManualReview === true && manualReviewCompleted !== true,
      blockReasons
    }),
    rollback: Object.freeze({
      rollbackSafe: true,
      rollbackFlag: "ENABLE_EVIDENCE_ARBITRATION_MIGRATION",
      restoresLegacyArbitration: true,
      restoresCoordinateResult: true,
      restoresKmlBehavior: true,
      preservesShadowObservation: true
    }),
    safety: Object.freeze({
      migrationApplied: false,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
