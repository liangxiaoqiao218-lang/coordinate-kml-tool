import { createHash } from "node:crypto";

export const GOLDEN_MATURITY_VALUES = Object.freeze([
  "CONFIRMED_TRUTH",
  "CONFIRMED_POLICY",
  "PROVIDER_VARIANCE_TRACKED",
  "BASELINE_ONLY",
  "PLACEHOLDER_POLICY",
  "MISSING_TRUTH"
]);

export const RUNNER_CLASSIFICATIONS = Object.freeze([
  "COORDINATE_TRUTH_FAILURE",
  "GEOMETRY_FAILURE",
  "CRS_FAILURE",
  "UNSAFE_GATE_FAILURE",
  "CONSERVATIVE_REVIEW",
  "METADATA_MISMATCH",
  "PROVIDER_VARIANCE",
  "RELIABILITY_TIMEOUT"
]);

export const CONFIRMED_TRUTH_FIELDS = Object.freeze([
  "expected_coordinate_type",
  "expected_group_count",
  "expected_point_count",
  "expected_group_point_counts",
  "expected_exact_dms_rows",
  "expected_polygon_count",
  "expected_first_point_per_group",
  "expected_last_point_per_group",
  "expected_first_point",
  "expected_last_point",
  "expected_geometry",
  "expected_grid_rows",
  "expected_rows",
  "expected_v1_precision_mode",
  "expected_v2_precision_mode"
]);

const POLICY_FIELDS = new Set([
  "requires_review",
  "kml_ready",
  "decision_state",
  "confirmation_status",
  "family_policy_id",
  "family_policy_version"
]);
const METADATA_FIELDS = new Set(["reviewGroupIndexes"]);
const TRUTH_FIELDS = new Set([
  "coordinate_type",
  "v1_precisionMode",
  "v2_precision_mode",
  "groupCount",
  "pointCount",
  "group_point_counts",
  "polygon_count",
  "geometry",
  "firstPoint",
  "lastPoint",
  "flatten_to_single_group",
  "cross_group_edges",
  "forbidden_coordinate_type",
  "forbidden_v2_precision_mode",
  "fallback_takeover"
]);
const GEOMETRY_REASON_CODES = new Set([
  "GEOMETRY_INVALID",
  "STRUCTURED_GEOMETRY_MISSING"
]);

function isTruthField(field = "") {
  return TRUTH_FIELDS.has(field)
    || /^grid /.test(field)
    || /^mozambique /.test(field)
    || /^dms row/.test(field);
}

export function getGoldenMaturity(sample = {}) {
  const rule = sample.golden_governance || {};
  const baselineStatus = String(sample.baseline_status || "").toLowerCase();
  const inferredTruthMaturity = ["locked", "locked_experimental"].includes(baselineStatus)
    ? "CONFIRMED_TRUTH"
    : baselineStatus === "missing"
      ? "MISSING_TRUTH"
      : "BASELINE_ONLY";
  return Object.freeze({
    truthMaturity: rule.truthMaturity || inferredTruthMaturity,
    policyMaturity: rule.policyMaturity || "BASELINE_ONLY",
    providerVarianceMaturity: rule.providerVarianceMaturity || null
  });
}

export function createConfirmedTruthSnapshot(sample = {}) {
  return Object.fromEntries(CONFIRMED_TRUTH_FIELDS
    .filter(field => Object.hasOwn(sample, field))
    .map(field => [field, sample[field]]));
}

export function createConfirmedTruthHash(sample = {}) {
  return createHash("sha256").update(JSON.stringify(createConfirmedTruthSnapshot(sample))).digest("hex");
}

export function validateGoldenGovernance(governance = {}) {
  const errors = [];
  if (governance.schemaVersion !== "sr08d5_golden_policy_governance_v1") errors.push("unsupported governance schemaVersion");
  if (governance.approval?.status !== "APPROVED") errors.push("human approval is required");
  if (governance.approval?.source !== "SR-08D.4_HUMAN_APPROVAL") errors.push("approval source mismatch");
  if (governance.approval?.date !== "2026-08-26") errors.push("approval date mismatch");
  if (!governance.cases || typeof governance.cases !== "object") errors.push("governance cases are required");
  for (const [sampleId, rule] of Object.entries(governance.cases || {})) {
    if (!GOLDEN_MATURITY_VALUES.includes(rule.truthMaturity)) errors.push(`${sampleId} truthMaturity invalid`);
    if (!GOLDEN_MATURITY_VALUES.includes(rule.policyMaturity)) errors.push(`${sampleId} policyMaturity invalid`);
    if (!rule.releasePolicy?.policy) errors.push(`${sampleId} releasePolicy.policy required`);
  }
  return errors;
}

export function applyGoldenGovernance(sample = {}, governance = {}) {
  const rule = governance.cases?.[sample.sample_id];
  if (!rule) return sample;
  const policy = rule.releasePolicy || {};
  const metadata = rule.evidenceMetadata || {};
  return {
    ...sample,
    expected_requires_review: policy.expectedRequiresReview ?? sample.expected_requires_review,
    expected_kml_ready: policy.expectedKmlReady ?? sample.expected_kml_ready,
    expected_review_group_indexes: metadata.expectedReviewGroupIndexes ?? sample.expected_review_group_indexes,
    expected_decision_states: policy.expectedDecisionStates,
    expected_confirmation_status: policy.expectedConfirmationStatus,
    expected_family_policy_id: policy.familyPolicyId,
    expected_family_policy_version: policy.familyPolicyVersion,
    golden_governance: rule
  };
}

function isGeometryFailure(actual = {}) {
  return (Array.isArray(actual.geometryWarnings) && actual.geometryWarnings.length > 0)
    || (Array.isArray(actual.reasonCodes) && actual.reasonCodes.some(code => GEOMETRY_REASON_CODES.has(code)))
    || actual.selfIntersection === true;
}

function isCrsFailure(actual = {}) {
  return Array.isArray(actual.reasonCodes)
    && actual.reasonCodes.some(code => /^CRS_/i.test(String(code || "")));
}

function isRequestDeadlineTimeout(actual = {}) {
  const code = String(actual.responseCode || actual.errorCode || actual.raw?.code || "").toUpperCase();
  const reason = String(actual.responseReason || actual.raw?.reason || "").toLowerCase();
  return actual.timeout === true
    && (code === "RECOGNITION_DEADLINE_EXCEEDED" || (Number(actual.httpStatus) === 504 && reason === "timeout"));
}

function isTruthCriticalFinding(diff = {}) {
  return String(diff.severity || "").toUpperCase() === "BLOCKER" && isTruthField(diff.field);
}

export function classifyGoldenRun({ sample = {}, actual = {}, diffs = [] } = {}) {
  const rule = sample.golden_governance || {};
  const metadata = rule.evidenceMetadata || {};
  const maturity = getGoldenMaturity(sample);
  const deadlineTimeout = isRequestDeadlineTimeout(actual);
  const finalizerEvaluated = actual.finalizerEvaluated === true;
  const comparableResult = !deadlineTimeout && finalizerEvaluated;
  const truthQualified = maturity.truthMaturity === "CONFIRMED_TRUTH";
  const policyQualified = maturity.policyMaturity === "CONFIRMED_POLICY";
  const truthDiffs = truthQualified
    ? diffs.filter(isTruthCriticalFinding)
    : [];
  const policyDiffs = policyQualified && comparableResult
    ? diffs.filter(diff => POLICY_FIELDS.has(diff.field) && String(diff.severity || "").toUpperCase() === "BLOCKER")
    : [];
  const metadataQualified = metadata.expectedReviewGroupIndexes !== undefined;
  const metadataDiffs = metadataQualified && comparableResult
    ? diffs.filter(diff => METADATA_FIELDS.has(diff.field))
    : [];
  const truthMismatch = truthDiffs.length > 0;
  const geometryFailure = comparableResult && isGeometryFailure(actual);
  const crsFailure = comparableResult && isCrsFailure(actual);
  const decisionState = String(actual.decisionState || "").toUpperCase();
  const autoExport = decisionState === "AUTO_EXPORT";
  const confirmationAccepted = String(actual.confirmationStatus || "").toLowerCase() === "accepted";
  const confirmationFirst = rule.releasePolicy?.policy === "REVIEW_REQUIRED_UNTIL_CONFIRMATION";
  const policyRequiresFailClosed = policyQualified && (rule.releasePolicy?.policy === "REVIEW_REQUIRED"
    || (confirmationFirst && !confirmationAccepted));
  const unsafe = comparableResult && autoExport && (truthMismatch || geometryFailure || crsFailure || policyRequiresFailClosed);
  const providerVarianceTracked = maturity.providerVarianceMaturity === "PROVIDER_VARIANCE_TRACKED";
  const classifications = [];
  if (deadlineTimeout) classifications.push("RELIABILITY_TIMEOUT");
  if (comparableResult && truthMismatch) classifications.push("COORDINATE_TRUTH_FAILURE");
  if (geometryFailure) classifications.push("GEOMETRY_FAILURE");
  if (crsFailure) classifications.push("CRS_FAILURE");
  if (unsafe) classifications.push("UNSAFE_GATE_FAILURE");
  if (comparableResult && !autoExport) classifications.push("CONSERVATIVE_REVIEW");
  if (metadataDiffs.length > 0) classifications.push("METADATA_MISMATCH");
  if (comparableResult && providerVarianceTracked) classifications.push("PROVIDER_VARIANCE");

  const gateSafetyStatus = deadlineTimeout
    ? "FAIL_CLOSED_TIMEOUT"
    : !finalizerEvaluated
      ? "NOT_EVALUATED"
      : unsafe
    ? "UNSAFE_GATE_FAILURE"
    : truthMismatch && !autoExport
      ? "TRUTH_FAILURE_SAFE_FAIL_CLOSED"
      : geometryFailure && !autoExport
        ? "GEOMETRY_FAILURE_SAFE_FAIL_CLOSED"
        : !autoExport
          ? "CONSERVATIVE_REVIEW"
          : "SAFE_AUTO_EXPORT";

  return Object.freeze({
    truthMaturity: maturity.truthMaturity,
    policyMaturity: maturity.policyMaturity,
    truthStatus: deadlineTimeout || !finalizerEvaluated
      ? "NOT_EVALUATED"
      : truthQualified ? (truthMismatch ? "MISMATCH" : "MATCH") : "NOT_QUALIFIED",
    policyStatus: deadlineTimeout || !finalizerEvaluated
      ? "NOT_EVALUATED"
      : policyQualified ? (policyDiffs.length > 0 ? "MISMATCH" : "MATCH") : "NOT_QUALIFIED",
    gateSafetyStatus,
    metadataStatus: deadlineTimeout || !finalizerEvaluated
      ? "NOT_EVALUATED"
      : !metadataQualified
      ? "NOT_QUALIFIED"
      : metadataDiffs.length > 0 ? "MISMATCH" : "MATCH",
    providerVarianceStatus: deadlineTimeout || !finalizerEvaluated
      ? "NOT_EVALUATED"
      : providerVarianceTracked
      ? truthMismatch ? "DETECTED" : "TRACKED"
      : "NOT_TRACKED",
    reliabilityStatus: deadlineTimeout ? "TIMEOUT" : "PASS",
    finalizerStatus: finalizerEvaluated ? "EVALUATED" : "NOT_EVALUATED",
    timeoutStage: deadlineTimeout ? "UNKNOWN_AFTER_REQUEST_DEADLINE" : "NOT_APPLICABLE",
    classifications: Object.freeze(classifications),
    releaseSeverity: unsafe ? "P0" : deadlineTimeout ? "RELIABILITY_FAILURE" : truthMismatch || metadataDiffs.length > 0 ? "GOVERNANCE_FAILURE" : "NONE"
  });
}

export function summarizeSemanticRuns(runs = []) {
  const semantics = runs.map(run => run.semantics).filter(Boolean);
  const choose = (field, order, fallback) => {
    for (const value of order) if (semantics.some(item => item[field] === value)) return value;
    return semantics[0]?.[field] || fallback;
  };
  return Object.freeze({
    truthStatus: choose("truthStatus", ["MISMATCH", "MATCH", "NOT_EVALUATED", "NOT_QUALIFIED"], "NOT_QUALIFIED"),
    policyStatus: choose("policyStatus", ["MISMATCH", "MATCH", "NOT_EVALUATED", "NOT_QUALIFIED"], "NOT_QUALIFIED"),
    gateSafetyStatus: choose("gateSafetyStatus", ["UNSAFE_GATE_FAILURE", "TRUTH_FAILURE_SAFE_FAIL_CLOSED", "GEOMETRY_FAILURE_SAFE_FAIL_CLOSED", "FAIL_CLOSED_TIMEOUT", "CONSERVATIVE_REVIEW", "SAFE_AUTO_EXPORT", "NOT_EVALUATED"], "NOT_EVALUATED"),
    metadataStatus: choose("metadataStatus", ["MISMATCH", "MATCH", "NOT_EVALUATED", "NOT_QUALIFIED"], "NOT_QUALIFIED"),
    providerVarianceStatus: choose("providerVarianceStatus", ["DETECTED", "TRACKED", "NOT_EVALUATED", "NOT_TRACKED"], "NOT_TRACKED"),
    reliabilityStatus: choose("reliabilityStatus", ["TIMEOUT", "PASS"], "PASS"),
    finalizerStatus: choose("finalizerStatus", ["NOT_EVALUATED", "EVALUATED"], "NOT_EVALUATED"),
    timeoutStage: choose("timeoutStage", ["UNKNOWN_AFTER_REQUEST_DEADLINE", "NOT_APPLICABLE"], "NOT_APPLICABLE"),
    classifications: Object.freeze([...new Set(semantics.flatMap(item => item.classifications))])
  });
}

export function summarizeReleaseSemantics(items = []) {
  const semantics = items.map(item => item?.semanticSummary || item?.semantics || item).filter(Boolean);
  const count = (field, value) => semantics.filter(item => item[field] === value).length;
  const hasClassification = name => semantics.filter(item => item.classifications?.includes(name)).length;
  const truthPassCount = count("truthStatus", "MATCH");
  const truthFailureCount = count("truthStatus", "MISMATCH");
  const policyPassCount = count("policyStatus", "MATCH");
  const policyFailureCount = count("policyStatus", "MISMATCH");
  const timeoutCount = count("reliabilityStatus", "TIMEOUT");
  const unsafeGateFailureCount = hasClassification("UNSAFE_GATE_FAILURE");
  const metadataMismatchCount = hasClassification("METADATA_MISMATCH");
  return Object.freeze({
    truthPassCount,
    truthFailureCount,
    truthEvaluatedCount: truthPassCount + truthFailureCount,
    truthNotEvaluatedCount: count("truthStatus", "NOT_EVALUATED"),
    truthNotQualifiedCount: count("truthStatus", "NOT_QUALIFIED"),
    policyPassCount,
    policyFailureCount,
    policyEvaluatedCount: policyPassCount + policyFailureCount,
    policyNotEvaluatedCount: count("policyStatus", "NOT_EVALUATED"),
    policyNotQualifiedCount: count("policyStatus", "NOT_QUALIFIED"),
    timeoutCount,
    unsafeGateFailureCount,
    conservativeReviewCount: hasClassification("CONSERVATIVE_REVIEW"),
    metadataMismatchCount,
    providerVarianceCount: hasClassification("PROVIDER_VARIANCE"),
    safetyStatus: unsafeGateFailureCount > 0 ? "FAIL_P0" : "PASS",
    reliabilityStatus: timeoutCount > 0 ? "FAIL_TIMEOUT_RATE" : "PASS",
    truthStatus: truthFailureCount > 0 ? "FAIL" : "PASS",
    policyStatus: policyFailureCount > 0 ? "FAIL" : "PASS",
    governanceStatus: metadataMismatchCount > 0 ? "FAIL" : "PASS",
    liveCoordinateGoldenStatus: unsafeGateFailureCount > 0
      ? "FAIL_P0"
      : timeoutCount > 0
        ? "FAIL_RELIABILITY"
        : truthFailureCount > 0 || policyFailureCount > 0 || metadataMismatchCount > 0
          ? "FAIL_GOVERNANCE"
          : "PASS",
    postDeadlineWorkStatus: "UNPROVEN"
  });
}
