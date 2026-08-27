import { COORDINATE_CONFIRMATION_STATUS } from "./reason-codes.js";

export const POINT_AZ_TEMPORARY_REVIEW_POLICY = Object.freeze({
  policyId: "POINT_AZ_TEMPORARY_REVIEW_POLICY",
  policyVersion: "1",
  family: "point-az-dms-table",
  reasonCode: "PROVIDER_EVIDENCE_COVERAGE_INSUFFICIENT",
  reason: "Point A-Z multi-vision evidence cannot yet cover and align all 26 confirmed points consistently, so AUTO_EXPORT safety is unproven.",
  effectiveState: "REVIEW_REQUIRED_UNTIL_EXACT_IDENTITY_CONFIRMED",
  productionEligible: true,
  removalConditions: Object.freeze([
    "POINT_AZ_CONFIRMED_TRUTH_CORPUS_EXPANDED",
    "MULTIPLE_REAL_IMAGE_FIXTURES_VALIDATED",
    "PROVIDER_OUTPUT_STABILITY_VALIDATED",
    "GENERAL_AND_FINAL_VISION_COVERAGE_MEASURABLE",
    "ALL_26_LABELS_AND_ROWS_AUTHORITATIVELY_ALIGNED",
    "AUTO_EXPORT_RUN_LEVEL_TRUTH_EVIDENCE_CAPTURED",
    "FALSE_POSITIVE_REVIEW_MEASURED",
    "FALSE_NEGATIVE_REVIEW_MEASURED",
    "REAL_PROVIDER_REGRESSION_PASSED",
    "RELEASE_AUTHORITY_APPROVED_REMOVAL"
  ])
});

function isPointAzFamily(structuredResult = {}) {
  return String(structuredResult.coordinate_type || "").toLowerCase() === "standard_dms_table"
    && String(structuredResult.precision_mode || "").toLowerCase() === POINT_AZ_TEMPORARY_REVIEW_POLICY.family;
}

function serializeUnderlyingGroups(groups = []) {
  return groups.map(group => Object.freeze({
    groupId: group.groupId || null,
    requiresReview: group.requiresReview === true,
    kmlReady: group.kmlReady === true
  }));
}

export function applyFamilySafetyPolicy({
  structuredResult = {},
  confirmationStatus,
  underlyingRequiresReview,
  underlyingKmlReady,
  underlyingGroups = []
} = {}) {
  if (!isPointAzFamily(structuredResult)) {
    return Object.freeze({
      applied: false,
      confirmationRequired: false,
      requiresReview: underlyingRequiresReview === true,
      kmlReady: underlyingKmlReady === true,
      groups: serializeUnderlyingGroups(underlyingGroups),
      policy: null
    });
  }

  const confirmed = confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED;
  const effectiveGroups = serializeUnderlyingGroups(underlyingGroups).map(group => Object.freeze({
    ...group,
    requiresReview: confirmed ? group.requiresReview : true,
    kmlReady: confirmed ? group.kmlReady : false
  }));
  const policy = Object.freeze({
    ...POINT_AZ_TEMPORARY_REVIEW_POLICY,
    active: true,
    applied: true,
    confirmationRequired: true,
    confirmationReleaseMode: "EXACT_RESULT_ID_REVISION_GEOMETRY_HASH",
    confirmationOverridesIndependentBlockers: false,
    effectiveState: confirmed
      ? "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES"
      : POINT_AZ_TEMPORARY_REVIEW_POLICY.effectiveState,
    exportEligible: confirmed && underlyingRequiresReview !== true && underlyingKmlReady === true,
    underlyingReadiness: Object.freeze({
      requiresReview: underlyingRequiresReview === true,
      kmlReady: underlyingKmlReady === true,
      groups: serializeUnderlyingGroups(underlyingGroups)
    })
  });

  return Object.freeze({
    applied: true,
    confirmationRequired: true,
    requiresReview: confirmed ? underlyingRequiresReview === true : true,
    kmlReady: confirmed ? underlyingKmlReady === true : false,
    groups: effectiveGroups,
    policy
  });
}

export function releaseConfirmedFamilySafetyPolicy(finalizedResult = {}) {
  const policy = finalizedResult.familySafetyPolicy;
  if (policy?.policyId !== POINT_AZ_TEMPORARY_REVIEW_POLICY.policyId
    || policy?.policyVersion !== POINT_AZ_TEMPORARY_REVIEW_POLICY.policyVersion
    || policy?.applied !== true) {
    return Object.freeze({
      requiresReview: finalizedResult.requiresReview,
      kmlReady: finalizedResult.kmlReady,
      groups: finalizedResult.groups,
      familySafetyPolicy: policy || null
    });
  }

  const underlying = policy.underlyingReadiness || {};
  return Object.freeze({
    requiresReview: underlying.requiresReview === true,
    kmlReady: underlying.kmlReady === true,
    groups: serializeUnderlyingGroups(underlying.groups),
    familySafetyPolicy: Object.freeze({
      ...policy,
      effectiveState: "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES",
      exportEligible: underlying.requiresReview !== true && underlying.kmlReady === true
    })
  });
}

