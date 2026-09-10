import { COORDINATE_CONFIRMATION_STATUS } from "./reason-codes.js";
import { normalizeDmsBoundaryIdentity } from "../recognition/dms-source-structure.js";

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

export const DMS_GROUPED_ACQUISITION_DELTA_POLICY = Object.freeze({
  policyId: "DMS_GROUPED_ACQUISITION_DELTA_POLICY",
  policyVersion: "1",
  family: "dms_grouped",
  reasonCode: "ACQUISITION_DELTA_CONFIRMATION_REQUIRED",
  reason: "A structured reread found additional coordinate rows; the expanded candidate must be confirmed before Map or KML use.",
  effectiveState: "REVIEW_REQUIRED_UNTIL_EXACT_IDENTITY_CONFIRMED",
  productionEligible: true
});

function isPointAzFamily(structuredResult = {}) {
  return String(structuredResult.coordinate_type || "").toLowerCase() === "standard_dms_table"
    && String(structuredResult.precision_mode || "").toLowerCase() === POINT_AZ_TEMPORARY_REVIEW_POLICY.family;
}

function hasBoundThreeGroupTopology(structuredResult = {}, provenance = {}) {
  const baselineSizes = Array.isArray(provenance.baselineGroupSizes) ? provenance.baselineGroupSizes : [];
  const retrySizes = Array.isArray(provenance.retryGroupSizes) ? provenance.retryGroupSizes : [];
  const baselineIdentities = Array.isArray(provenance.baselineGroupIdentities) ? provenance.baselineGroupIdentities : [];
  const retryIdentities = Array.isArray(provenance.retryGroupIdentities) ? provenance.retryGroupIdentities : [];
  const groups = Array.isArray(structuredResult.groups) ? structuredResult.groups : [];
  return provenance.groupLocalBaselineRowsPreserved === true
    && provenance.baselineGroupCount === 3
    && provenance.retryGroupCount === 3
    && baselineSizes.length === 3
    && retrySizes.length === 3
    && baselineIdentities.length === 3
    && retryIdentities.length === 3
    && baselineSizes.every((size, index) => Number.isSafeInteger(size) && size > 0 && size <= retrySizes[index])
    && retrySizes.every(size => Number.isSafeInteger(size) && size > 0)
    && baselineSizes.reduce((sum, size) => sum + size, 0) === 13
    && retrySizes.reduce((sum, size) => sum + size, 0) === 16
    && baselineIdentities.every((identity, index) => typeof identity === "string"
      && identity.length > 0
      && identity === retryIdentities[index])
    && new Set(retryIdentities).size === 3
    && groups.length === 3
    && groups.every((group, index) => Array.isArray(group?.points)
      && group.points.length === retrySizes[index]
      && normalizeDmsBoundaryIdentity(group?.group_name || group?.groupName || group?.name) === retryIdentities[index]);
}

export function isDmsGroupedAcquisitionDeltaProvenance(structuredResult = {}) {
  const provenance = structuredResult?.acquisition_delta_provenance;
  return provenance?.schemaVersion === "dms_grouped_acquisition_delta_v1"
    && provenance?.ownerFamily === "dms_grouped"
    && provenance?.candidateRole === "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    && Number.isSafeInteger(provenance?.baselineRowCount)
    && provenance.baselineRowCount === 13
    && Number.isSafeInteger(provenance?.retryRowCount)
    && provenance.retryRowCount === 16
    && Number.isSafeInteger(provenance?.addedRowCount)
    && provenance.addedRowCount === provenance.retryRowCount - provenance.baselineRowCount
    && provenance?.baselineRowsPreserved === true
    && hasBoundThreeGroupTopology(structuredResult, provenance)
    && provenance?.strongBoundariesProven === true
    && provenance?.labelsContinuous === true
    && provenance?.sourceCandidateSeparate === true
    && provenance?.directCanonicalPromotion === false
    && /^[a-f0-9]{64}$/.test(String(provenance?.stage1CandidateSha256 || ""))
    && /^[a-f0-9]{64}$/.test(String(provenance?.retryCandidateSha256 || ""))
    && provenance.stage1CandidateSha256 !== provenance.retryCandidateSha256;
}

function serializeAcquisitionDeltaProvenance(structuredResult = {}) {
  const provenance = structuredResult.acquisition_delta_provenance;
  return Object.freeze({
    schemaVersion: provenance.schemaVersion,
    ownerFamily: provenance.ownerFamily,
    candidateRole: provenance.candidateRole,
    baselineRowCount: provenance.baselineRowCount,
    retryRowCount: provenance.retryRowCount,
    addedRowCount: provenance.addedRowCount,
    baselineRowsPreserved: true,
    groupLocalBaselineRowsPreserved: true,
    strongBoundariesProven: true,
    labelsContinuous: true,
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    baselineGroupCount: provenance.baselineGroupCount,
    baselineGroupSizes: Object.freeze([...provenance.baselineGroupSizes]),
    baselineGroupIdentities: Object.freeze([...provenance.baselineGroupIdentities]),
    retryGroupCount: provenance.retryGroupCount,
    retryGroupSizes: Object.freeze([...provenance.retryGroupSizes]),
    retryGroupIdentities: Object.freeze([...provenance.retryGroupIdentities]),
    stage1CandidateSha256: provenance.stage1CandidateSha256,
    retryCandidateSha256: provenance.retryCandidateSha256
  });
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
  const pointAzApplies = isPointAzFamily(structuredResult);
  const acquisitionDeltaApplies = isDmsGroupedAcquisitionDeltaProvenance(structuredResult);
  if (!pointAzApplies && !acquisitionDeltaApplies) {
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
  const basePolicy = acquisitionDeltaApplies
    ? DMS_GROUPED_ACQUISITION_DELTA_POLICY
    : POINT_AZ_TEMPORARY_REVIEW_POLICY;
  const policy = Object.freeze({
    ...basePolicy,
    active: true,
    applied: true,
    confirmationRequired: true,
    confirmationReleaseMode: "EXACT_RESULT_ID_REVISION_GEOMETRY_HASH",
    confirmationOverridesIndependentBlockers: false,
    effectiveState: confirmed
      ? "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES"
      : basePolicy.effectiveState,
    exportEligible: confirmed && underlyingRequiresReview !== true && underlyingKmlReady === true,
    acquisitionDeltaProvenance: acquisitionDeltaApplies
      ? serializeAcquisitionDeltaProvenance(structuredResult)
      : null,
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
  const recognizedPolicy = (policy?.policyId === POINT_AZ_TEMPORARY_REVIEW_POLICY.policyId
      && policy?.policyVersion === POINT_AZ_TEMPORARY_REVIEW_POLICY.policyVersion)
    || (policy?.policyId === DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyId
      && policy?.policyVersion === DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyVersion);
  if (!recognizedPolicy
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

export function isPendingDmsGroupedAcquisitionDeltaPolicy(policy = {}) {
  if (policy?.policyId !== DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyId) return false;
  return policy?.policyVersion !== DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyVersion
    || policy?.applied !== true
    || policy?.confirmationRequired !== true
    || policy?.effectiveState !== "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES"
    || policy?.exportEligible !== true;
}

