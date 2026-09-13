import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS
} from "./reason-codes.js";
import { FAMILY_AVAILABILITY_STATUS } from "./family-availability-policy.js";
import {
  normalizeDmsBoundaryIdentity,
  STAGE1_FULL_MULTISITE_POLICY_ID
} from "../recognition/dms-source-structure.js";

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

export const DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY = Object.freeze({
  policyId: STAGE1_FULL_MULTISITE_POLICY_ID,
  policyVersion: "1",
  family: "dms_grouped",
  reasonCode: "STAGE1_FULL_MULTISITE_CONFIRMATION_REQUIRED",
  reason: "A complete Stage-1 multi-site DMS result must preserve its proven group topology and be confirmed before Map or KML use.",
  effectiveState: "REVIEW_REQUIRED_UNTIL_EXACT_IDENTITY_CONFIRMED",
  productionEligible: true
});

export const STAGE1_FULL_MULTISITE_CONFIRMATION_SCOPE = "STAGE1_FULL_MULTISITE_GROUPING";
export const STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE = "STAGE1_FULL_MULTISITE_GROUPING_ONLY";

const STAGE1_FULL_MULTISITE_POLICY_SIGNING_SECRET = randomBytes(32);

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

function isClosedRingWithPointCount(ring, pointCount) {
  if (!Array.isArray(ring) || ring.length !== pointCount + 1) return false;
  const first = ring[0];
  const last = ring[ring.length - 1];
  return Array.isArray(first)
    && Array.isArray(last)
    && first.length === 2
    && last.length === 2
    && first[0] === last[0]
    && first[1] === last[1];
}

function parseStage1GroupedCoordinatePositions(groupedCoordinates = "") {
  const groups = String(groupedCoordinates || "").trim().split(/\n\s*\n/);
  if (groups.length !== 3) return null;
  const parsedGroups = groups.map(group => group.split(/\r?\n/).map(row => {
    const match = row.trim().match(
      /^([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)(?:\s*,\s*[-+]?\d+(?:\.\d+)?)?$/
    );
    if (!match) return null;
    const longitude = Number(match[1]);
    const latitude = Number(match[2]);
    const longitudeDecimals = (match[1].split(".")[1] || "").length;
    const latitudeDecimals = (match[2].split(".")[1] || "").length;
    return Number.isFinite(longitude) && Number.isFinite(latitude)
      ? Object.freeze({
          longitude,
          latitude,
          longitudeTolerance: (0.5 * (10 ** -longitudeDecimals)) + Number.EPSILON,
          latitudeTolerance: (0.5 * (10 ** -latitudeDecimals)) + Number.EPSILON
        })
      : null;
  }));
  return parsedGroups.some(group => group.some(position => !position)) ? null : parsedGroups;
}

function ringMatchesOrderedPositions(ring, positions) {
  return isClosedRingWithPointCount(ring, positions.length)
    && positions.every((position, index) => Array.isArray(ring[index])
      && ring[index].length === 2
      && Math.abs(ring[index][0] - position.longitude) <= position.longitudeTolerance
      && Math.abs(ring[index][1] - position.latitude) <= position.latitudeTolerance);
}

function hasExactStage1GroupTopology(stage1Safety = {}, finalizedResult = {}) {
  const safetySizes = Array.isArray(stage1Safety.groupSizes) ? stage1Safety.groupSizes : [];
  const expectedPositions = parseStage1GroupedCoordinatePositions(stage1Safety.groupedCoordinates);
  const groups = Array.isArray(finalizedResult.groups) ? finalizedResult.groups : [];
  const groupIds = groups.map(group => String(group?.groupId || "").trim());
  const geometry = finalizedResult.geometry;
  const polygons = geometry?.type === "MultiPolygon" && Array.isArray(geometry.coordinates)
    ? geometry.coordinates
    : [];
  return stage1Safety.gateRequired === true
    && stage1Safety.acceptedForReview === true
    && stage1Safety.failClosed === false
    && safetySizes.length === 3
    && [8, 4, 4].every((size, index) => safetySizes[index] === size)
    && groups.length === 3
    && groupIds.every(Boolean)
    && new Set(groupIds).size === 3
    && polygons.length === 3
    && polygons.every((polygon, index) => Array.isArray(polygon)
      && polygon.length === 1
      && Array.isArray(expectedPositions?.[index])
      && expectedPositions[index].length === safetySizes[index]
      && ringMatchesOrderedPositions(polygon[0], expectedPositions[index]));
}

function verificationReasonValues(verification = {}) {
  const reasonFields = [
    "reason",
    "reasonCode",
    "reasonCodes",
    "reviewReason",
    "reviewReasons",
    "review_reason",
    "review_reasons"
  ];
  return reasonFields.flatMap(field => {
    const value = verification[field];
    if (Array.isArray(value)) return value.map(item => String(item || "").trim()).filter(Boolean);
    const normalized = String(value || "").trim();
    return normalized ? [normalized] : [];
  });
}

function isGroupingOnlyQualityReview(verification = {}, confirmationSource = "") {
  const reasons = verificationReasonValues(verification);
  return verification.status === "REVIEW"
    && confirmationSource === STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
    && verification.validation_scope === "coordinate_and_geometry"
    && verification.geometry_validation === "EVALUATED"
    && Number(verification.verification_score) >= 0.85
    && Array.isArray(verification.warnings)
    && verification.warnings.length === 0
    && Array.isArray(verification.conflicts)
    && verification.conflicts.length === 0
    && Array.isArray(verification.geometryWarnings)
    && verification.geometryWarnings.length === 0
    && reasons.every(reason => reason === STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE);
}

function stage1PolicyAuthorityBinding(finalizedResult = {}) {
  return Object.freeze({
    resultId: typeof finalizedResult.resultId === "string" ? finalizedResult.resultId : null,
    resultRevision: Number.isSafeInteger(finalizedResult.resultRevision) ? finalizedResult.resultRevision : null,
    geometryHash: typeof finalizedResult.geometryHash === "string" ? finalizedResult.geometryHash : null
  });
}

function stage1PolicySignaturePayload(policy = {}) {
  return JSON.stringify({
    policyId: policy.policyId,
    policyVersion: policy.policyVersion,
    confirmationScope: policy.confirmationScope,
    confirmationSource: policy.confirmationSource,
    authorityBinding: policy.authorityBinding,
    underlyingReadinessProven: policy.underlyingReadinessProven,
    underlyingReadiness: policy.underlyingReadiness
  });
}

function signStage1Policy(policy = {}) {
  return createHmac("sha256", STAGE1_FULL_MULTISITE_POLICY_SIGNING_SECRET)
    .update(stage1PolicySignaturePayload(policy))
    .digest("hex");
}

function stage1PolicySignatureValid(policy = {}, finalizedResult = {}) {
  const signature = String(policy?.integrity?.signature || "");
  const binding = policy?.authorityBinding || {};
  const identityBound = binding.resultId === finalizedResult.resultId
    && binding.resultRevision === finalizedResult.resultRevision
    && binding.geometryHash === finalizedResult.geometryHash;
  if (!identityBound
    || policy?.integrity?.algorithm !== "HMAC-SHA256"
    || !/^[a-f0-9]{64}$/.test(signature)) return false;
  const expected = signStage1Policy(policy);
  return timingSafeEqual(Buffer.from(signature, "hex"), Buffer.from(expected, "hex"));
}

export function buildStage1FullMultisiteConfirmationPolicy({
  finalizedResult = {},
  verification = {},
  stage1Safety = {},
  confirmationSource = ""
} = {}) {
  const groups = Array.isArray(finalizedResult.groups) ? finalizedResult.groups : [];
  const topologyProven = hasExactStage1GroupTopology(stage1Safety, finalizedResult);
  const groupingOnlyQualityReview = isGroupingOnlyQualityReview(verification, confirmationSource)
    && finalizedResult.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED;
  const independentGateClear = finalizedResult.decisionState !== COORDINATE_DECISION_STATE.BLOCKED
    && finalizedResult.kmlAuthorityBlocked !== true
    && finalizedResult.technicalKmlReady === true
    && finalizedResult.availabilityStatus === FAMILY_AVAILABILITY_STATUS.AVAILABLE
    && finalizedResult.geometry !== null
    && finalizedResult.crs !== null;
  const underlyingReady = topologyProven && groupingOnlyQualityReview && independentGateClear;
  const underlyingGroups = groups.map(group => ({
    groupId: group?.groupId || null,
    requiresReview: !underlyingReady,
    kmlReady: underlyingReady
  }));
  const unsignedPolicy = {
    ...DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY,
    active: true,
    applied: true,
    confirmationRequired: true,
    confirmationScope: STAGE1_FULL_MULTISITE_CONFIRMATION_SCOPE,
    confirmationSource: confirmationSource === STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
      ? confirmationSource
      : null,
    confirmationReleaseMode: "EXACT_RESULT_ID_REVISION_GEOMETRY_HASH",
    confirmationOverridesIndependentBlockers: false,
    effectiveState: DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY.effectiveState,
    exportEligible: false,
    underlyingReadinessProven: underlyingReady,
    underlyingReadiness: Object.freeze({
      requiresReview: !underlyingReady,
      kmlReady: underlyingReady,
      groups: serializeUnderlyingGroups(underlyingGroups)
    }),
    authorityBinding: stage1PolicyAuthorityBinding(finalizedResult)
  };
  return Object.freeze({
    ...unsignedPolicy,
    integrity: Object.freeze({
      algorithm: "HMAC-SHA256",
      signature: signStage1Policy(unsignedPolicy)
    })
  });
}

function failClosedUnderlyingReadiness(finalizedResult = {}) {
  return Object.freeze({
    requiresReview: true,
    kmlReady: false,
    groups: serializeUnderlyingGroups((Array.isArray(finalizedResult.groups) ? finalizedResult.groups : []).map(group => ({
      groupId: group?.groupId || null,
      requiresReview: true,
      kmlReady: false
    })))
  });
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
  const stage1FullMultisitePolicy = policy?.policyId === DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY.policyId
    && policy?.policyVersion === DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY.policyVersion;
  const recognizedPolicy = (policy?.policyId === POINT_AZ_TEMPORARY_REVIEW_POLICY.policyId
      && policy?.policyVersion === POINT_AZ_TEMPORARY_REVIEW_POLICY.policyVersion)
    || (policy?.policyId === DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyId
      && policy?.policyVersion === DMS_GROUPED_ACQUISITION_DELTA_POLICY.policyVersion)
    || stage1FullMultisitePolicy;
  if (!recognizedPolicy
    || policy?.applied !== true) {
    return Object.freeze({
      requiresReview: finalizedResult.requiresReview,
      kmlReady: finalizedResult.kmlReady,
      groups: finalizedResult.groups,
      familySafetyPolicy: policy || null
    });
  }

  const stage1ReadinessBound = !stage1FullMultisitePolicy || (
    policy.confirmationScope === STAGE1_FULL_MULTISITE_CONFIRMATION_SCOPE
    && policy.confirmationSource === STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
    && stage1PolicySignatureValid(policy, finalizedResult)
    && policy.underlyingReadinessProven === true
    && policy.underlyingReadiness?.requiresReview === false
    && policy.underlyingReadiness?.kmlReady === true
  );
  const underlying = stage1ReadinessBound
    ? (policy.underlyingReadiness || {})
    : failClosedUnderlyingReadiness(finalizedResult);
  return Object.freeze({
    requiresReview: underlying.requiresReview === true,
    kmlReady: underlying.kmlReady === true,
    ...(stage1FullMultisitePolicy && !stage1ReadinessBound ? { kmlAuthorityBlocked: true } : {}),
    groups: serializeUnderlyingGroups(underlying.groups),
    familySafetyPolicy: Object.freeze({
      ...policy,
      effectiveState: "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES",
      exportEligible: underlying.requiresReview !== true && underlying.kmlReady === true,
      underlyingReadiness: Object.freeze({
        requiresReview: underlying.requiresReview === true,
        kmlReady: underlying.kmlReady === true,
        groups: serializeUnderlyingGroups(underlying.groups)
      })
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

