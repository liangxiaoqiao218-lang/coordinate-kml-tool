import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS
} from "./reason-codes.js";
import { geometryFromStructuredGroups } from "./geometry-finalizer.js";
import { applyFamilySafetyPolicy } from "./family-safety-policy.js";
import {
  FAMILY_AVAILABILITY_STATUS,
  isFamilyAvailabilityBlocked
} from "./family-availability-policy.js";

function verificationQualityStatus(verification) {
  if (verification?.status === "PASS") return COORDINATE_QUALITY_GATE_STATUS.PASSED;
  if (verification?.status === "BLOCK") return COORDINATE_QUALITY_GATE_STATUS.FAILED;
  if (verification?.status === "REVIEW") return COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED;
  return COORDINATE_QUALITY_GATE_STATUS.UNKNOWN;
}

function confirmationRequired(engine) {
  const type = String(engine?.coordinate_type || "").toLowerCase();
  const precision = String(engine?.precision_mode || "").toLowerCase();
  return type.includes("handwritten") || precision.includes("handwritten");
}

function commonInput({
  sourceAuthority,
  recognitionResult = {},
  structuredResult = {},
  verification = {},
  revision = {},
  familyAvailability = null
}) {
  const groups = Array.isArray(structuredResult.groups) ? structuredResult.groups : [];
  const geometryResult = geometryFromStructuredGroups(groups);
  const underlyingRequiresReview = Boolean(structuredResult.requires_review || groups.some(group => group?.requires_review !== false));
  const underlyingKmlReady = groups.length > 0 && groups.every(group => group?.kml_ready === true);
  const technicalKmlReady = geometryResult.ok && verification?.status !== "BLOCK";
  const underlyingGroups = groups.map(group => ({
    groupId: group?.group_id || null,
    requiresReview: group?.requires_review !== false,
    kmlReady: group?.kml_ready === true
  }));
  const familyPolicyApplies = String(structuredResult.coordinate_type || "").toLowerCase() === "standard_dms_table"
    && String(structuredResult.precision_mode || "").toLowerCase() === "point-az-dms-table";
  const reviewOnlyTechnicalKmlReady = verification?.status === "REVIEW" && technicalKmlReady;
  const needsConfirmation = confirmationRequired(structuredResult) || familyPolicyApplies || reviewOnlyTechnicalKmlReady;
  const confirmationOnlyReview = needsConfirmation && reviewOnlyTechnicalKmlReady;
  const confirmationStatus = revision.confirmationStatus || (needsConfirmation
    ? COORDINATE_CONFIRMATION_STATUS.PENDING
    : COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED);
  const familySafety = applyFamilySafetyPolicy({
    structuredResult,
    confirmationStatus,
    underlyingRequiresReview: confirmationOnlyReview ? false : underlyingRequiresReview,
    underlyingKmlReady: confirmationOnlyReview ? true : underlyingKmlReady,
    underlyingGroups: confirmationOnlyReview
      ? underlyingGroups.map(group => ({ ...group, requiresReview: false, kmlReady: true }))
      : underlyingGroups
  });
  const availabilityStatus = familyAvailability?.status || FAMILY_AVAILABILITY_STATUS.AVAILABLE;
  const availabilityBlocked = isFamilyAvailabilityBlocked({ status: availabilityStatus });
  return {
    resultId: revision.resultId,
    resultRevision: revision.resultRevision ?? 1,
    currentRevision: revision.currentRevision ?? revision.resultRevision ?? 1,
    confirmedRevision: revision.confirmedRevision ?? null,
    sourceAuthority,
    coordinateType: structuredResult.coordinate_type || recognitionResult.coordinateType || null,
    precisionMode: structuredResult.precision_mode || recognitionResult.precisionMode || null,
    family: familyAvailability?.family || structuredResult.coordinate_type || recognitionResult.coordinateType || null,
    availabilityStatus,
    availabilityReasonCode: familyAvailability?.reasonCode || null,
    familyAvailabilityPolicy: familyAvailability || null,
    crs: FINALIZED_COORDINATE_CRS,
    geometry: geometryResult.ok ? geometryResult.geometry : null,
    geometryFailureReason: geometryResult.ok ? null : geometryResult.reasonCode,
    confirmationStatus,
    qualityGateStatus: availabilityBlocked
      ? COORDINATE_QUALITY_GATE_STATUS.FAILED
      : verificationQualityStatus(verification),
    technicalKmlReady: availabilityBlocked ? false : technicalKmlReady,
    requiresReview: availabilityBlocked ? false : familySafety.requiresReview,
    kmlReady: availabilityBlocked ? false : familySafety.kmlReady,
    groups: familySafety.groups,
    familySafetyPolicy: familySafety.policy,
    warnings: [
      ...(Array.isArray(structuredResult.warnings) ? structuredResult.warnings : []),
      ...(Array.isArray(verification.warnings) ? verification.warnings : [])
    ]
  };
}

export function createLegacyFinalizerInput(options = {}) {
  return commonInput({ ...options, structuredResult: options.coordinateEngineV2, sourceAuthority: "legacy" });
}

export function createManualFinalizerInput(options = {}) {
  return commonInput({ ...options, structuredResult: options.coordinateEngineV2, sourceAuthority: "manual_input" });
}

export function createV2FinalizerInput(options = {}) {
  return commonInput({ ...options, structuredResult: options.coordinateEngineV2, sourceAuthority: "coordinate_engine_v2" });
}

export function createV3FinalizerInput(options = {}) {
  if (options.productionAuthority !== true) {
    return {
      ...commonInput({ ...options, structuredResult: options.coordinateEngineV3, sourceAuthority: "coordinate_engine_v3" }),
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.UNKNOWN,
      requiresReview: true,
      kmlReady: false
    };
  }
  return commonInput({ ...options, structuredResult: options.coordinateEngineV3, sourceAuthority: "coordinate_engine_v3" });
}
