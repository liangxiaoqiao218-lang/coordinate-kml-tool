import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS
} from "./reason-codes.js";
import { geometryFromStructuredGroups } from "./geometry-finalizer.js";
import {
  applyFamilySafetyPolicy,
  isDmsGroupedAcquisitionDeltaProvenance
} from "./family-safety-policy.js";
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
  const acquisitionDeltaDeclared = structuredResult?.acquisition_delta_provenance !== null
    && structuredResult?.acquisition_delta_provenance !== undefined;
  const acquisitionDeltaApplies = isDmsGroupedAcquisitionDeltaProvenance(structuredResult);
  const acquisitionDeltaInvalid = acquisitionDeltaDeclared && !acquisitionDeltaApplies;
  const underlyingRequiresReview = Boolean(structuredResult.requires_review || groups.some(group => group?.requires_review !== false));
  const underlyingKmlReady = groups.length > 0 && groups.every(group => group?.kml_ready === true);
  const technicalKmlReady = geometryResult.ok && verification?.status !== "BLOCK" && !acquisitionDeltaInvalid;
  const underlyingGroups = groups.map(group => ({
    groupId: group?.group_id || null,
    requiresReview: group?.requires_review !== false,
    kmlReady: group?.kml_ready === true
  }));
  const familyPolicyApplies = String(structuredResult.coordinate_type || "").toLowerCase() === "standard_dms_table"
    && String(structuredResult.precision_mode || "").toLowerCase() === "point-az-dms-table";
  const reviewOnlyTechnicalKmlReady = verification?.status === "REVIEW" && technicalKmlReady;
  const needsConfirmation = confirmationRequired(structuredResult)
    || familyPolicyApplies
    || acquisitionDeltaApplies
    || reviewOnlyTechnicalKmlReady;
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
  const technicalFailure = recognitionResult.transformStatus === "FAILED"
    || recognitionResult.indonesiaUtm50?.transformStatus === "FAILED"
    || groups.some(group => group.points?.some(point => point.transformStatus === "FAILED"));
  const authorityRejected = recognitionResult.explicitAuthorityRejected === true || revision.explicitAuthorityRejected === true;
  const invalidCrs = recognitionResult.invalidCrsConfirmation === true || revision.invalidCrsConfirmation === true;
  const productionSource = ["legacy", "manual_input", "coordinate_engine_v2"].includes(sourceAuthority);
  const currentAuthorizedGeometryExportable = geometryResult.ok && productionSource
    && !technicalFailure && !authorityRejected && !invalidCrs && !acquisitionDeltaDeclared;
  // Provider availability governs acquisition, not an already valid deterministic result.
  const availabilityStatus = currentAuthorizedGeometryExportable ? FAMILY_AVAILABILITY_STATUS.AVAILABLE
    : familyAvailability?.status || FAMILY_AVAILABILITY_STATUS.AVAILABLE;
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
    crs: invalidCrs ? null : FINALIZED_COORDINATE_CRS,
    explicitAuthorityRejected: authorityRejected,
    kmlAuthorityBlocked: technicalFailure || invalidCrs || authorityRejected || acquisitionDeltaInvalid,
    geometry: geometryResult.ok && !acquisitionDeltaInvalid ? geometryResult.geometry : null,
    geometryFailureReason: acquisitionDeltaInvalid
      ? "ACQUISITION_DELTA_PROVENANCE_INVALID"
      : (geometryResult.ok ? null : geometryResult.reasonCode),
    confirmationStatus,
    qualityGateStatus: acquisitionDeltaInvalid
      ? COORDINATE_QUALITY_GATE_STATUS.FAILED
      : availabilityBlocked
      ? COORDINATE_QUALITY_GATE_STATUS.FAILED
      : verificationQualityStatus(verification),
    technicalKmlReady: availabilityBlocked ? false : technicalKmlReady,
    currentAuthorizedGeometryExportable,
    qualityFailureAuthorityImpact: currentAuthorizedGeometryExportable && verification?.authorityImpact === "confidence_only"
      ? "confidence_only"
      : null,
    confirmationRejectionAuthorityImpact: currentAuthorizedGeometryExportable
      && revision?.confirmationRejectionAuthorityImpact === "confidence_only"
      ? "confidence_only"
      : null,
    crsUncertaintyConfidenceOnly: currentAuthorizedGeometryExportable
      && verification?.crsUncertaintyConfidenceOnly === true,
    requiresReview: availabilityBlocked ? false : underlyingRequiresReview || familySafety.requiresReview,
    kmlReady: availabilityBlocked ? false : familySafety.kmlReady,
    groups: familySafety.groups,
    familySafetyPolicy: familySafety.policy,
    warnings: [
      ...(acquisitionDeltaApplies
        ? ["结构化复读补充了坐标行；确认当前精确结果前，地图及 KML 均不可用。"]
        : []),
      ...(acquisitionDeltaInvalid ? ["结构化复读候选的安全来源证明无效，结果已阻断。"] : []),
      ...(currentAuthorizedGeometryExportable && (underlyingRequiresReview || confirmationStatus === "pending")
        ? ["当前坐标仍需核对；地图及 KML 使用服务端当前有效几何。"] : []),
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
      kmlReady: false,
      kmlAuthorityBlocked: true,
      v3ProductionAuthority: false
    };
  }
  return {
    ...commonInput({ ...options, structuredResult: options.coordinateEngineV3, sourceAuthority: "coordinate_engine_v3" }),
    v3ProductionAuthority: true
  };
}
