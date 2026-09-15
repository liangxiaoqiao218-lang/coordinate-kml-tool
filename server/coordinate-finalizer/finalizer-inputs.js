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
import {
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  partialMultisiteRecoveryProvenanceMatches
} from "../recognition/dms-source-structure.js";

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

function verifiedPartialMultisiteRecovery(recognitionResult = {}, structuredResult = {}) {
  const declaredCandidate = recognitionResult?.partialMultisiteRecoveryCandidate;
  const stage1Candidate = recognitionResult?.stage1Candidate || recognitionResult?.sourceCandidates?.stage1;
  const provenanceDeclarations = [
    structuredResult?.partial_multisite_recovery_provenance,
    recognitionResult?.partialMultisiteRecoveryProvenance,
    declaredCandidate?.provenance
  ].filter(value => value !== null && value !== undefined);
  const provenance = provenanceDeclarations[0];
  if (!declaredCandidate || typeof declaredCandidate !== "object"
    || !stage1Candidate || typeof stage1Candidate !== "object"
    || provenance?.schemaVersion !== "dms_grouped_partial_multisite_recovery_v1") return null;
  const rebuilt = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: String(stage1Candidate.rawText || ""),
    stage1Coordinates: String(stage1Candidate.coordinates || ""),
    retryRawText: String(declaredCandidate.rawText || ""),
    retryCoordinates: String(declaredCandidate.coordinates || ""),
    expansion: {
      accepted: true,
      recoveryMode: provenance.recoveryMode,
      baselineRowCount: provenance.baselineRowCount,
      retryRowCount: provenance.retryRowCount,
      addedRowCount: provenance.addedRowCount,
      baselineRowsPreserved: provenance.baselineRowsPreserved,
      groupLocalBaselineRowsPreserved: provenance.groupLocalBaselineRowsPreserved,
      baselineGroupCount: provenance.baselineGroupCount,
      baselineGroupSizes: provenance.baselineGroupSizes,
      baselineGroupIdentities: provenance.baselineGroupIdentities,
      groupCount: provenance.retryGroupCount,
      groupSizes: provenance.retryGroupSizes,
      groupIdentities: provenance.retryGroupIdentities,
      weakPartialQualification: provenance.weakPartialQualification,
      stage1RejectedEvidence: provenance.stage1RejectedEvidence,
      weakPartialInputEvidence: recognitionResult?.partialMultisiteRecoveryInputEvidence
    },
    ownerFamily: provenance.ownerFamily,
    weakPartialInputEvidence: recognitionResult?.partialMultisiteRecoveryInputEvidence
  });
  if (rebuilt.accepted !== true) return null;
  const declaredSources = recognitionResult?.sourceCandidates;
  const declaredSourcesMatch = Boolean(declaredSources) && (
    String(declaredSources?.stage1?.rawText || "") === rebuilt.stage1Candidate?.rawText
    && String(declaredSources?.stage1?.coordinates || "") === rebuilt.stage1Candidate?.coordinates
    && String(declaredSources?.structuredReread?.rawText || "") === rebuilt.rawText
    && String(declaredSources?.structuredReread?.coordinates || "") === rebuilt.normalizedCoordinates
    && declaredSources.stage1.rowCount === rebuilt.provenance.baselineRowCount
    && declaredSources.structuredReread.rowCount === rebuilt.provenance.retryRowCount
    && declaredSources.stage1.candidateRole === "STAGE1_ACQUISITION_CANDIDATE"
    && declaredSources.structuredReread.candidateRole === "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    && declaredSources.stage1.candidateSha256 === rebuilt.provenance.stage1CandidateSha256
    && declaredSources.structuredReread.candidateSha256 === rebuilt.provenance.retryCandidateSha256
  );
  if (provenanceDeclarations.length === 0
    || provenanceDeclarations.some(declared => (
      !partialMultisiteRecoveryProvenanceMatches(declared, rebuilt.provenance)
    ))
    || declaredCandidate.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || recognitionResult.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || recognitionResult.sourceCandidateSeparate !== true
    || recognitionResult.directCanonicalPromotion !== false
    || (provenance.recoveryMode === "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16"
      && JSON.stringify(recognitionResult?.partialMultisiteRecoveryInputEvidence)
        !== JSON.stringify(rebuilt.weakPartialInputEvidence))
    || !declaredSourcesMatch) return null;
  return Object.freeze({
    provenance: rebuilt.provenance,
    sourceCandidates: Object.freeze({
      stage1: Object.freeze({
        ...rebuilt.stage1Candidate,
        rowCount: rebuilt.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: rebuilt.provenance.stage1CandidateSha256
      }),
      structuredReread: Object.freeze({
        rawText: rebuilt.rawText,
        coordinates: rebuilt.normalizedCoordinates,
        rowCount: rebuilt.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: rebuilt.provenance.retryCandidateSha256
      })
    }),
    inputEvidence: rebuilt.weakPartialInputEvidence
  });
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
  const explicitPartialRecoveryDeclared = structuredResult?.partial_multisite_recovery_provenance !== null
    && structuredResult?.partial_multisite_recovery_provenance !== undefined
    || recognitionResult?.partialMultisiteRecoveryProvenance !== null
      && recognitionResult?.partialMultisiteRecoveryProvenance !== undefined
    || recognitionResult?.partialMultisiteRecoveryCandidate !== null
      && recognitionResult?.partialMultisiteRecoveryCandidate !== undefined;
  const separatedReviewIdentityDeclared = !acquisitionDeltaDeclared && (
    recognitionResult?.candidateRole !== null && recognitionResult?.candidateRole !== undefined
    || recognitionResult?.sourceCandidateSeparate !== null
      && recognitionResult?.sourceCandidateSeparate !== undefined
    || recognitionResult?.directCanonicalPromotion !== null
      && recognitionResult?.directCanonicalPromotion !== undefined
    || recognitionResult?.sourceCandidates !== null
      && recognitionResult?.sourceCandidates !== undefined
  );
  const partialRecoveryDeclared = explicitPartialRecoveryDeclared || separatedReviewIdentityDeclared;
  const partialRecovery = verifiedPartialMultisiteRecovery(recognitionResult, structuredResult);
  const partialRecoveryApplies = partialRecovery !== null;
  const partialRecoveryInvalid = partialRecoveryDeclared && !partialRecoveryApplies;
  const underlyingRequiresReview = Boolean(structuredResult.requires_review || groups.some(group => group?.requires_review !== false));
  const underlyingKmlReady = groups.length > 0 && groups.every(group => group?.kml_ready === true);
  const technicalKmlReady = geometryResult.ok && verification?.status !== "BLOCK"
    && !acquisitionDeltaInvalid && !partialRecoveryInvalid;
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
    || partialRecoveryApplies
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
    && !technicalFailure && !authorityRejected && !invalidCrs
    && !acquisitionDeltaDeclared && !partialRecoveryDeclared;
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
    kmlAuthorityBlocked: technicalFailure || invalidCrs || authorityRejected
      || acquisitionDeltaInvalid || partialRecoveryInvalid,
    geometry: geometryResult.ok && !acquisitionDeltaInvalid && !partialRecoveryInvalid
      ? geometryResult.geometry
      : null,
    geometryFailureReason: acquisitionDeltaInvalid
      ? "ACQUISITION_DELTA_PROVENANCE_INVALID"
      : partialRecoveryInvalid
        ? "PARTIAL_MULTISITE_RECOVERY_PROVENANCE_INVALID"
      : (geometryResult.ok ? null : geometryResult.reasonCode),
    confirmationStatus,
    qualityGateStatus: acquisitionDeltaInvalid || partialRecoveryInvalid
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
    candidateRole: partialRecoveryApplies ? "NONAUTHORITATIVE_REVIEW_CANDIDATE" : null,
    sourceCandidateSeparate: partialRecoveryApplies ? true : false,
    directCanonicalPromotion: partialRecoveryApplies ? false : null,
    partialMultisiteRecoveryProvenance: partialRecovery?.provenance || null,
    partialMultisiteRecoveryInputEvidence: partialRecovery?.inputEvidence || null,
    sourceCandidates: partialRecovery?.sourceCandidates || null,
    warnings: [
      ...(acquisitionDeltaApplies
        ? ["结构化复读补充了坐标行；确认当前精确结果前，地图及 KML 均不可用。"]
        : []),
      ...(acquisitionDeltaInvalid ? ["结构化复读候选的安全来源证明无效，结果已阻断。"] : []),
      ...(partialRecoveryApplies
        ? ["多站点结构化恢复仅作为非权威复核候选；确认前地图及 KML 均不可用。"]
        : []),
      ...(partialRecoveryInvalid ? ["多站点结构化恢复候选的逐点绑定证明无效，结果已阻断。"] : []),
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
