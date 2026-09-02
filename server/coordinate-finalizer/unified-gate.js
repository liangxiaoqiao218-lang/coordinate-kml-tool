import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_SOURCE_AUTHORITIES
} from "./reason-codes.js";
import { validateFinalizedCrs, validateFinalizedGeometry } from "./geometry-finalizer.js";
import { getRecognitionBudget } from "./recognition-deadline.js";
import { FAMILY_AVAILABILITY_STATUS } from "./family-availability-policy.js";

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

export function evaluateCoordinateReleaseGate(candidate = {}) {
  const diagnosticReasons = [];
  const authorityBlockingReasons = [];
  const kmlBlockingReasons = [];
  const confirmationAccepted = candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED;
  const revisionValid = Number.isSafeInteger(candidate.resultRevision) && candidate.resultRevision >= 1;
  if (!revisionValid) authorityBlockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_INVALID);
  if (revisionValid && candidate.currentRevision !== undefined && candidate.currentRevision !== candidate.resultRevision) {
    authorityBlockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_STALE);
  }
  const sourceAuthorityValid = FINALIZED_COORDINATE_SOURCE_AUTHORITIES.includes(candidate.sourceAuthority);
  if (!sourceAuthorityValid || candidate.explicitAuthorityRejected === true) {
    authorityBlockingReasons.push(COORDINATE_GATE_REASON.SOURCE_AUTHORITY_INVALID);
  }

  const budget = getRecognitionBudget();
  const crsStage = budget?.stageStarted("crs");
  let crsResult;
  let crsStageResult = "success";
  try {
    crsResult = validateFinalizedCrs(candidate.crs);
  } catch (error) {
    crsStageResult = "failed";
    throw error;
  } finally {
    budget?.stageCompleted(crsStage, { result: crsStageResult });
  }
  const geometryStage = budget?.stageStarted("geometry");
  let geometryResult;
  let geometryStageResult = "success";
  try {
    geometryResult = validateFinalizedGeometry(candidate.geometry);
  } catch (error) {
    geometryStageResult = "failed";
    throw error;
  } finally {
    budget?.stageCompleted(geometryStage, { result: geometryStageResult });
  }
  if (!geometryResult.ok) authorityBlockingReasons.push(candidate.geometry
    ? geometryResult.reasonCode
    : candidate.geometryFailureReason || COORDINATE_GATE_REASON.STRUCTURED_GEOMETRY_MISSING);

  const productionAuthorizedSource = candidate.sourceAuthority === "legacy"
    || candidate.sourceAuthority === "manual_input"
    || candidate.sourceAuthority === "coordinate_engine_v2"
    || (candidate.sourceAuthority === "coordinate_engine_v3" && candidate.v3ProductionAuthority === true);
  const currentRevisionMatches = revisionValid
    && (candidate.currentRevision === undefined || candidate.currentRevision === candidate.resultRevision);
  const currentAuthorizedGeometryExportable = candidate.currentAuthorizedGeometryExportable === true
    && currentRevisionMatches
    && sourceAuthorityValid
    && productionAuthorizedSource
    && candidate.explicitAuthorityRejected !== true
    && candidate.kmlAuthorityBlocked !== true
    && geometryResult.ok;
  const acceptedTechnicalGeometryExportable = confirmationAccepted
    && candidate.confirmedRevision === candidate.resultRevision
    && currentRevisionMatches
    && sourceAuthorityValid
    && productionAuthorizedSource
    && candidate.explicitAuthorityRejected !== true
    && candidate.kmlAuthorityBlocked !== true
    && candidate.technicalKmlReady === true
    && crsResult.ok
    && geometryResult.ok;
  const geometryExportableForKml = currentAuthorizedGeometryExportable || acceptedTechnicalGeometryExportable;

  if (!crsResult.ok) {
    if (currentAuthorizedGeometryExportable && candidate.crsUncertaintyConfidenceOnly === true) {
      diagnosticReasons.push(crsResult.reasonCode);
    } else {
      authorityBlockingReasons.push(crsResult.reasonCode);
    }
  }

  const availabilityReason = candidate.availabilityStatus === FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER
    ? COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER
    : candidate.availabilityStatus === FAMILY_AVAILABILITY_STATUS.TEMPORARILY_UNAVAILABLE
      ? COORDINATE_GATE_REASON.FAMILY_TEMPORARILY_UNAVAILABLE
      : null;
  if (availabilityReason) {
    (currentAuthorizedGeometryExportable ? diagnosticReasons : authorityBlockingReasons).push(availabilityReason);
  }

  if (candidate.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.FAILED) {
    if (currentAuthorizedGeometryExportable && candidate.qualityFailureAuthorityImpact === "confidence_only") {
      diagnosticReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_FAILED);
    } else {
      authorityBlockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_FAILED);
    }
  } else if (candidate.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED) {
    if (!confirmationAccepted) diagnosticReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED);
  } else if (candidate.qualityGateStatus !== COORDINATE_QUALITY_GATE_STATUS.PASSED) {
    authorityBlockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_UNKNOWN);
  }

  if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.PENDING) {
    diagnosticReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED);
  } else if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.REJECTED) {
    if (currentAuthorizedGeometryExportable && candidate.confirmationRejectionAuthorityImpact === "confidence_only") {
      diagnosticReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REJECTED);
    } else {
      authorityBlockingReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REJECTED);
    }
  } else if (![COORDINATE_CONFIRMATION_STATUS.ACCEPTED, COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED].includes(candidate.confirmationStatus)) {
    diagnosticReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED);
  }
  if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED
    && candidate.confirmedRevision !== candidate.resultRevision) {
    authorityBlockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_STALE);
  }
  if (candidate.requiresReview !== false && !confirmationAccepted) {
    diagnosticReasons.push(COORDINATE_GATE_REASON.REVIEW_REQUIRED);
  }

  if (candidate.kmlAuthorityBlocked === true) {
    authorityBlockingReasons.push(COORDINATE_GATE_REASON.KML_NOT_READY);
  } else if (candidate.kmlReady !== true && !geometryExportableForKml) {
    if (diagnosticReasons.length > 0) kmlBlockingReasons.push(COORDINATE_GATE_REASON.KML_NOT_READY);
    else authorityBlockingReasons.push(COORDINATE_GATE_REASON.KML_NOT_READY);
  }
  if (!geometryExportableForKml) {
    if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.PENDING) {
      kmlBlockingReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED);
    }
    if (candidate.requiresReview !== false && !confirmationAccepted) {
      kmlBlockingReasons.push(COORDINATE_GATE_REASON.REVIEW_REQUIRED);
    }
    if (candidate.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED && !confirmationAccepted) {
      kmlBlockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED);
    }
  }

  const authorityBlockingReasonCodes = unique(authorityBlockingReasons);
  const kmlBlockingReasonCodes = unique([...authorityBlockingReasonCodes, ...kmlBlockingReasons]);
  const reasonCodes = unique([...diagnosticReasons, ...authorityBlockingReasonCodes]);
  const decisionState = authorityBlockingReasonCodes.length > 0
    ? COORDINATE_DECISION_STATE.BLOCKED
    : reasonCodes.length > 0
      ? COORDINATE_DECISION_STATE.REVIEW_REQUIRED
      : COORDINATE_DECISION_STATE.AUTO_EXPORT;

  return Object.freeze({
    decisionState,
    // KML eligibility follows current validated geometry + identity, not review/warning state.
    kmlReady: kmlBlockingReasonCodes.length === 0,
    reasonCodes: Object.freeze(reasonCodes),
    blockingReasons: Object.freeze(kmlBlockingReasonCodes.map(code => Object.freeze({ code })))
  });
}
