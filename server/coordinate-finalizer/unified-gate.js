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
  const blockingReasons = [];
  if (candidate.availabilityStatus === FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER) {
    blockingReasons.push(COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER);
  } else if (candidate.availabilityStatus === FAMILY_AVAILABILITY_STATUS.TEMPORARILY_UNAVAILABLE) {
    blockingReasons.push(COORDINATE_GATE_REASON.FAMILY_TEMPORARILY_UNAVAILABLE);
  }
  const revisionValid = Number.isSafeInteger(candidate.resultRevision) && candidate.resultRevision >= 1;
  if (!revisionValid) blockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_INVALID);
  if (revisionValid && candidate.currentRevision !== undefined && candidate.currentRevision !== candidate.resultRevision) {
    blockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_STALE);
  }
  if (!FINALIZED_COORDINATE_SOURCE_AUTHORITIES.includes(candidate.sourceAuthority)) {
    blockingReasons.push(COORDINATE_GATE_REASON.SOURCE_AUTHORITY_INVALID);
  }

  if (candidate.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.FAILED) {
    blockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_FAILED);
  } else if (candidate.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED) {
    blockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED);
  } else if (candidate.qualityGateStatus !== COORDINATE_QUALITY_GATE_STATUS.PASSED) {
    blockingReasons.push(COORDINATE_GATE_REASON.QUALITY_GATE_UNKNOWN);
  }

  if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.PENDING) {
    blockingReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED);
  } else if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.REJECTED) {
    blockingReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REJECTED);
  } else if (![COORDINATE_CONFIRMATION_STATUS.ACCEPTED, COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED].includes(candidate.confirmationStatus)) {
    blockingReasons.push(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED);
  }
  if (candidate.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED
    && candidate.confirmedRevision !== candidate.resultRevision) {
    blockingReasons.push(COORDINATE_GATE_REASON.RESULT_REVISION_STALE);
  }
  if (candidate.requiresReview !== false) blockingReasons.push(COORDINATE_GATE_REASON.REVIEW_REQUIRED);
  if (candidate.kmlReady !== true) blockingReasons.push(COORDINATE_GATE_REASON.KML_NOT_READY);

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
  if (!crsResult.ok) blockingReasons.push(crsResult.reasonCode);
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
  if (!geometryResult.ok) blockingReasons.push(candidate.geometry
    ? geometryResult.reasonCode
    : candidate.geometryFailureReason || COORDINATE_GATE_REASON.STRUCTURED_GEOMETRY_MISSING);

  const reasonCodes = unique(blockingReasons);
  const reviewReasons = new Set([
    COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED,
    COORDINATE_GATE_REASON.REVIEW_REQUIRED,
    COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED
  ]);
  const decisionState = reasonCodes.length === 0
    ? COORDINATE_DECISION_STATE.AUTO_EXPORT
    : reasonCodes.every(reason => reviewReasons.has(reason))
      ? COORDINATE_DECISION_STATE.REVIEW_REQUIRED
      : COORDINATE_DECISION_STATE.BLOCKED;

  return Object.freeze({
    decisionState,
    reasonCodes: Object.freeze(reasonCodes),
    blockingReasons: Object.freeze(reasonCodes.map(code => Object.freeze({ code })))
  });
}
