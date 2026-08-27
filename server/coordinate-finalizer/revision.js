import { randomUUID } from "node:crypto";
import { COORDINATE_CONFIRMATION_STATUS } from "./reason-codes.js";

export function createCoordinateRevision({
  resultId = randomUUID(),
  resultRevision = 1,
  confirmationRequired = false,
  confirmationStatus
} = {}) {
  if (!Number.isSafeInteger(resultRevision) || resultRevision < 1) throw new TypeError("result_revision_invalid");
  return Object.freeze({
    resultId,
    resultRevision,
    confirmationRequired: Boolean(confirmationRequired),
    confirmationStatus: confirmationStatus || (confirmationRequired
      ? COORDINATE_CONFIRMATION_STATUS.PENDING
      : COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED),
    confirmedRevision: confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED ? resultRevision : null
  });
}

export function acceptCoordinateRevision(state) {
  return createCoordinateRevision({
    ...state,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED
  });
}

export function rejectCoordinateRevision(state) {
  return Object.freeze({
    ...state,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED,
    confirmedRevision: null
  });
}

export function incrementCoordinateRevision(state) {
  return createCoordinateRevision({
    resultId: state.resultId,
    resultRevision: state.resultRevision + 1,
    confirmationRequired: state.confirmationRequired,
    confirmationStatus: state.confirmationRequired
      ? COORDINATE_CONFIRMATION_STATUS.PENDING
      : COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED
  });
}
