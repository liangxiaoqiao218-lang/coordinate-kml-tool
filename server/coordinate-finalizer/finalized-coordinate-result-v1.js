import { randomUUID } from "node:crypto";
import { createGeometryHash } from "./geometry-hash.js";
import { evaluateCoordinateReleaseGate } from "./unified-gate.js";
import {
  COORDINATE_DECISION_STATE,
  FINALIZED_COORDINATE_SCHEMA_VERSION
} from "./reason-codes.js";
import {
  FAMILY_AVAILABILITY_STATUS,
  isFamilyAvailabilityBlocked
} from "./family-availability-policy.js";

function uniqueStrings(values) {
  return Object.freeze([...new Set((Array.isArray(values) ? values : []).map(value => String(value || "").trim()).filter(Boolean))]);
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

export function finalizeCoordinateResult(candidate = {}, { clock = () => new Date().toISOString() } = {}) {
  const now = clock();
  const resultId = candidate.resultId || randomUUID();
  const resultRevision = candidate.resultRevision ?? 1;
  const availabilityStatus = candidate.availabilityStatus || FAMILY_AVAILABILITY_STATUS.AVAILABLE;
  const availabilityBlocked = isFamilyAvailabilityBlocked({ status: availabilityStatus });
  const effectiveCandidate = {
    ...candidate,
    availabilityStatus,
    requiresReview: availabilityBlocked ? false : candidate.requiresReview,
    kmlReady: availabilityBlocked ? false : candidate.kmlReady,
    resultId,
    resultRevision
  };
  const gate = evaluateCoordinateReleaseGate(effectiveCandidate);
  const geometry = candidate.geometry ? structuredClone(candidate.geometry) : null;
  const geometryHash = geometry ? createGeometryHash(geometry) : null;
  const result = {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId,
    resultRevision,
    geometryHash,
    sourceAuthority: candidate.sourceAuthority || null,
    coordinateType: candidate.coordinateType || null,
    precisionMode: candidate.precisionMode || null,
    family: candidate.family || candidate.coordinateType || null,
    availabilityStatus,
    availabilityReasonCode: candidate.availabilityReasonCode || null,
    familyAvailabilityPolicy: candidate.familyAvailabilityPolicy
      ? deepFreeze(structuredClone(candidate.familyAvailabilityPolicy))
      : null,
    crs: candidate.crs ? Object.freeze({ ...candidate.crs }) : null,
    geometry: geometry ? Object.freeze(geometry) : null,
    confirmationStatus: candidate.confirmationStatus || null,
    qualityGateStatus: candidate.qualityGateStatus || null,
    decisionState: gate.decisionState,
    requiresReview: effectiveCandidate.requiresReview !== false,
    kmlReady: effectiveCandidate.kmlReady === true,
    reasonCodes: gate.reasonCodes,
    blockingReasons: gate.blockingReasons,
    warnings: uniqueStrings(candidate.warnings),
    limitations: uniqueStrings(candidate.limitations),
    groups: Object.freeze((Array.isArray(candidate.groups) ? candidate.groups : []).map(group => Object.freeze({ ...group }))),
    familySafetyPolicy: candidate.familySafetyPolicy ? deepFreeze(structuredClone(candidate.familySafetyPolicy)) : null,
    createdAt: candidate.createdAt || now,
    finalizedAt: now
  };
  result.gate = Object.freeze({
    decisionState: result.decisionState,
    qualityGateStatus: result.qualityGateStatus,
    confirmationStatus: result.confirmationStatus,
    availabilityStatus: result.availabilityStatus,
    availabilityReasonCode: result.availabilityReasonCode
  });
  return Object.freeze(result);
}

export function consumeFinalizedGeometry(finalizedResult, consumer) {
  if (finalizedResult?.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION
    || finalizedResult?.decisionState !== COORDINATE_DECISION_STATE.AUTO_EXPORT) {
    return Object.freeze({ consumed: false, reasonCodes: finalizedResult?.reasonCodes || [] });
  }
  return Object.freeze({ consumed: true, value: consumer(finalizedResult.geometry, finalizedResult) });
}
