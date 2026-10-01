import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  createGeometryHash,
  validateFinalizedCrs,
  validateFinalizedGeometry
} from "../coordinate-finalizer/index.js";

export const RECOGNITION_OUTPUT_CAPABILITY_VERSION = "recognition_output_capability_v1";

const REASON = Object.freeze({
  RESULT_MISSING: "RESULT_MISSING",
  RESULT_SCHEMA_INVALID: "RESULT_SCHEMA_INVALID",
  RESULT_IDENTITY_MISSING: "RESULT_IDENTITY_MISSING",
  RESULT_REVISION_STALE: "RESULT_REVISION_STALE",
  CRS_NOT_WGS84: "CRS_NOT_WGS84",
  GEOMETRY_INVALID: "GEOMETRY_INVALID",
  GEOMETRY_SELF_INTERSECTION: "GEOMETRY_SELF_INTERSECTION",
  GEOMETRY_HASH_MISMATCH: "GEOMETRY_HASH_MISMATCH"
});

function uniqueStrings(values = []) {
  return Object.freeze([...new Set(values.map(value => String(value || "").trim()).filter(Boolean))]);
}

/**
 * Decide whether the current result can be inspected as a map and serialized
 * as an unverified KML. This is intentionally independent from accuracy,
 * review, confirmation, source-authority and formal-export decisions.
 */
export function evaluateRecognitionOutputCapability(result = null, { formalAuthorized = null } = {}) {
  const blockers = [];
  if (!result || typeof result !== "object" || Array.isArray(result)) {
    blockers.push(REASON.RESULT_MISSING);
  } else {
    if (result.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION) {
      blockers.push(REASON.RESULT_SCHEMA_INVALID);
    }
    if (!String(result.resultId || "").trim()
      || !Number.isSafeInteger(result.resultRevision)
      || result.resultRevision < 1
      || !String(result.geometryHash || "").trim()) {
      blockers.push(REASON.RESULT_IDENTITY_MISSING);
    }
    if (result.currentRevision != null
      && Number(result.currentRevision) !== Number(result.resultRevision)) {
      blockers.push(REASON.RESULT_REVISION_STALE);
    }
    if (!validateFinalizedCrs(result.crs).ok) blockers.push(REASON.CRS_NOT_WGS84);
    const geometry = validateFinalizedGeometry(result.geometry);
    if (!geometry.ok) {
      blockers.push(geometry.reasonCode === "GEOMETRY_SELF_INTERSECTION"
        ? REASON.GEOMETRY_SELF_INTERSECTION
        : REASON.GEOMETRY_INVALID);
    } else if (String(result.geometryHash || "") !== createGeometryHash(geometry.geometry)) {
      blockers.push(REASON.GEOMETRY_HASH_MISMATCH);
    }
  }

  const blockReasons = uniqueStrings(blockers);
  const technicallyGeneratable = blockReasons.length === 0;
  const warningReasons = uniqueStrings([
    ...(Array.isArray(result?.reasonCodes) ? result.reasonCodes : []),
    ...(Array.isArray(result?.warnings) ? result.warnings : []),
    ...(Array.isArray(result?.limitations) ? result.limitations : []),
    ...(result?.requiresReview === true ? ["REVIEW_REQUIRED"] : []),
    ...(result?.confirmationStatus === "pending" ? ["CONFIRMATION_PENDING"] : [])
  ]);

  return Object.freeze({
    schemaVersion: RECOGNITION_OUTPUT_CAPABILITY_VERSION,
    technicallyGeneratable,
    mapReady: technicallyGeneratable,
    kmlReady: technicallyGeneratable,
    unverified: formalAuthorized == null
      ? result?.decisionState !== "AUTO_EXPORT" || result?.requiresReview === true
      : formalAuthorized !== true,
    targetCrs: FINALIZED_COORDINATE_CRS,
    resultId: result?.resultId || null,
    resultRevision: Number.isSafeInteger(result?.resultRevision) ? result.resultRevision : null,
    geometryHash: result?.geometryHash || null,
    blockReasons,
    warningReasons
  });
}

export { REASON as RECOGNITION_OUTPUT_CAPABILITY_REASON };
