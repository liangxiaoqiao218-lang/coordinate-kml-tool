import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  createGeometryHash,
  validateFinalizedGeometry,
  validateFinalizedCrs
} from "../../coordinate-finalizer/index.js";

export const MAP_PREVIEW_SCHEMA_VERSION = "map_preview_object_v1";

const BLOCK_REASON = Object.freeze({
  NO_STRUCTURED_RESULT: "NO_STRUCTURED_RESULT",
  NO_VALID_COORDINATES: "NO_VALID_COORDINATES",
  NO_DRAWABLE_GEOMETRY: "NO_DRAWABLE_GEOMETRY",
  GEOMETRY_CONSTRUCTION_FAILED: "GEOMETRY_CONSTRUCTION_FAILED",
  FAMILY_UNAVAILABLE_WITHOUT_RESULT: "FAMILY_UNAVAILABLE_WITHOUT_RESULT",
  SOURCE_IDENTITY_INVALID: "SOURCE_IDENTITY_INVALID",
  STALE_SOURCE_REVISION: "STALE_SOURCE_REVISION",
  GEOMETRY_HASH_MISMATCH: "GEOMETRY_HASH_MISMATCH",
  CRS_NOT_DRAWABLE_AS_WGS84: "CRS_NOT_DRAWABLE_AS_WGS84"
});

function uniqueStrings(values) {
  return [...new Set(values.map(value => String(value || "").trim()).filter(Boolean))];
}

function freezeDeep(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(freezeDeep);
  return Object.freeze(value);
}

function sourceIdentity(input = {}) {
  return Object.freeze({
    sourceResultId: input.resultId || null,
    sourceRevision: Number.isSafeInteger(input.resultRevision) ? input.resultRevision : null,
    sourceGeometryHash: input.geometryHash || null
  });
}

function blocked(input, reasonCode, clock) {
  return freezeDeep({
    schemaVersion: MAP_PREVIEW_SCHEMA_VERSION,
    ...sourceIdentity(input),
    geometry: null,
    geometryType: null,
    crs: null,
    axisOrder: null,
    previewEligibility: { allowed: false, warning: false },
    previewReasonCodes: [reasonCode],
    previewWarnings: [],
    createdAt: clock()
  });
}

function identitiesMatch(input, expectedIdentity) {
  if (!expectedIdentity) return { ok: true };
  if (expectedIdentity.resultId !== input.resultId
    || expectedIdentity.resultRevision !== input.resultRevision) {
    return { ok: false, reasonCode: BLOCK_REASON.STALE_SOURCE_REVISION };
  }
  if (expectedIdentity.geometryHash !== input.geometryHash) {
    return { ok: false, reasonCode: BLOCK_REASON.GEOMETRY_HASH_MISMATCH };
  }
  return { ok: true };
}

export class MapPreviewAdapter {
  adapt(input, { expectedIdentity = null, clock = () => new Date().toISOString() } = {}) {
    if (!input || typeof input !== "object" || Array.isArray(input)
      || input.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION) {
      return blocked(input || {}, BLOCK_REASON.NO_STRUCTURED_RESULT, clock);
    }
    if (!input.geometry) {
      const unavailable = input.availabilityStatus && input.availabilityStatus !== "AVAILABLE";
      return blocked(input, unavailable
        ? BLOCK_REASON.FAMILY_UNAVAILABLE_WITHOUT_RESULT
        : BLOCK_REASON.NO_DRAWABLE_GEOMETRY, clock);
    }
    if (!input.resultId || !Number.isSafeInteger(input.resultRevision) || !input.geometryHash) {
      return blocked(input, BLOCK_REASON.SOURCE_IDENTITY_INVALID, clock);
    }

    const identityMatch = identitiesMatch(input, expectedIdentity);
    if (!identityMatch.ok) return blocked(input, identityMatch.reasonCode, clock);

    const crsResult = validateFinalizedCrs(input.crs);
    if (!crsResult.ok) return blocked(input, BLOCK_REASON.CRS_NOT_DRAWABLE_AS_WGS84, clock);

    const geometryResult = validateFinalizedGeometry(input.geometry);
    if (!geometryResult.ok) return blocked(input, BLOCK_REASON.GEOMETRY_CONSTRUCTION_FAILED, clock);

    if (createGeometryHash(geometryResult.geometry) !== input.geometryHash) {
      return blocked(input, BLOCK_REASON.GEOMETRY_HASH_MISMATCH, clock);
    }

    const previewWarnings = uniqueStrings([
      ...(Array.isArray(input.warnings) ? input.warnings : []),
      ...(Array.isArray(input.blockingReasons) ? input.blockingReasons : []),
      ...(input.requiresReview ? ["REVIEW_REQUIRED"] : []),
      ...(input.confirmationStatus === "pending" ? ["CONFIRMATION_PENDING"] : []),
      ...(input.kmlReady === false ? ["KML_BLOCKED"] : [])
    ]);

    return freezeDeep({
      schemaVersion: MAP_PREVIEW_SCHEMA_VERSION,
      ...sourceIdentity(input),
      geometry: structuredClone(geometryResult.geometry),
      geometryType: geometryResult.geometry.type,
      crs: { ...FINALIZED_COORDINATE_CRS },
      axisOrder: FINALIZED_COORDINATE_CRS.axisOrder,
      previewEligibility: {
        allowed: true,
        warning: previewWarnings.length > 0
      },
      previewReasonCodes: [],
      previewWarnings,
      createdAt: clock()
    });
  }
}

export { BLOCK_REASON as MAP_PREVIEW_BLOCK_REASON };
