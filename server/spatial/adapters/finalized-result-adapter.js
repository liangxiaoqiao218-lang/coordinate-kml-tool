import {
  COORDINATE_DECISION_STATE,
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  validateFinalizedGeometry,
  createGeometryHash,
  validateFinalizedCoordinateIdentity
} from "../../coordinate-finalizer/index.js";

function failure(reasonCode) {
  return Object.freeze({ ok: false, reasonCode, details: Object.freeze([]) });
}

export class FinalizedResultSpatialGeometryAdapter {
  adapt(input = {}) {
    if (!input || typeof input !== "object" || Array.isArray(input)
      || input.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION) {
      return failure("SOURCE_STRUCTURE_INVALID");
    }
    if (!input.resultId || !Number.isSafeInteger(input.resultRevision) || input.resultRevision < 1
      || !input.geometryHash) return failure("SOURCE_IDENTITY_INCOMPLETE");
    const identity = validateFinalizedCoordinateIdentity(input);
    if (!identity.ok) return failure(identity.code);
    // Read authority from the registered canonical result, not caller-controlled gate fields.
    const canonical = identity.result;
    if (!input.geometry || createGeometryHash(input.geometry) !== canonical.geometryHash) return failure("GEOMETRY_HASH_MISMATCH");
    if (input.sourceAuthority !== canonical.sourceAuthority || input.decisionState !== canonical.decisionState
      || input.kmlReady !== canonical.kmlReady) return failure("SOURCE_AUTHORITY_INVALID");
    if (!["legacy", "manual_input", "coordinate_engine_v2"].includes(canonical.sourceAuthority)
      || canonical.decisionState === COORDINATE_DECISION_STATE.BLOCKED || canonical.kmlReady !== true) {
      return failure(canonical.reasonCodes?.[0] || "GATE_NOT_PASSED");
    }
    input = canonical;
    if (input.crs?.id !== FINALIZED_COORDINATE_CRS.id
      || input.crs?.axisOrder !== FINALIZED_COORDINATE_CRS.axisOrder) {
      return failure("CRS_NOT_WGS84");
    }
    const geometryResult = validateFinalizedGeometry(input.geometry);
    if (!geometryResult.ok) return failure(geometryResult.reasonCode);

    return Object.freeze({
      ok: true,
      geometry: Object.freeze({
        schemaVersion: "normalized_geometry_v1",
        geometry: Object.freeze(geometryResult.geometry),
        crs: FINALIZED_COORDINATE_CRS,
        source: Object.freeze({
          engine: input.sourceAuthority,
          resultId: input.resultId,
          resultRevision: input.resultRevision,
          geometryHash: input.geometryHash,
          coordinateType: input.coordinateType,
          precisionMode: input.precisionMode
        }),
        gate: Object.freeze({
          mode: "finalized_result",
          decisionState: input.decisionState,
          qualityGateStatus: input.qualityGateStatus,
          confirmationStatus: input.confirmationStatus,
          kmlReady: input.kmlReady,
          groupsReady: Array.isArray(input.groups)
            && input.groups.length > 0
            && input.groups.every(group => group.requiresReview === false && group.kmlReady === true)
        }),
        warnings: Object.freeze([...new Set([...(input.warnings || []), ...(input.reasonCodes || [])])]),
        createdAt: input.createdAt
      })
    });
  }
}
