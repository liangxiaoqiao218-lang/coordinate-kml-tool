import {
  COORDINATE_DECISION_STATE,
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  validateFinalizedGeometry
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
    if (input.decisionState !== COORDINATE_DECISION_STATE.AUTO_EXPORT) {
      return failure(input.reasonCodes?.[0] || "GATE_NOT_PASSED");
    }
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
        warnings: input.warnings,
        createdAt: input.createdAt
      })
    });
  }
}
