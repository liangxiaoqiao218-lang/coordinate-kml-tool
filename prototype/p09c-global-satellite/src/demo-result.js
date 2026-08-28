import { createCanonicalGeometryHash } from "./canonical-preview-adapter.js";
import { FINALIZED_SCHEMA_VERSION } from "./constants.js";

export async function createDemoResult(geometry) {
  return Object.freeze({
    schemaVersion: FINALIZED_SCHEMA_VERSION,
    resultId: "p09c-isolated-demo",
    resultRevision: 1,
    geometryHash: await createCanonicalGeometryHash(geometry),
    sourceAuthority: "legacy",
    crs: Object.freeze({ id: "EPSG:4326", axisOrder: "longitude_latitude" }),
    geometry: structuredClone(geometry),
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    decisionState: "REVIEW_REQUIRED",
    technicalKmlReady: true,
    requiresReview: true,
    kmlReady: false,
    warnings: Object.freeze(["ISOLATED_PROTOTYPE_DEMO"])
  });
}

export const DEMO_POLYGON = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([116.381, 39.901]),
    Object.freeze([116.407, 39.901]),
    Object.freeze([116.407, 39.919]),
    Object.freeze([116.381, 39.919]),
    Object.freeze([116.381, 39.901])
  ])])
});
