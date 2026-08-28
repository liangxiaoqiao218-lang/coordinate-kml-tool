import { createCanonicalGeometryHash } from "../src/canonical-preview-adapter.js";
import { FINALIZED_SCHEMA_VERSION } from "../src/constants.js";

export async function finalized(geometry, overrides = {}) {
  return {
    schemaVersion: FINALIZED_SCHEMA_VERSION,
    resultId: "p09c-test-result",
    resultRevision: 1,
    geometryHash: await createCanonicalGeometryHash(geometry),
    crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
    geometry: structuredClone(geometry),
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    decisionState: "AUTO_EXPORT",
    technicalKmlReady: true,
    requiresReview: false,
    kmlReady: true,
    warnings: [],
    ...overrides
  };
}

export const geometries = Object.freeze({
  Point: Object.freeze({ type: "Point", coordinates: Object.freeze([100.5, 14.5]) }),
  LineString: Object.freeze({ type: "LineString", coordinates: Object.freeze([
    Object.freeze([100.5, 14.5]), Object.freeze([100.6, 14.6])
  ]) }),
  Polygon: Object.freeze({ type: "Polygon", coordinates: Object.freeze([Object.freeze([
    Object.freeze([100.5, 14.5]), Object.freeze([100.6, 14.5]), Object.freeze([100.6, 14.6]), Object.freeze([100.5, 14.5])
  ])]) }),
  MultiPolygon: Object.freeze({ type: "MultiPolygon", coordinates: Object.freeze([
    Object.freeze([Object.freeze([
      Object.freeze([100.5, 14.5]), Object.freeze([100.6, 14.5]), Object.freeze([100.6, 14.6]), Object.freeze([100.5, 14.5])
    ])]),
    Object.freeze([Object.freeze([
      Object.freeze([100.7, 14.7]), Object.freeze([100.8, 14.7]), Object.freeze([100.8, 14.8]), Object.freeze([100.7, 14.7])
    ])])
  ]) })
});

export function polygonWithVertices(vertexCount) {
  const ring = Array.from({ length: vertexCount }, (_, index) => {
    const angle = (Math.PI * 2 * index) / vertexCount;
    return [100 + Math.cos(angle) * 0.2, 15 + Math.sin(angle) * 0.2];
  });
  ring.push([...ring[0]]);
  return { type: "Polygon", coordinates: [ring] };
}
