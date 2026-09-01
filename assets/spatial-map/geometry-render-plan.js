export const MAP_PREVIEW_SCHEMA_VERSION = "map_preview_object_v1";
export const MAP_PREVIEW_GATE = "MAP_PREVIEW_DRAWABLE_ELIGIBILITY";
export const GEOMETRY_SOURCE_ID = "p09e-canonical-geometry";

export const DEFAULT_GEOMETRY_STYLE = Object.freeze({
  stroke: "#E53935",
  fill: "#1976D2",
  fillOpacity: 0.15
});

const SUPPORTED_TYPES = new Set(["Point", "LineString", "Polygon", "MultiPolygon"]);

function positionIsValid(position) {
  return Array.isArray(position)
    && position.length === 2
    && Number.isFinite(position[0]) && position[0] >= -180 && position[0] <= 180
    && Number.isFinite(position[1]) && position[1] >= -90 && position[1] <= 90;
}

function positionsEqual(left, right) {
  return Array.isArray(left) && Array.isArray(right)
    && left[0] === right[0] && left[1] === right[1];
}

function lineIsValid(line, minimum) {
  return Array.isArray(line) && line.length >= minimum && line.every(positionIsValid);
}

function ringIsValid(ring) {
  return lineIsValid(ring, 4)
    && positionsEqual(ring[0], ring[ring.length - 1])
    && new Set(ring.slice(0, -1).map(position => `${position[0]}:${position[1]}`)).size >= 3;
}

export function geometryIsDrawable(geometry) {
  if (!geometry || typeof geometry !== "object" || !SUPPORTED_TYPES.has(geometry.type)) return false;
  if (geometry.type === "Point") return positionIsValid(geometry.coordinates);
  if (geometry.type === "LineString") return lineIsValid(geometry.coordinates, 2);
  if (geometry.type === "Polygon") {
    return Array.isArray(geometry.coordinates)
      && geometry.coordinates.length > 0
      && geometry.coordinates.every(ringIsValid);
  }
  return Array.isArray(geometry.coordinates)
    && geometry.coordinates.length > 0
    && geometry.coordinates.every(polygon => Array.isArray(polygon)
      && polygon.length > 0
      && polygon.every(ringIsValid));
}

export function validateMapPreviewObject(preview) {
  if (!preview || typeof preview !== "object" || Array.isArray(preview)
    || preview.schemaVersion !== MAP_PREVIEW_SCHEMA_VERSION) {
    return Object.freeze({ ok: false, reasonCode: "MAP_PREVIEW_OBJECT_V1_REQUIRED" });
  }
  if (!preview.sourceResultId || !Number.isSafeInteger(preview.sourceRevision)
    || !preview.sourceGeometryHash) {
    return Object.freeze({ ok: false, reasonCode: "CANONICAL_IDENTITY_INVALID" });
  }
  if (preview.crs?.id !== "EPSG:4326" || preview.axisOrder !== "longitude_latitude") {
    return Object.freeze({ ok: false, reasonCode: "CANONICAL_WGS84_CRS_REQUIRED" });
  }
  if (preview.previewEligibility?.allowed !== true) {
    return Object.freeze({ ok: false, reasonCode: "MAP_PREVIEW_NOT_ELIGIBLE" });
  }
  if (!geometryIsDrawable(preview.geometry)) {
    return Object.freeze({ ok: false, reasonCode: "GEOMETRY_NOT_DRAWABLE" });
  }
  return Object.freeze({ ok: true, reasonCode: null });
}

function flattenPositions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat(1);
  return geometry.coordinates.flat(2);
}

export function geometryBounds(geometry) {
  if (!geometryIsDrawable(geometry)) throw new Error("GEOMETRY_NOT_DRAWABLE");
  return flattenPositions(geometry).reduce((bounds, [longitude, latitude]) => ({
    west: Math.min(bounds.west, longitude),
    south: Math.min(bounds.south, latitude),
    east: Math.max(bounds.east, longitude),
    north: Math.max(bounds.north, latitude)
  }), { west: Infinity, south: Infinity, east: -Infinity, north: -Infinity });
}

export function createGeometryRenderPlan(geometry, style = DEFAULT_GEOMETRY_STYLE) {
  if (!geometryIsDrawable(geometry)) throw new Error("GEOMETRY_NOT_DRAWABLE");
  const layers = [];
  if (geometry.type === "Point") {
    layers.push({
      id: "p09e-point",
      type: "circle",
      source: GEOMETRY_SOURCE_ID,
      paint: {
        "circle-radius": 7,
        "circle-color": style.fill,
        "circle-stroke-color": style.stroke,
        "circle-stroke-width": 2
      }
    });
  } else if (geometry.type === "LineString") {
    layers.push({
      id: "p09e-line",
      type: "line",
      source: GEOMETRY_SOURCE_ID,
      paint: { "line-color": style.stroke, "line-width": 3 }
    });
  } else {
    layers.push({
      id: "p09e-polygon-fill",
      type: "fill",
      source: GEOMETRY_SOURCE_ID,
      paint: { "fill-color": style.fill, "fill-opacity": style.fillOpacity }
    });
    layers.push({
      id: "p09e-polygon-outline",
      type: "line",
      source: GEOMETRY_SOURCE_ID,
      paint: { "line-color": style.stroke, "line-width": 3 }
    });
  }
  return Object.freeze({
    sourceId: GEOMETRY_SOURCE_ID,
    source: Object.freeze({
      type: "geojson",
      data: Object.freeze({
        type: "Feature",
        properties: Object.freeze({}),
        geometry: structuredClone(geometry)
      })
    }),
    layers: Object.freeze(layers.map(layer => Object.freeze(layer))),
    bounds: Object.freeze(geometryBounds(geometry))
  });
}
