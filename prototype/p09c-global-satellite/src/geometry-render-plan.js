import { DEFAULT_GEOMETRY_STYLE, GEOMETRY_SOURCE_ID } from "./constants.js";
import { geometryIsDrawable } from "./canonical-preview-adapter.js";

function flattenPositions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat(1);
  return geometry.coordinates.flat(2);
}

export function geometryBounds(geometry) {
  if (!geometryIsDrawable(geometry)) throw new Error("GEOMETRY_NOT_DRAWABLE");
  const positions = flattenPositions(geometry);
  return positions.reduce((bounds, [longitude, latitude]) => ({
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
      id: "p09c-point",
      type: "circle",
      source: GEOMETRY_SOURCE_ID,
      paint: { "circle-radius": 7, "circle-color": style.fill, "circle-stroke-color": style.stroke, "circle-stroke-width": 2 }
    });
  } else if (geometry.type === "LineString") {
    layers.push({
      id: "p09c-line",
      type: "line",
      source: GEOMETRY_SOURCE_ID,
      paint: { "line-color": style.stroke, "line-width": 3 }
    });
  } else {
    layers.push({
      id: "p09c-polygon-fill",
      type: "fill",
      source: GEOMETRY_SOURCE_ID,
      paint: { "fill-color": style.fill, "fill-opacity": style.fillOpacity }
    });
    layers.push({
      id: "p09c-polygon-outline",
      type: "line",
      source: GEOMETRY_SOURCE_ID,
      paint: { "line-color": style.stroke, "line-width": 3 }
    });
  }
  return Object.freeze({
    sourceId: GEOMETRY_SOURCE_ID,
    source: Object.freeze({ type: "geojson", data: Object.freeze({ type: "Feature", properties: Object.freeze({}), geometry: structuredClone(geometry) }) }),
    layers: Object.freeze(layers.map(Object.freeze)),
    bounds: Object.freeze(geometryBounds(geometry))
  });
}
