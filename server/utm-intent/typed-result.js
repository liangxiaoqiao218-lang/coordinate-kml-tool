import { transformUtmWgs84Points } from "./utm-wgs84-transform.js";

function expectedWgs84UtmEpsg(zone, hemisphere) {
  const base = hemisphere === "north" ? 32600 : hemisphere === "south" ? 32700 : null;
  return base && Number.isInteger(zone) && zone >= 1 && zone <= 60 ? `EPSG:${base + zone}` : null;
}

function normalizeProjectedPoint(value, index) {
  if (Array.isArray(value) && value.length >= 2) {
    return { index, easting: Number(value[0]), northing: Number(value[1]) };
  }
  if (value && typeof value === "object") {
    return {
      index,
      easting: Number(value.easting ?? value.x ?? value.X),
      northing: Number(value.northing ?? value.y ?? value.Y)
    };
  }
  const parts = String(value || "").trim().split(/[;,|\s]+/).filter(Boolean);
  return { index, easting: Number(parts[0]), northing: Number(parts[1]) };
}

function normalizeProjectedPoints(values = []) {
  if (!Array.isArray(values)) throw new TypeError("projectedCoordinates must be an array");
  return values.map(normalizeProjectedPoint).map(point => {
    if (!Number.isFinite(point.easting) || !Number.isFinite(point.northing)) {
      throw new TypeError(`Invalid projected coordinate at index ${point.index}`);
    }
    return point;
  });
}

export function buildShadowTypedUtmResult({ shadowIntent, projectedCoordinates = [] } = {}) {
  if (!shadowIntent || shadowIntent.confidence !== "confirmed" || shadowIntent.projection !== "utm") {
    return null;
  }
  if (shadowIntent.datum !== "WGS84" || !Number.isInteger(shadowIntent.zone)) return null;
  if (shadowIntent.hemisphere !== "north" && shadowIntent.hemisphere !== "south") return null;
  if (Array.isArray(shadowIntent.conflicts) && shadowIntent.conflicts.length > 0) return null;

  const expectedEpsg = expectedWgs84UtmEpsg(shadowIntent.zone, shadowIntent.hemisphere);
  if (!expectedEpsg || shadowIntent.epsg !== expectedEpsg) return null;

  const typedUtmIntent = Object.freeze({
    coordinateType: "utm_projected_xy",
    projection: "utm",
    datum: "WGS84",
    zone: shadowIntent.zone,
    hemisphere: shadowIntent.hemisphere,
    epsg: expectedEpsg,
    source: "shadow"
  });
  const normalizedPoints = normalizeProjectedPoints(projectedCoordinates);
  const transformedWgs84 = transformUtmWgs84Points(normalizedPoints, typedUtmIntent);

  return {
    typedUtmIntent,
    projectedCoordinates: normalizedPoints,
    transformedWgs84
  };
}
