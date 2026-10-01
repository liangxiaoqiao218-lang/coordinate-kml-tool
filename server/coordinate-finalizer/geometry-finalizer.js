import { COORDINATE_GATE_REASON, FINALIZED_COORDINATE_CRS } from "./reason-codes.js";
import { finiteNumberOrNull } from "../coordinate-values.js";

const SUPPORTED_TYPES = new Set(["Point", "MultiPoint", "LineString", "Polygon", "MultiPolygon"]);

function finitePosition(value) {
  return Array.isArray(value)
    && value.length === 2
    && Number.isFinite(value[0])
    && value[0] >= -180
    && value[0] <= 180
    && Number.isFinite(value[1])
    && value[1] >= -90
    && value[1] <= 90;
}

function positionsEqual(left, right) {
  return Array.isArray(left) && Array.isArray(right)
    && left[0] === right[0] && left[1] === right[1];
}

function validateLine(coordinates, minimum) {
  return Array.isArray(coordinates)
    && coordinates.length >= minimum
    && coordinates.every(finitePosition);
}

function validateRing(ring) {
  return validateLine(ring, 4)
    && positionsEqual(ring[0], ring[ring.length - 1])
    && new Set(ring.slice(0, -1).map(position => `${position[0]}:${position[1]}`)).size >= 3;
}

function orientation(a, b, c) {
  return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
}

function orientationSign(value, tolerance = 1e-12) {
  if (Math.abs(value) <= tolerance) return 0;
  return value > 0 ? 1 : -1;
}

function pointOnSegment(a, b, point, tolerance = 1e-12) {
  return point[0] >= Math.min(a[0], b[0]) - tolerance
    && point[0] <= Math.max(a[0], b[0]) + tolerance
    && point[1] >= Math.min(a[1], b[1]) - tolerance
    && point[1] <= Math.max(a[1], b[1]) + tolerance;
}

function segmentsIntersect(a, b, c, d) {
  const o1 = orientation(a, b, c);
  const o2 = orientation(a, b, d);
  const o3 = orientation(c, d, a);
  const o4 = orientation(c, d, b);
  const s1 = orientationSign(o1);
  const s2 = orientationSign(o2);
  const s3 = orientationSign(o3);
  const s4 = orientationSign(o4);
  if (s1 * s2 < 0 && s3 * s4 < 0) return true;
  if (s1 === 0 && pointOnSegment(a, b, c)) return true;
  if (s2 === 0 && pointOnSegment(a, b, d)) return true;
  if (s3 === 0 && pointOnSegment(c, d, a)) return true;
  if (s4 === 0 && pointOnSegment(c, d, b)) return true;
  return false;
}

export function ringHasSelfIntersection(ring = []) {
  if (!validateRing(ring)) return false;
  const points = positionsEqual(ring[0], ring[ring.length - 1]) ? ring.slice(0, -1) : ring;
  if (points.length < 4) return false;
  for (let first = 0; first < points.length; first += 1) {
    const a = points[first];
    const b = points[(first + 1) % points.length];
    for (let second = first + 1; second < points.length; second += 1) {
      if (Math.abs(first - second) <= 1 || (first === 0 && second === points.length - 1)) continue;
      const c = points[second];
      const d = points[(second + 1) % points.length];
      if (segmentsIntersect(a, b, c, d)) return true;
    }
  }
  return false;
}

export function geometryHasSelfIntersection(geometry) {
  if (geometry?.type === "Polygon") {
    return Array.isArray(geometry.coordinates) && geometry.coordinates.some(ringHasSelfIntersection);
  }
  if (geometry?.type === "MultiPolygon") {
    return Array.isArray(geometry.coordinates)
      && geometry.coordinates.some(polygon => Array.isArray(polygon) && polygon.some(ringHasSelfIntersection));
  }
  return false;
}

export function validateFinalizedGeometry(geometry) {
  if (!geometry || typeof geometry !== "object" || !SUPPORTED_TYPES.has(geometry.type)) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
  }
  const coordinates = geometry.coordinates;
  const valid = geometry.type === "Point"
    ? finitePosition(coordinates)
    : geometry.type === "MultiPoint"
      ? validateLine(coordinates, 2)
    : geometry.type === "LineString"
      ? validateLine(coordinates, 2)
      : geometry.type === "Polygon"
        ? Array.isArray(coordinates) && coordinates.length > 0 && coordinates.every(validateRing)
        : Array.isArray(coordinates) && coordinates.length > 0
          && coordinates.every(polygon => Array.isArray(polygon) && polygon.length > 0 && polygon.every(validateRing));
  if (!valid) return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
  if (geometryHasSelfIntersection(geometry)) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_SELF_INTERSECTION };
  }
  return { ok: true, geometry: structuredClone(geometry) };
}

function pointFromStructuredPoint(point) {
  const longitude = finiteNumberOrNull(point?.lon);
  const latitude = finiteNumberOrNull(point?.lat);
  if (longitude === null || latitude === null) return null;
  return finitePosition([longitude, latitude]) ? [longitude, latitude] : null;
}

function closeRing(positions) {
  if (positions.length === 0 || positionsEqual(positions[0], positions[positions.length - 1])) return positions;
  return [...positions, [...positions[0]]];
}

export function geometryFromStructuredGroups(groups) {
  if (!Array.isArray(groups) || groups.length === 0) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.STRUCTURED_GEOMETRY_MISSING };
  }

  const geometries = [];
  for (const group of groups) {
    const positions = Array.isArray(group?.points) ? group.points.map(pointFromStructuredPoint) : [];
    if (positions.length === 0 || positions.some(position => !position)) {
      return { ok: false, reasonCode: COORDINATE_GATE_REASON.CRS_NOT_FINALIZED };
    }
    if (group.geometry === "point" && positions.length === 1) {
      geometries.push({ type: "Point", coordinates: positions[0] });
    } else if (group.geometry === "line" && positions.length >= 2) {
      geometries.push({ type: "LineString", coordinates: positions });
    } else if (group.geometry === "polygon" && positions.length >= 3) {
      geometries.push({ type: "Polygon", coordinates: [closeRing(positions)] });
    } else {
      return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
    }
  }

  let geometry;
  if (geometries.length === 1) {
    geometry = geometries[0];
  } else if (geometries.every(candidate => candidate.type === "Polygon")) {
    geometry = { type: "MultiPolygon", coordinates: geometries.map(candidate => candidate.coordinates) };
  } else {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
  }
  return validateFinalizedGeometry(geometry);
}

export function validateFinalizedCrs(crs) {
  if (crs?.id !== FINALIZED_COORDINATE_CRS.id) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.CRS_NOT_FINALIZED };
  }
  if (crs?.axisOrder !== FINALIZED_COORDINATE_CRS.axisOrder) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.AXIS_ORDER_INVALID };
  }
  return { ok: true, crs: FINALIZED_COORDINATE_CRS };
}
