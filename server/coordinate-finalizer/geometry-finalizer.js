import { COORDINATE_GATE_REASON, FINALIZED_COORDINATE_CRS } from "./reason-codes.js";
import { finiteNumberOrNull } from "../coordinate-values.js";

const SUPPORTED_TYPES = new Set(["Point", "LineString", "Polygon", "MultiPolygon"]);

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

export function validateFinalizedGeometry(geometry) {
  if (!geometry || typeof geometry !== "object" || !SUPPORTED_TYPES.has(geometry.type)) {
    return { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
  }
  const coordinates = geometry.coordinates;
  const valid = geometry.type === "Point"
    ? finitePosition(coordinates)
    : geometry.type === "LineString"
      ? validateLine(coordinates, 2)
      : geometry.type === "Polygon"
        ? Array.isArray(coordinates) && coordinates.length > 0 && coordinates.every(validateRing)
        : Array.isArray(coordinates) && coordinates.length > 0
          && coordinates.every(polygon => Array.isArray(polygon) && polygon.length > 0 && polygon.every(validateRing));
  return valid
    ? { ok: true, geometry: structuredClone(geometry) }
    : { ok: false, reasonCode: COORDINATE_GATE_REASON.GEOMETRY_INVALID };
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
