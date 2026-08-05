import { getDmsPointCoordinates } from "./dms-utils.js";

const EARTH_RADIUS_METERS = 6371008.8;

function toRadians(value) {
  return Number(value) * Math.PI / 180;
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
}

function getPointCoordinates(point = {}) {
  const lat = hasNumericValue(point.lat) ? Number(point.lat) : NaN;
  const lon = hasNumericValue(point.lon) ? Number(point.lon) : NaN;
  if (Number.isFinite(lat) && Number.isFinite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
    return { lat, lon };
  }
  return getDmsPointCoordinates(point.raw || "");
}

function haversineDistance(a, b) {
  const lat1 = toRadians(a.lat);
  const lat2 = toRadians(b.lat);
  const deltaLat = lat2 - lat1;
  const deltaLon = toRadians(b.lon - a.lon);
  const value = Math.sin(deltaLat / 2) ** 2
    + Math.cos(lat1) * Math.cos(lat2) * Math.sin(deltaLon / 2) ** 2;
  return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(value)));
}

function makeWarning(code, severity, message, details = {}) {
  return { code, severity, message, auto_correct: false, ...details };
}

function mapEngineWarning(warning, groupId) {
  const message = String(warning || "").trim();
  if (!message) return null;
  if (/自交|self.?intersect/i.test(message)) {
    return makeWarning("ENGINE_SELF_INTERSECTION", "high", message, {
      group_id: groupId,
      source: "coordinate_engine_v2"
    });
  }
  if (/面积|area|边长|edge/i.test(message)) {
    return makeWarning("ENGINE_GEOMETRY_WARNING", "medium", message, {
      group_id: groupId,
      source: "coordinate_engine_v2"
    });
  }
  return null;
}

function consumeCoordinateEngineGeometry(group, groupId) {
  const warnings = [];
  const validation = group.validation || {};
  const selectedCandidate = (Array.isArray(validation.candidates) ? validation.candidates : [])
    .find(candidate => candidate.interpretation === validation.selected_interpretation)
    || (Array.isArray(validation.candidates) ? validation.candidates[0] : null);

  if (selectedCandidate?.self_intersecting) {
    warnings.push(makeWarning("ENGINE_SELF_INTERSECTION", "high", "Coordinate Engine V2 detected polygon self-intersection.", {
      group_id: groupId,
      source: "coordinate_engine_v2"
    }));
  }

  [
    ...(Array.isArray(selectedCandidate?.warnings) ? selectedCandidate.warnings : []),
    ...(Array.isArray(group.warnings) ? group.warnings : [])
  ].forEach(warning => {
    const mapped = mapEngineWarning(warning, groupId);
    if (mapped) warnings.push(mapped);
  });

  if (validation.order_status === "ambiguous") {
    warnings.push(makeWarning("LAT_LON_SWAP_RISK", "medium", "经纬度顺序存在歧义。", {
      group_id: groupId,
      score_margin: validation.score_margin ?? null,
      source: "verification_specific"
    }));
  }

  return warnings;
}

function detectAbnormalJumps(group, groupId) {
  const rawPoints = Array.isArray(group.points) ? group.points : [];
  const points = rawPoints.map((point, pointIndex) => ({
    ...getPointCoordinates(point),
    point_id: String(point.label || pointIndex + 1)
  }));
  if (points.length < 3 || points.some(point => !Number.isFinite(point.lat) || !Number.isFinite(point.lon))) {
    return [];
  }

  const isPolygon = String(group.geometry || "").toLowerCase() === "polygon";
  const edgeCount = isPolygon ? points.length : points.length - 1;
  const edges = [];
  for (let index = 0; index < edgeCount; index += 1) {
    edges.push(haversineDistance(points[index], points[(index + 1) % points.length]));
  }
  const positiveEdges = edges.filter(value => value > 0).sort((a, b) => a - b);
  const typicalShortEdge = positiveEdges.length
    ? positiveEdges[Math.floor((positiveEdges.length - 1) * 0.25)]
    : 0;
  const indexes = isPolygon
    ? points.map((_, index) => index)
    : points.map((_, index) => index).slice(1, -1);

  return indexes.flatMap(index => {
    const point = points[index];
    const previous = points[(index - 1 + points.length) % points.length];
    const next = points[(index + 1) % points.length];
    const incidentDistance = haversineDistance(previous, point) + haversineDistance(point, next);
    const neighborChord = Math.max(1, haversineDistance(previous, next));
    const spikeRatio = incidentDistance / neighborChord;
    if (spikeRatio < 8 || incidentDistance < Math.max(5000, typicalShortEdge * 4)) {
      return [];
    }
    return [makeWarning("ABNORMAL_JUMP", "high", "坐标点与相邻点形成明显异常跳跃。", {
      group_id: groupId,
      point_id: point.point_id,
      spike_ratio: Number(spikeRatio.toFixed(2)),
      incident_distance_m: Number(incidentDistance.toFixed(2)),
      source: "verification_specific"
    })];
  });
}

export function validateCoordinateGeometry({ coordinateEngineV2 = {} } = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  const warnings = groups.flatMap((group, groupIndex) => {
    const groupId = group.group_id || `group_${groupIndex + 1}`;
    return [
      ...consumeCoordinateEngineGeometry(group, groupId),
      ...detectAbnormalJumps(group, groupId)
    ];
  });

  const unique = new Map();
  warnings.forEach(warning => {
    const key = `${warning.code}|${warning.group_id || ""}|${warning.point_id || ""}`;
    if (!unique.has(key)) unique.set(key, warning);
  });
  return Array.from(unique.values());
}

