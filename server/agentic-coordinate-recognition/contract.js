export const AGENTIC_COORDINATE_CONTRACT_VERSION = "agentic-coordinate-recognition/v1";

export const AGENTIC_RESULT_STATUS = Object.freeze({
  USABLE: "usable",
  NEEDS_REVIEW: "needs_review",
  FAILED: "failed"
});

export const AGENTIC_COORDINATE_KIND = Object.freeze({
  GEOGRAPHIC: "geographic",
  PROJECTED: "projected",
  UNKNOWN: "unknown"
});

export const AGENTIC_CRS_STATUS = Object.freeze({
  IDENTIFIED: "identified",
  NEEDS_CONFIRMATION: "needs_confirmation",
  UNKNOWN: "unknown"
});

export const AGENTIC_GEOMETRY_TYPE = Object.freeze({
  POINT: "Point",
  MULTI_POINT: "MultiPoint",
  LINE_STRING: "LineString",
  POLYGON: "Polygon",
  MULTI_POLYGON: "MultiPolygon",
  GRID: "Grid",
  UNKNOWN: "Unknown"
});

const RESULT_STATUSES = new Set(Object.values(AGENTIC_RESULT_STATUS));
const COORDINATE_KINDS = new Set(Object.values(AGENTIC_COORDINATE_KIND));
const CRS_STATUSES = new Set(Object.values(AGENTIC_CRS_STATUS));
const GEOMETRY_TYPES = new Set(Object.values(AGENTIC_GEOMETRY_TYPE));

function asNullableString(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function asRequiredString(value, fieldName) {
  const text = asNullableString(value);
  if (!text) throw new Error(`${fieldName} must be a non-empty string`);
  return text;
}

function asNullableNumber(value, fieldName) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  if (!Number.isFinite(number)) throw new Error(`${fieldName} must be a finite number or null`);
  return number;
}

function asBoolean(value, defaultValue = false) {
  return typeof value === "boolean" ? value : defaultValue;
}

function normalizePoint(point, groupIndex, pointIndex) {
  if (!point || typeof point !== "object" || Array.isArray(point)) {
    throw new Error(`groups[${groupIndex}].points[${pointIndex}] must be an object`);
  }

  const normalized = {
    label: asNullableString(point.label),
    sourceText: asRequiredString(
      point.sourceText,
      `groups[${groupIndex}].points[${pointIndex}].sourceText`
    ),
    x: asNullableNumber(point.x, `groups[${groupIndex}].points[${pointIndex}].x`),
    y: asNullableNumber(point.y, `groups[${groupIndex}].points[${pointIndex}].y`),
    latitude: asNullableNumber(
      point.latitude,
      `groups[${groupIndex}].points[${pointIndex}].latitude`
    ),
    longitude: asNullableNumber(
      point.longitude,
      `groups[${groupIndex}].points[${pointIndex}].longitude`
    ),
    needsReview: asBoolean(point.needsReview)
  };

  const hasGeographicPair = normalized.latitude !== null && normalized.longitude !== null;
  const hasProjectedPair = normalized.x !== null && normalized.y !== null;
  if (!hasGeographicPair && !hasProjectedPair) {
    throw new Error(`groups[${groupIndex}].points[${pointIndex}] has no complete coordinate pair`);
  }
  if ((normalized.latitude === null) !== (normalized.longitude === null)) {
    throw new Error(`groups[${groupIndex}].points[${pointIndex}] has an incomplete geographic pair`);
  }
  if ((normalized.x === null) !== (normalized.y === null)) {
    throw new Error(`groups[${groupIndex}].points[${pointIndex}] has an incomplete projected pair`);
  }
  if (hasGeographicPair) {
    if (normalized.latitude < -90 || normalized.latitude > 90) {
      throw new Error(`groups[${groupIndex}].points[${pointIndex}].latitude is out of range`);
    }
    if (normalized.longitude < -180 || normalized.longitude > 180) {
      throw new Error(`groups[${groupIndex}].points[${pointIndex}].longitude is out of range`);
    }
  }

  return Object.freeze(normalized);
}

function normalizeGroup(group, groupIndex) {
  if (!group || typeof group !== "object" || Array.isArray(group)) {
    throw new Error(`groups[${groupIndex}] must be an object`);
  }
  if (!Array.isArray(group.points) || group.points.length === 0) {
    throw new Error(`groups[${groupIndex}].points must contain at least one point`);
  }

  return Object.freeze({
    name: asNullableString(group.name),
    points: Object.freeze(group.points.map((point, pointIndex) => (
      normalizePoint(point, groupIndex, pointIndex)
    )))
  });
}

function normalizeWarnings(warnings) {
  if (warnings === undefined || warnings === null) return Object.freeze([]);
  if (!Array.isArray(warnings)) throw new Error("warnings must be an array");
  return Object.freeze(warnings.map((warning, index) => (
    asRequiredString(warning, `warnings[${index}]`)
  )));
}

export function normalizeAgenticCoordinateResult(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("recognition result must be an object");
  }

  const success = payload.success === true;
  const resultStatus = String(payload.resultStatus || "").trim();
  if (!RESULT_STATUSES.has(resultStatus)) {
    throw new Error("resultStatus is invalid");
  }
  if (success && resultStatus === AGENTIC_RESULT_STATUS.FAILED) {
    throw new Error("successful result cannot use failed status");
  }
  if (!success && resultStatus !== AGENTIC_RESULT_STATUS.FAILED) {
    throw new Error("unsuccessful result must use failed status");
  }

  const displayText = success
    ? asRequiredString(payload.displayText, "displayText")
    : String(payload.displayText || "");
  const coordinateSystem = payload.coordinateSystem;
  if (!coordinateSystem || typeof coordinateSystem !== "object" || Array.isArray(coordinateSystem)) {
    throw new Error("coordinateSystem must be an object");
  }

  const kind = String(coordinateSystem.kind || "").trim();
  const crsStatus = String(coordinateSystem.status || "").trim();
  if (!COORDINATE_KINDS.has(kind)) throw new Error("coordinateSystem.kind is invalid");
  if (!CRS_STATUSES.has(crsStatus)) throw new Error("coordinateSystem.status is invalid");

  const geometryType = String(payload.geometryType || "").trim();
  if (!GEOMETRY_TYPES.has(geometryType)) throw new Error("geometryType is invalid");

  const groups = payload.groups === undefined || payload.groups === null
    ? []
    : payload.groups;
  if (!Array.isArray(groups)) throw new Error("groups must be an array");
  if (success && groups.length === 0) throw new Error("successful result must contain at least one group");
  if (!success && groups.length > 0) throw new Error("failed result cannot contain coordinate groups");

  const normalizedGroups = Object.freeze(groups.map(normalizeGroup));
  const pointCount = normalizedGroups.reduce((sum, group) => sum + group.points.length, 0);
  const warnings = normalizeWarnings(payload.warnings);
  const hasReviewPoint = normalizedGroups.some(group => (
    group.points.some(point => point.needsReview)
  ));
  if (success && resultStatus === AGENTIC_RESULT_STATUS.USABLE && hasReviewPoint) {
    throw new Error("usable result cannot contain points marked for review");
  }

  return Object.freeze({
    contractVersion: AGENTIC_COORDINATE_CONTRACT_VERSION,
    success,
    resultStatus,
    displayText,
    coordinateSystem: Object.freeze({
      kind,
      name: asNullableString(coordinateSystem.name),
      epsg: asNullableString(coordinateSystem.epsg),
      status: crsStatus
    }),
    geometryType,
    groups: normalizedGroups,
    warnings,
    summary: Object.freeze({
      groupCount: normalizedGroups.length,
      pointCount
    })
  });
}

