export const COORDINATE_ENGINE_V3_SCHEMA_VERSION = "coordinate_engine_v3_foundation";

export const RECOGNIZER_PORT_STATUS = Object.freeze({
  NOT_PORTED: "NOT_PORTED",
  IMPLEMENTED: "IMPLEMENTED",
  STABLE: "STABLE",
});

export const RECOGNIZER_TYPES = Object.freeze([
  "wgs84_decimal",
  "wgs84_table",
  "generic_dms",
  "mgrs",
  "kyrgyzstan_gauss_kruger",
  "madagascar_cadastral",
  "indonesia_utm",
  "cote_divoire_dms",
]);

export const TECHNICAL_KML_BLOCK_REASONS = Object.freeze({
  NO_COORDINATES: "NO_COORDINATES",
  NON_NUMERIC_COORDINATES: "NON_NUMERIC_COORDINATES",
  INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY: "INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY",
  UNPARSABLE_COORDINATE_STRUCTURE: "UNPARSABLE_COORDINATE_STRUCTURE",
});

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function cleanStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => cleanString(item))
    .filter(Boolean);
}

function normalizeCoordinate(point = {}) {
  const label = cleanString(point.label || point.point || point.id);
  const longitude = Number(point.longitude ?? point.lon ?? point.x);
  const latitude = Number(point.latitude ?? point.lat ?? point.y);
  const sourceValue = cleanString(point.sourceValue);
  const mgrs = point.mgrs && typeof point.mgrs === "object" ? Object.freeze({
    zone: Number.isFinite(Number(point.mgrs.zone)) ? Number(point.mgrs.zone) : null,
    band: cleanString(point.mgrs.band),
    gridSquare: cleanString(point.mgrs.gridSquare),
    eastingDigits: cleanString(point.mgrs.eastingDigits),
    northingDigits: cleanString(point.mgrs.northingDigits),
    precisionDigits: Number.isFinite(Number(point.mgrs.precisionDigits)) ? Number(point.mgrs.precisionDigits) : null,
  }) : null;
  const sourceProjected = point.sourceProjected && typeof point.sourceProjected === "object" ? Object.freeze({
    x: Number.isFinite(Number(point.sourceProjected.x)) ? Number(point.sourceProjected.x) : null,
    y: Number.isFinite(Number(point.sourceProjected.y)) ? Number(point.sourceProjected.y) : null,
    axisSemantics: cleanString(point.sourceProjected.axisSemantics),
    sourceCrs: cleanString(point.sourceProjected.sourceCrs),
  }) : null;
  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    return {
      label,
      longitude: null,
      latitude: null,
      altitude: 0,
      numeric: false,
      sourceValue,
      mgrs,
      sourceProjected,
    };
  }
  return {
    label,
    longitude,
    latitude,
    altitude: Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0,
    numeric: true,
    sourceValue,
    mgrs,
    sourceProjected,
  };
}

function normalizeCoordinates(value) {
  if (!Array.isArray(value)) return [];
  return value.map(normalizeCoordinate);
}

function inferGeometryType(value, coordinates) {
  const explicit = cleanString(value).toLowerCase();
  if (["point", "line", "linestring", "polygon"].includes(explicit)) {
    return explicit === "linestring" ? "line" : explicit;
  }
  if (coordinates.length === 1) return "point";
  if (coordinates.length === 2) return "line";
  if (coordinates.length >= 3) return "polygon";
  return "unknown";
}

function getTechnicalKmlBlockReason(geometryType, coordinates) {
  if (!coordinates.length) return TECHNICAL_KML_BLOCK_REASONS.NO_COORDINATES;
  if (!coordinates.every((point) => point.numeric === true)) {
    return TECHNICAL_KML_BLOCK_REASONS.NON_NUMERIC_COORDINATES;
  }
  if (geometryType === "line" && coordinates.length < 2) {
    return TECHNICAL_KML_BLOCK_REASONS.INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY;
  }
  if (geometryType === "polygon" && coordinates.length < 3) {
    return TECHNICAL_KML_BLOCK_REASONS.INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY;
  }
  if (geometryType === "unknown") {
    return TECHNICAL_KML_BLOCK_REASONS.UNPARSABLE_COORDINATE_STRUCTURE;
  }
  return null;
}

export function createWarningMetadata(value = {}) {
  return Object.freeze({
    code: cleanString(value.code, "REVIEW_WARNING"),
    severity: cleanString(value.severity, "warning"),
    message: cleanString(value.message, "Coordinate result requires review."),
    point: cleanString(value.point || value.label),
    suspectedField: cleanString(value.suspectedField || value.field),
    currentValue: value.currentValue ?? null,
    reason: cleanString(value.reason),
  });
}

export function createSuspectedPoint(value = {}) {
  return Object.freeze({
    point: cleanString(value.point || value.label),
    suspectedField: cleanString(value.suspectedField || value.field),
    currentValue: value.currentValue ?? null,
    reason: cleanString(value.reason, "suspected_coordinate_issue"),
  });
}

export function createNormalizedCoordinateResult(value = {}) {
  const coordinates = normalizeCoordinates(value.coordinates);
  const geometryType = inferGeometryType(value.geometryType, coordinates);
  const technicalBlockReason = getTechnicalKmlBlockReason(geometryType, coordinates);
  return Object.freeze({
    schemaVersion: COORDINATE_ENGINE_V3_SCHEMA_VERSION,
    coordinateType: cleanString(value.coordinateType, "unknown"),
    recognizerId: cleanString(value.recognizerId, "unassigned"),
    coordinates: Object.freeze(coordinates.map(Object.freeze)),
    geometryType,
    crs: cleanString(value.crs, "unknown"),
    precisionMode: cleanString(value.precisionMode, "unknown"),
    warnings: Object.freeze((Array.isArray(value.warnings) ? value.warnings : [])
      .map(createWarningMetadata)),
    suspectedPoints: Object.freeze((Array.isArray(value.suspectedPoints) ? value.suspectedPoints : [])
      .map(createSuspectedPoint)),
    technicalKmlReady: technicalBlockReason === null,
    technicalKmlBlockReason: technicalBlockReason,
    sourceTrace: Object.freeze(cleanStringArray(value.sourceTrace)),
  });
}

export function validateNormalizedCoordinateResult(value = {}) {
  const errors = [];
  if (value.schemaVersion !== COORDINATE_ENGINE_V3_SCHEMA_VERSION) errors.push("schema_version_mismatch");
  if (!RECOGNIZER_TYPES.includes(value.coordinateType) && value.coordinateType !== "unknown") {
    errors.push("unsupported_coordinate_type");
  }
  if (!Array.isArray(value.coordinates)) errors.push("coordinates_not_array");
  if (value.confirmationStatus !== undefined) errors.push("confirmation_status_is_not_authority");
  if (value.shadowWinner !== undefined) errors.push("shadow_winner_is_not_authority");
  if (value.migrationStatus !== undefined) errors.push("migration_status_is_not_authority");
  if (value.arbitrationProposal !== undefined) errors.push("arbitration_proposal_is_not_authority");
  if (value.dryRun !== undefined) errors.push("dry_run_is_not_authority");
  return Object.freeze({
    valid: errors.length === 0,
    errors: Object.freeze(errors),
  });
}
