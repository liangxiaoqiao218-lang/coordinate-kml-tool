import crypto from "node:crypto";

export const COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION = "coordinate_evidence_candidate_v1";
export const COORDINATE_EVIDENCE_SHADOW_DECISION_SCHEMA_VERSION = "coordinate_evidence_shadow_decision_v1";

export const COORDINATE_EVIDENCE_RECOMMENDED_STATE = Object.freeze({
  AUTO_EXPORT: "AUTO_EXPORT",
  CONFIRM_REQUIRED: "CONFIRM_REQUIRED",
  BLOCKED_REVIEW: "BLOCKED_REVIEW"
});

export const AUTHORITY_CATEGORY = Object.freeze({
  EXPLICIT_LEGAL_COORDINATE: "explicit_legal_coordinate",
  VERIFIED_TRANSFORMATION: "verified_transformation",
  CRS_CONTEXT: "crs_context",
  CONTEXT_HINT: "context_hint",
  WEAK_NUMERIC: "weak_numeric",
  UNKNOWN: "unknown"
});

export const CONFIDENCE_LEVEL = Object.freeze({
  HIGH: "high",
  MEDIUM: "medium",
  LOW: "low",
  UNKNOWN: "unknown"
});

const COORDINATE_SOURCE_ALLOWLIST = new Set(["dms_deterministic"]);

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function normalizeBoolean(value, fallback = false) {
  return value === true || value === false ? value : fallback;
}

function normalizeInteger(value, fallback = 0) {
  const number = Number(value);
  return Number.isInteger(number) ? number : fallback;
}

function normalizeAuthority(value = {}) {
  const level = Math.max(0, Math.min(5, normalizeInteger(value.level, 0)));
  return Object.freeze({
    level,
    category: cleanString(value.category, AUTHORITY_CATEGORY.UNKNOWN),
    reason: cleanString(value.reason, "unspecified_authority")
  });
}

function normalizeConfidence(value = {}) {
  const level = cleanString(value.level, CONFIDENCE_LEVEL.UNKNOWN).toLowerCase();
  const normalizedLevel = Object.values(CONFIDENCE_LEVEL).includes(level)
    ? level
    : CONFIDENCE_LEVEL.UNKNOWN;
  return Object.freeze({
    level: normalizedLevel,
    reason: cleanString(value.reason, "unspecified_confidence")
  });
}

function normalizeAttributes(value = {}) {
  return Object.freeze({
    hasExplicitHemisphere: normalizeBoolean(value.hasExplicitHemisphere),
    hasExplicitCoordinateOrder: normalizeBoolean(value.hasExplicitCoordinateOrder),
    hasStructuredTable: normalizeBoolean(value.hasStructuredTable),
    crsEvidence: normalizeBoolean(value.crsEvidence),
    transformVerified: normalizeBoolean(value.transformVerified),
    geometryValid: value.geometryValid === false ? false : true,
    hemisphereAmbiguous: normalizeBoolean(value.hemisphereAmbiguous),
    coordinateOrderAmbiguous: normalizeBoolean(value.coordinateOrderAmbiguous)
  });
}

function normalizeCoordinateSummary(value = {}) {
  return Object.freeze({
    pointCount: Math.max(0, normalizeInteger(value.pointCount, 0)),
    geometryType: cleanString(value.geometryType, "unknown"),
    groupCount: Math.max(0, normalizeInteger(value.groupCount, 0))
  });
}

function normalizeCoordinatePoint(value = {}, index = 0) {
  const lat = Number(value.lat);
  const lon = Number(value.lon);
  return Object.freeze({
    point: cleanString(value.point || value.label || value.id, String(index + 1)),
    lat: Number.isFinite(lat) ? Number(lat.toFixed(9)) : null,
    lon: Number.isFinite(lon) ? Number(lon.toFixed(9)) : null,
    source: COORDINATE_SOURCE_ALLOWLIST.has(cleanString(value.source))
      ? cleanString(value.source)
      : "dms_deterministic"
  });
}

function normalizeSourceDmsToken(value = {}) {
  return Object.freeze({
    role: cleanString(value.role),
    degrees: Number.isFinite(Number(value.degrees)) ? Number(value.degrees) : null,
    minutes: Number.isFinite(Number(value.minutes)) ? Number(value.minutes) : null,
    seconds: Number.isFinite(Number(value.seconds)) ? Number(value.seconds) : null,
    hemisphere: cleanString(value.hemisphere)
  });
}

function normalizeSourceDmsRow(value = {}, index = 0) {
  return Object.freeze({
    point: cleanString(value.point, String(index + 1)),
    latitude: normalizeSourceDmsToken(value.latitude || {}),
    longitude: normalizeSourceDmsToken(value.longitude || {})
  });
}

function normalizeCoordinateInterpretation(value = {}) {
  if (!value || typeof value !== "object") return null;
  const normalizedCoordinates = Array.isArray(value.normalizedCoordinates)
    ? value.normalizedCoordinates.map(normalizeCoordinatePoint)
      .filter(point => point.lat !== null && point.lon !== null)
    : [];
  const sourceRows = Array.isArray(value.sourceRows)
    ? value.sourceRows.map(normalizeSourceDmsRow)
    : [];
  const status = cleanString(value.interpretationStatus || value.status, "INCOMPLETE").toUpperCase();
  const normalizedStatus = ["COMPLETE", "INCOMPLETE", "INVALID"].includes(status) ? status : "INCOMPLETE";
  return Object.freeze({
    schemaVersion: cleanString(value.schemaVersion, "dms_coordinate_interpretation_v1"),
    interpretationStatus: normalizedStatus,
    deterministicConversion: normalizeBoolean(value.deterministicConversion),
    hemisphereResolved: normalizeBoolean(value.hemisphereResolved),
    pointCount: Math.max(0, normalizeInteger(value.pointCount, normalizedCoordinates.length)),
    normalizedCoordinates: Object.freeze(normalizedCoordinates),
    sourceRows: Object.freeze(sourceRows),
    errors: Object.freeze(Array.isArray(value.errors) ? value.errors.map(error => cleanString(error)).filter(Boolean) : []),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

function normalizeRecommendedState(value) {
  const state = cleanString(value, COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW);
  return Object.values(COORDINATE_EVIDENCE_RECOMMENDED_STATE).includes(state)
    ? state
    : COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW;
}

function buildEvidenceId(value = {}) {
  const explicitId = cleanString(value.evidenceId || value.evidence_id);
  if (explicitId) return explicitId;

  const signature = [
    value.evidenceType,
    value.sourceParser,
    value.coordinateSource,
    value.authority?.level,
    value.authority?.category,
    value.reason
  ].map(item => String(item ?? "")).join("|");
  return `ev_${crypto.createHash("sha256").update(signature).digest("hex").slice(0, 16)}`;
}

function inferCanGenerateKml(value = {}, attributes = {}, recommendedState) {
  if (value.canGenerateKml === true || value.canGenerateKml === false) {
    return value.canGenerateKml;
  }
  return recommendedState === COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT
    && attributes.geometryValid
    && !attributes.hemisphereAmbiguous
    && !attributes.coordinateOrderAmbiguous;
}

export function createCoordinateEvidenceCandidate(value = {}) {
  const attributes = normalizeAttributes(value.attributes || {});
  const recommendedState = normalizeRecommendedState(value.recommendedState);
  return Object.freeze({
    schemaVersion: COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION,
    evidenceId: buildEvidenceId(value),
    evidenceType: cleanString(value.evidenceType, "unknown_evidence"),
    sourceParser: cleanString(value.sourceParser, "unknown_parser"),
    coordinateSource: cleanString(value.coordinateSource, "unknown_source"),
    authority: normalizeAuthority(value.authority || {}),
    confidence: normalizeConfidence(value.confidence || {}),
    attributes,
    coordinateInterpretation: normalizeCoordinateInterpretation(value.coordinateInterpretation),
    coordinateSummary: normalizeCoordinateSummary(value.coordinateSummary || {}),
    conflicts: Object.freeze(Array.isArray(value.conflicts) ? [...value.conflicts] : []),
    recommendedState,
    canGenerateKml: inferCanGenerateKml(value, attributes, recommendedState),
    reason: cleanString(value.reason, "unspecified_evidence")
  });
}

export function isCoordinateEvidenceCandidate(value = {}) {
  return Boolean(
    value
    && value.schemaVersion === COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION
    && typeof value.evidenceId === "string"
    && typeof value.evidenceType === "string"
    && Number.isInteger(value.authority?.level)
    && typeof value.confidence?.level === "string"
  );
}
