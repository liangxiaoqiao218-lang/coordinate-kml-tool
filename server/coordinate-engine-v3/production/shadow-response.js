import { createNormalizedCoordinateResult } from "../contracts.js";
import { V3_PRODUCTION_RESULT_SCHEMA_VERSION } from "./contracts.js";
import { mapV3ProductionResult } from "./result-mapper.js";

const SHADOW_SCHEMA_VERSION = "coordinate_engine_v3_production_shadow_v1";

const LEGACY_SUPPORTED_TYPE_MAP = Object.freeze({
  cote_divoire_geographic_dms_table: "cote_divoire_dms",
  indonesia_utm: "indonesia_utm",
  standard_dms_table: "generic_dms",
  decimal_latlon: "wgs84_decimal",
});

const LEGACY_SUPPORTED_PRECISION_MAP = Object.freeze({
  "cote-divoire-geographic-dms-table": "cote_divoire_dms",
  "indonesia-utm-wgs84-zone-50s": "indonesia_utm",
  "dms-grouped-coordinates": "dms_grouped_coordinates",
  "dms-coordinates": "generic_dms",
  "wgs84-decimal": "wgs84_decimal",
  "decimal-latlon": "wgs84_decimal",
});

const REVIEW_ONLY_LEGACY_PRECISION = new Set([
  "wgs84-table-coordinates",
  "wgs84-chat-coordinates",
  "mgrs-utm-grid-reference",
  "kyrgyz-gk-point-x-y",
  "cadastral-grid-num-xv-yv",
  "handwritten-dms-coordinates",
  "point-az-dms-table",
  "french-perimeter-dms-prose",
  "local-ocr-dms-fallback",
]);

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function cleanArray(value) {
  return Object.freeze((Array.isArray(value) ? value : []).filter((item) => item !== undefined && item !== null));
}

function getLegacyCoordinateType(payload = {}, coordinateEngineV2 = {}) {
  return cleanString(coordinateEngineV2.coordinate_type || coordinateEngineV2.coordinateType || payload.coordinate_type);
}

function getLegacyPrecisionMode(payload = {}, coordinateEngineV2 = {}) {
  return cleanString(coordinateEngineV2.precision_mode || coordinateEngineV2.precisionMode || payload.precisionMode || payload.precision_mode);
}

function getSupportedOwner(payload = {}, coordinateEngineV2 = {}) {
  const coordinateType = getLegacyCoordinateType(payload, coordinateEngineV2);
  const precisionMode = getLegacyPrecisionMode(payload, coordinateEngineV2);

  if (LEGACY_SUPPORTED_PRECISION_MAP[precisionMode]) return LEGACY_SUPPORTED_PRECISION_MAP[precisionMode];
  if (LEGACY_SUPPORTED_TYPE_MAP[coordinateType]) {
    if (coordinateType === "standard_dms_table" && REVIEW_ONLY_LEGACY_PRECISION.has(precisionMode)) return "";
    if (coordinateType === "decimal_latlon" && REVIEW_ONLY_LEGACY_PRECISION.has(precisionMode)) return "";
    return LEGACY_SUPPORTED_TYPE_MAP[coordinateType];
  }
  return "";
}

function getCrsForOwner(owner = "") {
  if (owner === "indonesia_utm") return "EPSG:32750";
  return "EPSG:4326";
}

function inferGeometryFromGroups(groups = [], pointCount = 0, owner = "") {
  if (owner === "dms_grouped_coordinates" && groups.length > 1) return "multipolygon";
  const firstGeometry = cleanString(groups[0]?.geometry).toLowerCase();
  if (["point", "line", "linestring", "polygon", "multipolygon"].includes(firstGeometry)) {
    return firstGeometry === "linestring" ? "line" : firstGeometry;
  }
  if (pointCount === 1) return "point";
  if (pointCount === 2) return "line";
  if (pointCount >= 3) return "polygon";
  return "unknown";
}

function getLegacyGroups(coordinateEngineV2 = {}) {
  return Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
}

function getLegacyPoints(groups = []) {
  return groups.flatMap((group, groupIndex) => (
    Array.isArray(group?.points)
      ? group.points.map((point, pointIndex) => ({ group, groupIndex, point, pointIndex }))
      : []
  ));
}

function hasLatLon(point = {}) {
  const lat = Number(point.lat ?? point.latitude);
  const lon = Number(point.lon ?? point.longitude);
  return Number.isFinite(lat)
    && Number.isFinite(lon)
    && lat >= -90
    && lat <= 90
    && lon >= -180
    && lon <= 180;
}

function buildCoordinatesFromLegacyGroups(groups = []) {
  return getLegacyPoints(groups)
    .filter(({ point }) => hasLatLon(point))
    .map(({ point, groupIndex, pointIndex }) => ({
      label: cleanString(point.label, `${groupIndex + 1}.${pointIndex + 1}`),
      latitude: Number(point.lat ?? point.latitude),
      longitude: Number(point.lon ?? point.longitude),
      altitude: 0,
      sourceValue: cleanString(point.raw),
    }));
}

function buildWarnings(coordinateEngineV2 = {}) {
  const groupWarnings = getLegacyGroups(coordinateEngineV2).flatMap((group) => (
    Array.isArray(group?.warnings) ? group.warnings : []
  ));
  return Object.freeze([...new Set([
    ...(Array.isArray(coordinateEngineV2.warnings) ? coordinateEngineV2.warnings : []),
    ...groupWarnings,
  ].map(cleanString).filter(Boolean))].map((message) => ({
    code: "LEGACY_REVIEW_WARNING",
    severity: "warning",
    message,
  })));
}

function buildLegacyProductionInput({ payload = {}, coordinateEngineV2 = {}, productionMetadata = {} } = {}) {
  const owner = getSupportedOwner(payload, coordinateEngineV2);
  const groups = getLegacyGroups(coordinateEngineV2);
  const coordinates = buildCoordinatesFromLegacyGroups(groups);
  const coordinateType = getLegacyCoordinateType(payload, coordinateEngineV2);
  const precisionMode = getLegacyPrecisionMode(payload, coordinateEngineV2);
  const availableRows = getLegacyPoints(groups).length;
  const meaningfulEvidence = groups.length > 0 || availableRows > 0 || cleanString(payload.coordinates);

  if (owner && coordinates.length > 0) {
    return {
      normalized: createNormalizedCoordinateResult({
        coordinateType: owner,
        recognizerId: owner,
        coordinates,
        geometryType: inferGeometryFromGroups(groups, coordinates.length, owner),
        crs: getCrsForOwner(owner),
        precisionMode: precisionMode || owner,
        warnings: buildWarnings(coordinateEngineV2),
        suspectedPoints: groups.flatMap((group) => (
          Array.isArray(group?.points)
            ? group.points
                .filter((point) => point?.requires_review)
                .map((point) => ({
                  point: cleanString(point.label),
                  suspectedField: "coordinate",
                  currentValue: cleanString(point.raw),
                  reason: "legacy_review_required",
                }))
            : []
        )),
      }),
      productionMetadata,
    };
  }

  return {
    productionMetadata: {
      ...productionMetadata,
      meaningfulEvidence,
      availableRows,
      evidenceLabel: coordinateType || precisionMode || "legacy_response",
      recognizerNotAvailable: Boolean(meaningfulEvidence),
    },
  };
}

function compactProductionResult(result = {}) {
  return Object.freeze({
    schemaVersion: SHADOW_SCHEMA_VERSION,
    mapperSchemaVersion: result.schemaVersion || V3_PRODUCTION_RESULT_SCHEMA_VERSION,
    shadowOnly: true,
    status: result.status || null,
    reasonCode: result.reasonCode || null,
    recognizerId: result.recognizerId || null,
    coordinateType: result.coordinateType || null,
    productionSupported: result.productionSupported === true,
    productionScopeStatus: result.productionScopeStatus || null,
    technicalKmlReady: result.technicalKmlReady === true,
    technicalKmlBlockReason: result.technicalKmlBlockReason || null,
    warnings: cleanArray(result.warnings),
    availableData: result.availableData || Object.freeze({}),
    missingRequirement: result.missingRequirement || null,
    reasonCodes: cleanArray(result.reasonCodes),
  });
}

export function buildCoordinateEngineV3ProductionShadow({
  payload = {},
  coordinateEngineV2 = payload.coordinateEngineV2 || payload.coordinate_engine_v2 || {},
  productionMetadata = {},
  productionInput = null,
} = {}) {
  const input = productionInput || buildLegacyProductionInput({ payload, coordinateEngineV2, productionMetadata });
  return compactProductionResult(mapV3ProductionResult(input));
}
