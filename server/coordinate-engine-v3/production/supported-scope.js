export const V3_PRODUCTION_SCOPE_STATUS = Object.freeze({
  SUPPORTED: "SUPPORTED",
  EXPERIMENTAL: "EXPERIMENTAL",
  REVIEW_ONLY: "REVIEW_ONLY",
  UNSUPPORTED: "UNSUPPORTED",
});

export const V3_PRODUCTION_SUPPORTED_SCOPE_V1 = Object.freeze({
  schemaVersion: "coordinate_engine_v3_production_supported_scope_v1",
  supported: Object.freeze([
    "cote_divoire_dms",
    "indonesia_utm",
    "wgs84_decimal",
    "generic_dms",
    "dms_grouped_coordinates",
  ]),
  experimental: Object.freeze([
    "table_context_composite",
    "full_image_ocr",
    "structural_router",
    "indonesia_utm_complex_table_experimental",
  ]),
  reviewOnly: Object.freeze([
    "wgs84_table",
    "mgrs",
    "kyrgyzstan_gauss_kruger",
    "madagascar_cadastral",
    "point_az_dms_table",
    "handwritten_dms",
    "french_perimeter_dms",
    "mozambique_geographic_table",
  ]),
});

function cleanKey(value = "") {
  return String(value ?? "").trim();
}

function includesKey(list = [], key = "") {
  const normalized = cleanKey(key);
  return Boolean(normalized) && list.includes(normalized);
}

export function getV3ProductionScopeStatus({
  recognizerId,
  coordinateType,
  family,
  scope = V3_PRODUCTION_SUPPORTED_SCOPE_V1,
} = {}) {
  const keys = [recognizerId, coordinateType, family].map(cleanKey).filter(Boolean);
  if (keys.some((key) => includesKey(scope.supported, key))) return V3_PRODUCTION_SCOPE_STATUS.SUPPORTED;
  if (keys.some((key) => includesKey(scope.experimental, key))) return V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL;
  if (keys.some((key) => includesKey(scope.reviewOnly, key))) return V3_PRODUCTION_SCOPE_STATUS.REVIEW_ONLY;
  return V3_PRODUCTION_SCOPE_STATUS.UNSUPPORTED;
}

export function isV3ProductionSupported(value = {}) {
  return getV3ProductionScopeStatus(value) === V3_PRODUCTION_SCOPE_STATUS.SUPPORTED;
}
