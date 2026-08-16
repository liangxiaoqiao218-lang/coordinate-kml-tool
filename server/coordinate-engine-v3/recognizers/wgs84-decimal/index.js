import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const WGS84_DECIMAL_RECOGNIZER_ID = "wgs84_decimal";
export const WGS84_DECIMAL_PRECISION_MODE = "wgs84-decimal";
export const WGS84_DECIMAL_CRS = "EPSG:4326";

const DECIMAL_NUMBER = "[-+]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)";
const LABELED_DECIMAL_ROW = new RegExp(
  `^(?:(?<label>[A-Za-z][A-Za-z0-9_-]{0,23})\\s*:\\s*)?(?<first>${DECIMAL_NUMBER})(?:\\s*,\\s*|\\s+)(?<second>${DECIMAL_NUMBER})\\s*$`,
);

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function hasDmsSyntax(text) {
  return /[°º˚'′"″]|(?:^|[\s,;:])(?:N|S|E|W|NE|NW|SE|SW)(?:$|[\s,;:])/i.test(text);
}

function hasMgrsSyntax(text) {
  return /\b\d{1,2}[C-HJ-NP-X]\s*[A-HJ-NP-Z]{2}\s*\d{2,}\b/i.test(text);
}

function splitRows(text) {
  return String(text || "")
    .split(/\r?\n|;/)
    .map((row) => row.trim())
    .filter(Boolean);
}

function parseDecimalRows(input = {}) {
  const text = getInputText(input);
  if (!text) return { status: "not_handled", rows: [], warnings: [], reason: "empty_input" };
  if (hasDmsSyntax(text)) return { status: "not_handled", rows: [], warnings: [], reason: "dms_not_supported" };
  if (hasMgrsSyntax(text)) return { status: "not_handled", rows: [], warnings: [], reason: "mgrs_not_supported" };

  const rows = [];
  const warnings = [];
  const rejected = [];
  const sourceRows = splitRows(text);
  for (let index = 0; index < sourceRows.length; index += 1) {
    const sourceRow = sourceRows[index];
    const match = sourceRow.match(LABELED_DECIMAL_ROW);
    if (!match) {
      rejected.push({ row: sourceRow, reason: "row_not_decimal_pair" });
      continue;
    }

    const first = Number(match.groups.first);
    const second = Number(match.groups.second);
    const label = String(match.groups.label || index + 1);
    if (!Number.isFinite(first) || !Number.isFinite(second)) {
      rejected.push({ row: sourceRow, label, reason: "non_finite_coordinate" });
      continue;
    }

    if (Math.abs(first) > 180 || Math.abs(second) > 180) {
      rejected.push({ row: sourceRow, label, reason: "projected_xy_or_out_of_world_range" });
      continue;
    }

    if (Math.abs(first) > 90 && Math.abs(second) <= 90) {
      warnings.push({
        code: "POSSIBLE_LAT_LON_SWAP",
        severity: "warning",
        message: "First value is outside latitude range while second value could be latitude.",
        point: label,
        suspectedField: "latitude",
        currentValue: first,
        reason: "first_value_outside_latitude_range",
        suspectedInterpretation: {
          latitude: second,
          longitude: first,
        },
      });
      rejected.push({ row: sourceRow, label, reason: "possible_lat_lon_swap" });
      continue;
    }

    if (Math.abs(first) > 90) {
      rejected.push({ row: sourceRow, label, reason: "invalid_latitude" });
      continue;
    }

    if (Math.abs(second) > 180) {
      rejected.push({ row: sourceRow, label, reason: "invalid_longitude" });
      continue;
    }

    rows.push({
      label,
      latitude: first,
      longitude: second,
      altitude: 0,
      source: "lat_lon_decimal_pair",
    });
  }

  if (!rows.length) {
    return {
      status: warnings.length ? "rejected" : "not_handled",
      rows: [],
      warnings,
      rejected,
      reason: warnings.length ? "decimal_rows_rejected_with_warning" : "no_supported_decimal_rows",
    };
  }

  return {
    status: rejected.length ? "partial" : "accepted",
    rows,
    warnings,
    rejected,
    reason: rejected.length ? "some_rows_rejected" : "wgs84_decimal_rows_accepted",
  };
}

function inferGeometryType(rows) {
  if (rows.length === 1) return "point";
  if (rows.length === 2) return "line";
  if (rows.length >= 3) return "polygon";
  return "unknown";
}

export function canHandleWgs84Decimal(input = {}) {
  const parsed = parseDecimalRows(input);
  return parsed.rows.length > 0 || parsed.warnings.some((warning) => warning.code === "POSSIBLE_LAT_LON_SWAP");
}

export async function recognizeWgs84Decimal(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      rows: Object.freeze([]),
      warnings: Object.freeze([createWarningMetadata({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }

  const parsed = parseDecimalRows(input);
  return Object.freeze({
    handled: parsed.rows.length > 0,
    status: parsed.status,
    rows: Object.freeze(parsed.rows.map(Object.freeze)),
    warnings: Object.freeze(parsed.warnings.map(Object.freeze)),
    rejected: Object.freeze((parsed.rejected || []).map(Object.freeze)),
    reason: parsed.reason,
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeWgs84Decimal(result = {}) {
  return createNormalizedCoordinateResult({
    coordinateType: "wgs84_decimal",
    recognizerId: WGS84_DECIMAL_RECOGNIZER_ID,
    coordinates: Array.isArray(result.rows) ? result.rows : [],
    geometryType: inferGeometryType(Array.isArray(result.rows) ? result.rows : []),
    crs: WGS84_DECIMAL_CRS,
    precisionMode: WGS84_DECIMAL_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: (result.warnings || [])
      .filter((warning) => warning.code === "POSSIBLE_LAT_LON_SWAP")
      .map((warning) => ({
        point: warning.point,
        suspectedField: warning.suspectedField,
        currentValue: warning.currentValue,
        reason: warning.reason,
      })),
    sourceTrace: ["wgs84_decimal:deterministic"],
  });
}

export async function verifyWgs84Decimal(normalized = {}) {
  const coordinates = Array.isArray(normalized.coordinates) ? normalized.coordinates : [];
  const invalid = coordinates.filter((point) => (
    !Number.isFinite(Number(point.latitude))
    || !Number.isFinite(Number(point.longitude))
    || Math.abs(Number(point.latitude)) > 90
    || Math.abs(Number(point.longitude)) > 180
  ));
  return Object.freeze({
    verified: invalid.length === 0 && coordinates.length > 0,
    status: invalid.length === 0 && coordinates.length > 0 ? "pass" : "failed",
    invalidPointLabels: Object.freeze(invalid.map((point) => point.label).filter(Boolean)),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function toKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const wgs84DecimalRecognizer = createRecognizerContract({
  recognizerId: WGS84_DECIMAL_RECOGNIZER_ID,
  coordinateType: "wgs84_decimal",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleWgs84Decimal,
  recognize: recognizeWgs84Decimal,
  normalize: normalizeWgs84Decimal,
  verify: verifyWgs84Decimal,
});

export default wgs84DecimalRecognizer;
