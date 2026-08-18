import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const WGS84_TABLE_RECOGNIZER_ID = "wgs84_table";
export const WGS84_TABLE_PRECISION_MODE = "wgs84-table";
export const WGS84_TABLE_CRS = "EPSG:4326";

const DECIMAL_PATTERN = /^[-+]?(?:\d+(?:\.\d+)?|\.\d+)$/;

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function getStructuredRows(input = {}) {
  if (Array.isArray(input)) return input;
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.tableRows)) return input.tableRows;
  if (Array.isArray(input.structuredRows)) return input.structuredRows;
  if (Array.isArray(input.wgs84Rows)) return input.wgs84Rows;
  return [];
}

function normalizeHeader(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[：:]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function cleanSourceValue(value) {
  return String(value ?? "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
}

function splitTextRows(text = "") {
  return String(text || "")
    .split(/\r?\n/)
    .map((row) => row.trim())
    .filter(Boolean);
}

function splitTableLine(line = "") {
  const value = String(line || "").trim();
  if (!value) return [];
  if (/[|\t;]/.test(value)) {
    return value.split(/[|\t;]/).map((item) => item.trim()).filter((item) => item.length > 0);
  }
  if (value.includes(",")) {
    return value.split(",").map((item) => item.trim()).filter((item) => item.length > 0);
  }
  return value.split(/\s{2,}|\s+/).map((item) => item.trim()).filter((item) => item.length > 0);
}

function isDmsToken(value = "") {
  return /[°º˚'′"″]|(?:^|[\s,;:])(?:N|S|E|W|NE|NW|SE|SW)(?:$|[\s,;:])/i.test(String(value || ""));
}

function isProjectedHeader(header = "") {
  const normalized = normalizeHeader(header);
  return /^(x|y|easting|northing|x\/y|utm|xv|yv)$/i.test(normalized);
}

function getLabelRole(header = "") {
  const normalized = normalizeHeader(header).replace(/[.。]/g, "");
  if (/^(point|pt|id|no|nº|num|number|name|label|序号|点号|编号|名称)$/.test(normalized)) return "label";
  return "";
}

function getHemisphere(header = "") {
  const normalized = normalizeHeader(header);
  if (/西经|经度.*西|west longitude|longitude west|\bw\b/.test(normalized)) return "W";
  if (/东经|经度.*东|east longitude|longitude east|\be\b/.test(normalized)) return "E";
  if (/南纬|纬度.*南|south latitude|latitude south|\bs\b/.test(normalized)) return "S";
  if (/北纬|纬度.*北|north latitude|latitude north|\bn\b/.test(normalized)) return "N";
  return "";
}

function getCoordinateRole(header = "") {
  const normalized = normalizeHeader(header);
  if (/经度|东经|西经|longitude|\blon\b|\blng\b/.test(normalized)) return "longitude";
  if (/纬度|北纬|南纬|latitude|\blat\b/.test(normalized)) return "latitude";
  return "";
}

function classifyHeader(header = "", index = 0) {
  const role = getCoordinateRole(header);
  const labelRole = getLabelRole(header);
  return Object.freeze({
    index,
    source: String(header || "").trim(),
    role: role || labelRole || "unknown",
    coordinateRole: role,
    hemisphere: getHemisphere(header),
    isProjected: isProjectedHeader(header),
  });
}

function resolveHeaderMapping(headers = []) {
  const classified = headers.map(classifyHeader);
  if (classified.some((header) => header.isProjected)) {
    return Object.freeze({ accepted: false, reason: "projected_header_not_geographic", headers: Object.freeze(classified) });
  }
  const latitudeColumns = classified.filter((header) => header.coordinateRole === "latitude");
  const longitudeColumns = classified.filter((header) => header.coordinateRole === "longitude");
  if (latitudeColumns.length !== 1 || longitudeColumns.length !== 1) {
    return Object.freeze({ accepted: false, reason: "header_role_mapping_unresolved", headers: Object.freeze(classified) });
  }
  if (latitudeColumns[0].index === longitudeColumns[0].index) {
    return Object.freeze({ accepted: false, reason: "same_column_for_latitude_and_longitude", headers: Object.freeze(classified) });
  }
  const labelColumn = classified.find((header) => header.role === "label") || null;
  return Object.freeze({
    accepted: true,
    latitudeColumn: latitudeColumns[0],
    longitudeColumn: longitudeColumns[0],
    labelColumn,
    headers: Object.freeze(classified),
  });
}

function normalizeNumberToken(value = "") {
  const raw = String(value ?? "").trim();
  if (!DECIMAL_PATTERN.test(raw)) return null;
  const number = Number(raw);
  return Number.isFinite(number) ? number : null;
}

function applyHemisphereSign(value, role, hemisphere) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return { ok: false, reason: "non_finite_coordinate" };
  const direction = String(hemisphere || "").toUpperCase();
  if (direction === "W" || direction === "S") return { ok: true, value: -Math.abs(numeric) };
  if ((direction === "E" || direction === "N") && numeric < 0) {
    return { ok: false, reason: `${role}_sign_conflicts_with_${direction}_header` };
  }
  return { ok: true, value: numeric };
}

function validateCoordinate(latitude, longitude) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return "non_finite_coordinate";
  if (latitude < -90 || latitude > 90) return "invalid_latitude";
  if (longitude < -180 || longitude > 180) return "invalid_longitude";
  return "";
}

function makeWarning(label, code, message, suspectedField, currentValue, reason) {
  return createWarningMetadata({
    code,
    severity: "warning",
    message,
    point: label,
    suspectedField,
    currentValue,
    reason,
  });
}

function parseRowCells(cells = [], mapping, rowIndex) {
  const label = mapping.labelColumn
    ? String(cells[mapping.labelColumn.index] ?? "").trim()
    : String(rowIndex + 1);
  const rawLatitude = cells[mapping.latitudeColumn.index];
  const rawLongitude = cells[mapping.longitudeColumn.index];
  if (rawLatitude === undefined || rawLongitude === undefined) {
    return { row: null, warning: makeWarning(label, "MISSING_COORDINATE_FIELD", "WGS84 table row is missing a latitude or longitude field.", "coordinate", null, "missing_coordinate_field") };
  }
  if (isDmsToken(rawLatitude) || isDmsToken(rawLongitude)) {
    return { row: null, warning: makeWarning(label, "UNSUPPORTED_CELL_FORMAT", "WGS84 table recognizer only accepts decimal cells.", "coordinate", cleanSourceValue(`${rawLongitude} ${rawLatitude}`), "dms_cell_not_supported") };
  }
  const parsedLatitude = normalizeNumberToken(rawLatitude);
  const parsedLongitude = normalizeNumberToken(rawLongitude);
  if (parsedLatitude === null || parsedLongitude === null) {
    return { row: null, warning: makeWarning(label, "INVALID_NUMERIC_TOKEN", "WGS84 table row contains an invalid numeric token.", "coordinate", cleanSourceValue(`${rawLongitude} ${rawLatitude}`), "invalid_numeric_token") };
  }
  const signedLatitude = applyHemisphereSign(parsedLatitude, "latitude", mapping.latitudeColumn.hemisphere);
  if (!signedLatitude.ok) {
    return { row: null, warning: makeWarning(label, "HEADER_SIGN_CONFLICT", "Latitude sign conflicts with the table header hemisphere.", "latitude", parsedLatitude, signedLatitude.reason) };
  }
  const signedLongitude = applyHemisphereSign(parsedLongitude, "longitude", mapping.longitudeColumn.hemisphere);
  if (!signedLongitude.ok) {
    return { row: null, warning: makeWarning(label, "HEADER_SIGN_CONFLICT", "Longitude sign conflicts with the table header hemisphere.", "longitude", parsedLongitude, signedLongitude.reason) };
  }
  const validationError = validateCoordinate(signedLatitude.value, signedLongitude.value);
  if (validationError) {
    return { row: null, warning: makeWarning(label, "INVALID_WGS84_TABLE_COORDINATE", "WGS84 table row is outside valid latitude/longitude range.", validationError.includes("latitude") ? "latitude" : "longitude", validationError.includes("latitude") ? signedLatitude.value : signedLongitude.value, validationError) };
  }
  return {
    row: Object.freeze({
      label,
      latitude: signedLatitude.value,
      longitude: signedLongitude.value,
      altitude: 0,
      sourceValue: cleanSourceValue(cells.join(" | ")),
      source: "wgs84_table_row",
    }),
    warning: null,
  };
}

function dedupeExactRows(rows = []) {
  const seen = new Set();
  return rows.filter((row) => {
    const key = `${row.latitude}|${row.longitude}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function parseStructuredRows(input = {}) {
  const rows = getStructuredRows(input);
  if (!rows.length) return null;
  const headers = Array.from(rows.reduce((set, row) => {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      Object.keys(row).forEach((key) => set.add(key));
    }
    return set;
  }, new Set()));
  const mapping = resolveHeaderMapping(headers);
  if (!mapping.accepted) return { status: "not_handled", rows: [], warnings: [], reason: mapping.reason, mapping };
  const parsedRows = [];
  const warnings = [];
  rows.forEach((row, rowIndex) => {
    const cells = headers.map((header) => row?.[header]);
    const parsed = parseRowCells(cells, mapping, rowIndex);
    if (parsed.row) parsedRows.push(parsed.row);
    if (parsed.warning) warnings.push(parsed.warning);
  });
  const deduped = dedupeExactRows(parsedRows);
  return {
    status: deduped.length ? (warnings.length ? "partial" : "accepted") : "rejected",
    rows: deduped,
    warnings,
    mapping,
    reason: deduped.length ? "wgs84_table_rows_accepted" : "no_valid_wgs84_table_rows",
  };
}

function parseTextTable(input = {}) {
  const text = getInputText(input);
  if (!text) return { status: "not_handled", rows: [], warnings: [], reason: "empty_input" };
  const sourceRows = splitTextRows(text);
  if (sourceRows.length < 2) return { status: "not_handled", rows: [], warnings: [], reason: "table_requires_header_and_rows" };
  const headerCells = splitTableLine(sourceRows[0]);
  const mapping = resolveHeaderMapping(headerCells);
  if (!mapping.accepted) return { status: "not_handled", rows: [], warnings: [], reason: mapping.reason, mapping };

  const parsedRows = [];
  const warnings = [];
  sourceRows.slice(1).forEach((line, rowIndex) => {
    const cells = splitTableLine(line);
    const parsed = parseRowCells(cells, mapping, rowIndex);
    if (parsed.row) parsedRows.push(parsed.row);
    if (parsed.warning) warnings.push(parsed.warning);
  });

  const deduped = dedupeExactRows(parsedRows);
  return {
    status: deduped.length ? (warnings.length ? "partial" : "accepted") : "rejected",
    rows: deduped,
    warnings,
    mapping,
    reason: deduped.length ? "wgs84_table_rows_accepted" : "no_valid_wgs84_table_rows",
  };
}

export function parseWgs84Table(input = {}) {
  const structured = parseStructuredRows(input);
  if (structured) return structured;
  return parseTextTable(input);
}

function inferGeometryType(rows = []) {
  if (rows.length === 1) return "point";
  if (rows.length === 2) return "line";
  if (rows.length >= 3) return "polygon";
  return "unknown";
}

export function canHandleWgs84Table(input = {}) {
  const parsed = parseWgs84Table(input);
  return parsed.rows.length > 0;
}

export async function recognizeWgs84Table(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      rows: Object.freeze([]),
      warnings: Object.freeze([createWarningMetadata({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic WGS84 table parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }
  const parsed = parseWgs84Table(input);
  return Object.freeze({
    handled: parsed.rows.length > 0,
    status: parsed.status,
    rows: Object.freeze(parsed.rows.map(Object.freeze)),
    warnings: Object.freeze(parsed.warnings.map(Object.freeze)),
    mapping: parsed.mapping || null,
    reason: parsed.reason,
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeWgs84Table(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "wgs84_table",
    recognizerId: WGS84_TABLE_RECOGNIZER_ID,
    coordinates: rows,
    geometryType: inferGeometryType(rows),
    crs: WGS84_TABLE_CRS,
    precisionMode: WGS84_TABLE_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: ["wgs84_table:deterministic_header_roles", WGS84_TABLE_CRS],
  });
}

export async function verifyWgs84Table(normalized = {}) {
  const coordinates = Array.isArray(normalized.coordinates) ? normalized.coordinates : [];
  const invalid = coordinates.filter((point) => (
    !Number.isFinite(Number(point.latitude))
    || !Number.isFinite(Number(point.longitude))
    || Number(point.latitude) < -90
    || Number(point.latitude) > 90
    || Number(point.longitude) < -180
    || Number(point.longitude) > 180
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

export function toWgs84TableKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const wgs84TableRecognizer = createRecognizerContract({
  recognizerId: WGS84_TABLE_RECOGNIZER_ID,
  coordinateType: "wgs84_table",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleWgs84Table,
  recognize: recognizeWgs84Table,
  normalize: normalizeWgs84Table,
  verify: verifyWgs84Table,
});

export default wgs84TableRecognizer;
