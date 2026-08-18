import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const COTE_DIVOIRE_DMS_RECOGNIZER_ID = "cote_divoire_dms";
export const COTE_DIVOIRE_DMS_PRECISION_MODE = "cote-divoire-geographic-dms-table";
export const COTE_DIVOIRE_DMS_CRS = "EPSG:4326";

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function getStructuredRows(input = {}) {
  if (Array.isArray(input)) return input;
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.tableRows)) return input.tableRows;
  if (Array.isArray(input.structuredRows)) return input.structuredRows;
  if (Array.isArray(input.coteDivoireRows)) return input.coteDivoireRows;
  return [];
}

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[，]/g, ",")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, "\"")
    .replace(/\u00a0/g, " ");
}

function normalizeHeader(value = "") {
  return normalizeText(value)
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
  const wideColumns = value.split(/\s{2,}/).map((item) => item.trim()).filter((item) => item.length > 0);
  if (wideColumns.length > 1) return wideColumns;
  return value.split(/\s+/).map((item) => item.trim()).filter((item) => item.length > 0);
}

function normalizeHemisphere(value = "") {
  const direction = normalizeHeader(value).toUpperCase();
  if (["O", "OUEST", "W", "WEST"].includes(direction)) return "W";
  if (["E", "EST", "EAST"].includes(direction)) return "E";
  if (["N", "NORD", "NORTH"].includes(direction)) return "N";
  if (["S", "SUD", "SOUTH"].includes(direction)) return "S";
  return "";
}

function getHeaderHemisphere(header = "") {
  const normalized = normalizeHeader(header);
  if (/ouest|west|longitude\s*w|\bw\b|\bo\b/.test(normalized)) return "W";
  if (/\best\b|east|longitude\s*e|\be\b/.test(normalized)) return "E";
  if (/nord|north|latitude\s*n|\bn\b/.test(normalized)) return "N";
  if (/sud|south|latitude\s*s|\bs\b/.test(normalized)) return "S";
  return "";
}

function getCoordinateRole(header = "") {
  const normalized = normalizeHeader(header);
  if (/latitude|nord|north|sud|south|^n$|^s$|纬度|北纬|南纬/.test(normalized)) return "latitude";
  if (/longitude|ouest|west|\bo\b|\bw\b|\best\b|east|^e$|经度|东经|西经/.test(normalized)) return "longitude";
  return "";
}

function getLabelRole(header = "") {
  const normalized = normalizeHeader(header).replace(/[.。]/g, "");
  if (/^(point|points|pt|id|no|n|n°|num|number|label|编号|点号)$/.test(normalized)) return "label";
  return "";
}

function getProjectedRole(header = "") {
  const normalized = normalizeHeader(header).replace(/[.。]/g, "");
  if (normalized === "x") return "x";
  if (normalized === "y") return "y";
  return "";
}

function classifyHeader(header = "", index = 0) {
  const coordinateRole = getCoordinateRole(header);
  const labelRole = getLabelRole(header);
  const projectedRole = getProjectedRole(header);
  return Object.freeze({
    index,
    source: String(header || "").trim(),
    role: coordinateRole || labelRole || projectedRole || "unknown",
    coordinateRole,
    projectedRole,
    hemisphere: getHeaderHemisphere(header),
  });
}

function resolveHeaderMapping(headers = []) {
  const classified = headers.map(classifyHeader);
  const hasProjectedX = classified.some((header) => header.projectedRole === "x");
  const hasProjectedY = classified.some((header) => header.projectedRole === "y");
  if (hasProjectedX && hasProjectedY) {
    return Object.freeze({ accepted: false, reason: "projected_table_not_cote_divoire_dms", headers: Object.freeze(classified) });
  }
  const latitudeColumns = classified.filter((header) => header.coordinateRole === "latitude");
  const longitudeColumns = classified.filter((header) => header.coordinateRole === "longitude");
  if (latitudeColumns.length !== 1 || longitudeColumns.length !== 1) {
    return Object.freeze({ accepted: false, reason: "header_role_mapping_unresolved", headers: Object.freeze(classified) });
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

function parseDmsCell(value = "") {
  const source = normalizeText(value)
    .replace(/,/g, ".")
    .replace(/\bNORD\b|\bNORTH\b/gi, "N")
    .replace(/\bSUD\b|\bSOUTH\b/gi, "S")
    .replace(/\bOUEST\b|\bWEST\b/gi, "W")
    .replace(/\bEST\b|\bEAST\b/gi, "E");
  const numbers = source.match(/\d+(?:\.\d+)?/g) || [];
  if (numbers.length < 3) return null;
  const degrees = Number(numbers[0]);
  const minutes = Number(numbers[1]);
  const seconds = Number(numbers[2]);
  const sign = /^\s*-/.test(source) ? -1 : 1;
  const hemisphere = normalizeHemisphere((source.match(/\b(N|S|E|W|O)\b/i) || [])[1] || "");
  if (!Number.isFinite(degrees)
    || !Number.isFinite(minutes)
    || !Number.isFinite(seconds)
    || minutes < 0
    || minutes >= 60
    || seconds < 0
    || seconds >= 60) {
    return null;
  }
  return Object.freeze({
    raw: cleanSourceValue(value),
    degrees,
    minutes,
    seconds,
    sign,
    hemisphere,
  });
}

function resolveEffectiveHemisphere(role, headerHemisphere, cellHemisphere) {
  const header = normalizeHemisphere(headerHemisphere);
  const cell = normalizeHemisphere(cellHemisphere);
  if (header && cell && header !== cell) return { ok: false, reason: "hemisphere_conflict" };
  const effective = cell || header;
  if (!effective) return { ok: false, reason: `${role}_hemisphere_missing` };
  if (role === "latitude" && !["N", "S"].includes(effective)) return { ok: false, reason: "latitude_hemisphere_invalid" };
  if (role === "longitude" && !["E", "W"].includes(effective)) return { ok: false, reason: "longitude_hemisphere_invalid" };
  return { ok: true, hemisphere: effective };
}

function dmsToDecimal(parts, role, headerHemisphere = "") {
  if (!parts) return { ok: false, reason: "incomplete_dms" };
  const effective = resolveEffectiveHemisphere(role, headerHemisphere, parts.hemisphere);
  if (!effective.ok) return effective;
  const degreeLimit = role === "latitude" ? 90 : 180;
  if (parts.degrees > degreeLimit) return { ok: false, reason: `${role}_degree_out_of_range` };
  if (parts.sign < 0 && ["N", "E"].includes(effective.hemisphere)) {
    return { ok: false, reason: `${role}_sign_conflicts_with_${effective.hemisphere}_hemisphere` };
  }
  const magnitude = parts.degrees + (parts.minutes / 60) + (parts.seconds / 3600);
  const signed = ["S", "W"].includes(effective.hemisphere) ? -magnitude : magnitude;
  return { ok: true, value: signed, hemisphere: effective.hemisphere };
}

function validateCoordinate(latitude, longitude) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return "non_finite_coordinate";
  if (latitude < -90 || latitude > 90) return "invalid_latitude";
  if (longitude < -180 || longitude > 180) return "invalid_longitude";
  return "";
}

function makeWarning(label, code, suspectedField, currentValue, reason) {
  return createWarningMetadata({
    code,
    severity: "warning",
    message: "Cote d'Ivoire DMS table row could not be deterministically normalized.",
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
    return { row: null, warning: makeWarning(label, "MISSING_COORDINATE_FIELD", "coordinate", null, "missing_coordinate_field") };
  }
  const latitudeParts = parseDmsCell(rawLatitude);
  const longitudeParts = parseDmsCell(rawLongitude);
  const latitude = dmsToDecimal(latitudeParts, "latitude", mapping.latitudeColumn.hemisphere);
  if (!latitude.ok) return { row: null, warning: makeWarning(label, latitude.reason === "hemisphere_conflict" ? "HEMISPHERE_CONFLICT" : "INVALID_DMS_CELL", "latitude", cleanSourceValue(rawLatitude), latitude.reason) };
  const longitude = dmsToDecimal(longitudeParts, "longitude", mapping.longitudeColumn.hemisphere);
  if (!longitude.ok) return { row: null, warning: makeWarning(label, longitude.reason === "hemisphere_conflict" ? "HEMISPHERE_CONFLICT" : "INVALID_DMS_CELL", "longitude", cleanSourceValue(rawLongitude), longitude.reason) };
  const validationError = validateCoordinate(latitude.value, longitude.value);
  if (validationError) {
    return { row: null, warning: makeWarning(label, "INVALID_WGS84_DMS_COORDINATE", validationError.includes("latitude") ? "latitude" : "longitude", validationError.includes("latitude") ? latitude.value : longitude.value, validationError) };
  }
  return {
    row: Object.freeze({
      label,
      latitude: latitude.value,
      longitude: longitude.value,
      altitude: 0,
      sourceValue: cleanSourceValue(cells.join(" | ")),
      source: "cote_divoire_dms_table_row",
      latitudeHemisphere: latitude.hemisphere,
      longitudeHemisphere: longitude.hemisphere,
    }),
    warning: null,
  };
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
  if (!mapping?.accepted) return { status: "not_handled", rows: [], warnings: [], reason: "header_role_mapping_unresolved", mapping };
  const parsedRows = [];
  const warnings = [];
  rows.forEach((row, rowIndex) => {
    const cells = headers.map((header) => row?.[header]);
    const parsed = parseRowCells(cells, mapping, rowIndex);
    if (parsed.row) parsedRows.push(parsed.row);
    if (parsed.warning) warnings.push(parsed.warning);
  });
  return {
    status: parsedRows.length ? (warnings.length ? "partial" : "accepted") : "rejected",
    rows: parsedRows,
    warnings,
    mapping,
    reason: parsedRows.length ? "cote_divoire_dms_rows_accepted" : "no_valid_cote_divoire_dms_rows",
  };
}

function parseTextTable(input = {}) {
  const text = getInputText(input);
  if (!text) return { status: "not_handled", rows: [], warnings: [], reason: "empty_input" };
  const sourceRows = splitTextRows(text);
  if (sourceRows.length < 2) return { status: "not_handled", rows: [], warnings: [], reason: "table_requires_header_and_rows" };
  const headerSearchLimit = Math.min(sourceRows.length - 1, 8);
  let headerIndex = -1;
  let mapping = null;
  for (let index = 0; index < headerSearchLimit; index += 1) {
    const candidate = resolveHeaderMapping(splitTableLine(sourceRows[index]));
    if (candidate.accepted) {
      headerIndex = index;
      mapping = candidate;
      break;
    }
  }
  if (!mapping?.accepted) return { status: "not_handled", rows: [], warnings: [], reason: "header_role_mapping_unresolved", mapping };
  const parsedRows = [];
  const warnings = [];
  sourceRows.slice(headerIndex + 1).forEach((line, rowIndex) => {
    const cells = splitTableLine(line);
    const parsed = parseRowCells(cells, mapping, rowIndex);
    if (parsed.row) parsedRows.push(parsed.row);
    if (parsed.warning) warnings.push(parsed.warning);
  });
  return {
    status: parsedRows.length ? (warnings.length ? "partial" : "accepted") : "rejected",
    rows: parsedRows,
    warnings,
    mapping,
    reason: parsedRows.length ? "cote_divoire_dms_rows_accepted" : "no_valid_cote_divoire_dms_rows",
  };
}

export function parseCoteDivoireDmsTable(input = {}) {
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

export function canHandleCoteDivoireDms(input = {}) {
  const parsed = parseCoteDivoireDmsTable(input);
  return parsed.rows.length > 0;
}

export async function recognizeCoteDivoireDms(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      rows: Object.freeze([]),
      warnings: Object.freeze([createWarningMetadata({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic Cote d'Ivoire DMS table parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }
  const parsed = parseCoteDivoireDmsTable(input);
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

export function normalizeCoteDivoireDms(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "cote_divoire_dms",
    recognizerId: COTE_DIVOIRE_DMS_RECOGNIZER_ID,
    coordinates: rows,
    geometryType: inferGeometryType(rows),
    crs: COTE_DIVOIRE_DMS_CRS,
    precisionMode: COTE_DIVOIRE_DMS_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: ["cote_divoire_dms:deterministic_table_roles", COTE_DIVOIRE_DMS_CRS],
  });
}

export async function verifyCoteDivoireDms(normalized = {}) {
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

export function toCoteDivoireDmsKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const coteDivoireDmsRecognizer = createRecognizerContract({
  recognizerId: COTE_DIVOIRE_DMS_RECOGNIZER_ID,
  coordinateType: "cote_divoire_dms",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleCoteDivoireDms,
  recognize: recognizeCoteDivoireDms,
  normalize: normalizeCoteDivoireDms,
  verify: verifyCoteDivoireDms,
});

export default coteDivoireDmsRecognizer;
