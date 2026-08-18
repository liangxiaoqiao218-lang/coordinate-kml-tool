import { hasExplicitUtmWgs84Context, parseIndonesiaUtmCrs } from "./crs.js";

const PROJECTED_NUMBER_PATTERN = /^[+-]?\d+(?:[.,]\d+)?$/;

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function getStructuredRows(input = {}) {
  if (Array.isArray(input)) return input;
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.tableRows)) return input.tableRows;
  if (Array.isArray(input.structuredRows)) return input.structuredRows;
  if (Array.isArray(input.indonesiaUtmRows)) return input.indonesiaUtmRows;
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
    .replace(/[.。]/g, "")
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
    return value.split(/[|\t;]/).map((item) => item.trim()).filter(Boolean);
  }
  const wide = value.split(/\s{2,}/).map((item) => item.trim()).filter(Boolean);
  if (wide.length > 1) return wide;
  return value.split(/\s+/).map((item) => item.trim()).filter(Boolean);
}

function getHeaderRole(header = "") {
  const normalized = normalizeHeader(header);
  if (/^(no|nomor|number|point|pt|id|label|titik|n)$/.test(normalized)) return "label";
  if (/^(x|easting|east|utm x|koordinat x)$/.test(normalized)) return "x";
  if (/^(y|northing|north|utm y|koordinat y)$/.test(normalized)) return "y";
  if (/^(latitude|lat|lintang)$/.test(normalized)) return "latitude";
  if (/^(longitude|lon|lng|bujur)$/.test(normalized)) return "longitude";
  return "";
}

function classifyHeaders(headers = []) {
  return headers.map((header, index) => Object.freeze({
    index,
    source: String(header || "").trim(),
    role: getHeaderRole(header) || "unknown",
  }));
}

export function resolveIndonesiaUtmHeaderMapping(headers = []) {
  const classified = classifyHeaders(headers);
  const xColumn = classified.find((header) => header.role === "x") || null;
  const yColumn = classified.find((header) => header.role === "y") || null;
  const labelColumn = classified.find((header) => header.role === "label") || null;
  const latitudeColumn = classified.find((header) => header.role === "latitude") || null;
  const longitudeColumn = classified.find((header) => header.role === "longitude") || null;
  if (!xColumn || !yColumn) {
    return Object.freeze({ accepted: false, reason: "projected_xy_header_unresolved", headers: Object.freeze(classified) });
  }
  if (xColumn.index === yColumn.index) {
    return Object.freeze({ accepted: false, reason: "same_column_for_x_y", headers: Object.freeze(classified) });
  }
  return Object.freeze({
    accepted: true,
    labelColumn,
    xColumn,
    yColumn,
    latitudeColumn,
    longitudeColumn,
    headers: Object.freeze(classified),
  });
}

export function parseLocalizedProjectedNumber(value = "") {
  const raw = String(value ?? "").normalize("NFKC").trim().replace(/\s+/g, "");
  const normalized = raw.replace(/,/g, ".");
  if (!PROJECTED_NUMBER_PATTERN.test(raw) || !/^[+-]?\d+(?:\.\d+)?$/.test(normalized)) return null;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function isValidProjectedPair(easting, northing) {
  return Number.isFinite(easting)
    && Number.isFinite(northing)
    && easting >= 100000
    && easting <= 900000
    && northing >= 0
    && northing <= 10000000;
}

export function parseDmsReference(value = "", axis = "") {
  const raw = normalizeText(value).trim();
  if (!raw) return null;
  const decimalCandidate = raw.replace(/,/g, ".");
  if (/^[+-]?\d+(?:\.\d+)?$/.test(decimalCandidate)) {
    const numeric = Number(decimalCandidate);
    const limit = axis === "latitude" ? 90 : 180;
    return Number.isFinite(numeric) && Math.abs(numeric) <= limit ? numeric : null;
  }
  const directionMatch = raw.match(/\b([NSEW])\b/i);
  const direction = directionMatch ? directionMatch[1].toUpperCase() : "";
  if (axis === "latitude" && direction && !/[NS]/.test(direction)) return null;
  if (axis === "longitude" && direction && !/[EW]/.test(direction)) return null;
  const numbers = raw
    .replace(/,/g, ".")
    .match(/[+-]?\d+(?:\.\d+)?/g)
    ?.map(Number)
    .filter(Number.isFinite) || [];
  if (numbers.length < 1 || numbers.length > 3) return null;
  const degrees = Math.abs(numbers[0]);
  const minutes = Math.abs(numbers[1] || 0);
  const seconds = Math.abs(numbers[2] || 0);
  if (minutes >= 60 || seconds >= 60) return null;
  const limit = axis === "latitude" ? 90 : 180;
  if (degrees > limit) return null;
  const negative = numbers[0] < 0 || direction === "S" || direction === "W";
  const decimal = degrees + minutes / 60 + seconds / 3600;
  return negative ? -decimal : decimal;
}

function parseRowCells(cells = [], mapping, rowIndex) {
  const label = mapping.labelColumn
    ? String(cells[mapping.labelColumn.index] ?? "").trim()
    : String(rowIndex + 1);
  const easting = parseLocalizedProjectedNumber(cells[mapping.xColumn.index]);
  const northing = parseLocalizedProjectedNumber(cells[mapping.yColumn.index]);
  if (!isValidProjectedPair(easting, northing)) return null;
  const latitudeText = mapping.latitudeColumn ? String(cells[mapping.latitudeColumn.index] ?? "").trim() : "";
  const longitudeText = mapping.longitudeColumn ? String(cells[mapping.longitudeColumn.index] ?? "").trim() : "";
  return Object.freeze({
    index: rowIndex,
    point: label || String(rowIndex + 1),
    easting,
    northing,
    xText: String(cells[mapping.xColumn.index] ?? "").trim(),
    yText: String(cells[mapping.yColumn.index] ?? "").trim(),
    latitudeText,
    longitudeText,
    referenceLatitude: parseDmsReference(latitudeText, "latitude"),
    referenceLongitude: parseDmsReference(longitudeText, "longitude"),
    sourceValue: cleanSourceValue(cells.join(" | ")),
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
  const mapping = resolveIndonesiaUtmHeaderMapping(headers);
  const sourceText = getInputText(input);
  const crs = parseIndonesiaUtmCrs(`${sourceText} ${String(input.crsText || input.crs || "")}`);
  if (!mapping.accepted) return { status: "not_handled", rows: [], crs, mapping, reason: mapping.reason };
  const parsedRows = rows
    .map((row, rowIndex) => parseRowCells(headers.map((header) => row?.[header]), mapping, rowIndex))
    .filter(Boolean);
  return {
    status: parsedRows.length ? "accepted" : "rejected",
    rows: parsedRows,
    crs,
    mapping,
    reason: parsedRows.length ? "indonesia_utm_structured_rows_accepted" : "no_valid_projected_rows",
  };
}

function parseTextTable(input = {}) {
  const text = getInputText(input);
  const sourceRows = splitTextRows(text);
  const crs = parseIndonesiaUtmCrs(text);
  if (!text || !hasExplicitUtmWgs84Context(text)) {
    return { status: "not_handled", rows: [], crs, mapping: null, reason: "missing_explicit_wgs84_utm_context" };
  }
  const headerSearchLimit = Math.min(sourceRows.length - 1, 12);
  let headerIndex = -1;
  let mapping = null;
  for (let index = 0; index < headerSearchLimit; index += 1) {
    const candidate = resolveIndonesiaUtmHeaderMapping(splitTableLine(sourceRows[index]));
    if (candidate.accepted) {
      headerIndex = index;
      mapping = candidate;
      break;
    }
  }
  if (!mapping?.accepted) return { status: "not_handled", rows: [], crs, mapping, reason: "projected_xy_header_unresolved" };
  const parsedRows = sourceRows.slice(headerIndex + 1)
    .map((line, rowIndex) => parseRowCells(splitTableLine(line), mapping, rowIndex))
    .filter(Boolean);
  return {
    status: parsedRows.length ? "accepted" : "rejected",
    rows: parsedRows,
    crs,
    mapping,
    reason: parsedRows.length ? "indonesia_utm_text_table_accepted" : "no_valid_projected_rows",
  };
}

export function parseIndonesiaUtmTable(input = {}) {
  const structured = parseStructuredRows(input);
  if (structured) return structured;
  return parseTextTable(input);
}

export function canHandleIndonesiaUtm(input = {}) {
  const parsed = parseIndonesiaUtmTable(input);
  return parsed.rows.length > 0 && parsed.crs?.hasUtmWgs84 === true;
}
