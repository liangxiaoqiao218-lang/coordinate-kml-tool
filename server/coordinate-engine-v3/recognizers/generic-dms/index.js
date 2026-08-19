import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const GENERIC_DMS_RECOGNIZER_ID = "generic_dms";
export const GENERIC_DMS_PRECISION_MODE = "dms";
export const GENERIC_DMS_CRS = "EPSG:4326";

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function hasStructuredCoordinateTableMetadata(input = {}) {
  if (!input || typeof input !== "object" || typeof input === "string") return false;
  return Array.isArray(input.headers)
    && input.headers.length > 0
    && Array.isArray(input.structuredRows)
    && input.structuredRows.length > 0;
}

function cleanSourceValue(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
}

function normalizeDmsText(value = "") {
  return String(value || "")
    .replace(/[，]/g, ",")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, "\"")
    .replace(/\bNorth\b|\bNord\b/gi, "N")
    .replace(/\bSouth\b|\bSud\b/gi, "S")
    .replace(/\bEast\b|\bEst\b/gi, "E")
    .replace(/\bWest\b|\bOuest\b/gi, "W")
    .replace(/\bO\b/gi, "W");
}

function splitRows(text = "") {
  return String(text || "")
    .split(/\r?\n|;/)
    .map((row) => row.trim())
    .filter(Boolean);
}

function splitPossibleHeaderCells(line = "") {
  const value = String(line || "").trim();
  if (!value) return [];
  if (/[|\t,]/.test(value)) {
    return value.split(/[|\t,]/).map((item) => item.trim()).filter(Boolean);
  }
  const wideColumns = value.split(/\s{2,}/).map((item) => item.trim()).filter(Boolean);
  if (wideColumns.length > 1) return wideColumns;
  return value.split(/\s+/).map((item) => item.trim()).filter(Boolean);
}

function classifyHeaderCell(cell = "") {
  const normalized = normalizeDmsText(cell)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[.。:：()[\]{}]/g, "")
    .toLowerCase()
    .trim();
  if (/^(point|points|pt|id|no|n°|num|number|label|编号|点号)$/.test(normalized)) return "label";
  if (/^(latitude|lat|n|s|nord|north|sud|south|纬度|北纬|南纬)$/.test(normalized)) return "latitude";
  if (/^(longitude|lon|lng|e|w|o|est|east|ouest|west|经度|东经|西经)$/.test(normalized)) return "longitude";
  if (/^x$/.test(normalized)) return "x";
  if (/^y$/.test(normalized)) return "y";
  return "";
}

function hasGeographicDmsTableHeader(text = "") {
  const headerRegion = splitRows(text).slice(0, 8);
  return headerRegion.some((row) => {
    if (/[°º˚'′"″]/.test(row)) return false;
    const hasColumnStructure = /[|\t,]/.test(row)
      || row.split(/\s{2,}/).filter(Boolean).length > 1
      || splitPossibleHeaderCells(row).length <= 8;
    if (!hasColumnStructure) return false;

    const roles = splitPossibleHeaderCells(row).map(classifyHeaderCell).filter(Boolean);
    const unknownCount = splitPossibleHeaderCells(row).length - roles.length;
    const hasPointHeader = roles.includes("label");
    const hasLatitudeHeader = roles.includes("latitude");
    const hasLongitudeHeader = roles.includes("longitude");
    const hasX = roles.includes("x");
    const hasY = roles.includes("y");
    const isMostlyHeaderCells = roles.length >= 2 && unknownCount <= 1;

    if (!isMostlyHeaderCells) return false;
    if (hasLatitudeHeader && hasLongitudeHeader) return true;
    if (hasX && hasY && (hasLatitudeHeader || hasLongitudeHeader)) return true;
    if (hasPointHeader && ((hasLatitudeHeader && hasLongitudeHeader) || (hasX && hasY))) return true;
    return false;
  });
}

function extractLabel(line = "") {
  const match = String(line || "").match(/^\s*(?:point|pt|ponto|sommet|vertex)?\s*([A-Za-z]|\d{1,3})\s*[:.)|、-]\s+/i);
  if (!match) return { label: "", body: line };
  return {
    label: match[1].toUpperCase(),
    body: line.slice(match[0].length),
  };
}

function normalizeHemisphere(value = "") {
  const direction = String(value || "").trim().toUpperCase();
  if (direction === "O") return "W";
  if (["N", "S", "E", "W"].includes(direction)) return direction;
  return "";
}

function getRole(direction = "") {
  const normalized = normalizeHemisphere(direction);
  if (["N", "S"].includes(normalized)) return "latitude";
  if (["E", "W"].includes(normalized)) return "longitude";
  return "";
}

function parseNumberParts(body = "") {
  const numbers = String(body || "").replace(/,/g, ".").match(/[-+]?\d+(?:\.\d+)?/g) || [];
  if (numbers.length < 3) return null;

  let degrees = numbers[0];
  let minutes = numbers[1];
  let seconds = numbers[2];

  if (numbers.length >= 4) {
    seconds = `${numbers[2]}.${numbers.slice(3).join("")}`;
  } else if (numbers.length === 3 && numbers[1].includes(".") && !numbers[2].includes(".")) {
    const [minutePart, secondPart] = numbers[1].split(".");
    minutes = minutePart;
    seconds = `${secondPart}.${numbers[2]}`;
  }

  return { degrees, minutes, seconds };
}

export function dmsToDecimal({ degrees, minutes, seconds, hemisphere } = {}) {
  const direction = normalizeHemisphere(hemisphere);
  const degreeValue = Math.abs(Number(degrees));
  const minuteValue = Number(minutes);
  const secondValue = Number(String(seconds ?? "").replace(",", "."));
  if (!Number.isFinite(degreeValue)
    || !Number.isFinite(minuteValue)
    || !Number.isFinite(secondValue)
    || minuteValue < 0
    || minuteValue >= 60
    || secondValue < 0
    || secondValue >= 60
    || !["N", "S", "E", "W"].includes(direction)) {
    return null;
  }

  const limit = ["N", "S"].includes(direction) ? 90 : 180;
  if (degreeValue > limit) return null;

  const sign = ["S", "W"].includes(direction) ? -1 : 1;
  return sign * (degreeValue + (minuteValue / 60) + (secondValue / 3600));
}

export function parseDmsTokens(text = "") {
  const source = normalizeDmsText(text);
  const pattern = /(?<body>[-+]?\d{1,3}\s*(?:°\s*)?\d{1,2}(?:[.,]\d{1,2})?(?:\s*['"]?\s*|\s+|\.)\d{1,2}(?:[.,]\d+)?(?:\s+\d{1,2})?)\s*["]?\s*(?<hemisphere>[NSEW])\b/gi;
  const prefixPattern = /(?:^|[\r\n])\s*(?<hemisphere>[NSEW])\s+(?<body>[-+]?\d{1,3}\s*(?:°\s*)?\d{1,2}(?:[.,]\d{1,2})?(?:\s*['"]?\s*|\s+|\.)\d{1,2}(?:[.,]\d+)?(?:\s+\d{1,2})?)\s*["]?/gi;
  const tokens = [];
  function pushToken(match, rawValue = match[0]) {
    const parts = parseNumberParts(match.groups?.body || "");
    if (!parts) return;
    const hemisphere = normalizeHemisphere(match.groups?.hemisphere);
    const decimal = dmsToDecimal({ ...parts, hemisphere });
    const role = getRole(hemisphere);
    tokens.push(Object.freeze({
      raw: cleanSourceValue(rawValue),
      degrees: String(Math.abs(Number(parts.degrees))),
      minutes: String(Number(parts.minutes)),
      seconds: String(Number(parts.seconds)),
      hemisphere,
      role,
      decimal,
      valid: decimal !== null && Boolean(role),
    }));
  }
  for (const match of source.matchAll(pattern)) {
    pushToken(match);
  }
  for (const match of source.matchAll(prefixPattern)) {
    pushToken(match, `${match.groups?.body || ""} ${match.groups?.hemisphere || ""}`);
  }
  return tokens;
}

function buildPointFromTokens(tokens = [], label = "", sourceValue = "") {
  const validTokens = tokens.filter((token) => token.valid === true);
  const latitudeTokens = validTokens.filter((token) => token.role === "latitude");
  const longitudeTokens = validTokens.filter((token) => token.role === "longitude");
  if (validTokens.length !== 2 || latitudeTokens.length !== 1 || longitudeTokens.length !== 1) return null;
  const latitude = latitudeTokens[0].decimal;
  const longitude = longitudeTokens[0].decimal;
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return Object.freeze({
    label,
    latitude,
    longitude,
    altitude: 0,
    sourceValue: cleanSourceValue(sourceValue || `${latitudeTokens[0].raw} ${longitudeTokens[0].raw}`),
    tokens: Object.freeze(validTokens),
  });
}

export function parseGenericDmsRows(input = {}) {
  if (hasStructuredCoordinateTableMetadata(input)) return [];
  const text = getInputText(input);
  if (!text) return [];
  if (hasGeographicDmsTableHeader(text)) return [];
  const rows = [];
  const pendingSingles = [];
  const sourceRows = splitRows(text);

  for (const sourceRow of sourceRows) {
    const { label, body } = extractLabel(sourceRow);
    const tokens = parseDmsTokens(body);
    const rowPoint = buildPointFromTokens(tokens, label, body);
    if (rowPoint) {
      rows.push(rowPoint);
      continue;
    }

    if (tokens.filter((token) => token.valid === true).length === 1) {
      pendingSingles.push({
        label,
        source: body,
        token: tokens.find((token) => token.valid === true),
      });
      if (pendingSingles.length >= 2) {
        const pair = pendingSingles.splice(0, 2);
        const point = buildPointFromTokens(pair.map((item) => item.token), pair.find((item) => item.label)?.label || "", pair.map((item) => item.source).join(" "));
        if (point) rows.push(point);
      }
    } else {
      pendingSingles.length = 0;
    }
  }

  return rows.map((row, index) => Object.freeze({
    ...row,
    label: row.label || String(index + 1),
  }));
}

export function canHandleGenericDms(input = {}) {
  if (hasStructuredCoordinateTableMetadata(input)) return false;
  return parseGenericDmsRows(input).length > 0;
}

export async function recognizeGenericDms(input = {}, context = {}) {
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

  const rows = parseGenericDmsRows(input);
  return Object.freeze({
    handled: rows.length > 0,
    status: rows.length ? "accepted" : "not_handled",
    rows: Object.freeze(rows),
    warnings: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeGenericDms(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "generic_dms",
    recognizerId: GENERIC_DMS_RECOGNIZER_ID,
    coordinates: rows.map((row) => ({
      label: row.label,
      latitude: row.latitude,
      longitude: row.longitude,
      altitude: 0,
      sourceValue: row.sourceValue,
    })),
    crs: GENERIC_DMS_CRS,
    precisionMode: GENERIC_DMS_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: ["generic_dms:deterministic"],
  });
}

export async function verifyGenericDms(normalized = {}) {
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

export function toGenericDmsKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const genericDmsRecognizer = createRecognizerContract({
  recognizerId: GENERIC_DMS_RECOGNIZER_ID,
  coordinateType: "generic_dms",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleGenericDms,
  recognize: recognizeGenericDms,
  normalize: normalizeGenericDms,
  verify: verifyGenericDms,
});

export default genericDmsRecognizer;
