import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const DMS_GROUPED_COORDINATES_RECOGNIZER_ID = "dms_grouped_coordinates";
export const DMS_GROUPED_COORDINATES_PRECISION_MODE = "dms-grouped-coordinates";
export const DMS_GROUPED_COORDINATES_CRS = "EPSG:4326";

function cleanString(value, fallback = "") {
  const text = String(value ?? "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text || fallback;
}

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[，]/g, ",")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, "\"")
    .replace(/\bNorth\b|\bNord\b/gi, "N")
    .replace(/\bSouth\b|\bSud\b/gi, "S")
    .replace(/\bEast\b|\bEst\b/gi, "E")
    .replace(/\bWest\b|\bOuest\b/gi, "W")
    .replace(/\bO\b/gi, "W")
    .replace(/\u00a0/g, " ");
}

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function getStructuredRows(input = {}) {
  if (Array.isArray(input)) return input;
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.tableRows)) return input.tableRows;
  if (Array.isArray(input.structuredRows)) return input.structuredRows;
  return [];
}

function getDocumentCues(input = {}) {
  if (!input || typeof input !== "object") return [];
  return Array.isArray(input.documentCues) ? input.documentCues.map((cue) => cleanString(cue)).filter(Boolean) : [];
}

function hasGroupedDmsContext(text = "", cues = []) {
  const value = normalizeText(`${cues.join("\n")}\n${text}`);
  return /\bmining\s+area\b|\barea\s+(?:one|two|three|\d+)\b|\bcoordinates?\s+are\s+as\s+follows\b/i.test(value)
    || (/[°'"]/.test(value) && /\b1\s*[.)]\s*[\d\s°'".,]+[NS]\b[\s\S]{0,700}\b1\s*[.)]\s*[\d\s°'".,]+[NS]\b/i.test(value));
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

function dmsToDecimal({ degrees, minutes, seconds, hemisphere } = {}) {
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

function parseDmsTokens(text = "") {
  const source = normalizeText(text);
  const pattern = /(?<body>[-+]?\d{1,3}\s*(?:°\s*)?\d{1,2}(?:[.,]\d{1,2})?(?:\s*['"]?\s*|\s+|\.)\d{1,2}(?:[.,]\d+)?(?:\s+\d{1,2})?)\s*["]?\s*(?<hemisphere>[NSEW])\b/gi;
  const tokens = [];
  for (const match of source.matchAll(pattern)) {
    const parts = parseNumberParts(match.groups?.body || "");
    if (!parts) continue;
    const hemisphere = normalizeHemisphere(match.groups?.hemisphere);
    const decimal = dmsToDecimal({ ...parts, hemisphere });
    const role = getRole(hemisphere);
    tokens.push(Object.freeze({
      raw: cleanString(match[0]).slice(0, 160),
      index: match.index ?? 0,
      degrees: String(Math.abs(Number(parts.degrees))),
      minutes: String(Number(parts.minutes)),
      seconds: String(Number(parts.seconds)),
      hemisphere,
      role,
      decimal,
      valid: decimal !== null && Boolean(role),
    }));
  }
  return tokens;
}

function extractLabelBefore(text = "", index = 0, fallback = "") {
  const prefix = String(text || "").slice(Math.max(0, index - 80), index);
  const match = prefix.match(/(?:^|[\s:;,(])(?:point|pt)?\s*([A-Za-z]|\d{1,3})\s*[.)、:-]\s*$/i)
    || prefix.match(/(?:^|\s)([A-Za-z]|\d{1,3})\s*$/i);
  return match ? match[1].toUpperCase() : fallback;
}

function buildPointFromTokens(tokens = [], label = "", sourceValue = "", groupIndex = 0, pointIndex = 0) {
  const validTokens = tokens.filter((token) => token.valid === true);
  const latitudeTokens = validTokens.filter((token) => token.role === "latitude");
  const longitudeTokens = validTokens.filter((token) => token.role === "longitude");
  if (validTokens.length !== 2 || latitudeTokens.length !== 1 || longitudeTokens.length !== 1) return null;
  const latitude = latitudeTokens[0].decimal;
  const longitude = longitudeTokens[0].decimal;
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return Object.freeze({
    label: cleanString(label, String(pointIndex + 1)),
    latitude,
    longitude,
    altitude: 0,
    sourceValue: cleanString(sourceValue || `${latitudeTokens[0].raw} ${longitudeTokens[0].raw}`).slice(0, 240),
    groupId: `group_${groupIndex + 1}`,
    groupIndex: groupIndex + 1,
    pointIndex: pointIndex + 1,
  });
}

function parseCoordinateLine(line = "", groupIndex = 0, existingCount = 0) {
  const tokens = parseDmsTokens(line).filter((token) => token.valid === true);
  const points = [];
  for (let index = 0; index + 1 < tokens.length; index += 2) {
    const pair = [tokens[index], tokens[index + 1]];
    const label = extractLabelBefore(line, pair[0].index, String(existingCount + points.length + 1));
    const sourceStart = Math.max(0, pair[0].index - 24);
    const sourceEnd = Math.min(String(line).length, pair[1].index + pair[1].raw.length + 8);
    const point = buildPointFromTokens(pair, label, String(line).slice(sourceStart, sourceEnd), groupIndex, existingCount + points.length);
    if (point) points.push(point);
  }
  return points;
}

function extractLeadingLabelNumber(line = "") {
  const match = String(line || "").match(/^\s*(?:point|pt)?\s*(\d{1,3})\s*[.)、:-]\s+/i);
  return match ? Number(match[1]) : null;
}

function splitGroupedTextLines(text = "") {
  return normalizeText(text)
    .replace(/\b(Mining\s+Area(?:\s+(?:One|Two|Three|\d+))?)/gi, "\n$1")
    .replace(/(\s)(\d{1,3}\s*[.)]\s+(?=\d{1,3}\s*°))/g, "\n$2")
    .split(/\r?\n/)
    .map((line) => line.trim());
}

function parseTextGroups(input = {}) {
  const text = getInputText(input);
  const cues = getDocumentCues(input);
  if (!text || !hasGroupedDmsContext(text, cues)) return { groups: [], reason: "grouped_context_missing" };
  const groups = [];
  let currentGroup = [];
  let previousLabelNumber = null;
  let reason = "";

  function closeGroup(closeReason = "") {
    if (currentGroup.length > 0) {
      groups.push(currentGroup);
      currentGroup = [];
      previousLabelNumber = null;
      reason = reason || closeReason;
    }
  }

  for (const line of splitGroupedTextLines(text)) {
    if (!line) {
      closeGroup("blank_line");
      continue;
    }
    if (/\bmining\s+area\b|\barea\s+(?:one|two|three|\d+)\b|\bcoordinates?\s+are\s+as\s+follows\b/i.test(line)) {
      closeGroup("title_context");
    }
    const labelNumber = extractLeadingLabelNumber(line);
    if (currentGroup.length > 0
      && Number.isInteger(labelNumber)
      && Number.isInteger(previousLabelNumber)
      && labelNumber <= previousLabelNumber) {
      closeGroup("number_restart");
    }
    const points = parseCoordinateLine(line, groups.length, currentGroup.length);
    if (!points.length) continue;
    currentGroup.push(...points);
    previousLabelNumber = Number.isInteger(labelNumber)
      ? labelNumber
      : Number(previousLabelNumber ?? currentGroup.length);
  }
  closeGroup(reason || "single_group");
  const validGroups = groups.filter((group) => group.length >= 3);
  if (validGroups.length < 2) return { groups: [], reason: "multiple_groups_not_detected" };
  return { groups: validGroups, reason: reason || "grouped_dms_context" };
}

function rowToText(row = {}) {
  if (Array.isArray(row)) return row.map((cell) => cleanString(cell)).filter(Boolean).join(" ");
  if (!row || typeof row !== "object") return cleanString(row);
  if (Array.isArray(row.cells)) {
    return [
      row.label ?? row.point ?? row.no ?? row.number,
      ...row.cells,
    ].map((cell) => cleanString(cell)).filter(Boolean).join(" ");
  }
  return Object.values(row)
    .flatMap((value) => (Array.isArray(value) ? value : [value]))
    .map((cell) => cleanString(cell))
    .filter(Boolean)
    .join(" ");
}

function getRowGroupKey(row = {}) {
  if (!row || typeof row !== "object" || Array.isArray(row)) return "";
  return cleanString(row.group || row.groupId || row.groupLabel || row.section || row.area || row.miningArea);
}

function parseStructuredGroups(input = {}) {
  const rows = getStructuredRows(input);
  if (!rows.length) return { groups: [], reason: "structured_rows_absent" };
  const groups = [];
  let currentGroup = [];
  let currentKey = "";
  let previousLabelNumber = null;
  let reason = "";

  function closeGroup(closeReason = "") {
    if (currentGroup.length > 0) {
      groups.push(currentGroup);
      currentGroup = [];
      previousLabelNumber = null;
      reason = reason || closeReason;
    }
  }

  rows.forEach((row) => {
    const groupKey = getRowGroupKey(row);
    if (groupKey && currentKey && groupKey !== currentKey) closeGroup("group_metadata");
    if (groupKey) currentKey = groupKey;
    const text = rowToText(row);
    const labelNumber = extractLeadingLabelNumber(text);
    if (currentGroup.length > 0
      && Number.isInteger(labelNumber)
      && Number.isInteger(previousLabelNumber)
      && labelNumber <= previousLabelNumber) {
      closeGroup("number_restart");
    }
    const points = parseCoordinateLine(text, groups.length, currentGroup.length);
    if (!points.length) return;
    currentGroup.push(...points);
    previousLabelNumber = Number.isInteger(labelNumber)
      ? labelNumber
      : Number(previousLabelNumber ?? currentGroup.length);
  });
  closeGroup(reason || "single_group");
  const validGroups = groups.filter((group) => group.length >= 3);
  if (validGroups.length < 2) return { groups: [], reason: "structured_multiple_groups_not_detected" };
  return { groups: validGroups, reason: reason || "structured_grouped_rows" };
}

export function parseDmsGroupedCoordinates(input = {}) {
  const structured = parseStructuredGroups(input);
  if (structured.groups.length >= 2) return structured;
  return parseTextGroups(input);
}

export function canHandleDmsGroupedCoordinates(input = {}) {
  return parseDmsGroupedCoordinates(input).groups.length >= 2;
}

export async function recognizeDmsGroupedCoordinates(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      groups: Object.freeze([]),
      rows: Object.freeze([]),
      warnings: Object.freeze([createWarningMetadata({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic grouped DMS parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }
  const parsed = parseDmsGroupedCoordinates(input);
  const rows = parsed.groups.flat();
  return Object.freeze({
    handled: parsed.groups.length >= 2,
    status: parsed.groups.length >= 2 ? "accepted" : "not_handled",
    groups: Object.freeze(parsed.groups.map((group) => Object.freeze(group.map(Object.freeze)))),
    rows: Object.freeze(rows.map(Object.freeze)),
    warnings: Object.freeze([]),
    reason: parsed.reason,
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeDmsGroupedCoordinates(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "dms_grouped_coordinates",
    recognizerId: DMS_GROUPED_COORDINATES_RECOGNIZER_ID,
    coordinates: rows,
    geometryType: "multipolygon",
    crs: DMS_GROUPED_COORDINATES_CRS,
    precisionMode: DMS_GROUPED_COORDINATES_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: [
      "dms_grouped_coordinates:deterministic_grouped_dms",
      `groups:${Array.isArray(result.groups) ? result.groups.length : 0}`,
      DMS_GROUPED_COORDINATES_CRS,
    ],
  });
}

function inferGroupCountFromLabels(coordinates = []) {
  let groupCount = coordinates.length > 0 ? 1 : 0;
  let previousNumber = null;
  for (const point of coordinates) {
    const number = Number(String(point.label || "").match(/\d+/)?.[0]);
    if (Number.isInteger(number) && Number.isInteger(previousNumber) && number <= previousNumber) {
      groupCount += 1;
    }
    if (Number.isInteger(number)) previousNumber = number;
  }
  return groupCount;
}

export async function verifyDmsGroupedCoordinates(normalized = {}) {
  const coordinates = Array.isArray(normalized.coordinates) ? normalized.coordinates : [];
  const groupCount = inferGroupCountFromLabels(coordinates);
  const invalid = coordinates.filter((point) => (
    !Number.isFinite(Number(point.latitude))
    || !Number.isFinite(Number(point.longitude))
    || Number(point.latitude) < -90
    || Number(point.latitude) > 90
    || Number(point.longitude) < -180
    || Number(point.longitude) > 180
  ));
  return Object.freeze({
    verified: invalid.length === 0 && coordinates.length > 0 && groupCount >= 2,
    status: invalid.length === 0 && coordinates.length > 0 && groupCount >= 2 ? "pass" : "failed",
    groupCount,
    pointCount: coordinates.length,
    invalidPointLabels: Object.freeze(invalid.map((point) => point.label).filter(Boolean)),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function toDmsGroupedCoordinatesKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const dmsGroupedCoordinatesRecognizer = createRecognizerContract({
  recognizerId: DMS_GROUPED_COORDINATES_RECOGNIZER_ID,
  coordinateType: "dms_grouped_coordinates",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleDmsGroupedCoordinates,
  recognize: recognizeDmsGroupedCoordinates,
  normalize: normalizeDmsGroupedCoordinates,
  verify: verifyDmsGroupedCoordinates,
});

export default dmsGroupedCoordinatesRecognizer;
