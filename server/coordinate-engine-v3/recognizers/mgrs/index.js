import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const MGRS_RECOGNIZER_ID = "mgrs";
export const MGRS_PRECISION_MODE = "mgrs";
export const MGRS_CRS = "EPSG:4326";

const MGRS_BANDS = "CDEFGHJKLMNPQRSTUVWX";
const MGRS_COLUMN_SETS = ["ABCDEFGH", "JKLMNPQR", "STUVWXYZ"];
const MGRS_ROW_SETS = ["ABCDEFGHJKLMNPQRSTUV", "FGHJKLMNPQRSTUVABCDE"];

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function normalizeMgrsText(text) {
  return String(text || "")
    .toUpperCase()
    .replace(/[，,]/g, ",")
    .replace(/[：:]/g, ":")
    .replace(/[。．]/g, ".")
    .replace(/\s+/g, " ")
    .trim();
}

function getMgrsBandRange(band) {
  const index = MGRS_BANDS.indexOf(String(band || "").toUpperCase());
  if (index < 0) return null;
  const min = -80 + index * 8;
  return {
    min,
    max: band === "X" ? 84 : min + 8,
  };
}

function utmToWgs84(zone, easting, northing, northernHemisphere = true) {
  const a = 6378137;
  const e = 0.08181919084262149;
  const e1sq = 0.006739496742276434;
  const k0 = 0.9996;
  const x = Number(easting) - 500000;
  let y = Number(northing);

  if (!northernHemisphere) y -= 10000000;

  const longOrigin = (Number(zone) - 1) * 6 - 180 + 3;
  const m = y / k0;
  const mu = m / (a * (1 - (e ** 2) / 4 - (3 * e ** 4) / 64 - (5 * e ** 6) / 256));
  const e1 = (1 - Math.sqrt(1 - e ** 2)) / (1 + Math.sqrt(1 - e ** 2));
  const j1 = (3 * e1 / 2) - (27 * e1 ** 3 / 32);
  const j2 = (21 * e1 ** 2 / 16) - (55 * e1 ** 4 / 32);
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const fp = mu + j1 * Math.sin(2 * mu) + j2 * Math.sin(4 * mu) + j3 * Math.sin(6 * mu) + j4 * Math.sin(8 * mu);
  const c1 = e1sq * Math.cos(fp) ** 2;
  const t1 = Math.tan(fp) ** 2;
  const n1 = a / Math.sqrt(1 - e ** 2 * Math.sin(fp) ** 2);
  const r1 = a * (1 - e ** 2) / ((1 - e ** 2 * Math.sin(fp) ** 2) ** 1.5);
  const d = x / (n1 * k0);
  const q1 = n1 * Math.tan(fp) / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * e1sq) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * e1sq - 3 * c1 ** 2) * d ** 6 / 720;
  const lat = fp - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * e1sq + 24 * t1 ** 2) * d ** 5 / 120;
  const lon = (q5 - q6 + q7) / Math.cos(fp);

  return {
    latitude: lat * 180 / Math.PI,
    longitude: longOrigin + lon * 180 / Math.PI,
  };
}

function parseMgrsMatch(match = {}) {
  const zone = Number(match.zone);
  const band = String(match.band || "").toUpperCase();
  const gridSquare = String(match.grid || "").toUpperCase();
  let eastingDigits = String(match.east || "");
  let northingDigits = String(match.north || "");

  if (match.digits) {
    const digits = String(match.digits || "");
    if (digits.length % 2 !== 0 || digits.length < 2 || digits.length > 10) return null;
    eastingDigits = digits.slice(0, digits.length / 2);
    northingDigits = digits.slice(digits.length / 2);
  }

  if (!Number.isInteger(zone) || zone < 1 || zone > 60 || !MGRS_BANDS.includes(band)) return null;
  if (!/^[A-HJ-NP-Z]{2}$/.test(gridSquare) || /[IO]/.test(gridSquare)) return null;
  if (!/^\d{1,5}$/.test(eastingDigits) || !/^\d{1,5}$/.test(northingDigits)) return null;
  if (eastingDigits.length !== northingDigits.length) return null;

  const columnSet = MGRS_COLUMN_SETS[(zone - 1) % 3];
  const rowSet = MGRS_ROW_SETS[(zone - 1) % 2];
  const columnIndex = columnSet.indexOf(gridSquare[0]);
  const rowIndex = rowSet.indexOf(gridSquare[1]);
  if (columnIndex < 0 || rowIndex < 0) return null;

  const scale = 10 ** (5 - eastingDigits.length);
  const easting = (columnIndex + 1) * 100000 + Number(eastingDigits) * scale;
  const baseNorthing = rowIndex * 100000 + Number(northingDigits) * scale;
  const range = getMgrsBandRange(band);
  if (!range) return null;

  const northernHemisphere = band >= "N";
  let northing = baseNorthing;
  let converted = null;
  for (let index = 0; index < 6; index += 1) {
    converted = utmToWgs84(zone, easting, northing, northernHemisphere);
    if (converted.latitude >= range.min - 0.000001 && converted.latitude < range.max + 0.000001) break;
    northing += 2000000;
  }
  if (!converted || converted.latitude < range.min - 0.01 || converted.latitude > range.max + 0.01) return null;

  return {
    label: match.label ? String(match.label).toUpperCase() : "",
    sourceValue: `${zone}${band}${gridSquare} ${eastingDigits} ${northingDigits}`,
    zone,
    band,
    gridSquare,
    eastingDigits,
    northingDigits,
    precisionDigits: eastingDigits.length,
    easting,
    northing,
    latitude: converted.latitude,
    longitude: converted.longitude,
    altitude: 0,
  };
}

export function parseMgrsRows(input = {}) {
  const value = normalizeMgrsText(getInputText(input));
  if (!value) return [];
  const rows = [];
  const seen = new Set();
  const separatedPattern = /\b(?:(?<label>[A-Z]|\d{1,3})\s*[:.)|、-]\s*)?(?<zone>0\d|[1-9]|[1-5]\d|6[01])\s*(?<band>[A-Z])\s*(?<grid>[A-Z]{2})\s*(?:[,;\s]\s*)+(?<east>\d{1,6})(?!\d)\s*(?:[,;\s]\s*)+(?<north>\d{1,6})(?!\d)\b/gi;
  const compactPattern = /\b(?:(?<label>[A-Z]|\d{1,3})\s*[:.)|、-]\s*)?(?<zone>0\d|[1-9]|[1-5]\d|6[01])\s*(?<band>[A-Z])\s*(?<grid>[A-Z]{2})\s*(?<digits>\d{2,12})(?!\d)(?!\s+\d)\b/gi;

  for (const pattern of [separatedPattern, compactPattern]) {
    for (const match of value.matchAll(pattern)) {
      const row = parseMgrsMatch(match.groups || {});
      if (!row) continue;
      const key = `${row.zone}${row.band}${row.gridSquare}${row.eastingDigits}${row.northingDigits}`;
      if (seen.has(key)) continue;
      seen.add(key);
      row.label = row.label || String(rows.length + 1);
      rows.push(Object.freeze(row));
    }
  }

  return rows;
}

export function canHandleMgrs(input = {}) {
  return parseMgrsRows(input).length > 0;
}

export async function recognizeMgrs(input = {}, context = {}) {
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

  const rows = parseMgrsRows(input);
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

export function normalizeMgrs(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "mgrs",
    recognizerId: MGRS_RECOGNIZER_ID,
    coordinates: rows.map((row) => ({
      label: row.label,
      latitude: row.latitude,
      longitude: row.longitude,
      altitude: 0,
      sourceValue: row.sourceValue,
      mgrs: {
        zone: row.zone,
        band: row.band,
        gridSquare: row.gridSquare,
        eastingDigits: row.eastingDigits,
        northingDigits: row.northingDigits,
        precisionDigits: row.precisionDigits,
      },
    })),
    crs: MGRS_CRS,
    precisionMode: MGRS_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: ["mgrs:deterministic"],
  });
}

export async function verifyMgrs(normalized = {}) {
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

export function toMgrsKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const mgrsRecognizer = createRecognizerContract({
  recognizerId: MGRS_RECOGNIZER_ID,
  coordinateType: "mgrs",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleMgrs,
  recognize: recognizeMgrs,
  normalize: normalizeMgrs,
  verify: verifyMgrs,
});

export default mgrsRecognizer;
