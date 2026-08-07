import { buildShadowTypedUtmResult } from "./typed-result.js";
import { transformUtmWgs84Point } from "./utm-wgs84-transform.js";

export const STRUCTURED_UTM_TABLE_PROMPT = `Read ONLY the structured coordinate table in this image.

This pass is used only after separate CRS evidence has confirmed a WGS84 UTM zone and hemisphere.

Target columns:
- point / No.;
- X / Easting;
- Y / Northing;
- optional Latitude;
- optional Longitude.

Rules:
- Read X and Y from the same table row.
- Preserve every visible row in point order.
- X/Y are the primary projected coordinates.
- Latitude/Longitude are reference values only. Copy them literally; do not convert or correct them.
- Do not infer missing values from the country, map location, coordinate ranges, or neighboring rows.
- Do not read map-frame ticks, scale values, areas, pixel positions, or OCR bounding boxes.
- If no table with at least three readable X/Y rows exists, return status none.

Return strict JSON only, without markdown:
{
  "status": "observed" | "none",
  "rows": [
    {
      "point": "literal point label",
      "x": "literal X value",
      "y": "literal Y value",
      "latitude": "literal latitude text or empty string",
      "longitude": "literal longitude text or empty string"
    }
  ]
}`;

export const PROJECTED_XY_ONLY_PROMPT = `Read ONLY the point, X, and Y columns from the structured coordinate table in this image.

Rules:
- Ignore Latitude and Longitude completely.
- Read X and Y from the same horizontal row.
- Preserve all visible rows in point order.
- Do not read map-frame ticks, scale values, areas, pixel positions, or OCR boxes.
- Do not infer or transform values.

Return strict JSON only:
{
  "status": "observed" | "none",
  "rows": [
    { "point": "literal point label", "x": "literal X", "y": "literal Y", "latitude": "", "longitude": "" }
  ]
}`;

function stripMarkdownFence(value = "") {
  return String(value || "")
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/, "")
    .trim();
}

function parseJsonObject(value = "") {
  const clean = stripMarkdownFence(value);
  try {
    return JSON.parse(clean);
  } catch {
    const start = clean.indexOf("{");
    const end = clean.lastIndexOf("}");
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(clean.slice(start, end + 1));
    } catch {
      return null;
    }
  }
}

function parseLocalizedNumber(value) {
  const normalized = String(value ?? "")
    .normalize("NFKC")
    .replace(/\s+/g, "")
    .replace(/,/g, ".")
    .replace(/[^0-9.+-]/g, "");
  if (!normalized || !/^[+-]?\d+(?:\.\d+)?$/.test(normalized)) return null;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function parseDmsReference(value, axis) {
  const raw = String(value || "").normalize("NFKC").trim();
  if (!raw) return null;
  const directionMatch = raw.match(/[NSEW]/i);
  const direction = directionMatch ? directionMatch[0].toUpperCase() : "";
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

function normalizeRow(value, index) {
  if (!value || typeof value !== "object") return null;
  const easting = parseLocalizedNumber(value.x ?? value.easting);
  const northing = parseLocalizedNumber(value.y ?? value.northing);
  if (!Number.isFinite(easting) || !Number.isFinite(northing)) return null;
  if (easting < 100000 || easting > 900000 || northing < 0 || northing > 10000000) return null;
  const latitudeText = String(value.latitude ?? value.lat ?? "").trim();
  const longitudeText = String(value.longitude ?? value.lon ?? "").trim();
  return {
    index,
    point: String(value.point ?? value.label ?? index + 1).trim() || String(index + 1),
    easting,
    northing,
    xText: String(value.x ?? value.easting ?? easting).trim(),
    yText: String(value.y ?? value.northing ?? northing).trim(),
    latitudeText,
    longitudeText,
    referenceLatitude: parseDmsReference(latitudeText, "latitude"),
    referenceLongitude: parseDmsReference(longitudeText, "longitude")
  };
}

export function parseStructuredUtmTableModelText(modelText = "") {
  const parsed = parseJsonObject(modelText);
  const rows = (Array.isArray(parsed?.rows) ? parsed.rows : [])
    .map(normalizeRow)
    .filter(Boolean);
  return {
    status: rows.length >= 3 ? "observed" : "none",
    rows,
    rawModelText: String(modelText || "")
  };
}

export async function runStructuredUtmTablePass({ imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") {
    throw new TypeError("runStructuredUtmTablePass requires an invokeVision function");
  }
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runStructuredUtmTablePass requires at least one image item");
  }
  const modelText = await invokeVision({ prompt: STRUCTURED_UTM_TABLE_PROMPT, imageItems });
  return parseStructuredUtmTableModelText(modelText);
}

export async function runProjectedXyOnlyPass({ imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") throw new TypeError("runProjectedXyOnlyPass requires an invokeVision function");
  const modelText = await invokeVision({ prompt: PROJECTED_XY_ONLY_PROMPT, imageItems });
  return parseStructuredUtmTableModelText(modelText);
}

export function buildStructuredUtmTableRetryPrompt(priority = {}) {
  const mismatchedPoints = (priority.transformationVerification?.rows || [])
    .filter(row => row.status === "mismatch")
    .map(row => row.point);
  return `Zoom in on the coordinate table and re-read ONLY point(s): ${mismatchedPoints.join(", ") || "unknown"}.

The first literal transcription failed an independent UTM-to-WGS84 comparison. This retry is still transcription, not correction.

Rules:
- Return only the listed point rows.
- Read X and Y from the same row.
- Copy Latitude and Longitude literally as independent reference fields.
- Inspect every digit at maximum visual attention. Common visual confusions include 807/087, 980/880, and 775/795.
- Do not derive X/Y from Latitude/Longitude and do not change any value merely to force a match.
- Do not infer from country, coordinate range, or neighboring rows.

Return strict JSON only:
{
  "status": "observed" | "none",
  "rows": [
    { "point": "listed point", "x": "literal X", "y": "literal Y", "latitude": "literal latitude", "longitude": "literal longitude" }
  ]
}`;
}

export function buildProjectedXyRetryPrompt(priority = {}) {
  const mismatchedPoints = (priority.transformationVerification?.rows || [])
    .filter(row => row.status === "mismatch")
    .map(row => row.point);
  return `Zoom in on the X/Y columns and re-read ONLY point(s): ${mismatchedPoints.join(", ") || "unknown"}.

This is a literal projected-coordinate transcription pass.
- Read only Point, X, and Y.
- Ignore Latitude and Longitude completely.
- Keep every visible digit and decimal place.
- Do not derive values from the reference coordinates.
- Do not copy values from an adjacent row.

Return strict JSON only:
{
  "status": "observed" | "none",
  "rows": [
    { "point": "listed point", "x": "literal X", "y": "literal Y", "latitude": "", "longitude": "" }
  ]
}`;
}

function rowVerificationDifference(row, shadowIntent) {
  if (!Number.isFinite(row?.referenceLatitude) || !Number.isFinite(row?.referenceLongitude)) return Infinity;
  try {
    const transformed = transformUtmWgs84Point({
      easting: row.easting,
      northing: row.northing,
      zone: shadowIntent?.zone,
      hemisphere: shadowIntent?.hemisphere
    });
    return Math.max(
      Math.abs(transformed.latitude - row.referenceLatitude),
      Math.abs(transformed.longitude - row.referenceLongitude)
    );
  } catch {
    return Infinity;
  }
}

export function mergeStructuredUtmTableRows(originalTable = {}, retryTable = {}, { shadowIntent } = {}) {
  const replacements = new Map((retryTable.rows || []).map(row => [String(row.point), row]));
  const rows = (originalTable.rows || []).map(row => {
    const replacement = replacements.get(String(row.point));
    if (!replacement) return row;
    return rowVerificationDifference(replacement, shadowIntent) < rowVerificationDifference(row, shadowIntent)
      ? replacement
      : row;
  });
  return {
    status: rows.length >= 3 ? "observed" : "none",
    rows,
    rawModelText: String(retryTable.rawModelText || originalTable.rawModelText || "")
  };
}

export function mergeProjectedXyRows(referenceTable = {}, xyTable = {}, { shadowIntent } = {}) {
  const xyByPoint = new Map((xyTable.rows || []).map(row => [String(row.point), row]));
  const rows = (referenceTable.rows || []).map(row => {
    const xy = xyByPoint.get(String(row.point));
    if (!xy) return row;
    const candidate = {
      ...row,
      easting: xy.easting,
      northing: xy.northing,
      xText: xy.xText,
      yText: xy.yText
    };
    return rowVerificationDifference(candidate, shadowIntent) < rowVerificationDifference(row, shadowIntent)
      ? candidate
      : row;
  });
  return { status: rows.length >= 3 ? "observed" : "none", rows, rawModelText: referenceTable.rawModelText || "" };
}

function formatProjectedNumber(value, fallback) {
  const literal = String(fallback || "").trim().replace(/\s+/g, "").replace(/,/g, ".");
  return /^[+-]?\d+(?:\.\d+)?$/.test(literal) ? literal : String(value);
}

export function buildStructuredUtmPriority({ shadowIntent, table, tolerance = 1e-6 } = {}) {
  const rows = Array.isArray(table?.rows) ? table.rows : [];
  if (rows.length < 3) return null;
  const typedResult = buildShadowTypedUtmResult({
    shadowIntent,
    projectedCoordinates: rows.map(row => ({ easting: row.easting, northing: row.northing }))
  });
  if (!typedResult) return null;

  const comparisons = rows.map((row, index) => {
    const transformed = typedResult.transformedWgs84[index];
    const hasReference = Number.isFinite(row.referenceLatitude) && Number.isFinite(row.referenceLongitude);
    if (!hasReference) return { point: row.point, status: "not_available" };
    const latitudeDifference = Math.abs(transformed.latitude - row.referenceLatitude);
    const longitudeDifference = Math.abs(transformed.longitude - row.referenceLongitude);
    return {
      point: row.point,
      status: Math.max(latitudeDifference, longitudeDifference) <= tolerance ? "match" : "mismatch",
      latitudeDifference,
      longitudeDifference
    };
  });
  const comparable = comparisons.filter(item => item.status !== "not_available");
  const mismatches = comparable.filter(item => item.status === "mismatch");
  const verification = {
    status: comparable.length === 0 ? "not_available" : mismatches.length === 0 ? "match" : "mismatch",
    tolerance,
    comparedRows: comparable.length,
    maximumDifference: comparable.reduce((maximum, item) => Math.max(
      maximum,
      Number(item.latitudeDifference || 0),
      Number(item.longitudeDifference || 0)
    ), 0),
    rows: comparisons
  };

  if (verification.status === "mismatch") {
    return {
      accepted: false,
      reason: "transformation_verification_failed",
      coordinates: rows.map(row => `${formatProjectedNumber(row.easting, row.xText)},${formatProjectedNumber(row.northing, row.yText)}`).join("\n"),
      typedUtmIntent: typedResult.typedUtmIntent,
      transformationVerification: verification,
      table
    };
  }

  return {
    accepted: true,
    reason: "explicit_crs_structured_projected_xy",
    coordinates: rows.map(row => `${formatProjectedNumber(row.easting, row.xText)},${formatProjectedNumber(row.northing, row.yText)}`).join("\n"),
    typedUtmIntent: {
      ...typedResult.typedUtmIntent,
      source: "crs_evidence_and_structured_table"
    },
    transformedWgs84: typedResult.transformedWgs84,
    transformationVerification: verification,
    table
  };
}
