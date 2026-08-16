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
- Inspect each digit in X/Y independently; do not drop, transpose, or smooth repeated digits.
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

function normalizeReferenceRow(value = {}, index) {
  if (!value || typeof value !== "object") return null;
  const latitudeText = String(value.latitude ?? value.lat ?? value.latitudeText ?? "").trim();
  const longitudeText = String(value.longitude ?? value.lon ?? value.longitudeText ?? "").trim();
  const referenceLatitude = parseDmsReference(latitudeText, "latitude");
  const referenceLongitude = parseDmsReference(longitudeText, "longitude");
  if (!Number.isFinite(referenceLatitude) || !Number.isFinite(referenceLongitude)) return null;
  return {
    index,
    point: String(value.point ?? value.label ?? index + 1).trim() || String(index + 1),
    latitudeText,
    longitudeText,
    referenceLatitude,
    referenceLongitude
  };
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

export function getStructuredUtmVerificationMismatches(priority = {}) {
  return (priority.transformationVerification?.rows || [])
    .filter(row => row?.status === "mismatch")
    .map(row => {
      const latitudeDifference = Number(row.latitudeDifference || 0);
      const longitudeDifference = Number(row.longitudeDifference || 0);
      const suspectedField = longitudeDifference > latitudeDifference
        ? "X"
        : latitudeDifference > longitudeDifference
          ? "Y"
          : "BOTH";
      return {
        point: String(row.point || "").trim(),
        suspectedField,
        latitudeDifference,
        longitudeDifference,
        maximumDifference: Math.max(latitudeDifference, longitudeDifference)
      };
    })
    .filter(row => row.point);
}

export function buildSelectiveProjectedXyRereadPrompt(priority = {}) {
  const mismatches = getStructuredUtmVerificationMismatches(priority);
  const requested = mismatches.map(row => `${row.point} (${row.suspectedField})`).join(", ") || "unknown";
  return `Verification-guided selective reread.

Read ONLY these printed table point labels and their X/Y cells: ${requested}.

Target columns:
- Point / No.
- X
- Y

Rules:
- Return only the requested point labels.
- Read printed numeric cells exactly.
- Preserve every digit and decimal separator.
- Do not infer values from geography, neighboring rows, DMS, CRS, map position, or coordinate ranges.
- Do not calculate coordinates.
- Do not output DMS, CRS, map description, legend, or other text.
- If a requested point row is not clearly readable, omit that row rather than guessing.

Return strict JSON only:
{
  "status": "observed" | "none",
  "rows": [
    { "point": "requested point label", "x": "literal X", "y": "literal Y", "latitude": "", "longitude": "" }
  ]
}`;
}

export function buildSelectiveDmsReferenceRereadPrompt(priority = {}) {
  const mismatches = getStructuredUtmVerificationMismatches(priority);
  const requested = mismatches.map(row => String(row.point || "").trim()).filter(Boolean).join(", ") || "unknown";
  return `Verification-guided selective DMS reference reread.

Read ONLY these printed table point labels and their Latitude/Longitude DMS cells: ${requested}.

Target columns:
- Point / No.
- Latitude
- Longitude

Rules:
- Return only the requested point labels.
- Copy the printed DMS text exactly, preserving degrees, minutes, seconds, decimals, and hemisphere letters.
- Do not read or return X/Y, CRS, map legend, or other text.
- Do not calculate decimal coordinates.
- Do not infer from UTM, transformed coordinates, geography, neighboring rows, or coordinate ranges.
- If a requested point row is not clearly readable, omit that row rather than guessing.

Return strict JSON only:
{
  "status": "observed" | "none",
  "rows": [
    { "point": "requested point label", "latitude": "literal Latitude DMS", "longitude": "literal Longitude DMS" }
  ]
}`;
}

export async function runSelectiveProjectedXyRereadPass({ priority, imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") throw new TypeError("runSelectiveProjectedXyRereadPass requires an invokeVision function");
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runSelectiveProjectedXyRereadPass requires at least one image item");
  }
  const modelText = await invokeVision({
    prompt: buildSelectiveProjectedXyRereadPrompt(priority),
    imageItems
  });
  return parseStructuredUtmTableModelText(modelText);
}

export function parseSelectiveDmsReferenceModelText(modelText = "") {
  const parsed = parseJsonObject(modelText);
  const rows = (Array.isArray(parsed?.rows) ? parsed.rows : [])
    .map(normalizeReferenceRow)
    .filter(Boolean);
  return {
    status: rows.length > 0 ? "observed" : "none",
    rows,
    rawModelText: ""
  };
}

export async function runSelectiveDmsReferenceRereadPass({ priority, imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") throw new TypeError("runSelectiveDmsReferenceRereadPass requires an invokeVision function");
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runSelectiveDmsReferenceRereadPass requires at least one image item");
  }
  const modelText = await invokeVision({
    prompt: buildSelectiveDmsReferenceRereadPrompt(priority),
    imageItems
  });
  return parseSelectiveDmsReferenceModelText(modelText);
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
  const originalRows = Array.isArray(originalTable.rows) ? originalTable.rows : [];
  const retryRows = Array.isArray(retryTable.rows) ? retryTable.rows : [];
  if (originalRows.length === 0 && retryRows.length > 0) {
    return {
      status: retryRows.length >= 3 ? "observed" : "none",
      rows: retryRows,
      rawModelText: String(retryTable.rawModelText || originalTable.rawModelText || ""),
      source: "structured_retry_seed"
    };
  }
  const rows = originalRows.map(row => {
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
  const referenceRows = Array.isArray(referenceTable.rows) ? referenceTable.rows : [];
  const xyRows = Array.isArray(xyTable.rows) ? xyTable.rows : [];
  if (referenceRows.length === 0 && xyRows.length > 0) {
    return {
      status: xyRows.length >= 3 ? "observed" : "none",
      rows: xyRows,
      rawModelText: String(xyTable.rawModelText || ""),
      source: "xy_only_seed"
    };
  }
  const xyByPoint = new Map(xyRows.map(row => [String(row.point), row]));
  const rows = referenceRows.map(row => {
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

export function mergeSelectiveProjectedXyRows(referenceTable = {}, xyTable = {}, { shadowIntent } = {}) {
  const referenceRows = Array.isArray(referenceTable.rows) ? referenceTable.rows : [];
  const xyRows = Array.isArray(xyTable.rows) ? xyTable.rows : [];
  const xyByPoint = new Map(xyRows.map(row => [String(row.point), row]));
  const replacements = [];
  const rows = referenceRows.map(row => {
    const xy = xyByPoint.get(String(row.point));
    if (!xy) return row;
    const oldDifference = rowVerificationDifference(row, shadowIntent);
    const candidate = {
      ...row,
      easting: xy.easting,
      northing: xy.northing,
      xText: xy.xText,
      yText: xy.yText
    };
    const newDifference = rowVerificationDifference(candidate, shadowIntent);
    const accepted = newDifference < oldDifference;
    replacements.push({
      point: String(row.point),
      accepted,
      oldDifference,
      newDifference,
      reason: accepted ? "verification_improved" : "verification_not_improved"
    });
    return accepted ? candidate : row;
  });
  return {
    ...referenceTable,
    status: rows.length >= 3 ? "observed" : "none",
    rows,
    rawModelText: referenceTable.rawModelText || "",
    selectiveReread: {
      status: replacements.some(item => item.accepted) ? "accepted_partial" : "no_improvement",
      requestedRows: xyRows.length,
      acceptedRows: replacements.filter(item => item.accepted).length,
      replacements
    }
  };
}

function hasValidReferenceFormat(row = {}) {
  const latitudeText = String(row.latitudeText || row.latitude || "").trim();
  const longitudeText = String(row.longitudeText || row.longitude || "").trim();
  if (!Number.isFinite(row.referenceLatitude) || !Number.isFinite(row.referenceLongitude)) return false;
  const latitudeHasHemisphereOrSign = /[NS]/i.test(latitudeText) || /^[+-]/.test(latitudeText);
  const longitudeHasHemisphereOrSign = /[EW]/i.test(longitudeText) || /^[+-]/.test(longitudeText);
  return latitudeHasHemisphereOrSign && longitudeHasHemisphereOrSign;
}

export function mergeSelectiveDmsReferenceRows(referenceTable = {}, dmsReferenceTable = {}, { shadowIntent } = {}) {
  const referenceRows = Array.isArray(referenceTable.rows) ? referenceTable.rows : [];
  const dmsRows = (Array.isArray(dmsReferenceTable.rows) ? dmsReferenceTable.rows : [])
    .map(normalizeReferenceRow)
    .filter(Boolean);
  const dmsByPoint = new Map(dmsRows.map(row => [String(row.point), row]));
  const replacements = [];
  const rows = referenceRows.map(row => {
    const dms = dmsByPoint.get(String(row.point));
    if (!dms) return row;
    const oldDifference = rowVerificationDifference(row, shadowIntent);
    const candidate = {
      ...row,
      latitudeText: dms.latitudeText,
      longitudeText: dms.longitudeText,
      referenceLatitude: dms.referenceLatitude,
      referenceLongitude: dms.referenceLongitude,
      referenceSource: "selective_dms_reference_reread",
      referenceMergeMode: "point_label"
    };
    const newDifference = rowVerificationDifference(candidate, shadowIntent);
    const formatValid = hasValidReferenceFormat(dms);
    const accepted = formatValid && newDifference < oldDifference;
    replacements.push({
      point: String(row.point),
      accepted,
      oldDifference,
      newDifference,
      reason: !formatValid
        ? "invalid_dms_reference_format"
        : accepted
          ? "verification_improved"
          : "verification_not_improved"
    });
    return accepted ? candidate : row;
  });
  return {
    ...referenceTable,
    status: rows.length >= 3 ? "observed" : "none",
    rows,
    rawModelText: referenceTable.rawModelText || "",
    selectiveDmsReferenceReread: {
      status: replacements.some(item => item.accepted) ? "accepted_partial" : "no_improvement",
      requestedRows: dmsRows.length,
      acceptedRows: replacements.filter(item => item.accepted).length,
      replacements
    }
  };
}

function hasSequentialNumericPointLabels(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return false;
  const labels = rows.map(row => String(row.point || "").trim());
  const unique = new Set(labels);
  if (unique.size !== labels.length) return false;
  return labels.every((label, index) => label === String(index + 1));
}

export function mergeStructuredUtmReferenceRows(table = {}, referenceTable = {}, { allowVerifiedIndexMerge = false } = {}) {
  const tableRows = Array.isArray(table.rows) ? table.rows : [];
  const referenceRows = (Array.isArray(referenceTable.rows) ? referenceTable.rows : [])
    .map(normalizeReferenceRow)
    .filter(Boolean);
  if (tableRows.length === 0 || referenceRows.length === 0) {
    return {
      ...table,
      referenceMerge: {
        status: "skipped",
        reason: tableRows.length === 0 ? "no_projected_rows" : "no_reference_rows"
      }
    };
  }

  const referencesByPoint = new Map(referenceRows.map(row => [String(row.point), row]));
  let mergeMode = "point_label";
  const referenceSource = String(referenceTable.source || "merged_reference");
  const mergeReference = (row, reference, mode) => reference
    ? {
        ...row,
        ...reference,
        point: row.point,
        referenceSource,
        referenceMergeMode: mode
      }
    : row;
  let mergedRows = tableRows.map(row => {
    const reference = referencesByPoint.get(String(row.point));
    return mergeReference(row, reference, mergeMode);
  });
  let matchedRows = mergedRows.filter(row => Number.isFinite(row.referenceLatitude) && Number.isFinite(row.referenceLongitude)).length;

  if (
    matchedRows < Math.min(tableRows.length, referenceRows.length)
    && allowVerifiedIndexMerge
    && referenceTable.orderPreserved === true
    && tableRows.length === referenceRows.length
    && hasSequentialNumericPointLabels(tableRows)
  ) {
    mergeMode = "verified_index";
    mergedRows = tableRows.map((row, index) => {
      const reference = referenceRows[index];
      return mergeReference(row, reference, mergeMode);
    });
    matchedRows = mergedRows.filter(row => Number.isFinite(row.referenceLatitude) && Number.isFinite(row.referenceLongitude)).length;
  }

  return {
    ...table,
    rows: mergedRows,
    status: mergedRows.length >= 3 ? "observed" : "none",
    referenceMerge: {
      status: matchedRows > 0 ? "merged" : "unmatched",
      mode: mergeMode,
      matchedRows,
      referenceRows: referenceRows.length,
      source: referenceTable.source || "unknown"
    }
  };
}

function formatProjectedNumber(value, fallback) {
  const literal = String(fallback || "").trim().replace(/\s+/g, "").replace(/,/g, ".");
  return /^[+-]?\d+(?:\.\d+)?$/.test(literal) ? literal : String(value);
}

function numericOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function getReferenceSource(row = {}, table = {}) {
  const source = String(row.referenceSource || table.referenceMerge?.source || "").trim();
  if (source) return source;
  if (Number.isFinite(row.referenceLatitude) && Number.isFinite(row.referenceLongitude)) {
    return "structured_table";
  }
  return "other";
}

function getReferenceMergeMode(row = {}, table = {}) {
  return String(row.referenceMergeMode || table.referenceMerge?.mode || "").trim() || null;
}

function buildPointLevelVerificationRow({ row = {}, index = 0, transformed = {}, status = "not_available", latitudeDifference = null, longitudeDifference = null, table = {} } = {}) {
  const normalizedLatitudeDifference = numericOrNull(latitudeDifference);
  const normalizedLongitudeDifference = numericOrNull(longitudeDifference);
  const maximumDifference = Math.max(
    Number(normalizedLatitudeDifference || 0),
    Number(normalizedLongitudeDifference || 0)
  );
  return {
    point: String(row.point || index + 1),
    projected: {
      x: numericOrNull(row.easting),
      y: numericOrNull(row.northing)
    },
    transformed: {
      latitude: numericOrNull(transformed.latitude),
      longitude: numericOrNull(transformed.longitude)
    },
    reference: {
      latitude: numericOrNull(row.referenceLatitude),
      longitude: numericOrNull(row.referenceLongitude)
    },
    latitudeDifference: normalizedLatitudeDifference,
    longitudeDifference: normalizedLongitudeDifference,
    maximumDifference,
    status,
    referenceSource: getReferenceSource(row, table),
    referenceMergeMode: getReferenceMergeMode(row, table)
  };
}

function summarizeVerificationRows(rows = []) {
  const comparable = rows.filter(item => item.status !== "not_available");
  const mismatches = comparable.filter(item => item.status === "mismatch");
  return {
    comparedRows: comparable.length,
    matchedRows: comparable.filter(item => item.status === "match").length,
    mismatchedRows: mismatches.length,
    mismatchedPointLabels: mismatches.map(item => String(item.point || "")).filter(Boolean),
    maximumDifference: comparable.reduce((maximum, item) => Math.max(
      maximum,
      Number(item.maximumDifference || 0),
      Number(item.latitudeDifference || 0),
      Number(item.longitudeDifference || 0)
    ), 0)
  };
}

export function summarizeStructuredUtmTransformationVerification(verification = {}) {
  const rows = Array.isArray(verification.pointLevelVerification)
    ? verification.pointLevelVerification
    : Array.isArray(verification.rows)
      ? verification.rows
      : [];
  const summary = summarizeVerificationRows(rows);
  return {
    status: String(verification.status || ""),
    tolerance: numericOrNull(verification.tolerance),
    comparedRows: Number.isFinite(Number(verification.comparedRows)) ? Number(verification.comparedRows) : summary.comparedRows,
    matchedRows: Number.isFinite(Number(verification.matchedRows)) ? Number(verification.matchedRows) : summary.matchedRows,
    mismatchedRows: Number.isFinite(Number(verification.mismatchedRows)) ? Number(verification.mismatchedRows) : summary.mismatchedRows,
    mismatchedPointLabels: Array.isArray(verification.mismatchedPointLabels)
      ? verification.mismatchedPointLabels.map(label => String(label || "")).filter(Boolean)
      : summary.mismatchedPointLabels,
    maximumDifference: Number.isFinite(Number(verification.maximumDifference)) ? Number(verification.maximumDifference) : summary.maximumDifference
  };
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
    if (!hasReference) {
      return buildPointLevelVerificationRow({
        row,
        index,
        transformed,
        status: "not_available",
        table
      });
    }
    const latitudeDifference = Math.abs(transformed.latitude - row.referenceLatitude);
    const longitudeDifference = Math.abs(transformed.longitude - row.referenceLongitude);
    const status = Math.max(latitudeDifference, longitudeDifference) <= tolerance ? "match" : "mismatch";
    return buildPointLevelVerificationRow({
      row,
      index,
      transformed,
      status,
      latitudeDifference,
      longitudeDifference,
      table
    });
  });
  const comparable = comparisons.filter(item => item.status !== "not_available");
  const mismatches = comparable.filter(item => item.status === "mismatch");
  const summary = summarizeVerificationRows(comparisons);
  const verification = {
    status: comparable.length === 0 ? "not_available" : mismatches.length === 0 ? "match" : "mismatch",
    tolerance,
    comparedRows: summary.comparedRows,
    matchedRows: summary.matchedRows,
    mismatchedRows: summary.mismatchedRows,
    mismatchedPointLabels: summary.mismatchedPointLabels,
    maximumDifference: summary.maximumDifference,
    rows: comparisons,
    pointLevelVerification: comparisons
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
