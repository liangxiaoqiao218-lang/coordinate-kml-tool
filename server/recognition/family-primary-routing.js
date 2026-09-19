import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "./coordinate-image-safety.js";

export const WGS84_PRIMARY_STAGE_CAP_MS = 25_000;
export const KYRGYZ_PRIMARY_STAGE_CAP_MS = 25_000;
export const MADAGASCAR_PRIMARY_STAGE_CAP_MS = 25_000;
export const ONE_SHOT_STRUCTURED_FAMILY = Object.freeze({
  GENERIC_REVIEW: "generic_review",
  WGS84_SINGLE_POINT: "wgs84_single_point",
  WGS84_TABLE: "wgs84_table",
  DMS_TABLE: "dms_table",
  DMS_GROUPED: "dms_grouped",
  MAP_SCREENSHOT: "map_screenshot",
  PROJECTED_CRS_TABLE: "projected_crs_table"
});
export const ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS = Object.freeze({
  CONFORMANT: "CONFORMANT",
  REVIEW_REQUIRED: "REVIEW_REQUIRED"
});
export const ONE_SHOT_ACQUISITION_CONFORMANCE_REASON = Object.freeze({
  CONFORMANT: "CONFORMANT",
  CONTRACT_INVALID: "CONTRACT_INVALID",
  GENERIC_REVIEW_ONLY: "GENERIC_REVIEW_ONLY",
  FAMILY_MISMATCH: "FAMILY_MISMATCH",
  STRUCTURE_COUNT_MISMATCH: "STRUCTURE_COUNT_MISMATCH",
  HEADER_OR_CONTINUITY_MISMATCH: "HEADER_OR_CONTINUITY_MISMATCH",
  GROUP_BOUNDARY_MISMATCH: "GROUP_BOUNDARY_MISMATCH",
  MAP_ROLE_BINDING_MISMATCH: "MAP_ROLE_BINDING_MISMATCH",
  PROJECTED_CRS_MISMATCH: "PROJECTED_CRS_MISMATCH",
  PRIVATE_BINDING_UNAVAILABLE: "PRIVATE_BINDING_UNAVAILABLE",
  SPATIAL_PROVENANCE_UNAVAILABLE: "SPATIAL_PROVENANCE_UNAVAILABLE",
  OBSERVATION_SET_INCOMPLETE: "OBSERVATION_SET_INCOMPLETE",
  OBSERVATION_SET_AMBIGUOUS: "OBSERVATION_SET_AMBIGUOUS",
  VALUE_FIDELITY_MISMATCH: "VALUE_FIDELITY_MISMATCH",
  ROW_PROVENANCE_MISMATCH: "ROW_PROVENANCE_MISMATCH",
  MAP_ROLE_VALUE_MISMATCH: "MAP_ROLE_VALUE_MISMATCH",
  PROJECTED_VALUE_MISMATCH: "PROJECTED_VALUE_MISMATCH"
});
const ONE_SHOT_ACQUISITION_CONTRACT_SCHEMA = "one_shot_acquisition_contract_v1";
const privateAcquisitionBindings = new WeakMap();
const ONE_SHOT_ACQUISITION_FORMATS = Object.freeze([
  "WGS84_DECIMAL_SINGLE_POINT",
  "DMS_SINGLE_POINT",
  "WGS84_DECIMAL_TABLE",
  "DMS_TABLE",
  "DMS_GROUPED",
  "MAP_UI_ROLES",
  "PROJECTED_CRS_TABLE",
  "GENERIC_REVIEW"
]);
// Same angular tolerance as the approved isolated Indonesia UTM verifier.
export const INDONESIA_PROJECTED_DMS_TOLERANCE = 1e-6;

const MADAGASCAR_MAP_TICK_VALUES = new Set([
  "290625", "295625", "300625", "535625", "540625", "545625", "550625"
]);
const MADAGASCAR_LOCAL_ANCHOR = Object.freeze({
  projectedX: 293437.5,
  projectedY: 364062.5,
  longitude: 45.23,
  latitude: -22.68
});
const MADAGASCAR_METERS_PER_DEGREE_LATITUDE = 111320;
const MADAGASCAR_METERS_PER_DEGREE_LONGITUDE = 102600;
const MADAGASCAR_DEFAULT_CELL_SIZE_METERS = 625;

function normalizeEvidenceText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\u0000-\u001f]+/g, " ")
    .trim();
}

function normalizeCoordinateEvidenceText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\r\n/g, "\n")
    .replace(/\u00a0/g, " ")
    .replace(/[，]/g, ",")
    .replace(/[｜]/g, "|");
}

function normalizeDecimalToken(value) {
  return String(value || "").trim().replace(/,/g, ".").replace(/\s+/g, "");
}

function getSingleLabeledEntry(text, labels) {
  const matches = [];
  for (const line of normalizeCoordinateEvidenceText(text).split("\n")) {
    const match = line.match(/^\s*([^|:=]+?)\s*[|:=]\s*(.+?)\s*$/u);
    if (!match || !labels.test(match[1])) continue;
    matches.push(Object.freeze({ label: match[1].trim(), value: match[2].trim() }));
  }
  return matches.length === 1 ? matches[0] : null;
}

function parseStrictDecimalCoordinate(value, limit) {
  const normalized = normalizeDecimalToken(value);
  if (!/^[-+]?\d{1,3}(?:\.\d+)?$/.test(normalized)) return null;
  const numeric = Number(normalized);
  return Number.isFinite(numeric) && Math.abs(numeric) <= limit ? normalized : null;
}

function parseStrictDmsCoordinate(value, expectedDirections) {
  const normalized = String(value || "").trim();
  const match = normalized.match(
    /^\s*(\d{1,3})\s*[°º]\s*(\d{1,2})\s*['′’]?\s*(\d{1,2}(?:[.,]\d+)?)\s*["″”]?\s*([NSEWO])\s*$/iu
  );
  if (!match) return null;
  const direction = match[4].toUpperCase() === "O" ? "W" : match[4].toUpperCase();
  const allowedDirections = String(expectedDirections || "").toUpperCase().replace(/O/gu, "W");
  if (!allowedDirections.includes(direction)) return null;
  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3].replace(",", "."));
  const limit = allowedDirections === "NS" ? 90 : 180;
  if (!Number.isFinite(degrees) || !Number.isFinite(minutes) || !Number.isFinite(seconds)
    || minutes >= 60 || seconds >= 60 || degrees > limit
    || (degrees === limit && (minutes > 0 || seconds > 0))) return null;
  return (direction === "S" || direction === "W" ? -1 : 1)
    * (degrees + minutes / 60 + seconds / 3600);
}

function formatNormalizedCoordinate(value) {
  return Number(value).toFixed(10).replace(/\.?0+$/u, "");
}

function getDirectionalLabelExpectation(label, axis) {
  const normalized = String(label || "").replace(/\s+/gu, "");
  if (axis === "longitude") {
    if (/^(?:東經|东经)$/u.test(normalized)) return "E";
    if (/^(?:西經|西经)$/u.test(normalized)) return "W";
  }
  if (axis === "latitude") {
    if (/^(?:北緯|北纬)$/u.test(normalized)) return "N";
    if (/^(?:南緯|南纬)$/u.test(normalized)) return "S";
  }
  return "";
}

function isDecimalDirectionCompatible(value, expectation) {
  if (!expectation) return true;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return false;
  return ["E", "N"].includes(expectation) ? numeric >= 0 : numeric <= 0;
}

function isDmsDirectionCompatible(value, expectation) {
  if (!expectation) return true;
  const direction = String(value || "").trim().match(/([NSEW])\s*$/iu)?.[1]?.toUpperCase() || "";
  return direction === expectation;
}

export function getWgs84SinglePointEvidence(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const rejected = reason => Object.freeze({
    matched: false,
    family: "wgs84_single_point",
    coordinateFormat: "",
    coordinateLine: "",
    normalizedCoordinateLine: "",
    originalLongitude: "",
    originalLatitude: "",
    originalLongitudeLabel: "",
    originalLatitudeLabel: "",
    coordinateType: "",
    precisionMode: "",
    geometryType: "",
    forceRequiresReview: false,
    requiresReview: true,
    reason
  });
  const lines = text.split("\n").map(line => line.trim()).filter(Boolean);
  if (lines.length !== 3 || !/^WGS84\s+Single\s+Point$/iu.test(lines[0])) {
    return rejected("closed_contract_line_set_invalid");
  }
  if (/\b(?:UTM|MGRS|Easting|Northing|EPSG\s*:\s*(?!4326\b)\d+|projected|projection)\b/iu.test(text)) {
    return rejected("projected_crs_conflict");
  }
  const longitudeEntry = getSingleLabeledEntry(text, /^(?:longitude(?:\s+DMS)?|lon|经度|經度|東經|东经|西經|西经)$/iu);
  const latitudeEntry = getSingleLabeledEntry(text, /^(?:latitude(?:\s+DMS)?|lat|纬度|緯度|北纬|北緯|南纬|南緯)$/iu);
  if (!longitudeEntry || !latitudeEntry) return rejected("exact_axis_labels_missing_or_duplicated");
  const longitude = longitudeEntry.value;
  const latitude = latitudeEntry.value;
  const longitudeDmsLabel = /\bDMS\b/iu.test(longitudeEntry.label);
  const latitudeDmsLabel = /\bDMS\b/iu.test(latitudeEntry.label);
  if (longitudeDmsLabel !== latitudeDmsLabel) return rejected("coordinate_label_format_mismatch");
  const longitudeDirection = getDirectionalLabelExpectation(longitudeEntry.label, "longitude");
  const latitudeDirection = getDirectionalLabelExpectation(latitudeEntry.label, "latitude");

  const decimalLongitude = parseStrictDecimalCoordinate(longitude, 180);
  const decimalLatitude = parseStrictDecimalCoordinate(latitude, 90);
  if (!longitudeDmsLabel && decimalLongitude && decimalLatitude
    && isDecimalDirectionCompatible(decimalLongitude, longitudeDirection)
    && isDecimalDirectionCompatible(decimalLatitude, latitudeDirection)) {
    return Object.freeze({
      matched: true,
      family: "wgs84_single_point",
      coordinateFormat: "WGS84_DECIMAL",
      coordinateLine: `${decimalLongitude},${decimalLatitude}`,
      normalizedCoordinateLine: `${decimalLongitude},${decimalLatitude}`,
      originalLongitude: longitude,
      originalLatitude: latitude,
      originalLongitudeLabel: longitudeEntry.label,
      originalLatitudeLabel: latitudeEntry.label,
      coordinateType: "decimal_latlon",
      precisionMode: "wgs84-single-point-decimal",
      geometryType: "point",
      forceRequiresReview: true,
      requiresReview: true,
      reason: "explicit_labeled_decimal_single_point"
    });
  }

  const dmsLongitude = parseStrictDmsCoordinate(longitude, "EW");
  const dmsLatitude = parseStrictDmsCoordinate(latitude, "NS");
  if (Number.isFinite(dmsLongitude) && Number.isFinite(dmsLatitude)
    && isDmsDirectionCompatible(longitude, longitudeDirection)
    && isDmsDirectionCompatible(latitude, latitudeDirection)) {
    return Object.freeze({
      matched: true,
      family: "wgs84_single_point",
      coordinateFormat: "WGS84_DMS",
      coordinateLine: `${longitude},${latitude}`,
      normalizedCoordinateLine: `${formatNormalizedCoordinate(dmsLongitude)},${formatNormalizedCoordinate(dmsLatitude)}`,
      originalLongitude: longitude,
      originalLatitude: latitude,
      originalLongitudeLabel: longitudeEntry.label,
      originalLatitudeLabel: latitudeEntry.label,
      coordinateType: "decimal_latlon",
      precisionMode: "wgs84-single-point-dms",
      geometryType: "point",
      forceRequiresReview: true,
      requiresReview: true,
      reason: "explicit_labeled_dms_single_point"
    });
  }
  return rejected("coordinate_format_or_axis_range_invalid");
}

function normalizeMadagascarCellId(value) {
  return String(value || "")
    .trim()
    .replace(/[|,;:]/g, "")
    .replace(/\s+/g, "");
}

function isMadagascarGridCoordinate(value) {
  const number = Number(normalizeDecimalToken(value));
  return Number.isFinite(number) && number > 0 && number < 1000000;
}

function isValidMadagascarCellId(value) {
  const normalized = normalizeMadagascarCellId(value);
  return Boolean(normalized) && !/^\d{6,}$/.test(normalized) && /^[A-Za-z0-9-]{1,16}$/.test(normalized);
}

function parseDmsCoordinate(value, expectedDirections) {
  const match = String(value || "").match(/(\d{1,3})\s*[°º]\s*(\d{1,2})\s*['′]?\s*(\d{1,2}(?:[.,]\d+)?)\s*["″]?\s*([NSEW])/i);
  if (!match || !expectedDirections.includes(match[4].toUpperCase())) return null;
  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3].replace(",", "."));
  const limit = expectedDirections === "NS" ? 90 : 180;
  if (!Number.isFinite(degrees) || minutes >= 60 || seconds >= 60
    || degrees > limit || (degrees === limit && (minutes > 0 || seconds > 0))) return null;
  const sign = /[SW]/i.test(match[4]) ? -1 : 1;
  return sign * (degrees + minutes / 60 + seconds / 3600);
}

export function collapseExactRepeatedCoordinateSequence(rows = [], key = row => JSON.stringify(row)) {
  const source = Array.isArray(rows) ? rows.slice() : [];
  for (let blockLength = Math.floor(source.length / 2); blockLength >= 3; blockLength -= 1) {
    for (let start = 0; start + blockLength * 2 <= source.length; start += 1) {
      let exact = true;
      for (let offset = 0; offset < blockLength; offset += 1) {
        if (key(source[start + offset]) !== key(source[start + blockLength + offset])) {
          exact = false;
          break;
        }
      }
      if (exact) return source.slice(0, start + blockLength).concat(source.slice(start + blockLength * 2));
    }
  }
  return source;
}

export function hasIndonesiaUtm50StructuralEvidence(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  return /UTM\s*WGS\s*1984\s*ZONA\s*50S/i.test(text)
    && /(?:^|[|\s])(?:X|Easting)(?:[|\s]|$)/im.test(text)
    && /(?:^|[|\s])(?:Y|Northing)(?:[|\s]|$)/im.test(text);
}

export function hasStrongPrintedProjectedTableEvidence(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const lines = text.split("\n").map(line => line.trim()).filter(Boolean);
  const hasProjectedHeader = lines.some(line => {
    const normalized = line.replace(/[|;\t]+/g, " ").replace(/\s+/g, " ").trim();
    return /(?:^|\s)(?:No\.?|Point|Titik|ID|Label)?\s*(?:X|Easting)\s+(?:Y|Northing)(?:\s|$)/i.test(normalized);
  });
  const projectedRows = lines.filter(line => {
    const cells = line.split(/[|;\t]/).map(cell => cell.trim()).filter(Boolean);
    const coordinateCandidates = [];
    if (cells.length >= 3) {
      coordinateCandidates.push([cells[1], cells[2]]);
    } else {
      const whitespaceCells = line.split(/\s+/).map(cell => cell.trim()).filter(Boolean);
      if (whitespaceCells.length >= 3) coordinateCandidates.push([whitespaceCells[1], whitespaceCells[2]]);
      if (whitespaceCells.length >= 2) coordinateCandidates.push([whitespaceCells[0], whitespaceCells[1]]);
    }
    return coordinateCandidates.some(([xToken, yToken]) => {
      const x = Number(normalizeDecimalToken(xToken));
      const y = Number(normalizeDecimalToken(yToken));
      return Number.isFinite(x) && x >= 100000 && x <= 900000
        && Number.isFinite(y) && y >= 0 && y <= 10000000;
    });
  }).length;
  return hasProjectedHeader && projectedRows >= 3;
}

// Shared evidence for retry eligibility and final classification. File names,
// country, fixture IDs, Golden values, and row counts are not positive evidence.
export function getDmsDocumentEvidence(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const dmsPairLineCount = text.split("\n").filter(line => {
    const components = line.match(/[-+]?\d{1,3}\s*[°º]\s*\d{1,2}(?:\s*['′’]\s*\d{1,2}(?:[.,]\d+)?)?\s*["″”]?\s*(?:N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?/gi) || [];
    return components.length >= 2;
  }).length;
  const projectedTableSignal = hasStrongPrintedProjectedTableEvidence(text);
  const printedTableSignal = projectedTableSignal
    || /(?:latitude|longitude|easting|northing|coordonn[eé]es|\bPoint\b|\bOrdem\b|\bX\s*\|\s*Y\b)/i.test(text);
  const explicitHandwrittenSignal = /(?:识别提示[^\n]*手写|手写坐标|手寫坐標|\bhandwritten\s+(?:DMS|coordinates?)\b|涂改|覆盖|需核对字符|overwrit(?:ten|e))/i.test(text)
    && !/not\s+handwritten|非手写|非手寫/i.test(text);
  const damagedDmsSignal = /\d{1,3}\s*[°º]\s*\d{1,2}\.\d{3,5}\s*['′]?\s*[NSEWO]\b/i.test(text)
    || /\d{1,3}[.°º]\d{1,2}\.\d{1,2}\.\d+\s*[NSEWO]\b/i.test(text)
    || /\d[OIl?]\d[^\n]{0,15}[NSEW]\b/.test(text);
  // These structure-only signals deliberately ignore wording returned by the
  // Provider. Text such as "handwritten DMS" is an observation, not trusted
  // evidence that may acquire a second Provider call.
  const dmsStructureCandidateSignal = !projectedTableSignal
    && (dmsPairLineCount >= 3 || damagedDmsSignal);
  const printedDmsStructureSignal = printedTableSignal && !projectedTableSignal;
  const printedDmsCandidateSignal = printedTableSignal
    && !projectedTableSignal
    && !explicitHandwrittenSignal;
  const nonHandwrittenDmsCandidateSignal = !projectedTableSignal
    && !explicitHandwrittenSignal
    && (dmsPairLineCount >= 3 || damagedDmsSignal);
  // A complete 16-row printed DMS acquisition is a safety-risk signal only.
  // It does not prove a multi-site topology; the structure module must still
  // prove the individual group boundaries before Map/KML can consume it.
  const stage1FullMultisiteRiskSignal = nonHandwrittenDmsCandidateSignal
    && dmsPairLineCount === 16;
  const stage1FullMultisiteStructureSignal = dmsStructureCandidateSignal
    && dmsPairLineCount === 16;
  // Eight valid rows plus a separate morphology-risk decision may justify one
  // evidence-only grouped reread. This is deliberately not a multi-site or
  // handwritten identity signal; the grouped retry must prove the full 8/4/4
  // structure before the result can enter confirmation.
  const partialMultisiteRecoveryCandidateSignal = dmsStructureCandidateSignal
    && dmsPairLineCount === 8;
  // This is only a coarse Stage-1 signal. The structure module must separately
  // prove three ordered groups, two allowlisted boundaries, continuous labels,
  // and bounded repairable rejects before it can reserve dms_grouped.
  const weakPartialMultisiteRecoveryCandidateSignal = dmsStructureCandidateSignal
    && dmsPairLineCount >= 9
    && dmsPairLineCount <= 30;
  return Object.freeze({ printedTableSignal, projectedTableSignal,
    handwrittenPositiveSignal: explicitHandwrittenSignal || damagedDmsSignal,
    explicitHandwrittenSignal, damagedDmsSignal, printedDmsCandidateSignal,
    nonHandwrittenDmsCandidateSignal, stage1FullMultisiteRiskSignal,
    dmsStructureCandidateSignal, printedDmsStructureSignal,
    stage1FullMultisiteStructureSignal, partialMultisiteRecoveryCandidateSignal,
    weakPartialMultisiteRecoveryCandidateSignal,
    dmsPairLineCount });
}

export function getIndonesiaUtm50Info(value = "", { transform } = {}) {
  const text = normalizeCoordinateEvidenceText(value);
  if (!hasIndonesiaUtm50StructuralEvidence(text)) {
    return Object.freeze({ isIndonesiaUtm50: false, rows: Object.freeze([]), rowCount: 0, duplicateSequenceCollapsed: false });
  }
  const parsed = [];
  for (const line of text.split("\n").map(item => item.trim()).filter(Boolean)) {
    const parts = line.split("|").map(item => item.trim()).filter(Boolean);
    const numbers = line.match(/[-+]?\d+(?:[.,]\d+)?/g) || [];
    if (parts.length < 3 || numbers.length < 3) continue;
    const label = String(parts[0].match(/[A-Za-z0-9-]+/)?.[0] || numbers[0]);
    const xMatch = line.match(/(?:^|[|\s])X\s*[:=]?\s*([-+]?\d+(?:[.,]\d+)?)/i);
    const yMatch = line.match(/(?:^|[|\s])Y\s*[:=]?\s*([-+]?\d+(?:[.,]\d+)?)/i);
    const eastingText = normalizeDecimalToken(xMatch?.[1] || parts[1]);
    const northingText = normalizeDecimalToken(yMatch?.[1] || parts[2]);
    const easting = Number(eastingText);
    const northing = Number(northingText);
    if (!Number.isFinite(easting) || easting < 100000 || easting > 900000
      || !Number.isFinite(northing) || northing < 9000000 || northing > 10000000) continue;
    const latitudePart = parts.find(part => /[°º].*[NS]/i.test(part)) || "";
    const longitudePart = parts.find(part => /[°º].*[EW]/i.test(part)) || "";
    const referenceLatitude = parseDmsCoordinate(latitudePart, "NS");
    const referenceLongitude = parseDmsCoordinate(longitudePart, "EW");
    const referenceParsed = Number.isFinite(referenceLatitude) && Number.isFinite(referenceLongitude);
    parsed.push(Object.freeze({
      label,
      eastingText,
      northingText,
      latitudeDms: latitudePart,
      longitudeDms: longitudePart,
      projectedSourceCoordinates: Object.freeze({ easting, northing }),
      dmsReferenceCoordinates: referenceParsed ? Object.freeze({ lat: referenceLatitude, lon: referenceLongitude }) : null,
      dmsReferenceParsed: referenceParsed
    }));
  }
  const sourceRows = collapseExactRepeatedCoordinateSequence(parsed, row => [
    row.label, row.eastingText, row.northingText, row.latitudeDms, row.longitudeDms
  ].join("|"));
  if (sourceRows.length < 3) {
    return Object.freeze({ isIndonesiaUtm50: false, structureConfirmed: false,
      rows: Object.freeze([]), rowCount: 0, transformStatus: "NOT_ATTEMPTED" });
  }
  // Ownership is established from acquisition evidence, never from transform success.
  const failed = reason => Object.freeze({
    isIndonesiaUtm50: true, structureConfirmed: true, ownerIntent: "indonesia_utm50_projected",
    crs: "EPSG:32750", axisOrder: "easting_northing", transformStatus: "FAILED",
    failureCode: "INDONESIA_UTM50_TRANSFORM_FAILED", transformFailureReason: reason,
    projectedTransformExecuted: false, projectedDmsCrosscheck: "NOT_EXECUTED",
    genericDmsFallbackAllowed: false, finalGeometrySource: "NONE", requiresReview: true,
    sourceRows: Object.freeze(sourceRows), rows: Object.freeze([]), rowCount: 0
  });
  const collapsed = [];
  for (const row of sourceRows) {
    let point;
    try { point = typeof transform === "function"
      ? transform(50, row.projectedSourceCoordinates.easting, row.projectedSourceCoordinates.northing, false) : null;
    } catch { return failed("TRANSFORM_EXCEPTION"); }
    if (point == null) return failed("TRANSFORM_NO_RESULT");
    const { lat, lon } = point;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return failed("TRANSFORM_NONFINITE_OR_INCOMPLETE");
    if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return failed("TRANSFORM_OUT_OF_RANGE");
    const maximumDifference = row.dmsReferenceParsed
      ? Math.max(Math.abs(lat - row.dmsReferenceCoordinates.lat), Math.abs(lon - row.dmsReferenceCoordinates.lon)) : null;
    collapsed.push(Object.freeze({ ...row, lat, lon, projectedTransformExecuted: true,
      projectedDmsCrosscheckExecuted: row.dmsReferenceParsed, maximumDifference,
      projectedDmsCrosscheck: row.dmsReferenceParsed
        ? maximumDifference <= INDONESIA_PROJECTED_DMS_TOLERANCE ? "PASS" : "FAIL" : "NOT_AVAILABLE" }));
  }
  // A projected polygon must be constructible; references cannot repair its topology.
  const ring = collapsed.slice();
  if (ring.length > 3 && ring[0].lat === ring.at(-1).lat && ring[0].lon === ring.at(-1).lon) ring.pop();
  const cross = (a, b, c) => (b.lon - a.lon) * (c.lat - a.lat) - (b.lat - a.lat) * (c.lon - a.lon);
  const area = ring.reduce((sum, point, i) => sum + cross(ring[0], point, ring[(i + 1) % ring.length]), 0);
  if (new Set(ring.map(point => `${point.lon.toFixed(12)}|${point.lat.toFixed(12)}`)).size !== ring.length
    || ring.length < 3 || !Number.isFinite(area) || area === 0) return failed("TRANSFORM_INVALID_GEOMETRY");
  for (let i = 0; i < ring.length; i += 1) {
    for (let j = i + 2; j < ring.length; j += 1) {
      if (i === 0 && j === ring.length - 1) continue;
      const a = ring[i], b = ring[(i + 1) % ring.length], c = ring[j], d = ring[(j + 1) % ring.length];
      if (Math.max(a.lon, b.lon) >= Math.min(c.lon, d.lon) && Math.max(c.lon, d.lon) >= Math.min(a.lon, b.lon)
        && Math.max(a.lat, b.lat) >= Math.min(c.lat, d.lat) && Math.max(c.lat, d.lat) >= Math.min(a.lat, b.lat)
        && cross(a, b, c) * cross(a, b, d) <= 0 && cross(c, d, a) * cross(c, d, b) <= 0) return failed("TRANSFORM_INVALID_GEOMETRY");
    }
  }
  const referenceExpected = /Latitude[\s|]+Longitude/i.test(text)
    || collapsed.some(row => row.latitudeDms || row.longitudeDms);
  const comparedRows = collapsed.filter(row => row.projectedDmsCrosscheckExecuted).length;
  const crosscheck = collapsed.some(row => row.projectedDmsCrosscheck === "FAIL") ? "FAIL"
    : comparedRows === collapsed.length && comparedRows > 0 ? "PASS"
      : referenceExpected ? "INCOMPLETE" : "NOT_AVAILABLE";
  return Object.freeze({
    isIndonesiaUtm50: collapsed.length >= 3,
    structureConfirmed: true,
    ownerIntent: "indonesia_utm50_projected",
    transformStatus: "SUCCESS",
    crs: "EPSG:32750",
    axisOrder: "easting_northing",
    projectedTransformExecuted: collapsed.length >= 3,
    dmsReferenceParsed: comparedRows > 0,
    projectedDmsCrosscheckExecuted: comparedRows > 0,
    projectedDmsCrosscheck: crosscheck,
    crosscheckTolerance: INDONESIA_PROJECTED_DMS_TOLERANCE,
    comparedRows,
    requiresReview: crosscheck === "FAIL" || crosscheck === "INCOMPLETE",
    rows: Object.freeze(collapsed),
    rowCount: collapsed.length,
    duplicateSequenceCollapsed: collapsed.length !== parsed.length
  });
}

// Missing projected CRS never licenses a transform. Explicit document DMS is a separate source.
export function getPrintedProjectedDmsReference(value = "") {
  if (!hasStrongPrintedProjectedTableEvidence(value) || hasIndonesiaUtm50StructuralEvidence(value)) return null;
  const sourceRows = [];
  for (const line of normalizeCoordinateEvidenceText(value).split("\n")) {
    const parts = line.split("|").map(part => part.trim());
    if (parts.length < 3 || !/^[-+]?\d+(?:[.,]\d+)?$/.test(parts[1]) || !/^[-+]?\d+(?:[.,]\d+)?$/.test(parts[2])) continue;
    const latitudeDms = parts.find(part => /[°º].*[NS]/i.test(part)) || "";
    const longitudeDms = parts.find(part => /[°º].*[EW]/i.test(part)) || "";
    const lat = parseDmsCoordinate(latitudeDms, "NS"), lon = parseDmsCoordinate(longitudeDms, "EW");
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    sourceRows.push(Object.freeze({ label: parts[0], x: parts[1], y: parts[2], latitudeDms, longitudeDms, lat, lon }));
  }
  if (sourceRows.length < 3) return null;
  return Object.freeze({ geometrySource: "DMS_DOCUMENT_REFERENCE", projectedSourceStatus: "UNRESOLVED",
    projectedTransformExecuted: false, sourceCrs: null, sourceRows: Object.freeze(sourceRows),
    coordinates: sourceRows.map(row => `${row.latitudeDms},${row.longitudeDms}`).join("\n") });
}

export function formatIndonesiaUtm50Rows(info = {}) {
  if (info.transformStatus === "FAILED") return "";
  return (Array.isArray(info.rows) ? info.rows : []).map(row => {
    const sourceReference = row.latitudeDms && row.longitudeDms
      ? ` | ${row.latitudeDms} | ${row.longitudeDms}`
      : "";
    return `${row.label} | ${row.eastingText} | ${row.northingText}${sourceReference} | ${row.lon.toFixed(12)},${row.lat.toFixed(12)},0`;
  }).join("\n");
}

export function hasMadagascarCadastralStructuralSignature(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  return /liste[_\s-]*carr[eé]s?/i.test(text)
    && /\bX\s*V\b|\bXV\b/i.test(text)
    && /\bY\s*V\b|\bYV\b/i.test(text)
    && /\bNC\b|\bnum\b|n[°o]\b|CM[_\s-]*NOMFIR|cadastral|cadastre|grille|carreau/i.test(text);
}

export function hasMadagascarMapGridTickTakeover(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const hits = [...MADAGASCAR_MAP_TICK_VALUES].filter(tick => new RegExp(`\\b${tick}\\b`).test(text));
  return hits.length >= 2 && !hasMadagascarCadastralStructuralSignature(text);
}

export function extractMadagascarCadastralRows(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const hasContext = hasMadagascarCadastralStructuralSignature(text)
    || (/\bXV\b/i.test(text) && /\bYV\b/i.test(text) && /\bnum\b|cadastral|cadastre|grid|grille|carreau/i.test(text));
  if (!hasContext || hasMadagascarMapGridTickTakeover(text)) return [];
  const rows = [];
  const seen = new Set();
  for (const line of text.split("\n").map(item => item.trim()).filter(Boolean)) {
    if (/^(?:NC\s*)?(?:num|n[°o]?|#)?\s*[\|,;\s-]*x\s*v[\|,;\s-]*y\s*v(?:[\|,;\s-]*(?:CM[_\s-]*NOMFIR|num))?$/i.test(line)
      || /^(?:NC|#)\s*[\|,;\s-]*X\s*V\s*[\|,;\s-]*Y\s*V/i.test(line)) continue;
    const parts = line.split("|").map(item => item.trim()).filter(Boolean);
    let row = null;
    if (parts.length >= 3 && !/^NC$/i.test(parts[0]) && !/^num$/i.test(parts[0])) {
      if (parts.length >= 5 && /^[-+]?\d+$/.test(parts[0])
        && isMadagascarGridCoordinate(parts[1]) && isMadagascarGridCoordinate(parts[2])) {
        row = { num: normalizeMadagascarCellId(parts.at(-1)), xv: normalizeDecimalToken(parts[1]), yv: normalizeDecimalToken(parts[2]) };
      } else {
        row = { num: normalizeMadagascarCellId(parts[0]), xv: normalizeDecimalToken(parts[1]), yv: normalizeDecimalToken(parts[2]) };
      }
    }
    const labeled = line.match(/(?:^|\b)(?:num|n[°o]?|#)?\s*([A-Za-z0-9-]{1,16})\D+XV\D*([-+]?\d+(?:[.,]\d+)?)\D+YV\D*([-+]?\d+(?:[.,]\d+)?)/i);
    if (!row && labeled) {
      row = { num: normalizeMadagascarCellId(labeled[1]), xv: normalizeDecimalToken(labeled[2]), yv: normalizeDecimalToken(labeled[3]) };
    }
    if (!row) {
      const numericTokens = line.match(/[-+]?\d+(?:[.,]\d+)?/g) || [];
      if (numericTokens.length >= 4) {
        const nc = Number(normalizeDecimalToken(numericTokens[0]));
        if (Number.isInteger(nc) && nc >= 1 && nc <= 999
          && isMadagascarGridCoordinate(numericTokens[1])
          && isMadagascarGridCoordinate(numericTokens[2])
          && isValidMadagascarCellId(numericTokens.at(-1))) {
          row = { num: normalizeMadagascarCellId(numericTokens.at(-1)), xv: normalizeDecimalToken(numericTokens[1]), yv: normalizeDecimalToken(numericTokens[2]) };
        }
      }
    }
    if (!row) {
      const cleaned = line.replace(/\b(?:num|n[°o]?|xv|yv)\b/gi, " ").replace(/[|:;，,]/g, " ");
      const tokens = cleaned.match(/[-+]?\d+(?:[.,]\d+)?|[A-Za-z]?\d[A-Za-z0-9-]*/g) || [];
      if (tokens.length >= 3) {
        row = { num: normalizeMadagascarCellId(tokens[0]), xv: normalizeDecimalToken(tokens[1]), yv: normalizeDecimalToken(tokens[2]) };
      }
    }
    const num = normalizeMadagascarCellId(row?.num);
    const xv = Number(row?.xv);
    const yv = Number(row?.yv);
    if (!isValidMadagascarCellId(num)
      || !Number.isFinite(xv) || xv <= 0 || xv >= 1000000
      || !Number.isFinite(yv) || yv <= 0 || yv >= 1000000
      || MADAGASCAR_MAP_TICK_VALUES.has(num)) continue;
    const normalized = Object.freeze({ num, xv: row.xv, yv: row.yv });
    const key = `${normalized.num}|${normalized.xv}|${normalized.yv}`;
    if (!seen.has(key)) {
      seen.add(key);
      rows.push(normalized);
    }
  }
  return rows;
}

function inferMadagascarGridSpacing(values = []) {
  const sorted = Array.from(new Set(values.map(Number).filter(Number.isFinite))).sort((left, right) => left - right);
  const differences = sorted.slice(1).map((value, index) => value - sorted[index]).filter(value => value > 0);
  return differences.length > 0 ? Math.min(...differences) : MADAGASCAR_DEFAULT_CELL_SIZE_METERS;
}

export function convertMadagascarCadastralToWgs84(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  if (!Number.isFinite(easting) || !Number.isFinite(northing)) return null;
  const lon = MADAGASCAR_LOCAL_ANCHOR.longitude
    + ((easting - MADAGASCAR_LOCAL_ANCHOR.projectedX) / MADAGASCAR_METERS_PER_DEGREE_LONGITUDE);
  const lat = MADAGASCAR_LOCAL_ANCHOR.latitude
    + ((northing - MADAGASCAR_LOCAL_ANCHOR.projectedY) / MADAGASCAR_METERS_PER_DEGREE_LATITUDE);
  return lon >= 42 && lon <= 52 && lat >= -27 && lat <= -10
    ? Object.freeze({ lon, lat })
    : null;
}

export function buildMadagascarCadastralCellPolygons(rows = []) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const halfWidth = inferMadagascarGridSpacing(sourceRows.map(row => row.xv)) / 2;
  const halfHeight = inferMadagascarGridSpacing(sourceRows.map(row => row.yv)) / 2;
  return sourceRows.map(row => {
    const centerX = Number(row.xv);
    const centerY = Number(row.yv);
    const corners = [
      [centerX - halfWidth, centerY - halfHeight],
      [centerX + halfWidth, centerY - halfHeight],
      [centerX + halfWidth, centerY + halfHeight],
      [centerX - halfWidth, centerY + halfHeight]
    ].map(([x, y]) => convertMadagascarCadastralToWgs84(x, y));
    if (!corners.every(Boolean)) return null;
    return Object.freeze({
      label: String(row.num || ""),
      sourceProjectedCenter: Object.freeze({ x: centerX, y: centerY }),
      points: Object.freeze(corners)
    });
  }).filter(Boolean);
}

function buildUploadEvidenceText({ fileName = "", decodedFileName = "", rawHint = "" } = {}) {
  return [fileName, decodedFileName, rawHint]
    .map(normalizeEvidenceText)
    .filter(Boolean)
    .join("\n");
}

const conflictingFamilyPattern = /Kyrgyz|Kyrgyzstan|吉尔吉斯|Киргиз|Кыргыз|莫桑比克|Mozambique|Mo[çc]ambique|Tete|BFTM|MGRS|UTM|Gauss|Гаусс|Крюгер/i;

export function getWgs84StrongRouteEvidence(input = {}) {
  const text = buildUploadEvidenceText(input);
  const explicitWgs84 = /\bWGS\s*[-_ ]?84\b|EPSG\s*[:#-]?\s*4326/i.test(text);
  const longitudeEvidence = /经度(?:东)?|东经|longitude|\blon\b|east\s+longitude/i.test(text);
  const latitudeEvidence = /纬度|北纬|latitude|\blat\b|north\s+latitude/i.test(text);
  const tableEvidence = /表格|坐标表|table|coordinate\s+table/i.test(text);
  const conflictingFamily = conflictingFamilyPattern.test(text);
  const matched = !conflictingFamily
    && (explicitWgs84 || (longitudeEvidence && latitudeEvidence && tableEvidence));

  return Object.freeze({
    family: "wgs84_table",
    matched,
    source: matched ? "upload_metadata" : "",
    reasons: [
      explicitWgs84 ? "explicit_wgs84" : "",
      longitudeEvidence ? "longitude_axis" : "",
      latitudeEvidence ? "latitude_axis" : "",
      tableEvidence ? "table_context" : "",
      conflictingFamily ? "conflicting_family" : ""
    ].filter(Boolean)
  });
}

export function getMadagascarCadastralStrongRouteEvidence(input = {}) {
  const text = buildUploadEvidenceText(input);
  const madagascarEvidence = /Madagascar|马达加斯加|馬達加斯加|Malagasy/i.test(text);
  const cadastralEvidence = /cadastral|cadastre|grille\s+cadastrale|mineral\s+cadastral|carreau|liste[_\s-]*carr[eé]s?|矿权网格|地籍网格/i.test(text);
  const structuralGridEvidence = input.structuralEvidence?.hasTableGrid === true;
  const projectedConflict = /\bBFTM\b|\bUTM\b|projected[_\s-]*xy|投影坐标/i.test(text);
  const matched = madagascarEvidence && (cadastralEvidence || structuralGridEvidence) && !projectedConflict;

  return Object.freeze({
    family: "madagascar_cadastral_grid",
    matched,
    source: matched ? "upload_metadata" : "",
    reasons: [
      madagascarEvidence ? "madagascar_context" : "",
      cadastralEvidence ? "cadastral_grid_context" : "",
      structuralGridEvidence ? "ocr_independent_table_grid" : "",
      projectedConflict ? "projected_family_conflict" : ""
    ].filter(Boolean)
  });
}

function countTrueClusters(values) {
  let count = 0;
  let active = false;
  for (const value of values) {
    if (value && !active) count += 1;
    active = Boolean(value);
  }
  return count;
}

function paethPredictor(left, up, upperLeft) {
  const estimate = left + up - upperLeft;
  const leftDistance = Math.abs(estimate - left);
  const upDistance = Math.abs(estimate - up);
  const upperLeftDistance = Math.abs(estimate - upperLeft);
  if (leftDistance <= upDistance && leftDistance <= upperLeftDistance) return left;
  return upDistance <= upperLeftDistance ? up : upperLeft;
}

export function detectUploadTableStructure(buffer, mimeType = "") {
  const bytes = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
  const unsupported = Object.freeze({
    supported: false,
    hasTableGrid: false,
    horizontalLineClusters: 0,
    verticalLineClusters: 0
  });
  if (!/^image\/png$/i.test(String(mimeType || "")) || bytes.length < 33) return unsupported;
  if (!bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) return unsupported;

  let offset = 8;
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = -1;
  let interlace = 1;
  const idat = [];
  while (offset + 12 <= bytes.length) {
    const length = bytes.readUInt32BE(offset);
    const type = bytes.toString("ascii", offset + 4, offset + 8);
    const dataStart = offset + 8;
    const dataEnd = dataStart + length;
    if (dataEnd + 4 > bytes.length) return unsupported;
    if (type === "IHDR") {
      width = bytes.readUInt32BE(dataStart);
      height = bytes.readUInt32BE(dataStart + 4);
      bitDepth = bytes[dataStart + 8];
      colorType = bytes[dataStart + 9];
      interlace = bytes[dataStart + 12];
    } else if (type === "IDAT") {
      idat.push(bytes.subarray(dataStart, dataEnd));
    } else if (type === "IEND") {
      break;
    }
    offset = dataEnd + 4;
  }

  const channels = colorType === 0 ? 1 : colorType === 2 ? 3 : colorType === 6 ? 4 : 0;
  if (!width || !height || bitDepth !== 8 || !channels || interlace !== 0 || idat.length === 0) return unsupported;
  const stride = width * channels;
  let inflated;
  try {
    inflated = inflateSync(Buffer.concat(idat), { maxOutputLength: (stride + 1) * height });
  } catch {
    return unsupported;
  }
  if (inflated.length < (stride + 1) * height) return unsupported;

  const previous = Buffer.alloc(stride);
  const current = Buffer.alloc(stride);
  const rowDarkCounts = new Uint32Array(height);
  const columnDarkCounts = new Uint32Array(width);
  let sourceOffset = 0;
  for (let y = 0; y < height; y += 1) {
    const filter = inflated[sourceOffset];
    sourceOffset += 1;
    for (let x = 0; x < stride; x += 1) {
      const raw = inflated[sourceOffset + x];
      const left = x >= channels ? current[x - channels] : 0;
      const up = previous[x];
      const upperLeft = x >= channels ? previous[x - channels] : 0;
      current[x] = filter === 0 ? raw
        : filter === 1 ? (raw + left) & 255
          : filter === 2 ? (raw + up) & 255
            : filter === 3 ? (raw + Math.floor((left + up) / 2)) & 255
              : filter === 4 ? (raw + paethPredictor(left, up, upperLeft)) & 255
                : raw;
    }
    sourceOffset += stride;
    for (let x = 0; x < width; x += 1) {
      const pixelOffset = x * channels;
      const luminance = channels === 1
        ? current[pixelOffset]
        : (current[pixelOffset] * 299 + current[pixelOffset + 1] * 587 + current[pixelOffset + 2] * 114) / 1000;
      if (luminance < 110) {
        rowDarkCounts[y] += 1;
        columnDarkCounts[x] += 1;
      }
    }
    previous.set(current);
  }

  const horizontalLineClusters = countTrueClusters(Array.from(rowDarkCounts, count => count / width >= 0.35));
  const verticalLineClusters = countTrueClusters(Array.from(columnDarkCounts, count => count / height >= 0.25));
  return Object.freeze({
    supported: true,
    hasTableGrid: horizontalLineClusters >= 3 && verticalLineClusters >= 4,
    horizontalLineClusters,
    verticalLineClusters
  });
}

export function buildPrimaryRouteDecision({ family, evidence } = {}) {
  const selected = evidence?.matched === true;
  return Object.freeze({
    family: String(family || evidence?.family || ""),
    selected,
    genericProviderAllowed: !selected,
    genericSkippedReason: selected ? `${String(family || evidence?.family || "family")}_specialized_primary_selected` : "",
    failClosedOnSpecializedFailure: selected
  });
}

function countMatches(value, pattern) {
  return Array.from(String(value || "").matchAll(pattern)).length;
}

function countDecimalComponents(value) {
  const withoutDms = String(value || "").replace(
    /\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]?\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S|E|W|O)\b/giu,
    " "
  );
  return (withoutDms.match(/[-+]?\d{1,3}(?:[.,]\d+)/gu) || []).length;
}

function hasExactlyOneDecimalCoordinatePair(value) {
  return countDecimalComponents(value) === 2;
}

function countDmsComponents(value) {
  return countMatches(
    value,
    /\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]?\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S|E|W|O)\b/giu
  );
}

function getLineText(line) {
  return normalizeCoordinateEvidenceText(
    typeof line === "string" ? line : line?.text || line?.rawText || ""
  ).trim();
}

function getFiniteLayoutBox(line) {
  const box = line?.bbox || line?.box || line?.source_bbox || {};
  if (Array.isArray(box)) {
    const [x0, y0, x1, y1] = box.map(Number);
    return [x0, y0, x1, y1].every(Number.isFinite) && x1 > x0 && y1 > y0
      ? Object.freeze({ x0, y0, x1, y1 })
      : null;
  }
  const x0 = Number(box.x0 ?? box.left ?? box.x);
  const y0 = Number(box.y0 ?? box.top ?? box.y);
  const x1 = Number(box.x1 ?? (Number.isFinite(x0) ? x0 + Number(box.width) : NaN));
  const y1 = Number(box.y1 ?? (Number.isFinite(y0) ? y0 + Number(box.height) : NaN));
  return [x0, y0, x1, y1].every(Number.isFinite) && x1 > x0 && y1 > y0
    ? Object.freeze({ x0, y0, x1, y1 })
    : null;
}

function isFiniteLayoutBox(line) {
  return getFiniteLayoutBox(line) !== null;
}

function layoutBoxesAreDistinctAndNonOverlapping(leftLine, rightLine) {
  const left = getFiniteLayoutBox(leftLine);
  const right = getFiniteLayoutBox(rightLine);
  if (!left || !right) return false;
  const intersectionWidth = Math.min(left.x1, right.x1) - Math.max(left.x0, right.x0);
  const intersectionHeight = Math.min(left.y1, right.y1) - Math.max(left.y0, right.y0);
  return intersectionWidth <= 0 || intersectionHeight <= 0;
}

function extractStructuredRowLabel(line) {
  return String(line || "").match(
    /^\s*(?:point\s*)?([A-Z]|\d{1,3})\s*(?:(?:\.(?!\d))|[|):;,\-]|\s)/iu
  )?.[1]?.toUpperCase() || "";
}

function isExplicitGroupHeadingLine(line) {
  return /^(?:(?:location|coordinate)\s+group|group|site|sites|area|mining\s+area|矿区|礦區)\s*(?:[|:#-]\s*)?[A-Z0-9_-]+\s*$/iu.test(String(line || ""));
}

function splitStructuredFields(line, delimiter) {
  return String(line || "").split(delimiter).map(field => field.trim());
}

function parseExplicitAxisHeaderLine(line, index) {
  const source = String(line || "");
  const delimiter = source.includes("|") ? "|"
    : source.includes("\t") ? "\t"
      : source.includes(";") ? ";"
        : source.includes(",") ? ","
          : "";
  if (!delimiter) return null;
  const fields = splitStructuredFields(source, delimiter);
  if (fields.length < 2 || fields.some(field => /\d/u.test(field))) return false;
  const longitudeField = /^(?:longitude(?:\s+DMS)?|lon|经度|經度|东经|東經|西经|西經)$/iu;
  const latitudeField = /^(?:latitude(?:\s+DMS)?|lat|纬度|緯度|北纬|北緯|南纬|南緯)$/iu;
  const auxiliaryField = /^(?:point|no\.?|number|row|id|label|name|点号|點號|编号|編號|序号|序號)$/iu;
  const longitudeIndexes = fields.flatMap((field, index) => longitudeField.test(field) ? [index] : []);
  const latitudeIndexes = fields.flatMap((field, index) => latitudeField.test(field) ? [index] : []);
  const auxiliaryIndexes = fields.flatMap((field, fieldIndex) => auxiliaryField.test(field) ? [fieldIndex] : []);
  if (longitudeIndexes.length !== 1 || latitudeIndexes.length !== 1) return null;
  if (Math.abs(longitudeIndexes[0] - latitudeIndexes[0]) !== 1) return null;
  if (auxiliaryIndexes.length > 1 || fields.length !== 2 + auxiliaryIndexes.length) return null;
  if (!fields.every(field => longitudeField.test(field) || latitudeField.test(field) || auxiliaryField.test(field))) return null;
  return Object.freeze({
    index,
    delimiter,
    fieldCount: fields.length,
    longitudeIndex: longitudeIndexes[0],
    latitudeIndex: latitudeIndexes[0],
    labelIndex: auxiliaryIndexes[0] ?? null
  });
}

function structuredLabelsAreContinuous(entries) {
  const labels = entries.map(entry => entry.label);
  if (labels.length === 0 || labels.some(label => !label)) return false;
  if (labels.every(label => /^\d{1,3}$/u.test(label))) {
    const numbers = labels.map(Number);
    return numbers.every((value, index) => index === 0 || value === numbers[index - 1] + 1);
  }
  if (labels.every(label => /^[A-Z]$/u.test(label))) {
    return labels.every((value, index) => index === 0
      || value.charCodeAt(0) === labels[index - 1].charCodeAt(0) + 1);
  }
  return false;
}

function parseBoundCoordinateRow({ line, index, header, format }) {
  const fields = splitStructuredFields(line, header.delimiter);
  if (fields.length !== header.fieldCount || fields.some(field => !field)) return null;
  const longitudeValue = fields[header.longitudeIndex];
  const latitudeValue = fields[header.latitudeIndex];
  if (format === "decimal") {
    if (!parseStrictDecimalCoordinate(longitudeValue, 180)
      || !parseStrictDecimalCoordinate(latitudeValue, 90)) return null;
  } else {
    const strictLongitudeDms = /^\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]?\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:E|W|O)$/iu;
    const strictLatitudeDms = /^\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]?\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S)$/iu;
    if (!strictLongitudeDms.test(longitudeValue) || !strictLatitudeDms.test(latitudeValue)) return null;
  }
  const label = header.labelIndex === null ? "" : extractStructuredRowLabel(`${fields[header.labelIndex]} |`);
  if (header.labelIndex !== null && !label) return null;
  return Object.freeze({ index, line, label });
}

function collectBoundCoordinateSegments({ lines, headers, format }) {
  return headers.map(header => {
    const rows = [];
    for (let index = header.index + 1; index < lines.length; index += 1) {
      const row = parseBoundCoordinateRow({ line: lines[index], index, header, format });
      if (!row) break;
      rows.push(row);
    }
    return Object.freeze({ headerIndex: header.index, rows: Object.freeze(rows) });
  }).filter(segment => segment.rows.length > 0);
}

function isClosedAxisValueLine(line, axis) {
  const longitudeLabel = /^(?:longitude|lon|经度|經度|东经|東經|西经|西經)$/iu;
  const latitudeLabel = /^(?:latitude|lat|纬度|緯度|北纬|北緯|南纬|南緯)$/iu;
  const match = String(line || "").match(/^\s*([^:=|]+?)\s*[:=|]\s*(\S(?:.*\S)?)\s*$/u);
  if (!match) return false;
  const label = match[1].trim();
  const value = match[2].trim();
  const labelMatches = axis === "longitude" ? longitudeLabel.test(label) : latitudeLabel.test(label);
  if (!labelMatches) return false;
  const expectation = getDirectionalLabelExpectation(label, axis);
  const decimal = parseStrictDecimalCoordinate(value, axis === "longitude" ? 180 : 90);
  if (decimal) return isDecimalDirectionCompatible(decimal, expectation);
  const dms = parseStrictDmsCoordinate(value, axis === "longitude" ? "EWO" : "NS");
  return Number.isFinite(dms) && isDmsDirectionCompatible(value, expectation);
}

function isClosedProjectedAxisValueLine(line) {
  return /^\s*(?:Easting|Northing|X|Y)\s*[:=|]\s*[-+]?\d+(?:[.,]\d+)?\s*$/iu.test(String(line || ""));
}

function parseClosedProjectedAxisValueLine(line, index) {
  const match = String(line || "").match(/^\s*(Easting|Northing|X|Y)\s*[:=|]\s*([-+]?\d+(?:[.,]\d+)?)\s*$/iu);
  if (!match) return null;
  const axis = /^(?:Easting|X)$/iu.test(match[1]) ? "EASTING" : "NORTHING";
  return Object.freeze({ index, axis, value: match[2] });
}

function collectClosedProjectedAxisPairs(lines) {
  const pairs = [];
  const consumed = new Set();
  for (let index = 0; index < lines.length - 1; index += 1) {
    const first = parseClosedProjectedAxisValueLine(lines[index], index);
    const second = parseClosedProjectedAxisValueLine(lines[index + 1], index + 1);
    if (!first || !second || first.axis === second.axis || consumed.has(index) || consumed.has(index + 1)) continue;
    pairs.push(Object.freeze({
      first,
      second,
      axisOrder: first.axis === "EASTING" ? "EASTING_NORTHING" : "NORTHING_EASTING"
    }));
    consumed.add(index);
    consumed.add(index + 1);
    index += 1;
  }
  return Object.freeze({ pairs: Object.freeze(pairs), consumedLineCount: consumed.size });
}

function parseExplicitProjectedPointLine(line, index) {
  const fields = splitStructuredFields(line, "|");
  if (fields.length !== 4 || !/^POINT$/iu.test(fields[0])) return null;
  const label = extractStructuredRowLabel(`${fields[1]} |`);
  const first = fields[2].match(/^(EASTING|NORTHING|X|Y)\s*=\s*([-+]?\d{4,9}(?:[.,]\d+)?)$/iu);
  const second = fields[3].match(/^(EASTING|NORTHING|X|Y)\s*=\s*([-+]?\d{4,9}(?:[.,]\d+)?)$/iu);
  if (!label || !first || !second) return null;
  const firstAxis = /^(?:EASTING|X)$/iu.test(first[1]) ? "EASTING" : "NORTHING";
  const secondAxis = /^(?:EASTING|X)$/iu.test(second[1]) ? "EASTING" : "NORTHING";
  if (firstAxis === secondAxis) return null;
  return Object.freeze({
    index,
    label,
    firstAxis,
    secondAxis,
    firstValue: first[2],
    secondValue: second[2],
    axisOrder: firstAxis === "EASTING" ? "EASTING_NORTHING" : "NORTHING_EASTING"
  });
}

function parseProjectedAxisHeaderLine(line, index) {
  const source = String(line || "");
  const delimiter = source.includes("|") ? "|"
    : source.includes("\t") ? "\t"
      : source.includes(";") ? ";"
        : source.includes(",") ? ","
          : "";
  if (!delimiter) return null;
  const fields = splitStructuredFields(source, delimiter);
  const eastingField = /^(?:Easting|X)$/iu;
  const northingField = /^(?:Northing|Y)$/iu;
  const auxiliaryField = /^(?:point|no\.?|number|row|id|label|name|点号|點號|编号|編號|序号|序號)$/iu;
  const eastingIndexes = fields.flatMap((field, fieldIndex) => eastingField.test(field) ? [fieldIndex] : []);
  const northingIndexes = fields.flatMap((field, fieldIndex) => northingField.test(field) ? [fieldIndex] : []);
  const auxiliaryIndexes = fields.flatMap((field, fieldIndex) => auxiliaryField.test(field) ? [fieldIndex] : []);
  if (eastingIndexes.length !== 1 || northingIndexes.length !== 1) return null;
  if (Math.abs(eastingIndexes[0] - northingIndexes[0]) !== 1) return null;
  if (auxiliaryIndexes.length > 1 || fields.length !== 2 + auxiliaryIndexes.length) return null;
  if (!fields.every(field => eastingField.test(field) || northingField.test(field) || auxiliaryField.test(field))) return null;
  return Object.freeze({
    index,
    delimiter,
    fieldCount: fields.length,
    eastingIndex: eastingIndexes[0],
    northingIndex: northingIndexes[0],
    labelIndex: auxiliaryIndexes[0] ?? null
  });
}

function analyzeBoundProjectedRows(lines, headers) {
  let count = 0;
  let conflictCount = 0;
  for (const header of headers) {
    for (let index = header.index + 1; index < lines.length; index += 1) {
      if (parseProjectedAxisHeaderLine(lines[index], index)) break;
      const fields = splitStructuredFields(lines[index], header.delimiter);
      const projectedValue = /^[-+]?\d{4,9}(?:[.,]\d+)?$/u;
      const labelLooksValid = header.labelIndex !== null
        && fields[header.labelIndex]
        && extractStructuredRowLabel(`${fields[header.labelIndex]} |`);
      const declaredAxisValues = [fields[header.eastingIndex] || "", fields[header.northingIndex] || ""];
      const projectedDataLike = fields.filter(field => projectedValue.test(field)).length >= 2
        || (fields.length === header.fieldCount && labelLooksValid && declaredAxisValues.some(Boolean))
        || lines[index].includes(header.delimiter)
        || /\p{N}/u.test(lines[index]);
      const valid = fields.length === header.fieldCount
        && fields.every(Boolean)
        && projectedValue.test(fields[header.eastingIndex])
        && projectedValue.test(fields[header.northingIndex])
        && header.labelIndex !== null
        && extractStructuredRowLabel(`${fields[header.labelIndex]} |`);
      if (valid) {
        count += 1;
        continue;
      }
      if (projectedDataLike) {
        conflictCount += 1;
        continue;
      }
      break;
    }
  }
  return Object.freeze({ count, conflictCount });
}

function hemisphereFromLatitudeBand(value) {
  const band = String(value || "").toUpperCase();
  if (!/^[C-HJ-NP-X]$/u.test(band)) return "";
  return band >= "N" ? "N" : "S";
}

function parseStrictMgrsCoordinateLine(line) {
  const match = String(line || "").match(
    /^\s*MGRS\s*[|:]\s*([1-9]|[1-5]\d|60)([C-HJ-NP-X])\s*[|]\s*([A-HJ-NP-Z]{2})\s*[|]\s*(\d{1,5})\s*[|]\s*(\d{1,5})\s*$/iu
  );
  if (!match || match[4].length !== match[5].length) return null;
  return Object.freeze({
    zone: Number(match[1]),
    band: match[2].toUpperCase(),
    hemisphere: hemisphereFromLatitudeBand(match[2]),
    gridSquare: match[3].toUpperCase(),
    precisionDigits: match[4].length
  });
}

function collectProjectedCrsSignals(lines, mgrsCoordinates) {
  const zones = [];
  const hemispheres = [];
  const datumIdentities = [];
  const epsgCodes = [];
  let invalidZoneSuffix = false;
  let invalidCrsField = false;
  let explicitOtherProjectedCrs = false;
  const explicitProjectedDeclaration = lines.some(line => (
    /\b(?:projected\s+crs|projected\s+coordinate\s+system|projection)\b/iu.test(line)
      || /^CRS\s*\|/iu.test(line)
  ));
  for (const line of lines) {
    const source = String(line || "");
    if (/\bEPSG\s*[:#=-]?\s*\d{4,6}\s*\//iu.test(source)
      || /\b(?:zone|zona|fuso)\s*[:#=-]?\s*\d{1,2}[A-Z]?\s*\//iu.test(source)
      || /\bHemisphere\s*[:=-]?\s*(?:N|S|North|South)\s*\//iu.test(source)) {
      invalidCrsField = true;
    }
    const epsgMatches = [...source.matchAll(/\bEPSG\s*[:#=-]?\s*(\d{4,6})\b/giu)];
    if (/\bEPSG\b/iu.test(source) && epsgMatches.length !== 1) invalidCrsField = true;
    epsgMatches.forEach(match => epsgCodes.push(Number(match[1])));

    const canonicalDatums = [];
    if (/\bWGS\s*(?:84|1984)\b/iu.test(source)) canonicalDatums.push("WGS84");
    if (/\bNAD\s*83\b/iu.test(source)) canonicalDatums.push("NAD83");
    if (canonicalDatums.length > 0) datumIdentities.push(...canonicalDatums);
    if (/\bDatum\b/iu.test(source) && canonicalDatums.length === 0) {
      const customDatum = source.match(/(?:^|[|;])\s*Datum\s*[:=-]?\s*([A-Z][A-Z0-9_-]{2,20})\s*(?=$|[|;])/iu);
      if (customDatum) datumIdentities.push(customDatum[1].replace(/[-_\s]+/gu, "").toUpperCase());
      else invalidCrsField = true;
    }

    const zoneMatches = [...source.matchAll(/\b(?:UTM\s*(?:zone\s*)?|zone|zona|fuso)\s*[:#=-]?\s*(\d{1,2})([A-Z])?\b/giu)];
    if (/\b(?:zone|zona|fuso)\b/iu.test(source) && zoneMatches.length === 0) invalidCrsField = true;
    for (const match of zoneMatches) {
      zones.push(Number(match[1]));
      const suffix = String(match[2] || "").toUpperCase();
      if (suffix === "N" || suffix === "S") hemispheres.push(suffix);
      else if (suffix) {
        const hemisphere = hemisphereFromLatitudeBand(suffix);
        if (hemisphere) hemispheres.push(hemisphere);
        else invalidZoneSuffix = true;
      }
    }

    const explicitHemisphere = source.match(/\bHemisphere\s*[:=-]?\s*(N|S|North|South)\b/iu);
    if (explicitHemisphere) hemispheres.push(/^N/iu.test(explicitHemisphere[1]) ? "N" : "S");
    if (/\b(?:northern|north)\s+hemisphere\b/iu.test(source)) hemispheres.push("N");
    if (/\b(?:southern|south)\s+hemisphere\b/iu.test(source)) hemispheres.push("S");
    if (/\bHemisphere\b/iu.test(source) && !explicitHemisphere
      && !/\b(?:northern|southern|north|south)\s+hemisphere\b/iu.test(source)) invalidCrsField = true;
  }
  for (const coordinate of mgrsCoordinates) {
    zones.push(coordinate.zone);
    hemispheres.push(coordinate.hemisphere);
  }
  const uniqueEpsgCodes = [...new Set(epsgCodes)];
  let epsgSupported = uniqueEpsgCodes.length <= 1;
  for (const code of uniqueEpsgCodes) {
    if (code >= 32601 && code <= 32660) {
      datumIdentities.push("WGS84");
      zones.push(code - 32600);
      hemispheres.push("N");
    } else if (code >= 32701 && code <= 32760) {
      datumIdentities.push("WGS84");
      zones.push(code - 32700);
      hemispheres.push("S");
    } else if (code !== 4326 && explicitProjectedDeclaration) {
      datumIdentities.push(`EPSG${code}`);
      explicitOtherProjectedCrs = true;
    } else {
      epsgSupported = false;
    }
  }
  const uniqueDatumIdentities = [...new Set(datumIdentities)];
  const validZones = zones.length > 0 && zones.every(zone => Number.isInteger(zone) && zone >= 1 && zone <= 60);
  const oneZone = validZones && new Set(zones).size === 1;
  const boundedHemispheres = hemispheres.filter(Boolean);
  const oneHemisphere = boundedHemispheres.length > 0 && new Set(boundedHemispheres).size === 1;
  const datumEvidence = uniqueDatumIdentities.length > 0;
  const crsIdentityConsistent = datumEvidence && uniqueDatumIdentities.length === 1
    && uniqueEpsgCodes.length <= 1 && epsgSupported;
  const requiresGridIdentity = lines.some(line => /\b(?:UTM|MGRS)\b/iu.test(line))
    || mgrsCoordinates.length > 0 || zones.length > 0 || boundedHemispheres.length > 0
    || uniqueEpsgCodes.some(code => (code >= 32601 && code <= 32760));
  const conflicting = invalidZoneSuffix
    || invalidCrsField
    || zones.some(zone => !Number.isInteger(zone) || zone < 1 || zone > 60)
    || (zones.length > 0 && new Set(zones).size > 1)
    || (boundedHemispheres.length > 0 && new Set(boundedHemispheres).size > 1)
    || uniqueDatumIdentities.length > 1
    || uniqueEpsgCodes.length > 1
    || !epsgSupported;
  return Object.freeze({
    datumEvidence,
    zoneEvidence: requiresGridIdentity ? validZones : explicitOtherProjectedCrs,
    hemisphereEvidence: requiresGridIdentity ? boundedHemispheres.length > 0 : explicitOtherProjectedCrs,
    conflicting,
    consistent: crsIdentityConsistent && !invalidZoneSuffix && !invalidCrsField
      && (requiresGridIdentity ? oneZone && oneHemisphere : explicitOtherProjectedCrs),
    explicitOtherProjectedCrs
  });
}

function buildOneShotFamilyResult({ family, matched, reason, evidence = {} }) {
  return Object.freeze({
    family,
    matched: matched === true,
    reviewRequired: true,
    reason,
    evidence: Object.freeze({
      coordinateRowCount: Math.max(0, Number(evidence.coordinateRowCount) || 0),
      dmsRowCount: Math.max(0, Number(evidence.dmsRowCount) || 0),
      repeatedHeaderCount: Math.max(0, Number(evidence.repeatedHeaderCount) || 0),
      groupHeadingCount: Math.max(0, Number(evidence.groupHeadingCount) || 0),
      layoutRegionCount: Math.max(0, Number(evidence.layoutRegionCount) || 0),
      ambiguousMultiPairLineCount: Math.max(0, Number(evidence.ambiguousMultiPairLineCount) || 0),
      conflictingCoordinateLineCount: Math.max(0, Number(evidence.conflictingCoordinateLineCount) || 0),
      projectedCoordinateRowCount: Math.max(0, Number(evidence.projectedCoordinateRowCount) || 0),
      projectedConflictLineCount: Math.max(0, Number(evidence.projectedConflictLineCount) || 0),
      projectedEvidenceComplete: evidence.projectedEvidenceComplete === true
    })
  });
}

// Classifies only OCR-visible structure. It deliberately ignores file names,
// countries, fixture IDs and coordinate values as family-selection signals.
export function classifyOneShotStructuredFamily({ text = "", layoutLines = [] } = {}) {
  const normalizedText = normalizeCoordinateEvidenceText(text);
  const layout = Array.isArray(layoutLines) ? layoutLines : [];
  const layoutText = layout.map(getLineText).filter(Boolean);
  const textLines = normalizedText.split("\n").map(line => line.trim()).filter(Boolean);
  // OCR text and layout lines describe the same pixels. Count one source only
  // so duplicated OCR representations cannot manufacture repeated headers or
  // restarted row labels. Layout boxes remain available for region evidence.
  const lines = textLines.length > 0 ? textLines : layoutText;
  const joined = lines.join("\n");
  const decimalPairEntries = lines.flatMap((line, index) => (
    countDmsComponents(line) === 0 && hasExactlyOneDecimalCoordinatePair(line)
      ? [Object.freeze({ index, line, label: extractStructuredRowLabel(line) })]
      : []
  ));
  const dmsPairEntries = lines.flatMap((line, index) => (
    countDmsComponents(line) === 2 && countDecimalComponents(line) === 0
      ? [Object.freeze({ index, line, label: extractStructuredRowLabel(line) })]
      : []
  ));
  const decimalPairRows = decimalPairEntries.map(entry => entry.line);
  const dmsPairRows = dmsPairEntries.map(entry => entry.line);
  const ambiguousMultiPairLineCount = lines.filter(line => {
    const dmsComponentCount = countDmsComponents(line);
    const decimalComponentCount = countDecimalComponents(line);
    return dmsComponentCount > 2
      || decimalComponentCount > 2
      || (dmsComponentCount > 0 && decimalComponentCount > 0);
  }).length;
  const decimalEvidenceLineCount = lines.filter(line => countDecimalComponents(line) > 0).length;
  const dmsEvidenceLineCount = lines.filter(line => countDmsComponents(line) > 0).length;
  const coordinateEvidenceLineCount = lines.filter(line => (
    countDmsComponents(line) > 0 || countDecimalComponents(line) > 0
  )).length;
  const conflictingCoordinateLineCount = lines.filter(line => {
    const dmsComponentCount = countDmsComponents(line);
    const decimalComponentCount = countDecimalComponents(line);
    if (dmsComponentCount === 0 && decimalComponentCount === 0) return false;
    return !(dmsComponentCount === 0 && decimalComponentCount === 2)
      && !(dmsComponentCount === 2 && decimalComponentCount === 0);
  }).length;
  const coordinateRowCount = decimalPairRows.length + dmsPairRows.length;
  const axisHeaders = lines.flatMap((line, index) => {
    const header = parseExplicitAxisHeaderLine(line, index);
    return header ? [header] : [];
  });
  const axisHeaderIndexes = axisHeaders.map(header => header.index);
  const repeatedHeaderCount = axisHeaders.length;
  const groupHeadingCount = lines.filter(isExplicitGroupHeadingLine).length;
  const boundDecimalSegments = collectBoundCoordinateSegments({
    lines,
    headers: axisHeaders,
    format: "decimal"
  });
  const boundDmsSegments = collectBoundCoordinateSegments({
    lines,
    headers: axisHeaders,
    format: "dms"
  });
  const boundDecimalRows = boundDecimalSegments.flatMap(segment => segment.rows);
  const boundDmsRows = boundDmsSegments.flatMap(segment => segment.rows);
  const rowLabels = [...boundDecimalRows, ...boundDmsRows].map(entry => entry.label).filter(Boolean);
  const hasLabelRestart = new Set(rowLabels).size < rowLabels.length;
  const hasLongitudeAxis = lines.some(line => /(?:longitude|\blon\b|经度|經度|东经|東經|西经|西經)/iu.test(line));
  const hasLatitudeAxis = lines.some(line => /(?:latitude|\blat\b|纬度|緯度|北纬|北緯|南纬|南緯)/iu.test(line));
  const hasAxisHeader = axisHeaderIndexes.length > 0;
  const longitudeValueLineIndexes = lines.flatMap((line, index) => (
    isClosedAxisValueLine(line, "longitude") ? [index] : []
  ));
  const latitudeValueLineIndexes = lines.flatMap((line, index) => (
    isClosedAxisValueLine(line, "latitude") ? [index] : []
  ));

  const plusCode = /\b(?:plus\s*code|open\s*location\s*code)\b|\b[23456789CFGHJMPQRVWX]{4,8}\+[23456789CFGHJMPQRVWX]{2,}\b/iu.test(joined);
  const searchRole = /\b(?:search|rechercher|buscar)\b|搜索|搜尋/iu.test(joined);
  const detailRole = /\b(?:details?|place|location|address|directions?)\b|地点|地點|位置|地址|路线|路線/iu.test(joined);
  const coordinateLayoutLines = layout.filter(line => hasExactlyOneDecimalCoordinatePair(getLineText(line)) && isFiniteLayoutBox(line));
  const layoutRegionCount = coordinateLayoutLines.length;
  const searchLayoutLines = coordinateLayoutLines.filter(line => {
    const lineText = getLineText(line);
    return (/\b(?:search|rechercher|buscar)\b|搜索|搜尋/iu.test(lineText)) && textLines.includes(lineText);
  });
  const detailLayoutLines = coordinateLayoutLines.filter(line => {
    const lineText = getLineText(line);
    return (/\b(?:details?|place|location|address|directions?)\b|地点|地點|位置|地址|路线|路線/iu.test(lineText))
      && textLines.includes(lineText);
  });
  const hasBoundDistinctMapRegions = searchLayoutLines.some(searchLine => (
    detailLayoutLines.some(detailLine => (
      searchLine !== detailLine
      && getLineText(searchLine) !== getLineText(detailLine)
      && layoutBoxesAreDistinctAndNonOverlapping(searchLine, detailLine)
    ))
  ));
  const projectedAxisEvidenceLineCount = lines.filter(isClosedProjectedAxisValueLine).length;
  const projectedAxisHeaders = lines.flatMap((line, index) => {
    const header = parseProjectedAxisHeaderLine(line, index);
    return header ? [header] : [];
  });
  const projectedRowAnalysis = analyzeBoundProjectedRows(lines, projectedAxisHeaders);
  const boundProjectedRowCount = projectedRowAnalysis.count;
  const projectedAxisPairs = collectClosedProjectedAxisPairs(lines);
  const explicitProjectedPointLines = lines.map(parseExplicitProjectedPointLine).filter(Boolean);
  const malformedExplicitProjectedPointLineCount = lines.filter((line, index) => (
    /^\s*POINT\s*\|/iu.test(line) && !parseProjectedAxisHeaderLine(line, index)
  )).length
    - explicitProjectedPointLines.length;
  const mgrsLikeLines = lines.filter(line => /^\s*MGRS\s*[|:]\s*\d/iu.test(line));
  const strictMgrsCoordinates = mgrsLikeLines.map(parseStrictMgrsCoordinateLine).filter(Boolean);
  const strictMgrsCoordinateRowCount = strictMgrsCoordinates.length;
  const projectedCoordinateRowCount = boundProjectedRowCount
    + projectedAxisPairs.pairs.length
    + explicitProjectedPointLines.length
    + strictMgrsCoordinateRowCount;
  const projectedConflictLineCount = projectedRowAnalysis.conflictCount
    + Math.max(0, projectedAxisEvidenceLineCount - projectedAxisPairs.consumedLineCount)
    + Math.max(0, malformedExplicitProjectedPointLineCount)
    + (mgrsLikeLines.length - strictMgrsCoordinateRowCount);
  const projectedKeyword = /\b(?:UTM|MGRS|EPSG|datum|projection|projected|Easting|Northing)\b|投影|坐标系|座標系/iu.test(joined)
    || projectedAxisEvidenceLineCount > 0
    || projectedAxisHeaders.length > 0
    || boundProjectedRowCount > 0;
  const projectedCrsSignals = collectProjectedCrsSignals(lines, strictMgrsCoordinates);
  const axisOrderEvidence = projectedAxisHeaders.length > 0
    || projectedAxisPairs.pairs.length > 0
    || explicitProjectedPointLines.length > 0
    || lines.some(line => (
      /^\s*Axis\s+order\s*[:=]?\s*(?:Easting\s*[|,;]\s*Northing|X\s*[|,;]\s*Y)\s*$/iu.test(line)
    ));
  const projectedEvidenceComplete = projectedCrsSignals.datumEvidence
    && projectedCrsSignals.zoneEvidence
    && projectedCrsSignals.hemisphereEvidence
    && projectedCrsSignals.consistent
    && axisOrderEvidence
    && projectedCoordinateRowCount > 0
    && projectedConflictLineCount === 0;
  const decimalFamilyHasConflict = ambiguousMultiPairLineCount > 0
    || conflictingCoordinateLineCount > 0
    || dmsEvidenceLineCount > 0;
  const dmsFamilyHasConflict = ambiguousMultiPairLineCount > 0
    || conflictingCoordinateLineCount > 0
    || decimalEvidenceLineCount > 0;
  if (projectedKeyword) {
    const mixedMapAndProjectedStructure = plusCode || searchRole || detailRole;
    return buildOneShotFamilyResult({
      family: projectedEvidenceComplete && !mixedMapAndProjectedStructure
        ? ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE
        : ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW,
      matched: projectedEvidenceComplete && !mixedMapAndProjectedStructure,
      reason: mixedMapAndProjectedStructure
        ? "mixed_map_and_projected_structure"
        : projectedEvidenceComplete
          ? "explicit_projected_crs_structure"
          : projectedCrsSignals.conflicting || projectedConflictLineCount > 0
            ? "projected_crs_evidence_inconsistent"
            : "projected_crs_evidence_incomplete",
      evidence: {
        coordinateRowCount,
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount,
        projectedCoordinateRowCount,
        projectedConflictLineCount,
        projectedEvidenceComplete
      }
    });
  }
  if (plusCode && searchRole && detailRole && decimalPairRows.length >= 2 && layoutRegionCount >= 2
    && hasBoundDistinctMapRegions && !decimalFamilyHasConflict) {
    return buildOneShotFamilyResult({
      family: ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT,
      matched: true,
      reason: "map_search_detail_plus_code_structure",
      evidence: {
        coordinateRowCount,
        layoutRegionCount,
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount
      }
    });
  }

  const allDecimalRowsBoundAndLabeled = boundDecimalRows.length === decimalPairEntries.length
    && boundDecimalRows.every(entry => entry.label)
    && boundDecimalSegments.every(segment => structuredLabelsAreContinuous(segment.rows));
  const allDmsRowsBoundAndLabeled = boundDmsRows.length === dmsPairEntries.length
    && boundDmsRows.every(entry => entry.label)
    && boundDmsSegments.every(segment => structuredLabelsAreContinuous(segment.rows));
  if (!dmsFamilyHasConflict && dmsPairRows.length >= 4 && allDmsRowsBoundAndLabeled
    && boundDmsSegments.length >= 2
    && ((repeatedHeaderCount >= 2 && hasLabelRestart) || groupHeadingCount >= 2)) {
    return buildOneShotFamilyResult({
      family: ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED,
      matched: true,
      reason: "repeated_headers_or_group_boundaries_with_dms_rows",
      evidence: {
        coordinateRowCount,
        dmsRowCount: dmsPairRows.length,
        repeatedHeaderCount,
        groupHeadingCount,
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount
      }
    });
  }

  if (!dmsFamilyHasConflict && dmsPairRows.length >= 3 && hasAxisHeader
    && allDmsRowsBoundAndLabeled && boundDmsRows.length >= 3) {
    return buildOneShotFamilyResult({
      family: ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE,
      matched: true,
      reason: "labeled_dms_table_structure",
      evidence: {
        coordinateRowCount,
        dmsRowCount: dmsPairRows.length,
        repeatedHeaderCount,
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount
      }
    });
  }

  const singleAxisLinesCandidate = longitudeValueLineIndexes.length === 1
    && latitudeValueLineIndexes.length === 1
    && longitudeValueLineIndexes[0] !== latitudeValueLineIndexes[0]
    && decimalPairRows.length === 0
    && dmsPairRows.length === 0
    && coordinateEvidenceLineCount === 2
    && conflictingCoordinateLineCount === 2
    && ambiguousMultiPairLineCount === 0
    && repeatedHeaderCount <= 1
    && groupHeadingCount === 0;
  const singleHeaderRowCandidate = hasAxisHeader
    && decimalPairRows.length + dmsPairRows.length === 1
    && boundDecimalRows.length + boundDmsRows.length === 1
    && repeatedHeaderCount === 1
    && groupHeadingCount === 0
    && conflictingCoordinateLineCount === 0
    && ambiguousMultiPairLineCount === 0;
  const singlePointCandidate = singleAxisLinesCandidate || singleHeaderRowCandidate;
  if (singlePointCandidate) {
    return buildOneShotFamilyResult({
      family: ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT,
      matched: true,
      reason: "single_labeled_axis_pair",
      evidence: {
        coordinateRowCount: Math.max(1, coordinateRowCount),
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount
      }
    });
  }

  if (!decimalFamilyHasConflict && hasAxisHeader && decimalPairRows.length >= 3
    && allDecimalRowsBoundAndLabeled && boundDecimalRows.length >= 3) {
    return buildOneShotFamilyResult({
      family: ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE,
      matched: true,
      reason: "labeled_decimal_table_structure",
      evidence: {
        coordinateRowCount,
        repeatedHeaderCount,
        ambiguousMultiPairLineCount,
        conflictingCoordinateLineCount
      }
    });
  }

  return buildOneShotFamilyResult({
    family: ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW,
    matched: false,
    reason: "strong_structure_not_established",
    evidence: {
      coordinateRowCount,
      dmsRowCount: dmsPairRows.length,
      repeatedHeaderCount,
      groupHeadingCount,
      layoutRegionCount,
      ambiguousMultiPairLineCount,
      conflictingCoordinateLineCount
    }
  });
}

function sha256BoundedIdentity(value) {
  return createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function boundedStructureCount(value) {
  const numeric = Number(value);
  return Number.isInteger(numeric) && numeric >= 0 && numeric <= 10_000 ? numeric : 0;
}

function boundedStructureCounts(values) {
  if (!Array.isArray(values) || values.length > 256) return Object.freeze([]);
  return Object.freeze(values.map(boundedStructureCount));
}

function normalizeBindingLabel(value) {
  return String(value || "")
    .normalize("NFKC")
    .trim()
    .replace(/\s+/gu, " ")
    .toUpperCase();
}

function normalizeBindingDecimal(value, limit = Number.POSITIVE_INFINITY) {
  const normalized = normalizeDecimalToken(value);
  if (!/^[-+]?\d+(?:\.\d+)?$/u.test(normalized)) return "";
  const numeric = Number(normalized);
  if (!Number.isFinite(numeric) || Math.abs(numeric) > limit) return "";
  return normalized;
}

function normalizeBindingDms(value, expectedDirections) {
  const normalized = String(value || "").normalize("NFKC").trim();
  const match = normalized.match(
    /^(\d{1,3})\s*[°º]\s*(\d{1,2})\s*(['′’]?)\s*(\d{1,2}(?:[.,]\d+)?)\s*(["″”]?)\s*([NSEWO])\s*$/iu
  );
  if (!match) return "";
  const direction = match[6].toUpperCase() === "O" ? "W" : match[6].toUpperCase();
  if (!expectedDirections.includes(direction)) return "";
  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[4].replace(",", "."));
  const limit = expectedDirections === "NS" ? 90 : 180;
  if (!Number.isFinite(degrees) || !Number.isFinite(minutes) || !Number.isFinite(seconds)
    || minutes >= 60 || seconds >= 60 || degrees > limit
    || (degrees === limit && (minutes > 0 || seconds > 0))) return "";
  const minuteMarker = match[3] ? "'" : "";
  const secondMarker = match[5] ? "\"" : "";
  return `${match[1]}°${match[2]}${minuteMarker}${match[4].replace(",", ".")}${secondMarker}${direction}`;
}

function canonicalAxisValue(value, format, axis) {
  if (format === "decimal") {
    return normalizeBindingDecimal(value, axis === "longitude" ? 180 : 90);
  }
  return normalizeBindingDms(value, axis === "longitude" ? "EW" : "NS");
}

function axisTableRows(value, format) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const headers = lines.flatMap((line, index) => {
    const header = parseExplicitAxisHeaderLine(line, index);
    return header ? [header] : [];
  });
  const rows = [];
  for (const header of headers) {
    for (let index = header.index + 1; index < lines.length; index += 1) {
      if (parseExplicitAxisHeaderLine(lines[index], index)) break;
      const fields = splitStructuredFields(lines[index], header.delimiter);
      if (fields.length !== header.fieldCount || fields.some(field => !field)) break;
      const longitude = canonicalAxisValue(fields[header.longitudeIndex], format, "longitude");
      const latitude = canonicalAxisValue(fields[header.latitudeIndex], format, "latitude");
      const label = header.labelIndex === null ? String(rows.length + 1) : normalizeBindingLabel(fields[header.labelIndex]);
      if (!longitude || !latitude || !label) break;
      rows.push(Object.freeze({ label, longitude, latitude }));
    }
  }
  return Object.freeze(rows);
}

function singlePointIdentity(value, format) {
  const longitudeEntry = getSingleLabeledEntry(
    value,
    /^(?:longitude(?:\s+DMS)?|lon|经度|經度|東經|东经|西經|西经)$/iu
  );
  const latitudeEntry = getSingleLabeledEntry(
    value,
    /^(?:latitude(?:\s+DMS)?|lat|纬度|緯度|北纬|北緯|南纬|南緯)$/iu
  );
  if (longitudeEntry && latitudeEntry) {
    const coordinateFormat = format === "DMS_SINGLE_POINT" ? "dms" : "decimal";
    const longitude = canonicalAxisValue(longitudeEntry.value, coordinateFormat, "longitude");
    const latitude = canonicalAxisValue(latitudeEntry.value, coordinateFormat, "latitude");
    return longitude && latitude
      ? Object.freeze([`LONGITUDE:${longitude}`, `LATITUDE:${latitude}`])
      : Object.freeze([]);
  }
  const rows = axisTableRows(value, format === "DMS_SINGLE_POINT" ? "dms" : "decimal");
  return rows.length === 1
    ? Object.freeze([`LONGITUDE:${rows[0].longitude}`, `LATITUDE:${rows[0].latitude}`])
    : Object.freeze([]);
}

function tableIdentity(value, format) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const tokens = [];
  let segmentIndex = -1;
  let rowIndex = 0;
  let header = null;
  for (let index = 0; index < lines.length; index += 1) {
    const parsedHeader = parseExplicitAxisHeaderLine(lines[index], index);
    if (parsedHeader) {
      segmentIndex += 1;
      rowIndex = 0;
      header = parsedHeader;
      continue;
    }
    if (!header || segmentIndex < 0) continue;
    const fields = splitStructuredFields(lines[index], header.delimiter);
    if (fields.length !== header.fieldCount || fields.some(field => !field)) continue;
    const longitude = canonicalAxisValue(fields[header.longitudeIndex], format, "longitude");
    const latitude = canonicalAxisValue(fields[header.latitudeIndex], format, "latitude");
    const label = header.labelIndex === null ? String(rowIndex + 1) : normalizeBindingLabel(fields[header.labelIndex]);
    if (!longitude || !latitude || !label) continue;
    tokens.push(`${segmentIndex}:${rowIndex}:${label}:${longitude}:${latitude}`);
    rowIndex += 1;
  }
  return Object.freeze(tokens);
}

function normalizeGroupHeading(value) {
  const normalized = normalizeBindingLabel(value)
    .replace(/^GROUP\s*[|:#-]?\s*/u, "")
    .replace(/^(?:SITE|SITES|AREA|MINING AREA|矿区|礦區)\s*[|:#-]?\s*/u, "");
  return normalized || "REPEATED_HEADER_BOUNDARY";
}

function groupedDmsIdentity(value) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const providerStyle = lines.some(line => /^GROUP\s*\|/iu.test(line));
  const tokens = [];
  let groupIndex = -1;
  let groupHeading = "";
  let header = null;
  let rowIndex = 0;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (providerStyle && /^GROUP\s*\|/iu.test(line)) {
      groupIndex += 1;
      groupHeading = normalizeGroupHeading(line.split("|").slice(1).join("|"));
      header = null;
      rowIndex = 0;
      continue;
    }
    if (!providerStyle && isExplicitGroupHeadingLine(line)) {
      groupIndex += 1;
      groupHeading = normalizeGroupHeading(line);
      header = null;
      rowIndex = 0;
      continue;
    }
    if (providerStyle && /^HEADER\s*\|/iu.test(line)) {
      header = "PROVIDER";
      continue;
    }
    const parsedHeader = parseExplicitAxisHeaderLine(line, index);
    if (!providerStyle && parsedHeader) {
      if (groupIndex < 0 || (header && groupHeading === "REPEATED_HEADER_BOUNDARY")) {
        groupIndex += 1;
        groupHeading = "REPEATED_HEADER_BOUNDARY";
        rowIndex = 0;
      }
      header = parsedHeader;
      continue;
    }

    let label = "";
    let latitude = "";
    let longitude = "";
    if (providerStyle && /^POINT\s*\|/iu.test(line)) {
      const fields = splitStructuredFields(line, "|");
      if (fields.length === 4) {
        label = normalizeBindingLabel(fields[1]);
        latitude = normalizeBindingDms(fields[2], "NS");
        longitude = normalizeBindingDms(fields[3], "EW");
      }
    } else if (!providerStyle && header && header !== "PROVIDER") {
      const fields = splitStructuredFields(line, header.delimiter);
      if (fields.length === header.fieldCount) {
        label = header.labelIndex === null ? String(rowIndex + 1) : normalizeBindingLabel(fields[header.labelIndex]);
        latitude = normalizeBindingDms(fields[header.latitudeIndex], "NS");
        longitude = normalizeBindingDms(fields[header.longitudeIndex], "EW");
      }
    }
    if (label && latitude && longitude && groupIndex >= 0) {
      tokens.push(`${groupIndex}:${groupHeading}:${rowIndex}:${label}:${longitude}:${latitude}`);
      rowIndex += 1;
    }
  }
  return Object.freeze(tokens);
}

function groupedDmsHeadingIdentity(value, groupBoundaryMode) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const providerHeadings = lines
    .filter(line => /^GROUP\s*\|/iu.test(line))
    .map(line => normalizeGroupHeading(line.split("|").slice(1).join("|")));
  if (providerHeadings.length > 0) return Object.freeze(providerHeadings);
  const explicitHeadings = lines
    .filter(isExplicitGroupHeadingLine)
    .map(normalizeGroupHeading);
  if (explicitHeadings.length > 0) return Object.freeze(explicitHeadings);
  if (groupBoundaryMode === "REPEATED_HEADERS") {
    return Object.freeze(lines
      .filter((line, index) => Boolean(parseExplicitAxisHeaderLine(line, index)))
      .map(() => "REPEATED_HEADER_BOUNDARY"));
  }
  return Object.freeze([]);
}

function decimalLexemesInLine(value) {
  return (String(value || "").match(/[-+]?\d{1,3}(?:[.,]\d+)/gu) || [])
    .map(token => normalizeBindingDecimal(token, 180))
    .filter(Boolean);
}

function mapRoleIdentity(value) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const roles = [];
  for (const line of lines) {
    const upper = line.toUpperCase();
    const role = /^MAP_SEARCH_BOX\s*\|/u.test(upper)
      || /\b(?:SEARCH|RECHERCHER|BUSCAR)\b/u.test(upper) || /搜索|搜尋/u.test(line)
      ? "MAP_SEARCH_BOX"
      : /^MAP_PLACE_DETAILS\s*\|/u.test(upper)
        || /\b(?:DETAILS?|PLACE|LOCATION|ADDRESS|DIRECTIONS?)\b/u.test(upper) || /地点|地點|位置|地址|路线|路線/u.test(line)
        ? "MAP_PLACE_DETAILS"
        : /^PLUS_CODE\s*\|/u.test(upper) || /\bPLUS\s*CODE\b/u.test(upper)
          ? "PLUS_CODE"
          : "";
    if (!role) continue;
    if (role === "PLUS_CODE") {
      const plusCode = line.match(/\b[A-Z0-9]{4,12}\+[A-Z0-9]{2,}\b/iu)?.[0]
        ?.toUpperCase();
      if (plusCode) roles.push(`${role}:${plusCode}`);
      continue;
    }
    const coordinates = decimalLexemesInLine(line);
    if (coordinates.length === 2) roles.push(`${role}:${coordinates.join(",")}`);
  }
  return Object.freeze(roles);
}

function projectedIdentityRows(value) {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const explicitSourceRows = lines.flatMap((line, index) => {
    const parsed = parseExplicitProjectedPointLine(line, index);
    if (!parsed) return [];
    const first = normalizeBindingDecimal(parsed.firstValue);
    const second = normalizeBindingDecimal(parsed.secondValue);
    const label = normalizeBindingLabel(parsed.label);
    return first && second && label ? [`POINT:${index}:${label}:${first}:${second}`] : [];
  });
  const providerPointRows = lines.flatMap((line, index) => {
    const fields = splitStructuredFields(line, "|");
    if (fields.length !== 4 || !/^POINT$/iu.test(fields[0])) return [];
    const first = normalizeBindingDecimal(fields[2]);
    const second = normalizeBindingDecimal(fields[3]);
    const label = normalizeBindingLabel(fields[1]);
    return first && second && label ? [`POINT:${index}:${label}:${first}:${second}`] : [];
  });
  const sourceRows = [];
  const headers = lines.flatMap((line, index) => {
    const header = parseProjectedAxisHeaderLine(line, index);
    return header ? [header] : [];
  });
  for (const header of headers) {
    for (let index = header.index + 1; index < lines.length; index += 1) {
      if (parseProjectedAxisHeaderLine(lines[index], index)) break;
      const fields = splitStructuredFields(lines[index], header.delimiter);
      if (fields.length !== header.fieldCount || fields.some(field => !field)) break;
      const label = header.labelIndex === null ? "" : normalizeBindingLabel(fields[header.labelIndex]);
      const firstAxisIndex = Math.min(header.eastingIndex, header.northingIndex);
      const secondAxisIndex = Math.max(header.eastingIndex, header.northingIndex);
      const first = normalizeBindingDecimal(fields[firstAxisIndex]);
      const second = normalizeBindingDecimal(fields[secondAxisIndex]);
      if (!label || !first || !second) break;
      sourceRows.push(`POINT:${sourceRows.length}:${label}:${first}:${second}`);
    }
  }
  const mgrsRows = lines.flatMap((line, index) => {
    const match = line.match(
      /^\s*MGRS\s*[|:]\s*([1-9]|[1-5]\d|60)([C-HJ-NP-X])\s*[|]\s*([A-HJ-NP-Z]{2})\s*[|]\s*(\d{1,5})\s*[|]\s*(\d{1,5})\s*$/iu
    );
    if (!match || match[4].length !== match[5].length) return [];
    return [`MGRS:${index}:${Number(match[1])}${match[2].toUpperCase()}:${match[3].toUpperCase()}:${match[4]}:${match[5]}`];
  });
  const pointRows = explicitSourceRows.length > 0
    ? explicitSourceRows
    : providerPointRows.length > 0 ? providerPointRows : sourceRows;
  return Object.freeze([...pointRows.map((row, index) => row.replace(/^POINT:\d+:/u, `POINT:${index}:`)),
    ...mgrsRows.map((row, index) => row.replace(/^MGRS:\d+:/u, `MGRS:${index}:`))]);
}

function valueIdentityForFamily(family, format, value) {
  switch (family) {
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT:
      return singlePointIdentity(value, format);
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE:
      return tableIdentity(value, "decimal");
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE:
      return tableIdentity(value, "dms");
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED:
      return groupedDmsIdentity(value);
    case ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT:
      return mapRoleIdentity(value);
    case ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE:
      return projectedIdentityRows(value);
    default:
      return Object.freeze([]);
  }
}

function bindingMismatchReason(family) {
  if (family === ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT) {
    return ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.MAP_ROLE_VALUE_MISMATCH;
  }
  if (family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE) {
    return ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_VALUE_MISMATCH;
  }
  if ([
    ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE,
    ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE,
    ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
  ].includes(family)) {
    return ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH;
  }
  return ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.VALUE_FIDELITY_MISMATCH;
}

function normalizedSpatialLineText(value) {
  return normalizeCoordinateEvidenceText(value).replace(/\s+/gu, " ").trim();
}

function normalizePrivateSourceRegion(line, imageIdentity) {
  if (!line || typeof line !== "object") return null;
  const text = normalizedSpatialLineText(getLineText(line));
  const box = getFiniteLayoutBox(line);
  const localLineIndex = Number(line.local_line_index ?? line.localLineIndex);
  const sourceLineIndexes = Array.isArray(line.source_line_indexes)
    ? [...new Set(line.source_line_indexes.map(Number))].sort((left, right) => left - right)
    : [localLineIndex];
  const declaredPage = line.page === undefined ? imageIdentity.page : Number(line.page);
  const declaredRevision = line.resultRevision === undefined
    ? null
    : Number(line.resultRevision);
  if (!text || !box || !Number.isSafeInteger(localLineIndex) || localLineIndex < 0
    || sourceLineIndexes.length === 0
    || sourceLineIndexes.some(index => !Number.isSafeInteger(index) || index < 0)
    || sourceLineIndexes[0] !== localLineIndex
    || !Number.isInteger(declaredPage) || declaredPage !== imageIdentity.page
    || (declaredRevision !== null && (!Number.isSafeInteger(declaredRevision) || declaredRevision <= 0))
    || box.x0 < 0 || box.y0 < 0 || box.x1 > imageIdentity.width || box.y1 > imageIdentity.height) {
    return null;
  }
  const roundedBox = Object.freeze({
    x0: Math.round(box.x0),
    y0: Math.round(box.y0),
    x1: Math.round(box.x1),
    y1: Math.round(box.y1)
  });
  if (roundedBox.x1 <= roundedBox.x0 || roundedBox.y1 <= roundedBox.y0) return null;
  return Object.freeze({
    text,
    textDigest: sha256BoundedIdentity(text),
    localLineIndex,
    sourceLineIndexes: Object.freeze(sourceLineIndexes),
    declaredPage,
    declaredRevision,
    box: roundedBox
  });
}

function privateSourceRegionsOverlap(left, right) {
  const intersectionWidth = Math.min(left.box.x1, right.box.x1) - Math.max(left.box.x0, right.box.x0);
  const intersectionHeight = Math.min(left.box.y1, right.box.y1) - Math.max(left.box.y0, right.box.y0);
  return intersectionWidth > 0 && intersectionHeight > 0;
}

function privateObservationKinds(text) {
  const normalized = normalizedSpatialLineText(text);
  if (!normalized) return Object.freeze([]);
  const upper = normalized.toUpperCase();
  const decimalCount = countDecimalComponents(normalized);
  const dmsCount = countDmsComponents(normalized);
  if (/^PLUS_CODE\s*\|/u.test(upper)
    || /\bPLUS\s*CODE\b/u.test(upper)
    || /\b[A-Z0-9]{4,12}\+[A-Z0-9]{2,}\b/iu.test(normalized)) return Object.freeze(["MAP_PLUS_CODE"]);
  if ((/^MAP_SEARCH_BOX\s*\|/u.test(upper)
    || /\b(?:SEARCH|RECHERCHER|BUSCAR)\b/u.test(upper)
    || /搜索|搜尋/u.test(normalized)) && decimalCount === 2) return Object.freeze(["MAP_SEARCH_BOX"]);
  if ((/^MAP_PLACE_DETAILS\s*\|/u.test(upper)
    || /\b(?:DETAILS?|PLACE|LOCATION|ADDRESS|DIRECTIONS?)\b/u.test(upper)
    || /地点|地點|位置|地址|路线|路線/u.test(normalized)) && decimalCount === 2) return Object.freeze(["MAP_PLACE_DETAILS"]);
  if (isExplicitGroupHeadingLine(normalized)) return Object.freeze(["GROUP_HEADING"]);
  if (parseExplicitAxisHeaderLine(normalized, 0)) return Object.freeze(["AXIS_HEADER"]);
  if (/^\s*MGRS\s*[|:]\s*\d/iu.test(normalized)) return Object.freeze(["PROJECTED_ROW"]);
  if (parseExplicitProjectedPointLine(normalized, 0)) {
    return Object.freeze(["PROJECTED_AXIS_ORDER", "PROJECTED_ROW"]);
  }
  const projectedKinds = [];
  if (/\b(?:CRS|EPSG|Datum|UTM|Projection|Projected)\b/iu.test(normalized)) projectedKinds.push("PROJECTED_DATUM");
  if (/\b(?:Zone|Zona|Fuso)\b/iu.test(normalized)) projectedKinds.push("PROJECTED_ZONE");
  if (/\bHemisphere\b/iu.test(normalized)
    || /\b(?:UTM\s*(?:zone\s*)?|Zone|Zona|Fuso)\s*[:#=-]?\s*\d{1,2}[NS]\b/iu.test(normalized)) {
    projectedKinds.push("PROJECTED_HEMISPHERE");
  }
  if (parseProjectedAxisHeaderLine(normalized, 0)
    || /^\s*Axis\s+order\s*[:=]?/iu.test(normalized)) projectedKinds.push("PROJECTED_AXIS_ORDER");
  if (projectedKinds.length > 0) return Object.freeze(projectedKinds);
  if (isClosedProjectedAxisValueLine(normalized)) return Object.freeze(["PROJECTED_AXIS_VALUE"]);
  const projectedDelimiter = normalized.includes("|") ? "|"
    : normalized.includes("\t") ? "\t"
      : normalized.includes(";") ? ";"
        : "";
  const projectedFields = projectedDelimiter ? splitStructuredFields(normalized, projectedDelimiter) : [];
  if (projectedDelimiter
    && extractStructuredRowLabel(normalized)
    && projectedFields.filter(field => /^[-+]?\d{4,9}(?:[.,]\d+)?$/u.test(field)).length >= 2) {
    return Object.freeze(["PROJECTED_ROW"]);
  }
  if (isClosedAxisValueLine(normalized, "longitude") || isClosedAxisValueLine(normalized, "latitude")) {
    return Object.freeze(["AXIS_VALUE"]);
  }
  if (decimalCount > 0 || dmsCount > 0) {
    if ((decimalCount > 0 && dmsCount > 0) || decimalCount > 2 || dmsCount > 2
      || (decimalCount !== 2 && dmsCount !== 2)) return Object.freeze(["AMBIGUOUS_COORDINATE"]);
    return Object.freeze(["COORDINATE_ROW"]);
  }
  return Object.freeze([]);
}

function privateObservationKindAllowedForFamily(family, kind) {
  const allowed = {
    [ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT]: new Set(["AXIS_VALUE", "AXIS_HEADER", "COORDINATE_ROW"]),
    [ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE]: new Set(["AXIS_HEADER", "COORDINATE_ROW"]),
    [ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE]: new Set(["AXIS_HEADER", "COORDINATE_ROW"]),
    [ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED]: new Set(["GROUP_HEADING", "AXIS_HEADER", "COORDINATE_ROW"]),
    [ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT]: new Set(["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS", "MAP_PLUS_CODE"]),
    [ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE]: new Set([
      "PROJECTED_DATUM",
      "PROJECTED_ZONE",
      "PROJECTED_HEMISPHERE",
      "PROJECTED_AXIS_ORDER",
      "PROJECTED_AXIS_VALUE",
      "PROJECTED_ROW"
    ])
  };
  return allowed[family]?.has(kind) === true;
}

function privateObservationCoverageIdentity({ family, format, value, structure = {} }) {
  const valueIdentity = valueIdentityForFamily(family, format, value);
  const tokens = [];
  if ([ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE, ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE].includes(family)) {
    for (let index = 0; index < boundedStructureCount(structure.repeatedHeaderCount); index += 1) {
      tokens.push(`HEADER:${index}`);
    }
  } else if (family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED) {
    groupedDmsHeadingIdentity(value, structure.groupBoundaryMode)
      .forEach((heading, index) => tokens.push(`GROUP:${index}:${heading}`));
    if (structure.groupBoundaryMode !== "REPEATED_HEADERS") {
      for (let index = 0; index < boundedStructureCount(structure.repeatedHeaderCount); index += 1) {
        tokens.push(`HEADER:${index}`);
      }
    }
  } else if (family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE) {
    const crs = structure.crs || structure.identity || {};
    if (crs.datumClass && crs.datumIdentityDigest) tokens.push(`DATUM:${crs.datumClass}:${crs.datumIdentityDigest}`);
    if (Number.isInteger(crs.zone)) tokens.push(`ZONE:${crs.zone}`);
    if (crs.hemisphere) tokens.push(`HEMISPHERE:${crs.hemisphere}`);
    if (crs.axisOrder) tokens.push(`AXIS_ORDER:${crs.axisOrder}`);
  }
  return Object.freeze([...tokens, ...valueIdentity.map(item => `VALUE:${item}`)]);
}

function minimumPrivateSourceRegionCount({ family, expectedRowCount, groupBoundaryCount }) {
  const rows = boundedStructureCount(expectedRowCount);
  switch (family) {
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT:
      return 1;
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE:
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE:
      return rows > 0 ? rows + 1 : 0;
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED:
      return rows > 0 ? rows + Math.max(2, boundedStructureCount(groupBoundaryCount)) : 0;
    case ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT:
      return 3;
    case ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE:
      return rows > 0 ? rows + 1 : 0;
    default:
      return 0;
  }
}

function createPrivateSpatialProvenance({
  family,
  sourceText,
  layoutLines,
  imageIdentity,
  resultRevision,
  expectedRowCount,
  groupBoundaryCount,
  observationCoverageCount
}) {
  const unavailable = ({
    sourceRegionCount = 0,
    observedCandidateCount = 0,
    boundCandidateCount = 0,
    unassignedCandidateCount = 0,
    ambiguous = false
  } = {}) => Object.freeze({
    available: false,
    spatialAvailable: false,
    observationAvailable: false,
    observationAmbiguous: ambiguous,
    sourceRegionCount: boundedStructureCount(sourceRegionCount),
    observedCandidateCount: boundedStructureCount(observedCandidateCount),
    boundCandidateCount: boundedStructureCount(boundCandidateCount),
    unassignedCandidateCount: boundedStructureCount(unassignedCandidateCount),
    identity: Object.freeze([]),
    observationIdentity: Object.freeze([])
  });
  const revision = Number(resultRevision);
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)
    || !Number.isSafeInteger(revision) || revision <= 0
    || !Array.isArray(layoutLines) || layoutLines.length === 0 || layoutLines.length > 256) {
    return unavailable();
  }
  const expectedCandidates = normalizeCoordinateEvidenceText(sourceText)
    .split("\n")
    .map(normalizedSpatialLineText)
    .filter(Boolean)
    .flatMap(text => privateObservationKinds(text)
      .map((kind, subIndex) => Object.freeze({ text, kind, subIndex })));
  const normalized = layoutLines.map(line => normalizePrivateSourceRegion(line, imageIdentity));
  if (normalized.some(region => !region)) {
    return unavailable();
  }
  const candidateRegions = normalized.filter(region => privateObservationKinds(region.text).length > 0);
  const candidates = candidateRegions.flatMap(region => privateObservationKinds(region.text)
    .map((kind, subIndex) => Object.freeze({ region, kind, subIndex })));
  const observedCandidateCount = boundedStructureCount(candidates.length);
  let boundCandidateCount = 0;
  const comparisonLength = Math.min(candidates.length, expectedCandidates.length);
  for (let index = 0; index < comparisonLength; index += 1) {
    const observed = candidates[index];
    const expected = expectedCandidates[index];
    if (observed.region.text === expected.text
      && observed.kind === expected.kind
      && privateObservationKindAllowedForFamily(family, observed.kind)) boundCandidateCount += 1;
  }
  const unassignedCandidateCount = Math.max(
    candidates.length - boundCandidateCount,
    expectedCandidates.length - boundCandidateCount,
    Math.abs(expectedCandidates.length - boundedStructureCount(observationCoverageCount))
  );
  const observationAmbiguous = candidates.some(candidate => candidate.kind === "AMBIGUOUS_COORDINATE")
    || expectedCandidates.some(candidate => candidate.kind === "AMBIGUOUS_COORDINATE")
    || candidates.some(candidate => !privateObservationKindAllowedForFamily(family, candidate.kind))
    || expectedCandidates.some(candidate => !privateObservationKindAllowedForFamily(family, candidate.kind));
  const minimumCount = minimumPrivateSourceRegionCount({ family, expectedRowCount, groupBoundaryCount });
  const sourceLineIdentity = candidateRegions.flatMap(region => region.sourceLineIndexes);
  if (minimumCount <= 0 || candidateRegions.length < minimumCount
    || new Set(sourceLineIdentity).size !== sourceLineIdentity.length
    || candidateRegions.some(region => region.declaredRevision !== null && region.declaredRevision !== revision)) {
    return unavailable({
      sourceRegionCount: candidateRegions.length,
      observedCandidateCount,
      boundCandidateCount,
      unassignedCandidateCount,
      ambiguous: observationAmbiguous
    });
  }
  for (let left = 0; left < candidateRegions.length; left += 1) {
    for (let right = left + 1; right < candidateRegions.length; right += 1) {
      if (privateSourceRegionsOverlap(candidateRegions[left], candidateRegions[right])) {
        return unavailable({
          sourceRegionCount: candidateRegions.length,
          observedCandidateCount,
          boundCandidateCount,
          unassignedCandidateCount,
          ambiguous: observationAmbiguous
        });
      }
    }
  }
  const regionLineOrder = [...candidateRegions].sort((left, right) => left.localLineIndex - right.localLineIndex);
  const regionVisualOrder = [...candidateRegions].sort((left, right) => (
    left.box.y0 - right.box.y0 || left.box.x0 - right.box.x0 || left.localLineIndex - right.localLineIndex
  ));
  const visualOrderValid = regionLineOrder.every((region, index) => region === regionVisualOrder[index]);
  const lineOrder = regionLineOrder.flatMap(region => privateObservationKinds(region.text)
    .map((kind, subIndex) => Object.freeze({ region, kind, subIndex })));
  const sameCandidateCount = lineOrder.length === expectedCandidates.length;
  const sourceOrderValid = sameCandidateCount && lineOrder.every((candidate, index) => (
    candidate.region.text === expectedCandidates[index].text
      && candidate.kind === expectedCandidates[index].kind
      && candidate.subIndex === expectedCandidates[index].subIndex
  ));
  if (!visualOrderValid || (sameCandidateCount && !sourceOrderValid)) {
    return unavailable({
      sourceRegionCount: candidateRegions.length,
      observedCandidateCount,
      boundCandidateCount,
      unassignedCandidateCount,
      ambiguous: observationAmbiguous
    });
  }
  const hasExplicitGroupHeadings = family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
    && lineOrder.some(candidate => candidate.kind === "GROUP_HEADING");
  let sourceGroupIndex = 0;
  const regionIdentities = lineOrder.map((candidate, index) => {
    const { region, kind } = candidate;
    const startsExplicitGroup = family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED && kind === "GROUP_HEADING";
    const startsRepeatedTableSegment = (([
      ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE,
      ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE,
      ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
    ].includes(family)
      && (!hasExplicitGroupHeadings || family !== ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED)
      && kind === "AXIS_HEADER")
      || (family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE && kind === "PROJECTED_AXIS_HEADER"));
    if (index > 0 && (startsExplicitGroup || startsRepeatedTableSegment)) sourceGroupIndex += 1;
    return `REGION:${index}:${candidate.subIndex}:${kind}:G${sourceGroupIndex}:${region.sourceLineIndexes.join(".")}:${region.box.x0},${region.box.y0},${region.box.x1},${region.box.y1}:${region.textDigest}`;
  });
  const identity = Object.freeze([
    `IMAGE:${imageIdentity.image_sha256}:${imageIdentity.request_asset_id}:${imageIdentity.page}:${imageIdentity.width}x${imageIdentity.height}:R${revision}`,
    ...regionIdentities
  ]);
  const observationAvailable = sourceOrderValid
    && !observationAmbiguous
    && unassignedCandidateCount === 0
    && expectedCandidates.length === boundedStructureCount(observationCoverageCount)
    && boundCandidateCount === candidates.length;
  const observationIdentity = Object.freeze([
    `OBSERVATION_SET:${observedCandidateCount}:${boundedStructureCount(boundCandidateCount)}:${boundedStructureCount(unassignedCandidateCount)}`,
    ...identity
  ]);
  return Object.freeze({
    available: observationAvailable,
    spatialAvailable: true,
    observationAvailable,
    observationAmbiguous,
    sourceRegionCount: boundedStructureCount(candidateRegions.length),
    observedCandidateCount,
    boundCandidateCount: boundedStructureCount(boundCandidateCount),
    unassignedCandidateCount: boundedStructureCount(unassignedCandidateCount),
    identity,
    observationIdentity
  });
}

function createPrivateAcquisitionBinding({
  family,
  format,
  sourceText,
  structure,
  expectedRowCount,
  groupBoundaryCount,
  layoutLines,
  imageIdentity,
  resultRevision
}) {
  const identity = valueIdentityForFamily(family, format, sourceText);
  const observationCoverageIdentity = privateObservationCoverageIdentity({
    family,
    format,
    value: sourceText,
    structure
  });
  const expectedCount = family === ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT
    ? 2
    : family === ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT
      ? 3
      : boundedStructureCount(expectedRowCount);
  const valueAvailable = identity.length > 0 && (expectedCount === 0 || identity.length === expectedCount);
  const spatial = createPrivateSpatialProvenance({
    family,
    sourceText,
    layoutLines,
    imageIdentity,
    resultRevision,
    expectedRowCount,
    groupBoundaryCount,
    observationCoverageCount: observationCoverageIdentity.length
  });
  const available = valueAvailable && spatial.spatialAvailable && spatial.observationAvailable;
  const key = randomBytes(32);
  const valueDigest = valueAvailable
    ? createHmac("sha256", key).update(JSON.stringify({ family, format, identity }), "utf8").digest()
    : Buffer.alloc(0);
  const spatialDigest = spatial.spatialAvailable
    ? createHmac("sha256", key).update(JSON.stringify({
      family,
      format,
      spatialIdentity: spatial.identity
    }), "utf8").digest()
    : Buffer.alloc(0);
  const observationDigest = spatial.observationAvailable
    ? createHmac("sha256", key).update(JSON.stringify({
      family,
      format,
      observationIdentity: spatial.observationIdentity
    }), "utf8").digest()
    : Buffer.alloc(0);
  const observationCoverageDigest = spatial.observationAvailable
    ? createHmac("sha256", key).update(JSON.stringify({
      family,
      format,
      observationCoverageIdentity
    }), "utf8").digest()
    : Buffer.alloc(0);
  const digest = available
    ? createHmac("sha256", key).update(JSON.stringify({
      family,
      format,
      identity,
      spatialIdentity: spatial.identity,
      observationIdentity: spatial.observationIdentity,
      observationCoverageIdentity
    }), "utf8").digest()
    : Buffer.alloc(0);
  return Object.freeze({
    available,
    valueAvailable,
    spatialAvailable: spatial.spatialAvailable,
    observationAvailable: spatial.observationAvailable,
    observationAmbiguous: spatial.observationAmbiguous,
    sourceRegionCount: spatial.sourceRegionCount,
    observedCandidateCount: spatial.observedCandidateCount,
    boundCandidateCount: spatial.boundCandidateCount,
    unassignedCandidateCount: spatial.unassignedCandidateCount,
    spatialIdentity: spatial.identity,
    observationIdentity: spatial.observationIdentity,
    observationCoverageIdentity,
    family,
    format,
    expectedCount,
    key,
    valueDigest,
    spatialDigest,
    observationDigest,
    observationCoverageDigest,
    digest
  });
}

function validatePrivateAcquisitionBinding({ contract, payload, providerText, providerStructure = {} }) {
  const binding = privateAcquisitionBindings.get(contract);
  const counts = binding ? {
    sourceRegionCount: binding.sourceRegionCount,
    observedCandidateCount: binding.observedCandidateCount,
    boundCandidateCount: binding.boundCandidateCount,
    unassignedCandidateCount: binding.unassignedCandidateCount
  } : {
    sourceRegionCount: 0,
    observedCandidateCount: 0,
    boundCandidateCount: 0,
    unassignedCandidateCount: 0
  };
  if (!binding || binding.family !== payload.family || binding.format !== payload.format) {
    return Object.freeze({
      conformant: false,
      reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PRIVATE_BINDING_UNAVAILABLE,
      ...counts
    });
  }
  if (!binding.valueAvailable) return Object.freeze({
    conformant: false,
    reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PRIVATE_BINDING_UNAVAILABLE,
    ...counts
  });
  const identity = valueIdentityForFamily(payload.family, payload.format, providerText);
  const expectedCount = binding.expectedCount;
  if (identity.length === 0 || (expectedCount > 0 && identity.length !== expectedCount)) {
    return Object.freeze({
      conformant: false,
      reason: bindingMismatchReason(payload.family),
      ...counts
    });
  }
  const actualValueDigest = createHmac("sha256", binding.key)
    .update(JSON.stringify({
      family: payload.family,
      format: payload.format,
      identity
    }), "utf8")
    .digest();
  const valueConformant = binding.valueDigest.length === actualValueDigest.length
    && timingSafeEqual(binding.valueDigest, actualValueDigest);
  if (!valueConformant) return Object.freeze({
    conformant: false,
    reason: bindingMismatchReason(payload.family),
    ...counts
  });
  if (!binding.spatialAvailable) return Object.freeze({
    conformant: false,
    reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE,
    ...counts
  });
  const actualSpatialDigest = createHmac("sha256", binding.key)
    .update(JSON.stringify({
      family: payload.family,
      format: payload.format,
      spatialIdentity: binding.spatialIdentity
    }), "utf8")
    .digest();
  if (binding.spatialDigest.length !== actualSpatialDigest.length
    || !timingSafeEqual(binding.spatialDigest, actualSpatialDigest)) return Object.freeze({
    conformant: false,
    reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE,
    ...counts
  });
  if (!binding.observationAvailable) return Object.freeze({
    conformant: false,
    reason: binding.observationAmbiguous
      ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_AMBIGUOUS
      : ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE,
    ...counts
  });
  const actualObservationDigest = createHmac("sha256", binding.key)
    .update(JSON.stringify({
      family: payload.family,
      format: payload.format,
      observationIdentity: binding.observationIdentity
    }), "utf8")
    .digest();
  if (binding.observationDigest.length !== actualObservationDigest.length
    || !timingSafeEqual(binding.observationDigest, actualObservationDigest)) return Object.freeze({
    conformant: false,
    reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE,
    ...counts
  });
  const providerObservationCoverageIdentity = privateObservationCoverageIdentity({
    family: payload.family,
    format: payload.format,
    value: providerText,
    structure: providerStructure
  });
  const actualObservationCoverageDigest = createHmac("sha256", binding.key)
    .update(JSON.stringify({
      family: payload.family,
      format: payload.format,
      observationCoverageIdentity: providerObservationCoverageIdentity
    }), "utf8")
    .digest();
  if (binding.observationCoverageDigest.length !== actualObservationCoverageDigest.length
    || !timingSafeEqual(binding.observationCoverageDigest, actualObservationCoverageDigest)) {
    return Object.freeze({
      conformant: false,
      reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE,
      ...counts
    });
  }
  const actualDigest = createHmac("sha256", binding.key)
    .update(JSON.stringify({
      family: payload.family,
      format: payload.format,
      identity,
      spatialIdentity: binding.spatialIdentity,
      observationIdentity: binding.observationIdentity,
      observationCoverageIdentity: providerObservationCoverageIdentity
    }), "utf8")
    .digest();
  const conformant = binding.digest.length === actualDigest.length && timingSafeEqual(binding.digest, actualDigest);
  return Object.freeze({
    conformant,
    reason: conformant
      ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONFORMANT
      : ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE,
    ...counts
  });
}

function extractGroupedDmsStructure(value = "") {
  const lines = normalizeCoordinateEvidenceText(value).split("\n").map(line => line.trim()).filter(Boolean);
  const explicitCounts = [];
  let activeGroup = -1;
  for (const line of lines) {
    if (isExplicitGroupHeadingLine(line)) {
      explicitCounts.push(0);
      activeGroup = explicitCounts.length - 1;
      continue;
    }
    if (activeGroup >= 0 && countDmsComponents(line) === 2 && countDecimalComponents(line) === 0) {
      explicitCounts[activeGroup] += 1;
    }
  }
  if (explicitCounts.length > 0) {
    return Object.freeze({ mode: "EXPLICIT_GROUP_HEADINGS", rowCounts: boundedStructureCounts(explicitCounts) });
  }
  const headerCounts = [];
  let activeHeader = -1;
  for (let index = 0; index < lines.length; index += 1) {
    if (parseExplicitAxisHeaderLine(lines[index], index)) {
      headerCounts.push(0);
      activeHeader = headerCounts.length - 1;
      continue;
    }
    if (activeHeader >= 0 && countDmsComponents(lines[index]) === 2 && countDecimalComponents(lines[index]) === 0) {
      headerCounts[activeHeader] += 1;
    }
  }
  return Object.freeze({
    mode: headerCounts.length > 1 ? "REPEATED_HEADERS" : "NONE",
    rowCounts: boundedStructureCounts(headerCounts)
  });
}

function getAcquisitionFormat(family, evidence = {}, sourceText = "") {
  switch (family) {
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT:
      return boundedStructureCount(evidence.dmsRowCount) === 1 || countDmsComponents(sourceText) > 0
        ? "DMS_SINGLE_POINT"
        : "WGS84_DECIMAL_SINGLE_POINT";
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE:
      return "WGS84_DECIMAL_TABLE";
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE:
      return "DMS_TABLE";
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED:
      return "DMS_GROUPED";
    case ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT:
      return "MAP_UI_ROLES";
    case ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE:
      return "PROJECTED_CRS_TABLE";
    default:
      return "GENERIC_REVIEW";
  }
}

function normalizeProjectedDatumIdentity(value) {
  const normalized = String(value || "").replace(/[^A-Z0-9]/giu, "").toUpperCase();
  if (!normalized) return Object.freeze({ datumClass: "", datumIdentityDigest: "" });
  if (normalized.includes("WGS84") || normalized.includes("WGS1984")) {
    return Object.freeze({ datumClass: "WGS84", datumIdentityDigest: sha256BoundedIdentity("WGS84") });
  }
  if (normalized.includes("NAD83")) {
    return Object.freeze({ datumClass: "NAD83", datumIdentityDigest: sha256BoundedIdentity("NAD83") });
  }
  return Object.freeze({ datumClass: "OTHER_EXPLICIT", datumIdentityDigest: sha256BoundedIdentity(normalized) });
}

function extractProjectedContractIdentity(value = "") {
  const text = normalizeCoordinateEvidenceText(value);
  const lines = text.split("\n").map(line => line.trim()).filter(Boolean);
  const explicitProjectedDeclaration = lines.some(line => (
    /\b(?:projected\s+crs|projected\s+coordinate\s+system|projection)\b/iu.test(line)
      || /^CRS\s*\|/iu.test(line)
  ));
  const datumTokens = [];
  const zones = [];
  const hemispheres = [];
  const axisOrders = [];
  const epsgCodes = [];
  for (const line of lines) {
    for (const match of line.matchAll(/\bEPSG\s*[:#|=-]?\s*(\d{4,6})\b/giu)) {
      epsgCodes.push(Number(match[1]));
    }
    for (const match of line.matchAll(/\bDatum\s*[:#|=-]?\s*([A-Z][A-Z0-9 _-]{1,30})/giu)) {
      datumTokens.push(match[1].trim());
    }
    if (/^CRS\s*\|/iu.test(line)) {
      const crsValue = line.split("|").slice(1).join(" ").trim();
      if (crsValue) datumTokens.push(crsValue);
    }
    if (/\bWGS\s*(?:84|1984)\b/iu.test(line)) datumTokens.push("WGS84");
    if (/\bNAD\s*83\b/iu.test(line)) datumTokens.push("NAD83");
    for (const match of line.matchAll(/\b(?:ZONE|ZONA|FUSO)\s*(?:\||[:#=-])?\s*(\d{1,2})([NS])?\b/giu)) {
      zones.push(Number(match[1]));
      if (match[2]) hemispheres.push(match[2].toUpperCase());
    }
    for (const match of line.matchAll(/\bUTM\s*(\d{1,2})([NS])\b/giu)) {
      zones.push(Number(match[1]));
      hemispheres.push(match[2].toUpperCase());
    }
    for (const match of line.matchAll(/^MGRS\s*[|:]\s*(\d{1,2})([C-HJ-NP-X])\b/giu)) {
      zones.push(Number(match[1]));
      hemispheres.push(hemisphereFromLatitudeBand(match[2]));
    }
    const hemisphereMatch = line.match(/^(?:HEMISPHERE\s*[|:=]\s*|(?:NORTH|SOUTH)(?:ERN)?\s+HEMISPHERE\s*$)(N|S|NORTH|SOUTH)?/iu);
    if (hemisphereMatch) {
      const token = String(hemisphereMatch[1] || line).toUpperCase();
      if (/\bN(?:ORTH)?\b/u.test(token)) hemispheres.push("N");
      if (/\bS(?:OUTH)?\b/u.test(token)) hemispheres.push("S");
    }
    const header = parseProjectedAxisHeaderLine(line, 0);
    if (header) {
      axisOrders.push(header.eastingIndex < header.northingIndex ? "EASTING_NORTHING" : "NORTHING_EASTING");
    }
    if (/^AXIS(?:_|\s+)ORDER\b/iu.test(line)) {
      const upper = line.toUpperCase();
      const eastingIndex = Math.max(upper.indexOf("EASTING"), upper.search(/\bX\b/u));
      const northingIndex = Math.max(upper.indexOf("NORTHING"), upper.search(/\bY\b/u));
      if (eastingIndex >= 0 && northingIndex >= 0) {
        axisOrders.push(eastingIndex < northingIndex ? "EASTING_NORTHING" : "NORTHING_EASTING");
      }
    }
    const explicitPoint = parseExplicitProjectedPointLine(line, 0);
    if (explicitPoint) axisOrders.push(explicitPoint.axisOrder);
  }
  for (const code of epsgCodes) {
    if (code >= 32601 && code <= 32660) {
      datumTokens.push("WGS84");
      zones.push(code - 32600);
      hemispheres.push("N");
    } else if (code >= 32701 && code <= 32760) {
      datumTokens.push("WGS84");
      zones.push(code - 32700);
      hemispheres.push("S");
    } else if (code !== 4326 && explicitProjectedDeclaration) {
      datumTokens.push(`EPSG${code}`);
    }
  }
  const datumIdentities = datumTokens.map(normalizeProjectedDatumIdentity).filter(item => item.datumClass);
  const datumClasses = [...new Set(datumIdentities.map(item => item.datumClass))];
  const datumDigests = [...new Set(datumIdentities.map(item => item.datumIdentityDigest))];
  const validZones = [...new Set(zones.filter(zone => Number.isInteger(zone) && zone >= 1 && zone <= 60))];
  const validHemispheres = [...new Set(hemispheres.filter(value => value === "N" || value === "S"))];
  const validAxisOrders = [...new Set(axisOrders.filter(Boolean))];
  const gridIdentityRequired = lines.some(line => /\b(?:UTM|MGRS)\b/iu.test(line))
    || validZones.length > 0 || validHemispheres.length > 0
    || epsgCodes.some(code => code >= 32601 && code <= 32760);
  const explicitOtherProjectedIdentity = !gridIdentityRequired
    && explicitProjectedDeclaration
    && epsgCodes.length === 1
    && epsgCodes[0] !== 4326;
  const identity = Object.freeze({
    datumClass: datumClasses.length === 1 ? datumClasses[0] : "",
    datumIdentityDigest: datumDigests.length === 1 ? datumDigests[0] : "",
    zone: validZones.length === 1 ? validZones[0] : null,
    hemisphere: validHemispheres.length === 1 ? validHemispheres[0] : "",
    axisOrder: validAxisOrders.length === 1 ? validAxisOrders[0] : "",
    complete: datumClasses.length === 1
      && datumDigests.length === 1
      && (gridIdentityRequired
        ? validZones.length === 1 && validHemispheres.length === 1
        : explicitOtherProjectedIdentity)
      && validAxisOrders.length === 1
  });
  return identity;
}

function acquisitionContractPayload(contract = {}) {
  const structure = contract?.structure || {};
  const crs = structure?.crs || {};
  return Object.freeze({
    schemaVersion: ONE_SHOT_ACQUISITION_CONTRACT_SCHEMA,
    family: Object.values(ONE_SHOT_STRUCTURED_FAMILY).includes(contract?.family)
      ? contract.family
      : ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW,
    selected: contract?.selected === true,
    format: ONE_SHOT_ACQUISITION_FORMATS.includes(contract?.format)
      ? contract.format
      : "GENERIC_REVIEW",
    structure: Object.freeze({
      coordinateRowCount: boundedStructureCount(structure.coordinateRowCount),
      repeatedHeaderCount: boundedStructureCount(structure.repeatedHeaderCount),
      groupBoundaryCount: boundedStructureCount(structure.groupBoundaryCount),
      groupRowCounts: boundedStructureCounts(structure.groupRowCounts),
      groupBoundaryMode: ["NONE", "EXPLICIT_GROUP_HEADINGS", "REPEATED_HEADERS"].includes(structure.groupBoundaryMode)
        ? structure.groupBoundaryMode
        : "NONE",
      layoutRegionCount: boundedStructureCount(structure.layoutRegionCount),
      projectedCoordinateRowCount: boundedStructureCount(structure.projectedCoordinateRowCount),
      headerRequired: structure.headerRequired === true,
      rowContinuityRequired: structure.rowContinuityRequired === true,
      groupBoundaryRequired: structure.groupBoundaryRequired === true,
      mapUiRolesBound: structure.mapUiRolesBound === true,
      crs: Object.freeze({
        datumClass: ["WGS84", "NAD83", "OTHER_EXPLICIT"].includes(crs.datumClass)
          ? crs.datumClass
          : "",
        datumIdentityDigest: /^[a-f0-9]{64}$/u.test(String(crs.datumIdentityDigest || ""))
          ? String(crs.datumIdentityDigest)
          : "",
        zone: Number.isInteger(crs.zone) && crs.zone >= 1 && crs.zone <= 60 ? crs.zone : null,
        hemisphere: crs.hemisphere === "N" || crs.hemisphere === "S" ? crs.hemisphere : "",
        axisOrder: ["EASTING_NORTHING", "NORTHING_EASTING"].includes(crs.axisOrder) ? crs.axisOrder : "",
        complete: crs.complete === true
      })
    })
  });
}

export function createOneShotAcquisitionContract({
  route,
  sourceText = "",
  layoutLines = [],
  imageIdentity = null,
  resultRevision = 1
} = {}) {
  const family = Object.values(ONE_SHOT_STRUCTURED_FAMILY).includes(route?.family)
    ? route.family
    : ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW;
  const evidence = route?.evidence || {};
  const crs = family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE
    ? extractProjectedContractIdentity(sourceText)
    : Object.freeze({ datumClass: "", datumIdentityDigest: "", zone: null, hemisphere: "", axisOrder: "", complete: false });
  const groupedDmsStructure = family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
    ? extractGroupedDmsStructure(sourceText)
    : Object.freeze({ mode: "NONE", rowCounts: Object.freeze([]) });
  const payload = acquisitionContractPayload({
    family,
    selected: route?.matched === true,
    format: getAcquisitionFormat(family, evidence, sourceText),
    structure: {
      coordinateRowCount: evidence.coordinateRowCount,
      repeatedHeaderCount: evidence.repeatedHeaderCount,
      groupBoundaryCount: Math.max(
        boundedStructureCount(evidence.groupHeadingCount),
        boundedStructureCount(evidence.repeatedHeaderCount)
      ),
      groupRowCounts: groupedDmsStructure.rowCounts,
      groupBoundaryMode: groupedDmsStructure.mode,
      layoutRegionCount: evidence.layoutRegionCount,
      projectedCoordinateRowCount: evidence.projectedCoordinateRowCount,
      headerRequired: [
        ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE,
        ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE,
        ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
      ].includes(family),
      rowContinuityRequired: [
        ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE,
        ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE,
        ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED
      ].includes(family),
      groupBoundaryRequired: family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED,
      mapUiRolesBound: family === ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT
        && boundedStructureCount(evidence.layoutRegionCount) >= 2,
      crs
    }
  });
  const contract = Object.freeze({
    ...payload,
    contractDigest: sha256BoundedIdentity(JSON.stringify(payload))
  });
  privateAcquisitionBindings.set(contract, createPrivateAcquisitionBinding({
    family,
    format: payload.format,
    sourceText,
    structure: payload.structure,
    groupBoundaryCount: payload.structure.groupBoundaryCount,
    layoutLines,
    imageIdentity,
    resultRevision,
    expectedRowCount: family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE
      ? payload.structure.projectedCoordinateRowCount
      : payload.structure.coordinateRowCount
  }));
  return contract;
}

function buildAcquisitionConformanceResult({ contract, status, reason, counts = {} }) {
  return Object.freeze({
    family: String(contract?.family || ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW),
    status,
    reason,
    conformant: status === ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT,
    counts: Object.freeze({
      coordinateRowCount: boundedStructureCount(counts.coordinateRowCount),
      repeatedHeaderCount: boundedStructureCount(counts.repeatedHeaderCount),
      groupBoundaryCount: boundedStructureCount(counts.groupBoundaryCount),
      mapRoleCount: boundedStructureCount(counts.mapRoleCount),
      projectedCoordinateRowCount: boundedStructureCount(counts.projectedCoordinateRowCount),
      crsFieldCount: boundedStructureCount(counts.crsFieldCount),
      sourceRegionCount: boundedStructureCount(counts.sourceRegionCount),
      observedCandidateCount: boundedStructureCount(counts.observedCandidateCount),
      boundCandidateCount: boundedStructureCount(counts.boundCandidateCount),
      unassignedCandidateCount: boundedStructureCount(counts.unassignedCandidateCount)
    })
  });
}

function buildStructurallyValidatedConformanceResult({
  contract,
  payload,
  providerText,
  structuralConformant,
  structuralReason,
  counts = {}
}) {
  if (!structuralConformant) {
    return buildAcquisitionConformanceResult({
      contract: payload,
      status: ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED,
      reason: structuralReason,
      counts
    });
  }
  const privateConformance = validatePrivateAcquisitionBinding({
    contract,
    payload,
    providerText,
    providerStructure: {
      ...counts,
      groupBoundaryMode: payload.structure.groupBoundaryMode
    }
  });
  return buildAcquisitionConformanceResult({
    contract: payload,
    status: privateConformance.conformant
      ? ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT
      : ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED,
    reason: privateConformance.reason,
    counts: {
      ...counts,
      sourceRegionCount: privateConformance.sourceRegionCount,
      observedCandidateCount: privateConformance.observedCandidateCount,
      boundCandidateCount: privateConformance.boundCandidateCount,
      unassignedCandidateCount: privateConformance.unassignedCandidateCount
    }
  });
}

function parseGroupedDmsProviderEvidence(text) {
  const lines = normalizeCoordinateEvidenceText(text).split("\n").map(line => line.trim()).filter(Boolean);
  const groups = lines.filter(line => /^GROUP\s*\|\s*\S/iu.test(line));
  const headers = lines.filter(line => /^HEADER\s*\|\s*\S/iu.test(line));
  const groupRowCounts = [];
  const groupLabels = [];
  let activeGroup = -1;
  let closedGroupStructure = true;
  const points = lines.filter(line => {
    if (/^GROUP\s*\|\s*\S/iu.test(line)) {
      groupRowCounts.push(0);
      groupLabels.push(new Set());
      activeGroup = groupRowCounts.length - 1;
      return false;
    }
    const fields = splitStructuredFields(line, "|");
    if (fields.length !== 4 || !/^POINT$/iu.test(fields[0]) || !fields[1]) return false;
    const valid = Number.isFinite(parseStrictDmsCoordinate(fields[2], "NS"))
      && Number.isFinite(parseStrictDmsCoordinate(fields[3], "EWO"));
    if (!valid || activeGroup < 0 || groupLabels[activeGroup].has(fields[1])) {
      closedGroupStructure = false;
      return false;
    }
    groupLabels[activeGroup].add(fields[1]);
    groupRowCounts[activeGroup] += 1;
    return true;
  });
  const coordinateBearingLines = lines.filter(line => countDmsComponents(line) > 0 || countDecimalComponents(line) > 0);
  return Object.freeze({
    coordinateRowCount: points.length,
    groupBoundaryCount: groups.length,
    groupRowCounts: boundedStructureCounts(groupRowCounts),
    repeatedHeaderCount: headers.length,
    closed: closedGroupStructure && points.length === coordinateBearingLines.length && groups.length === headers.length
  });
}

function parseMapProviderEvidence(text) {
  const lines = normalizeCoordinateEvidenceText(text).split("\n").map(line => line.trim()).filter(Boolean);
  const search = lines.filter(line => /^MAP_SEARCH_BOX\s*\|/iu.test(line) && hasExactlyOneDecimalCoordinatePair(line));
  const details = lines.filter(line => /^MAP_PLACE_DETAILS\s*\|/iu.test(line) && hasExactlyOneDecimalCoordinatePair(line));
  const plusCodes = lines.filter(line => /^PLUS_CODE\s*\|\s*\S/iu.test(line));
  const coordinateBearingLines = lines.filter(line => countDmsComponents(line) > 0 || countDecimalComponents(line) > 0);
  return Object.freeze({
    coordinateRowCount: search.length + details.length,
    mapRoleCount: (search.length === 1 ? 1 : 0) + (details.length === 1 ? 1 : 0) + (plusCodes.length === 1 ? 1 : 0),
    closed: search.length === 1 && details.length === 1 && plusCodes.length === 1
      && coordinateBearingLines.length === 2
  });
}

function parseProjectedProviderEvidence(text) {
  const lines = normalizeCoordinateEvidenceText(text).split("\n").map(line => line.trim()).filter(Boolean);
  const pointRows = lines.filter(line => {
    const fields = splitStructuredFields(line, "|");
    return fields.length === 4 && /^POINT$/iu.test(fields[0]) && Boolean(fields[1])
      && /^[-+]?\d{4,9}(?:[.,]\d+)?$/u.test(fields[2])
      && /^[-+]?\d{4,9}(?:[.,]\d+)?$/u.test(fields[3]);
  });
  const mgrsRows = lines.map(parseStrictMgrsCoordinateLine).filter(Boolean);
  const coordinateBearingLines = lines.filter(line => (
    /^POINT\s*\|/iu.test(line) || /^MGRS\s*[|:]/iu.test(line)
  ));
  const identity = extractProjectedContractIdentity(text);
  const crsFieldCount = [identity.datumClass, identity.zone, identity.hemisphere, identity.axisOrder]
    .filter(value => value !== "" && value !== null).length;
  return Object.freeze({
    projectedCoordinateRowCount: pointRows.length + mgrsRows.length,
    crsFieldCount,
    identity,
    closed: pointRows.length + mgrsRows.length === coordinateBearingLines.length && identity.complete
  });
}

function projectedIdentityMatches(expected, actual) {
  return expected?.complete === true && actual?.complete === true
    && expected.datumClass === actual.datumClass
    && expected.datumIdentityDigest === actual.datumIdentityDigest
    && expected.zone === actual.zone
    && expected.hemisphere === actual.hemisphere
    && expected.axisOrder === actual.axisOrder;
}

export function validateOneShotAcquisitionContract({ contract, providerText = "" } = {}) {
  const payload = acquisitionContractPayload(contract);
  const validContract = contract?.schemaVersion === ONE_SHOT_ACQUISITION_CONTRACT_SCHEMA
    && /^[a-f0-9]{64}$/u.test(String(contract?.contractDigest || ""))
    && contract.contractDigest === sha256BoundedIdentity(JSON.stringify(payload));
  if (!validContract) {
    return buildAcquisitionConformanceResult({
      contract: payload,
      status: ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED,
      reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONTRACT_INVALID
    });
  }
  if (!payload.selected || payload.family === ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW) {
    return buildAcquisitionConformanceResult({
      contract: payload,
      status: ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED,
      reason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GENERIC_REVIEW_ONLY
    });
  }

  if (payload.family === ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED) {
    const evidence = parseGroupedDmsProviderEvidence(providerText);
    const counts = evidence;
    const rowsMatch = evidence.coordinateRowCount === payload.structure.coordinateRowCount;
    const groupsMatch = evidence.groupBoundaryCount === payload.structure.groupBoundaryCount
      && evidence.repeatedHeaderCount === payload.structure.groupBoundaryCount
      && JSON.stringify(evidence.groupRowCounts) === JSON.stringify(payload.structure.groupRowCounts);
    return buildStructurallyValidatedConformanceResult({
      contract,
      payload,
      providerText,
      structuralConformant: evidence.closed && rowsMatch && groupsMatch,
      structuralReason: groupsMatch
        ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.STRUCTURE_COUNT_MISMATCH
        : ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GROUP_BOUNDARY_MISMATCH,
      counts
    });
  }

  if (payload.family === ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT) {
    const evidence = parseMapProviderEvidence(providerText);
    const conformant = payload.structure.mapUiRolesBound
      && evidence.closed
      && evidence.coordinateRowCount === payload.structure.coordinateRowCount;
    return buildStructurallyValidatedConformanceResult({
      contract,
      payload,
      providerText,
      structuralConformant: conformant,
      structuralReason: ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.MAP_ROLE_BINDING_MISMATCH,
      counts: evidence
    });
  }

  if (payload.family === ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE) {
    const evidence = parseProjectedProviderEvidence(providerText);
    const identityMatch = projectedIdentityMatches(payload.structure.crs, evidence.identity);
    const conformant = evidence.closed
      && identityMatch
      && evidence.projectedCoordinateRowCount === payload.structure.projectedCoordinateRowCount;
    return buildStructurallyValidatedConformanceResult({
      contract,
      payload,
      providerText,
      structuralConformant: conformant,
      structuralReason: identityMatch
        ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.STRUCTURE_COUNT_MISMATCH
        : ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_CRS_MISMATCH,
      counts: evidence
    });
  }

  const providerRoute = classifyOneShotStructuredFamily({ text: providerText });
  const sameFamily = providerRoute.matched === true && providerRoute.family === payload.family;
  const providerFormat = getAcquisitionFormat(providerRoute.family, providerRoute.evidence, providerText);
  const formatMatches = providerFormat === payload.format;
  const rowCountMatches = providerRoute.evidence.coordinateRowCount === payload.structure.coordinateRowCount;
  const headerCountMatches = !payload.structure.headerRequired
    || providerRoute.evidence.repeatedHeaderCount === payload.structure.repeatedHeaderCount;
  const conformant = sameFamily && formatMatches && rowCountMatches && headerCountMatches;
  return buildStructurallyValidatedConformanceResult({
    contract,
    payload,
    providerText,
    structuralConformant: conformant,
    structuralReason: !sameFamily || !formatMatches
      ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.FAMILY_MISMATCH
      : !headerCountMatches
        ? ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.HEADER_OR_CONTINUITY_MISMATCH
        : ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.STRUCTURE_COUNT_MISMATCH,
    counts: {
      coordinateRowCount: providerRoute.evidence.coordinateRowCount,
      repeatedHeaderCount: providerRoute.evidence.repeatedHeaderCount,
      groupBoundaryCount: providerRoute.evidence.groupHeadingCount
    }
  });
}

export function buildOneShotStructuredFamilyPrompt({
  family = ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW,
  noCoordinatesText = "NO_COORDINATES_FOUND"
} = {}) {
  const commonRules = `
Use only structure visibly present in the image. Never infer a family, CRS, axis order, direction, missing row, group, or coordinate from a country, filename, historical sample, or numeric range.
Preserve every visible row label, original coordinate notation, decimal precision, repeated header, and group boundary.
Do not convert DMS or projected coordinates. Do not infer geometry. Do not output bbox, confidence, prose, markdown, or unrelated text.
For a specifically requested family, if its structure is not visibly complete, output only: ${noCoordinatesText}
For the generic review contract, output that same text only when no coordinate-bearing structure is visible.`;

  switch (family) {
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT:
      return `Read one clearly labelled WGS84 point from this image.${commonRules}
Output exactly three lines:
WGS84 Single Point
longitude | <original visible longitude value>
latitude | <original visible latitude value>`;
    case ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE:
      return `Read the complete labelled WGS84 decimal coordinate table in this image.${commonRules}
Output:
WGS84 Longitude Latitude Table
label | longitude | latitude
Then output every visible row in its original order.`;
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE:
      return `Read the complete labelled DMS coordinate table in this image.${commonRules}
Output:
DMS Coordinate Table
label | latitude DMS | longitude DMS
Then output every visible row in its original order.`;
    case ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED:
      return `Read every labelled DMS group in this image.${commonRules}
For each visible group output:
GROUP | <exact visible group heading, or REPEATED_HEADER_BOUNDARY when the boundary is only a repeated header>
HEADER | <exact visible coordinate header>
POINT | <exact row label> | <original latitude DMS> | <original longitude DMS>
Keep groups separate and never connect points across groups.`;
    case ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT:
      return `Read only the coordinate-bearing regions in this map screenshot.${commonRules}
Output each visible source region separately:
MAP_SEARCH_BOX | <original visible coordinate text>
MAP_PLACE_DETAILS | <original visible coordinate text>
PLUS_CODE | <original visible Plus Code>
Do not merge values or choose a preferred precision.`;
    case ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE:
      return `Read the complete projected-coordinate table and its explicit CRS evidence.${commonRules}
Output the visibly supported metadata first:
CRS | <datum or EPSG text>
ZONE | <zone text>
HEMISPHERE | <hemisphere text>
AXIS_ORDER | <visible axis order>
For an Easting/Northing or X/Y table, output every row as:
POINT | <label> | <first axis value> | <second axis value>
For MGRS, output every row as:
MGRS | <zone and latitude band> | <grid square> | <easting digits> | <northing digits>`;
    default:
      return `Read only visible coordinate-structure evidence from this image.${commonRules}
The family is not reliably classified. Preserve visible coordinate headers, row labels, original values, source-region labels, repeated headers, group boundaries, and explicit CRS text without choosing a family.
Output:
UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE
Then transcribe only the visible coordinate-bearing lines in original order.
This output must remain review-only.`;
  }
}

export function shouldRunWgs84TimeoutRescue({ localOcrAttempted = false } = {}) {
  return localOcrAttempted !== true;
}
import { inflateSync } from "node:zlib";
