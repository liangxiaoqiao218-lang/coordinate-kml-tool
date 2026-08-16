import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const MADAGASCAR_CADASTRAL_RECOGNIZER_ID = "madagascar_cadastral";
export const MADAGASCAR_CADASTRAL_PRECISION_MODE = "cadastral-grid-num-xv-yv";
export const MADAGASCAR_CADASTRAL_SOURCE_CRS = "EPSG:29702";
export const MADAGASCAR_CADASTRAL_OUTPUT_CRS = "EPSG:4326";
export const MADAGASCAR_CADASTRAL_CELL_SEMANTICS = "CENTER";

const MADAGASCAR_CADASTRAL_PROJ4_DEF = "+proj=omerc +lat_0=-18.9 +lonc=44.1 +alpha=18.9 +gamma=18.9 +k=0.9995 +x_0=400000 +y_0=800000 +ellps=intl +pm=paris +towgs84=-198.383,-240.517,-107.909,0,0,0,0 +units=m +no_defs +type=crs";
const MADAGASCAR_BOUNDS = Object.freeze({
  minLongitude: 42,
  maxLongitude: 52,
  minLatitude: -27,
  maxLatitude: -10,
});
const ILakaka_LOCAL_ANCHOR = Object.freeze({
  projectedX: 293437.5,
  projectedY: 364062.5,
  longitude: 45.23,
  latitude: -22.68,
});
const METERS_PER_DEGREE_LATITUDE = 111320;
const METERS_PER_DEGREE_LONGITUDE_AT_ILAKAKA = 102600;
const DEFAULT_CELL_SIZE_METERS = 625;

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function getStructuredRows(input = {}) {
  if (Array.isArray(input)) return input;
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.tableRows)) return input.tableRows;
  if (Array.isArray(input.structuredRows)) return input.structuredRows;
  if (Array.isArray(input.cadastralRows)) return input.cadastralRows;
  return [];
}

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’‘`´]/g, "'")
    .replace(/[，]/g, ",")
    .replace(/\u00a0/g, " ");
}

function cleanSourceValue(value) {
  return String(value ?? "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
}

function normalizeGridValue(value = "") {
  const raw = String(value ?? "").trim().replace(/\s+/g, "");
  if (!raw) return "";
  const normalized = raw.replace(/,/g, ".");
  if (!/^-?\d+(?:\.\d+)?$/.test(normalized)) return "";
  return normalized;
}

function normalizeLabel(value = "") {
  return String(value ?? "").trim().replace(/[^\dA-Za-z-]/g, "");
}

function numericValue(value) {
  const numeric = Number(normalizeGridValue(value));
  return Number.isFinite(numeric) ? numeric : null;
}

function hasHalfMeterCellCenter(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && Math.abs((numeric * 2) - Math.round(numeric * 2)) < 1e-9 && Math.abs(numeric - Math.round(numeric)) >= 0.49;
}

function looksLikeMadagascarCadastralPair(x, y) {
  const xv = Number(x);
  const yv = Number(y);
  return Number.isFinite(xv)
    && Number.isFinite(yv)
    && xv >= 250000
    && xv <= 350000
    && yv >= 300000
    && yv <= 450000
    && hasHalfMeterCellCenter(xv)
    && hasHalfMeterCellCenter(yv);
}

function hasListeCarresSignature(text = "") {
  const value = normalizeText(text);
  return /\bListe[\s_-]*Carres\b/i.test(value) && /\bXV\b/i.test(value) && /\bYV\b/i.test(value);
}

function hasMadagascarCadastralContext(text = "") {
  const value = normalizeText(text);
  return hasListeCarresSignature(value)
    || (/\bMadagascar\b|\bIlakaka\b|\bAndriandampy\b/i.test(value)
      && /\b(?:cadastral|cadastre|carres?|carreau|grille|grid|quadrillage)\b/i.test(value)
      && /\bXV\b/i.test(value)
      && /\bYV\b/i.test(value));
}

function parseStructuredRow(row = {}, index = 0) {
  const nc = normalizeLabel(row.NC ?? row.nc ?? row.index ?? row.row ?? row.sourceRow ?? index + 1);
  const num = normalizeLabel(row.num ?? row.Num ?? row.NUM ?? row.cadastralNum ?? row.label);
  const xv = normalizeGridValue(row.XV ?? row.xv ?? row.X ?? row.x);
  const yv = normalizeGridValue(row.YV ?? row.yv ?? row.Y ?? row.y);
  const cmNomfir = cleanSourceValue(row.CM_NOMFIR ?? row.cmNomfir ?? row.cm_nomfir ?? row.name ?? row.commune);
  if (!num || !xv || !yv || !looksLikeMadagascarCadastralPair(xv, yv)) return null;
  return Object.freeze({
    nc: nc || String(index + 1),
    num,
    xv,
    yv,
    cmNomfir,
    sourceOrder: index + 1,
    source: "structured_row",
  });
}

function parseDelimitedLine(line = "", index = 0) {
  const trimmed = String(line || "").trim();
  if (!trimmed || /\b(?:liste|carres?|xv|yv|cm_nomfir|nomfir)\b/i.test(normalizeText(trimmed))) return null;
  const pipeParts = trimmed
    .split(/[|\t;]/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);

  if (pipeParts.length >= 5) {
    const [nc, xv, yv, cmNomfir, num] = pipeParts;
    const row = parseStructuredRow({ NC: nc, XV: xv, YV: yv, CM_NOMFIR: cmNomfir, num }, index);
    if (row) return row;
  }

  if (pipeParts.length >= 3) {
    const [num, xv, yv] = pipeParts;
    const row = parseStructuredRow({ NC: index + 1, num, XV: xv, YV: yv }, index);
    if (row) return row;
  }

  const tokens = trimmed.match(/[-+]?\d+(?:[.,]\d+)?|[A-Za-z][A-Za-z0-9_-]*/g) || [];
  if (tokens.length >= 5) {
    const [nc, xv, yv, cmNomfir, num] = tokens;
    const row = parseStructuredRow({ NC: nc, XV: xv, YV: yv, CM_NOMFIR: cmNomfir, num }, index);
    if (row) return row;
  }
  if (tokens.length >= 3) {
    const [num, xv, yv] = tokens;
    const row = parseStructuredRow({ NC: index + 1, num, XV: xv, YV: yv }, index);
    if (row) return row;
  }

  return null;
}

function dedupeRows(rows = []) {
  const seen = new Set();
  return rows.filter((row) => {
    const key = `${row.num}|${row.xv}|${row.yv}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function parseMadagascarCadastralRows(input = {}) {
  const structuredRows = getStructuredRows(input)
    .map((row, index) => parseStructuredRow(row, index))
    .filter(Boolean);
  if (structuredRows.length > 0) return dedupeRows(structuredRows).map(Object.freeze);

  const text = getInputText(input);
  if (!text || !hasMadagascarCadastralContext(text)) return [];
  return dedupeRows(String(text || "")
    .split(/\r?\n/)
    .map((line, index) => parseDelimitedLine(line, index))
    .filter(Boolean)).map(Object.freeze);
}

export function formatMadagascarCadastralRows(rows = []) {
  return ["num | XV | YV", ...rows.map((row) => `${row.num} | ${row.xv} | ${row.yv}`)].join("\n");
}

function getUniqueSortedNumbers(values = []) {
  return Array.from(new Set(values
    .map(Number)
    .filter(Number.isFinite)))
    .sort((a, b) => a - b);
}

function inferSpacing(values = []) {
  const sorted = getUniqueSortedNumbers(values);
  const diffs = [];
  for (let index = 1; index < sorted.length; index += 1) {
    const diff = sorted[index] - sorted[index - 1];
    if (diff > 0) diffs.push(diff);
  }
  if (!diffs.length) return DEFAULT_CELL_SIZE_METERS;
  return diffs.reduce((min, value) => Math.min(min, value), Number.POSITIVE_INFINITY);
}

export function inferMadagascarCadastralCellGeometry(rows = []) {
  const width = inferSpacing(rows.map((row) => row.xv));
  const height = inferSpacing(rows.map((row) => row.yv));
  return Object.freeze({
    semantics: MADAGASCAR_CADASTRAL_CELL_SEMANTICS,
    width,
    height,
    sourceCrs: MADAGASCAR_CADASTRAL_SOURCE_CRS,
    construction: "source projected cell center +/- half spacing, then convert each corner to WGS84",
  });
}

export function convertMadagascarCadastralToWgs84(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  if (!Number.isFinite(easting) || !Number.isFinite(northing)) return null;
  const longitude = ILakaka_LOCAL_ANCHOR.longitude
    + ((easting - ILakaka_LOCAL_ANCHOR.projectedX) / METERS_PER_DEGREE_LONGITUDE_AT_ILAKAKA);
  const latitude = ILakaka_LOCAL_ANCHOR.latitude
    + ((northing - ILakaka_LOCAL_ANCHOR.projectedY) / METERS_PER_DEGREE_LATITUDE);
  if (longitude < MADAGASCAR_BOUNDS.minLongitude
    || longitude > MADAGASCAR_BOUNDS.maxLongitude
    || latitude < MADAGASCAR_BOUNDS.minLatitude
    || latitude > MADAGASCAR_BOUNDS.maxLatitude) {
    return null;
  }
  return Object.freeze({
    longitude,
    latitude,
    crs: MADAGASCAR_CADASTRAL_OUTPUT_CRS,
    transform: "tananarive_paris_laborde_grid_local_approximation",
  });
}

export function buildMadagascarCadastralCellPolygons(rows = []) {
  const geometry = inferMadagascarCadastralCellGeometry(rows);
  const halfWidth = geometry.width / 2;
  const halfHeight = geometry.height / 2;
  return rows.map((row) => {
    const centerX = Number(row.xv);
    const centerY = Number(row.yv);
    const projectedCorners = [
      { x: centerX - halfWidth, y: centerY - halfHeight },
      { x: centerX + halfWidth, y: centerY - halfHeight },
      { x: centerX + halfWidth, y: centerY + halfHeight },
      { x: centerX - halfWidth, y: centerY + halfHeight },
      { x: centerX - halfWidth, y: centerY - halfHeight },
    ];
    const wgs84Corners = projectedCorners
      .map((corner) => convertMadagascarCadastralToWgs84(corner.x, corner.y))
      .filter(Boolean);
    return Object.freeze({
      label: row.num,
      sourceProjectedCenter: Object.freeze({ x: centerX, y: centerY }),
      sourceProjectedCorners: Object.freeze(projectedCorners.map(Object.freeze)),
      wgs84Polygon: Object.freeze(wgs84Corners.map(Object.freeze)),
      kmlCoordinates: wgs84Corners.map((point) => `${point.longitude},${point.latitude},0`).join(" "),
    });
  }).map(Object.freeze);
}

function analyzeMadagascarRows(rows = []) {
  const nums = rows.map((row) => row.num).filter(Boolean);
  const ncs = rows.map((row) => row.nc).filter(Boolean);
  const duplicateNums = nums.filter((num, index) => nums.indexOf(num) !== index);
  const invalidProjected = rows
    .filter((row) => !looksLikeMadagascarCadastralPair(row.xv, row.yv))
    .map((row) => row.num);
  return Object.freeze({
    rowCount: rows.length,
    ncCount: ncs.length,
    numCount: nums.length,
    xvCount: rows.filter((row) => row.xv).length,
    yvCount: rows.filter((row) => row.yv).length,
    duplicateNums: Object.freeze(Array.from(new Set(duplicateNums))),
    invalidProjected: Object.freeze(invalidProjected),
    sourceOrder: Object.freeze(nums),
    isComplete: rows.length > 0 && duplicateNums.length === 0 && invalidProjected.length === 0,
  });
}

export function canHandleMadagascarCadastral(input = {}) {
  return parseMadagascarCadastralRows(input).length > 0;
}

export async function recognizeMadagascarCadastral(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      rows: Object.freeze([]),
      warnings: Object.freeze([createWarningMetadata({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic Madagascar cadastral parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }

  const rows = parseMadagascarCadastralRows(input);
  const cellGeometry = inferMadagascarCadastralCellGeometry(rows);
  return Object.freeze({
    handled: rows.length > 0,
    status: rows.length ? "accepted" : "not_handled",
    rows: Object.freeze(rows),
    formattedRows: formatMadagascarCadastralRows(rows),
    integrity: analyzeMadagascarRows(rows),
    cellGeometry,
    cells: Object.freeze(buildMadagascarCadastralCellPolygons(rows)),
    crsDecision: Object.freeze({
      sourceCrs: MADAGASCAR_CADASTRAL_SOURCE_CRS,
      outputCrs: MADAGASCAR_CADASTRAL_OUTPUT_CRS,
      sourceProj4: MADAGASCAR_CADASTRAL_PROJ4_DEF,
      rule: "Liste_Carres/Liste_Carrés + XV/YV + Madagascar cadastral structural table",
    }),
    warnings: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeMadagascarCadastral(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "madagascar_cadastral",
    recognizerId: MADAGASCAR_CADASTRAL_RECOGNIZER_ID,
    geometryType: "multipolygon",
    coordinates: rows
      .map((row) => {
        const converted = convertMadagascarCadastralToWgs84(row.xv, row.yv);
        if (!converted) return null;
        return {
          label: row.num,
          latitude: converted.latitude,
          longitude: converted.longitude,
          altitude: 0,
          sourceValue: `${row.num} | ${row.xv} | ${row.yv}`,
          sourceProjected: {
            x: Number(row.xv),
            y: Number(row.yv),
            axisSemantics: "XV/YV cadastral cell center",
            sourceCrs: MADAGASCAR_CADASTRAL_SOURCE_CRS,
          },
        };
      })
      .filter(Boolean),
    crs: MADAGASCAR_CADASTRAL_OUTPUT_CRS,
    precisionMode: MADAGASCAR_CADASTRAL_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: [
      "madagascar_cadastral:deterministic_table",
      MADAGASCAR_CADASTRAL_SOURCE_CRS,
      MADAGASCAR_CADASTRAL_OUTPUT_CRS,
      "XV/YV=CENTER",
    ],
  });
}

export async function verifyMadagascarCadastral(normalized = {}) {
  const coordinates = Array.isArray(normalized.coordinates) ? normalized.coordinates : [];
  const invalid = coordinates.filter((point) => (
    !Number.isFinite(Number(point.latitude))
    || !Number.isFinite(Number(point.longitude))
    || Number(point.longitude) < MADAGASCAR_BOUNDS.minLongitude
    || Number(point.longitude) > MADAGASCAR_BOUNDS.maxLongitude
    || Number(point.latitude) < MADAGASCAR_BOUNDS.minLatitude
    || Number(point.latitude) > MADAGASCAR_BOUNDS.maxLatitude
    || point.sourceProjected?.sourceCrs !== MADAGASCAR_CADASTRAL_SOURCE_CRS
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

export function toMadagascarCadastralKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const madagascarCadastralRecognizer = createRecognizerContract({
  recognizerId: MADAGASCAR_CADASTRAL_RECOGNIZER_ID,
  coordinateType: "madagascar_cadastral",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleMadagascarCadastral,
  recognize: recognizeMadagascarCadastral,
  normalize: normalizeMadagascarCadastral,
  verify: verifyMadagascarCadastral,
});

export default madagascarCadastralRecognizer;
