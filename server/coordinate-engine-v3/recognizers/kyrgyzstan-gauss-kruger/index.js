import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";

export const KYRGYZ_GK_RECOGNIZER_ID = "kyrgyzstan_gauss_kruger";
export const KYRGYZ_GK_PRECISION_MODE = "kyrgyzstan-gauss-kruger";
export const KYRGYZ_GK_CRS = "EPSG:28413";
export const KYRGYZ_GK_OUTPUT_CRS = "EPSG:4326";

const KYRGYZ_GK_PROJ4_DEF = "+proj=tmerc +lat_0=0 +lon_0=75 +k=1 +x_0=13500000 +y_0=0 +ellps=krass +towgs84=25,-141,-78.5,0,-0.35,-0.736,0 +units=m +no_defs +type=crs";
const KRASSOWSKY_A = 6378245;
const KRASSOWSKY_INV_F = 298.3;
const WGS84_A = 6378137;
const WGS84_INV_F = 298.257223563;
const KYRGYZ_BOUNDS = Object.freeze({
  minLongitude: 69,
  maxLongitude: 80,
  minLatitude: 39,
  maxLatitude: 43,
});

function getInputText(input = {}) {
  if (typeof input === "string") return input;
  return String(input.text ?? input.rawText ?? input.coordinatesText ?? input.coordinates ?? "").trim();
}

function normalizeText(value = "") {
  return String(value || "")
    .replace(/[，]/g, ",")
    .replace(/[№º]/g, (match) => (match === "№" ? "№" : "°"))
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeGkValue(value = "") {
  return String(value || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/,/g, ".")
    .replace(/[^\d.-]/g, "");
}

function normalizeNorthingValue(value = "") {
  const normalized = normalizeGkValue(value);
  if (/^60\d{4}$/.test(normalized)) return `4${normalized}`;
  return normalized;
}

function hasKyrgyzGkContext(text = "") {
  const value = String(text || "");
  return /Координаты\s+угловых\s+точек|лицензионн(?:ой|ая|ую)|прямоугольн(?:ой|ая|ую)\s+систем|№\s*точек|N\s*o?\s*points?|Kyrgyzstan|Киргиз|Кыргыз|Pulkovo|Gauss|Гаусс|Крюгер/i.test(value)
    && /\bX\b/i.test(value)
    && /\bY\b/i.test(value);
}

function hasKyrgyzGkHeader(text = "") {
  return /(?:point|points?|№|no\.?|n|№\s*(?:точек|points?)|N\s*o?\s*points?)\s*[\|,;\s-]*X\s*[\|,;\s-]*Y/i.test(String(text || ""));
}

export function looksLikeKyrgyzGkPair(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  return Number.isFinite(easting)
    && Number.isFinite(northing)
    && easting >= 13000000
    && easting <= 13999999
    && northing >= 3900000
    && northing <= 4800000;
}

function extractPairsFromLine(line = "") {
  const pairs = [];
  const pairPattern = /(?:^|[^\d])(13\d{5,7})\D+([46]\d{5,6})(?=$|[^\d])/g;
  let match;
  while ((match = pairPattern.exec(String(line || ""))) !== null) {
    const x = normalizeGkValue(match[1]);
    const y = normalizeNorthingValue(match[2]);
    if (looksLikeKyrgyzGkPair(x, y)) pairs.push({ x, y });
  }
  return pairs;
}

function analyzeRows(rows = []) {
  const points = rows
    .map((row) => Number(row?.point))
    .filter((point) => Number.isInteger(point) && point > 0)
    .sort((a, b) => a - b);
  const uniquePoints = Array.from(new Set(points));
  const firstPoint = uniquePoints[0] || null;
  const lastPoint = uniquePoints[uniquePoints.length - 1] || null;
  const abnormalPoints = uniquePoints.filter((point) => point > 200);
  const duplicatePoints = points.filter((point, index) => points.indexOf(point) !== index);
  const missingPoints = [];
  if (firstPoint === 1 && Number.isInteger(lastPoint)) {
    for (let point = 1; point <= lastPoint; point += 1) {
      if (!uniquePoints.includes(point)) missingPoints.push(point);
    }
  }
  return Object.freeze({
    rowCount: uniquePoints.length,
    firstPoint,
    lastPoint,
    startsAtOne: firstPoint === 1,
    continuous: firstPoint === 1 && missingPoints.length === 0,
    duplicatePoints: Object.freeze(Array.from(new Set(duplicatePoints))),
    missingPoints: Object.freeze(missingPoints),
    abnormalPoints: Object.freeze(abnormalPoints),
    isComplete: uniquePoints.length >= 3
      && firstPoint === 1
      && missingPoints.length === 0
      && abnormalPoints.length === 0
      && duplicatePoints.length === 0,
  });
}

function inferRowsByTableOrder(text = "") {
  const tableRows = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => extractPairsFromLine(line).slice(0, 2))
    .filter((pairs) => pairs.length > 0);
  if (tableRows.length < 20) return [];

  const leftColumnCount = tableRows.length;
  const dualColumnRows = tableRows.filter((pairs) => pairs.length >= 2).length;
  const rowsByPoint = new Map();
  tableRows.forEach((pairs, index) => {
    rowsByPoint.set(index + 1, {
      point: index + 1,
      x: pairs[0].x,
      y: pairs[0].y,
      source: "table_order_fallback",
    });
    if (pairs[1]) {
      const rightPoint = leftColumnCount + index + 1;
      rowsByPoint.set(rightPoint, {
        point: rightPoint,
        x: pairs[1].x,
        y: pairs[1].y,
        source: "table_order_fallback",
      });
    }
  });

  const inferredRows = Array.from(rowsByPoint.values()).sort((a, b) => a.point - b.point);
  const integrity = analyzeRows(inferredRows);
  const looksLikeTwoColumnTable = leftColumnCount >= 25
    && leftColumnCount <= 40
    && dualColumnRows >= Math.max(15, Math.floor(leftColumnCount * 0.65));
  const plausibleTotal = inferredRows.length >= 50 && inferredRows.length <= 80;
  if (!looksLikeTwoColumnTable
    || !plausibleTotal
    || integrity.firstPoint !== 1
    || !integrity.continuous
    || integrity.abnormalPoints.length > 0) {
    return [];
  }
  return inferredRows;
}

export function parseKyrgyzGkRows(input = {}) {
  const source = getInputText(input);
  if (!source) return [];
  const hasStrongSignature = hasKyrgyzGkContext(source) || hasKyrgyzGkHeader(source);
  const projectedPairCount = (source.match(/13\d{5,7}\s*[\|,;\s]+\s*[46]\d{5,6}/g) || []).length;
  if (!hasStrongSignature && projectedPairCount < 8) return [];

  const rowsByPoint = new Map();
  const duplicatePoints = new Set();
  const rowPattern = /(?:^|[^\d])(\d{1,3})\s*[\|,;\s]+\s*(13\d{5,7})\s*[\|,;\s]+\s*([46]\d{5,6})(?=$|[^\d])/g;
  String(source || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      if (/^(?:point|points?|№|no\.?|n|№\s*(?:точек|points?))\s*[\|,;\s-]*X\s*[\|,;\s-]*Y$/i.test(line) || /точек/i.test(line)) return;
      rowPattern.lastIndex = 0;
      let match;
      while ((match = rowPattern.exec(line)) !== null) {
        const point = Number(match[1]);
        const x = normalizeGkValue(match[2]);
        const y = normalizeNorthingValue(match[3]);
        if (!Number.isInteger(point) || point <= 0 || !looksLikeKyrgyzGkPair(x, y)) continue;
        if (rowsByPoint.has(point)) {
          duplicatePoints.add(point);
        } else {
          rowsByPoint.set(point, Object.freeze({
            point,
            label: String(point),
            x,
            y,
            source: "explicit_point_row",
          }));
        }
      }
    });

  const explicitRows = Array.from(rowsByPoint.values()).sort((a, b) => a.point - b.point);
  if (duplicatePoints.size > 0) return [];
  const inferredRows = inferRowsByTableOrder(source);
  const explicitIntegrity = analyzeRows(explicitRows);
  const inferredIntegrity = analyzeRows(inferredRows);

  if (inferredRows.length >= explicitRows.length
    && inferredRows.length >= 50
    && inferredIntegrity.firstPoint === 1
    && inferredIntegrity.abnormalPoints.length === 0) {
    return inferredRows.map(Object.freeze);
  }

  if (explicitIntegrity.duplicatePoints.length > 0 || explicitIntegrity.abnormalPoints.length > 0) return [];
  return explicitRows.map(Object.freeze);
}

function inverseTransverseMercator(easting, northing) {
  const f = 1 / KRASSOWSKY_INV_F;
  const e2 = 2 * f - f ** 2;
  const ep2 = e2 / (1 - e2);
  const e1 = (1 - Math.sqrt(1 - e2)) / (1 + Math.sqrt(1 - e2));
  const k0 = 1;
  const falseEasting = 13500000;
  const falseNorthing = 0;
  const longitudeOrigin = 75 * Math.PI / 180;
  const x = Number(easting) - falseEasting;
  const y = Number(northing) - falseNorthing;
  const m = y / k0;
  const mu = m / (KRASSOWSKY_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256));
  const fp = mu
    + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * Math.sin(2 * mu)
    + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * Math.sin(4 * mu)
    + (151 * e1 ** 3 / 96) * Math.sin(6 * mu)
    + (1097 * e1 ** 4 / 512) * Math.sin(8 * mu);
  const sinfp = Math.sin(fp);
  const cosfp = Math.cos(fp);
  const tanfp = Math.tan(fp);
  const c1 = ep2 * cosfp ** 2;
  const t1 = tanfp ** 2;
  const n1 = KRASSOWSKY_A / Math.sqrt(1 - e2 * sinfp ** 2);
  const r1 = KRASSOWSKY_A * (1 - e2) / ((1 - e2 * sinfp ** 2) ** 1.5);
  const d = x / (n1 * k0);
  const lat = fp - (n1 * tanfp / r1) * (
    d ** 2 / 2
    - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
    + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720
  );
  const lon = longitudeOrigin + (
    d
    - (1 + 2 * t1 + c1) * d ** 3 / 6
    + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120
  ) / cosfp;
  return { latitudeRad: lat, longitudeRad: lon };
}

function geodeticToCartesian(latitudeRad, longitudeRad, ellipsoidA, invF) {
  const f = 1 / invF;
  const e2 = 2 * f - f ** 2;
  const sinLat = Math.sin(latitudeRad);
  const cosLat = Math.cos(latitudeRad);
  const sinLon = Math.sin(longitudeRad);
  const cosLon = Math.cos(longitudeRad);
  const n = ellipsoidA / Math.sqrt(1 - e2 * sinLat ** 2);
  return {
    x: n * cosLat * cosLon,
    y: n * cosLat * sinLon,
    z: n * (1 - e2) * sinLat,
  };
}

function cartesianToGeodetic({ x, y, z }, ellipsoidA, invF) {
  const f = 1 / invF;
  const e2 = 2 * f - f ** 2;
  const p = Math.sqrt(x ** 2 + y ** 2);
  let latitude = Math.atan2(z, p * (1 - e2));
  for (let index = 0; index < 8; index += 1) {
    const sinLat = Math.sin(latitude);
    const n = ellipsoidA / Math.sqrt(1 - e2 * sinLat ** 2);
    latitude = Math.atan2(z + e2 * n * sinLat, p);
  }
  return {
    latitude: latitude * 180 / Math.PI,
    longitude: Math.atan2(y, x) * 180 / Math.PI,
  };
}

function pulkovo1942ToWgs84(latitudeRad, longitudeRad) {
  const source = geodeticToCartesian(latitudeRad, longitudeRad, KRASSOWSKY_A, KRASSOWSKY_INV_F);
  const arcSecondToRad = Math.PI / (180 * 3600);
  const dx = 25;
  const dy = -141;
  const dz = -78.5;
  const rx = 0 * arcSecondToRad;
  const ry = -0.35 * arcSecondToRad;
  const rz = -0.736 * arcSecondToRad;
  const scale = 1;
  const target = {
    x: dx + scale * source.x - rz * source.y + ry * source.z,
    y: dy + rz * source.x + scale * source.y - rx * source.z,
    z: dz - ry * source.x + rx * source.y + scale * source.z,
  };
  return cartesianToGeodetic(target, WGS84_A, WGS84_INV_F);
}

export function convertKyrgyzGkToWgs84(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  if (!looksLikeKyrgyzGkPair(easting, northing)) return null;
  const pulkovo = inverseTransverseMercator(easting, northing);
  const wgs84 = pulkovo1942ToWgs84(pulkovo.latitudeRad, pulkovo.longitudeRad);
  if (!Number.isFinite(wgs84.latitude)
    || !Number.isFinite(wgs84.longitude)
    || wgs84.longitude < KYRGYZ_BOUNDS.minLongitude
    || wgs84.longitude > KYRGYZ_BOUNDS.maxLongitude
    || wgs84.latitude < KYRGYZ_BOUNDS.minLatitude
    || wgs84.latitude > KYRGYZ_BOUNDS.maxLatitude) {
    return null;
  }
  return Object.freeze({
    latitude: wgs84.latitude,
    longitude: wgs84.longitude,
    crs: KYRGYZ_GK_OUTPUT_CRS,
  });
}

export function canHandleKyrgyzGk(input = {}) {
  return parseKyrgyzGkRows(input).length > 0;
}

export async function recognizeKyrgyzGk(input = {}, context = {}) {
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

  const rows = parseKyrgyzGkRows(input);
  return Object.freeze({
    handled: rows.length > 0,
    status: rows.length ? "accepted" : "not_handled",
    rows: Object.freeze(rows),
    integrity: analyzeRows(rows),
    crsDecision: Object.freeze({
      sourceCrs: KYRGYZ_GK_CRS,
      sourceProj4: KYRGYZ_GK_PROJ4_DEF,
      rule: "strong Kyrgyz/Russian GK table signature plus X=13xxxxxx full easting and Y=4xxxxxx northing",
    }),
    warnings: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeKyrgyzGk(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  return createNormalizedCoordinateResult({
    coordinateType: "kyrgyzstan_gauss_kruger",
    recognizerId: KYRGYZ_GK_RECOGNIZER_ID,
    coordinates: rows
      .map((row) => {
        const converted = convertKyrgyzGkToWgs84(row.x, row.y);
        if (!converted) return null;
        return {
          label: String(row.point),
          latitude: converted.latitude,
          longitude: converted.longitude,
          altitude: 0,
          sourceValue: `${row.point} | ${row.x} | ${row.y}`,
          sourceProjected: {
            x: Number(row.x),
            y: Number(row.y),
            axisSemantics: "X=EPSG:28413 full easting with zone prefix; Y=northing",
            sourceCrs: KYRGYZ_GK_CRS,
          },
        };
      })
      .filter(Boolean),
    crs: KYRGYZ_GK_OUTPUT_CRS,
    precisionMode: KYRGYZ_GK_PRECISION_MODE,
    warnings: result.warnings || [],
    suspectedPoints: [],
    sourceTrace: ["kyrgyzstan_gauss_kruger:deterministic", KYRGYZ_GK_CRS],
  });
}

export async function verifyKyrgyzGk(normalized = {}) {
  const coordinates = Array.isArray(normalized.coordinates) ? normalized.coordinates : [];
  const invalid = coordinates.filter((point) => (
    !Number.isFinite(Number(point.latitude))
    || !Number.isFinite(Number(point.longitude))
    || Number(point.longitude) < KYRGYZ_BOUNDS.minLongitude
    || Number(point.longitude) > KYRGYZ_BOUNDS.maxLongitude
    || Number(point.latitude) < KYRGYZ_BOUNDS.minLatitude
    || Number(point.latitude) > KYRGYZ_BOUNDS.maxLatitude
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

export function toKyrgyzGkKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export const kyrgyzGkRecognizer = createRecognizerContract({
  recognizerId: KYRGYZ_GK_RECOGNIZER_ID,
  coordinateType: "kyrgyzstan_gauss_kruger",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleKyrgyzGk,
  recognize: recognizeKyrgyzGk,
  normalize: normalizeKyrgyzGk,
  verify: verifyKyrgyzGk,
});

export default kyrgyzGkRecognizer;
