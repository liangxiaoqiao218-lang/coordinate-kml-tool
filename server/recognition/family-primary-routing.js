export const WGS84_PRIMARY_STAGE_CAP_MS = 25_000;
export const KYRGYZ_PRIMARY_STAGE_CAP_MS = 25_000;
export const MADAGASCAR_PRIMARY_STAGE_CAP_MS = 25_000;

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
  if (!Number.isFinite(degrees) || minutes >= 60 || seconds >= 60) return null;
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
    && /(?:^|[|\s])X(?:[|\s]|$)/im.test(text)
    && /(?:^|[|\s])Y(?:[|\s]|$)/im.test(text);
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
    if (cells.length < 3) return false;
    const x = Number(normalizeDecimalToken(cells[1]));
    const y = Number(normalizeDecimalToken(cells[2]));
    return Number.isFinite(x) && x >= 100000 && x <= 900000
      && Number.isFinite(y) && y >= 0 && y <= 10000000;
  }).length;
  return hasProjectedHeader && projectedRows >= 3;
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
    const eastingText = normalizeDecimalToken(xMatch?.[1] || numbers[1]);
    const northingText = normalizeDecimalToken(yMatch?.[1] || numbers[2]);
    const easting = Number(eastingText);
    const northing = Number(northingText);
    if (!Number.isFinite(easting) || easting < 100000 || easting > 900000
      || !Number.isFinite(northing) || northing < 9000000 || northing > 10000000) continue;
    const latitudePart = parts.find(part => /[°º].*[S]/i.test(part)) || "";
    const longitudePart = parts.find(part => /[°º].*[E]/i.test(part)) || "";
    const referenceLatitude = parseDmsCoordinate(latitudePart, "NS");
    const referenceLongitude = parseDmsCoordinate(longitudePart, "EW");
    const transformed = typeof transform === "function" ? transform(50, easting, northing, false) : null;
    const lat = Number.isFinite(referenceLatitude) ? referenceLatitude : transformed?.lat;
    const lon = Number.isFinite(referenceLongitude) ? referenceLongitude : transformed?.lon;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    parsed.push(Object.freeze({
      label,
      eastingText,
      northingText,
      latitudeDms: latitudePart,
      longitudeDms: longitudePart,
      lat,
      lon
    }));
  }
  const collapsed = collapseExactRepeatedCoordinateSequence(parsed, row => [
    row.label, row.eastingText, row.northingText, row.latitudeDms, row.longitudeDms
  ].join("|"));
  return Object.freeze({
    isIndonesiaUtm50: collapsed.length >= 3,
    crs: "EPSG:32750",
    axisOrder: "easting_northing",
    rows: Object.freeze(collapsed),
    rowCount: collapsed.length,
    duplicateSequenceCollapsed: collapsed.length !== parsed.length
  });
}

export function formatIndonesiaUtm50Rows(info = {}) {
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

export function shouldRunWgs84TimeoutRescue({ localOcrAttempted = false } = {}) {
  return localOcrAttempted !== true;
}
import { inflateSync } from "node:zlib";
