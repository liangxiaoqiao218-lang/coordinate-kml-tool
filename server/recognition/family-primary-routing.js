export const WGS84_PRIMARY_STAGE_CAP_MS = 25_000;
export const KYRGYZ_PRIMARY_STAGE_CAP_MS = 25_000;
export const MADAGASCAR_PRIMARY_STAGE_CAP_MS = 25_000;

function normalizeEvidenceText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\u0000-\u001f]+/g, " ")
    .trim();
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
