import sharp from "sharp";

export const RECOGNITION_FIRST_ACQUISITION_VERSION = "recognition_first_acquisition_v1";

const DEFAULTS = Object.freeze({
  overviewMaxDimension: 1800,
  tileMaxDimension: 2600,
  maxImages: 6,
  tileOverlapRatio: 0.12,
  longAspectRatio: 1.8,
  largePixelCount: 3_000_000,
  largeByteCount: 1_000_000
});

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}
function orientedDimensions(metadata) {
  const orientation = Number(metadata?.orientation || 1);
  const swapsAxes = orientation >= 5 && orientation <= 8;
  return {
    width: swapsAxes ? Number(metadata?.height || 0) : Number(metadata?.width || 0),
    height: swapsAxes ? Number(metadata?.width || 0) : Number(metadata?.height || 0)
  };
}

function createSlidingRegions(total, cross, { maxTiles, overlapRatio }) {
  if (maxTiles < 2 || total <= cross) return [];
  const targetSpan = Math.min(total, Math.max(cross, Math.round(cross * 1.45)));
  const minimumCount = Math.ceil(total / targetSpan);
  const count = clamp(minimumCount, 2, maxTiles);
  const overlap = Math.round(targetSpan * overlapRatio);
  const span = Math.min(total, Math.ceil((total + ((count - 1) * overlap)) / count));
  const step = count === 1 ? 0 : (total - span) / (count - 1);
  return Array.from({ length: count }, (_, index) => {
    const start = index === count - 1 ? total - span : Math.round(index * step);
    return { start, span };
  });
}

export function planRecognitionImageVariants({ width, height, bytes = 0 } = {}, options = {}) {
  const config = { ...DEFAULTS, ...options };
  const safeWidth = Math.max(1, Math.floor(Number(width) || 0));
  const safeHeight = Math.max(1, Math.floor(Number(height) || 0));
  const pixelCount = safeWidth * safeHeight;
  const aspectRatio = Math.max(safeWidth / safeHeight, safeHeight / safeWidth);
  const needsDetail = aspectRatio >= config.longAspectRatio
    || pixelCount >= config.largePixelCount
    || Number(bytes) >= config.largeByteCount;
  const regions = [{
    id: "overview",
    role: "overview",
    left: 0,
    top: 0,
    width: safeWidth,
    height: safeHeight
  }];

  if (needsDetail) {
    const maxTiles = config.maxImages - 1;
    if (safeHeight >= safeWidth * config.longAspectRatio) {
      for (const [index, region] of createSlidingRegions(safeHeight, safeWidth, {
        maxTiles,
        overlapRatio: config.tileOverlapRatio
      }).entries()) {
        regions.push({
          id: `detail_${index + 1}`,
          role: "detail",
          left: 0,
          top: region.start,
          width: safeWidth,
          height: region.span
        });
      }
    } else if (safeWidth >= safeHeight * config.longAspectRatio) {
      for (const [index, region] of createSlidingRegions(safeWidth, safeHeight, {
        maxTiles,
        overlapRatio: config.tileOverlapRatio
      }).entries()) {
        regions.push({
          id: `detail_${index + 1}`,
          role: "detail",
          left: region.start,
          top: 0,
          width: region.span,
          height: safeHeight
        });
      }
    } else {
      const halfWidth = Math.ceil(safeWidth / 2);
      const halfHeight = Math.ceil(safeHeight / 2);
      const overlapX = Math.round(safeWidth * config.tileOverlapRatio / 2);
      const overlapY = Math.round(safeHeight * config.tileOverlapRatio / 2);
      const grid = [
        [0, 0, Math.min(safeWidth, halfWidth + overlapX), Math.min(safeHeight, halfHeight + overlapY)],
        [Math.max(0, halfWidth - overlapX), 0, safeWidth - Math.max(0, halfWidth - overlapX), Math.min(safeHeight, halfHeight + overlapY)],
        [0, Math.max(0, halfHeight - overlapY), Math.min(safeWidth, halfWidth + overlapX), safeHeight - Math.max(0, halfHeight - overlapY)],
        [Math.max(0, halfWidth - overlapX), Math.max(0, halfHeight - overlapY), safeWidth - Math.max(0, halfWidth - overlapX), safeHeight - Math.max(0, halfHeight - overlapY)]
      ];
      grid.slice(0, maxTiles).forEach(([left, top, regionWidth, regionHeight], index) => {
        regions.push({
          id: `detail_${index + 1}`,
          role: "detail",
          left,
          top,
          width: regionWidth,
          height: regionHeight
        });
      });
    }
  }

  return Object.freeze({
    version: RECOGNITION_FIRST_ACQUISITION_VERSION,
    width: safeWidth,
    height: safeHeight,
    bytes: Number(bytes) || 0,
    pixelCount,
    aspectRatio,
    needsDetail,
    asyncRecommended: needsDetail && (
      aspectRatio >= 2.4
      || pixelCount >= 6_000_000
      || Number(bytes) >= 1_500_000
    ),
    regions: Object.freeze(regions.map(region => Object.freeze(region)))
  });
}

async function renderVariant(buffer, region, config) {
  let pipeline = sharp(buffer, { failOn: "warning" }).rotate();
  if (region.role === "detail") {
    pipeline = pipeline.extract({
      left: region.left,
      top: region.top,
      width: region.width,
      height: region.height
    });
  }
  const maxDimension = region.role === "overview"
    ? config.overviewMaxDimension
    : config.tileMaxDimension;
  const result = await pipeline
    .resize({
      width: maxDimension,
      height: maxDimension,
      fit: "inside",
      withoutEnlargement: true
    })
    .flatten({ background: "#ffffff" })
    .jpeg({ quality: 90, chromaSubsampling: "4:4:4", mozjpeg: true })
    .toBuffer({ resolveWithObject: true });
  return Object.freeze({
    ...region,
    outputWidth: result.info.width,
    outputHeight: result.info.height,
    mimeType: "image/jpeg",
    bytes: result.data.length,
    dataUrl: `data:image/jpeg;base64,${result.data.toString("base64")}`
  });
}

export async function createRecognitionImageVariants({ buffer, bytes = buffer?.length || 0 } = {}, options = {}) {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    throw new TypeError("recognition_image_buffer_required");
  }
  const config = { ...DEFAULTS, ...options };
  const metadata = await sharp(buffer, { failOn: "warning" }).metadata();
  const dimensions = orientedDimensions(metadata);
  if (!dimensions.width || !dimensions.height) {
    throw new Error("recognition_image_dimensions_unavailable");
  }
  const plan = planRecognitionImageVariants({ ...dimensions, bytes }, config);
  const variants = [];
  for (const region of plan.regions) {
    variants.push(await renderVariant(buffer, region, config));
  }
  return Object.freeze({
    ...plan,
    sourceFormat: String(metadata.format || "unknown"),
    images: Object.freeze(variants)
  });
}

export function buildRecognitionFirstPromptPrefix(acquisition) {
  const detailCount = Math.max(0, Number(acquisition?.images?.length || 0) - 1);
  return `RECOGNITION-FIRST ACQUISITION ${RECOGNITION_FIRST_ACQUISITION_VERSION}\n`
    + `The attached images are one source page: image 1 is the whole-page overview and the next ${detailCount} image(s) are overlapping high-resolution detail tiles in source reading order.\n`
    + "Use the overview for page, table, group, header, and CRS context. Use detail tiles to transcribe every visible coordinate row.\n"
    + "Overlaps are duplicate visual evidence: emit each physical source row once, preserving its visible label, order, precision, group boundary, table header, and CRS text.\n"
    + "Emit each visible title or nested section title that owns coordinate rows as `HEADING | <verbatim visible title>` immediately before those rows; repeat a visible title when it is repeated in the source.\n"
    + "For DMS rows emit exactly `<visible point label or empty> | <complete first DMS field> | <complete second DMS field>` and keep unlabelled rows unlabelled.\n"
    + "Do not infer missing rows, labels, signs, axis order, CRS, zone, hemisphere, or geometry. Do not omit a readable row merely because a format-specific contract is not yet satisfied.\n"
    + "Acquisition is evidence collection only. Return all visible coordinate candidates and visible CRS evidence; downstream validation decides authority, Map, and KML.\n\n";
}

const CRS_EVIDENCE_PATTERN = /\b(?:EPSG\s*:?\s*\d{4,6}|WGS\s*[- ]?84|UTM(?:\s+zone)?\s*\d{1,2}\s*[NS]?|BFTM|ITRF(?:\s*\d{4})?|GAUSS[\s-]*KRUGER|MGRS|DATUM\s*:?\s*[\p{L}\d._/-]+|PROJECTION\s*:?\s*[^|\n]{1,80})\b/giu;

export function extractVisibleCrsEvidence(rawText) {
  const matches = [];
  const seen = new Set();
  for (const match of String(rawText || "").matchAll(CRS_EVIDENCE_PATTERN)) {
    const text = String(match[0] || "").trim();
    const key = text.toLocaleUpperCase();
    if (!text || seen.has(key)) continue;
    seen.add(key);
    matches.push(Object.freeze({ text, offset: match.index }));
  }
  return Object.freeze(matches);
}

const STRICT_NUMBER_SOURCE = String.raw`[+-]?\d+(?:[.,]\d+)?`;
const STRICT_DMS_SOURCE = String.raw`[-+]?\d{1,3}\s*[°º]\s*\d{1,2}\s*(?:['′\s]*)?\d{1,2}(?:[.,]\d+)?\s*(?:["″\s]*)?(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\b`;
const STRICT_DMS_PATTERN = new RegExp(STRICT_DMS_SOURCE, "giu");
const STRICT_DMS_SIGNAL_PATTERN = new RegExp(STRICT_DMS_SOURCE, "iu");
const STRICT_NUMBER_PATTERN = new RegExp(`^${STRICT_NUMBER_SOURCE}$`, "u");
const VERIFIED_COORDINATE_HEADER_PATTERN = /(?:\b(?:LAT(?:ITUDE)?|PARALLÈLE)\b[\s\S]*\b(?:LON(?:GITUDE)?|MÉRIDIEN)\b|\b(?:LON(?:GITUDE)?|MÉRIDIEN)\b[\s\S]*\b(?:LAT(?:ITUDE)?|PARALLÈLE)\b|\b(?:EASTING|X)\b[\s\S]*\b(?:NORTHING|Y)\b|\b(?:NORTHING|Y)\b[\s\S]*\b(?:EASTING|X)\b)/iu;

function isVerifiedCoordinateHeaderLine(line) {
  const text = String(line || "").trim();
  if (!text || !VERIFIED_COORDINATE_HEADER_PATTERN.test(text)) return false;
  if (STRICT_DMS_SIGNAL_PATTERN.test(text)) return false;
  const tokens = text
    .replace(/[|\t,;:/#()[\]{}._°º-]+/gu, " ")
    .trim()
    .split(/\s+/u)
    .filter(Boolean);
  const allowedHeaderToken = /^(?:NO|N|NUMBER|NUM|POINT|PT|VERTEX|SOMMET|ID|X|Y|EASTING|NORTHING|LAT|LATITUDE|LON|LONG|LONGITUDE|PARALLÈLE|MÉRIDIEN)$/iu;
  return tokens.length >= 2 && tokens.every(token => allowedHeaderToken.test(token));
}

function hasOneLatitudeAndOneLongitudeDms(matches) {
  if (matches.length !== 2) return false;
  const directions = matches.map(match => (
    String(match[0] || "").match(/(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\s*$/iu)?.[0] || ""
  ).toUpperCase());
  const latitudeCount = directions.filter(value => /^(?:N|S|NORTH|SOUTH|NORD|SUD)$/u.test(value)).length;
  const longitudeCount = directions.filter(value => /^(?:E|W|O|EAST|WEST|EST|OUEST)$/u.test(value)).length;
  return latitudeCount === 1 && longitudeCount === 1;
}

function isStrictNumericCoordinateRow(line, { headerBound = false } = {}) {
  let text = String(line || "").trim();
  if (!text) return false;
  text = text.replace(/^\s*(?:ROW|COORDINATE)\s*\|\s*/iu, "").trim();

  const keyedProjected = new RegExp(
    `^(?:(?:POINT|PT|VERTEX|SOMMET)\\s*)?[1-9]\\d{0,5}\\s+(?:X|EASTING)\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE}\\s+(?:Y|NORTHING)\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE}$`,
    "iu"
  );
  const keyedGeographic = new RegExp(
    `^(?:(?:(?:POINT|PT|VERTEX|SOMMET)\\s*)?[1-9]\\d{0,5}\\s+)?(?:LAT(?:ITUDE)?\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE}\\s+(?:LON(?:GITUDE)?|LONGITUDE)\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE}|(?:LON(?:GITUDE)?|LONGITUDE)\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE}\\s+LAT(?:ITUDE)?\\s*[:=]?\\s*${STRICT_NUMBER_SOURCE})$`,
    "iu"
  );
  if (keyedProjected.test(text) || keyedGeographic.test(text)) return true;

  if (text.includes("|") || text.includes("\t") || text.includes(";")) {
    const fields = text.split(/[|\t;]/u).map(value => value.trim()).filter(Boolean);
    if (fields.length === 2) return headerBound && fields.every(value => STRICT_NUMBER_PATTERN.test(value));
    const structuredTriplet = fields.length === 3
      && /^[1-9]\d{0,5}$/u.test(fields[0])
      && fields.slice(1).every(value => STRICT_NUMBER_PATTERN.test(value));
    return structuredTriplet && (headerBound || fields.slice(1).some(value => /[.,]|^[+-]/u.test(value)));
  }

  if (/^[+-]?\d+(?:\.\d+)?\s*,\s*[+-]?\d+(?:\.\d+)?$/u.test(text)) return true;
  if (!headerBound) return false;
  const fields = text.split(/\s+/u);
  if (fields.length === 2) return fields.every(value => STRICT_NUMBER_PATTERN.test(value));
  return fields.length === 3
    && /^[1-9]\d{0,5}$/u.test(fields[0])
    && fields.slice(1).every(value => STRICT_NUMBER_PATTERN.test(value));
}

function isFullyConsumedDmsCoordinateRow(line, options = {}) {
  const text = String(line || "").trim();
  const matches = [...text.matchAll(STRICT_DMS_PATTERN)];
  if (!hasOneLatitudeAndOneLongitudeDms(matches)) return false;
  let remainder = text;
  for (const match of [...matches].reverse()) {
    remainder = `${remainder.slice(0, match.index)} ${remainder.slice(match.index + match[0].length)}`;
  }
  remainder = remainder.replace(/^\s*(?:ROW|COORDINATE)\s*\|\s*/iu, "").trim();
  const structuralRemainder = remainder.replace(/[\s|:;,()\[\]{}-]+/gu, "");
  if (!structuralRemainder) return true;
  if (/^(?:(?:POINT|PT|VERTEX|SOMMET))?[1-9]\d{0,5}$/iu.test(structuralRemainder)) return true;
  return isStrictNumericCoordinateRow(remainder, { ...options, headerBound: true });
}

function isCoordinateCandidateLine(line, options = {}) {
  return isFullyConsumedDmsCoordinateRow(line, options)
    || isStrictNumericCoordinateRow(line, options)
    || /^\s*(?:(?:POINT|PT|VERTEX)\s*\|\s*)?(?:[1-9]\d{0,5}\s*\|\s*)?(?:[1-9]|[1-5]\d|60)\s*[C-HJ-NP-X]\s*[A-HJ-NP-Z]{2}(?:\s*\d{2,10}){1,2}\s*$/iu.test(String(line || ""));
}

export function buildRecognitionAcquisitionEvidence({ rawText, acquisition, providerResponseId = null } = {}) {
  const exactRawText = String(rawText || "");
  const candidateLines = [];
  let headerBound = false;
  exactRawText.split(/\r?\n/u).forEach((line, index) => {
    const text = String(line || "").trim();
    if (!text) return;
    if (isVerifiedCoordinateHeaderLine(text)) {
      headerBound = true;
      return;
    }
    if (isCoordinateCandidateLine(text, { headerBound })) {
      candidateLines.push({ lineNumber: index + 1, text: line });
      return;
    }
    if (!/^\s*(?:CONTEXT|GROUP|HEADING|SECTION|TITLE)\s*\|/iu.test(text)) headerBound = false;
  });
  return Object.freeze({
    version: RECOGNITION_FIRST_ACQUISITION_VERSION,
    status: candidateLines.length > 0
      ? "COMPLETED"
      : (exactRawText.trim() ? "NO_COORDINATE_EVIDENCE" : "EMPTY"),
    providerResponseId: providerResponseId ? String(providerResponseId) : null,
    rawProviderText: exactRawText,
    candidateCoordinateLines: Object.freeze(candidateLines.map(value => Object.freeze(value))),
    visibleCrsEvidence: extractVisibleCrsEvidence(exactRawText),
    imageEvidence: Object.freeze({
      sourceWidth: Number(acquisition?.width || 0),
      sourceHeight: Number(acquisition?.height || 0),
      sourceBytes: Number(acquisition?.bytes || 0),
      imageCount: Number(acquisition?.images?.length || 0),
      overviewCount: Number(acquisition?.images?.filter(image => image.role === "overview").length || 0),
      detailTileCount: Number(acquisition?.images?.filter(image => image.role === "detail").length || 0),
      asyncRecommended: acquisition?.asyncRecommended === true
    }),
    authority: "EVIDENCE_ONLY"
  });
}
