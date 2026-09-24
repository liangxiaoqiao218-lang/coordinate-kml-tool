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

function isCoordinateCandidateLine(line) {
  const text = String(line || "").trim();
  if (!text) return false;
  const numericTokens = text.match(/[+-]?\d+(?:[.,]\d+)?/g) || [];
  const hasCoordinateMarks = /[°º'"′″]|\b[NSWE]\b|\b(?:LAT(?:ITUDE)?|LON(?:GITUDE)?|EASTING|NORTHING|POINT|SOMMET|X|Y)\b/iu.test(text);
  return numericTokens.length >= 2 && (hasCoordinateMarks || numericTokens.length >= 3);
}

export function buildRecognitionAcquisitionEvidence({ rawText, acquisition, providerResponseId = null } = {}) {
  const exactRawText = String(rawText || "");
  const candidateLines = exactRawText
    .split(/\r?\n/u)
    .map((line, index) => ({ lineNumber: index + 1, text: line }))
    .filter(({ text }) => isCoordinateCandidateLine(text));
  return Object.freeze({
    version: RECOGNITION_FIRST_ACQUISITION_VERSION,
    status: exactRawText.trim() ? "COMPLETED" : "EMPTY",
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
