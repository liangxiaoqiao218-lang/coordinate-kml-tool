import sharp from "sharp";
import {
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS
} from "../coordinate-finalizer/reason-codes.js";
import {
  buildRecognitionAcquisitionLogSummary,
  extractRecognitionCandidateEvidence
} from "./recognition-candidate-evidence.js";
import {
  extractProviderProjectedCoordinateEvidence,
  LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS
} from "../evidence-acquisition/local-ocr-map-layout-classifier.js";

export { buildRecognitionAcquisitionLogSummary };

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

function buildLegacyRecognitionAcquisitionEvidence({ rawText, acquisition, providerResponseId = null } = {}) {
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

export function buildRecognitionAcquisitionEvidence({ rawText, acquisition, providerResponseId = null } = {}) {
  const exactRawText = String(rawText || "");
  const visibleCrsEvidence = extractVisibleCrsEvidence(exactRawText);
  const candidates = extractRecognitionCandidateEvidence({
    rawText: exactRawText,
    visibleCrsEvidence
  });
  const projectedCoordinateEvidence = candidates.candidateCoordinates.some(candidate => candidate?.format === "PROJECTED_XY")
    ? extractProviderProjectedCoordinateEvidence({ sourceText: exactRawText, minimumRows: 1 })
    : null;
  const acquisitionStatus = candidates.candidateCoordinates.length > 0
    ? "COMPLETED"
    : (exactRawText.trim() ? "NO_COORDINATE_EVIDENCE" : "EMPTY");
  return Object.freeze({
    version: RECOGNITION_FIRST_ACQUISITION_VERSION,
    providerCompletionState: exactRawText.trim() || providerResponseId ? "SUCCEEDED" : "EMPTY",
    acquisitionStatus,
    normalizationStatus: candidates.candidateCoordinates.length === 0
      ? "NOT_AVAILABLE"
      : candidates.rejectedRows.length > 0 ? "PARTIAL" : "COMPLETED",
    authorizationStatus: candidates.candidateCoordinates.length > 0 ? "REVIEW_REQUIRED" : "NOT_ESTABLISHED",
    status: acquisitionStatus,
    providerResponseId: providerResponseId ? String(providerResponseId) : null,
    rawProviderText: exactRawText,
    candidateCoordinateLines: candidates.candidateCoordinateLines,
    candidateCoordinates: candidates.candidateCoordinates,
    candidateCoordinateGroups: candidates.candidateCoordinateGroups,
    unboundCandidates: candidates.unboundCandidates,
    rejectedRows: candidates.rejectedRows,
    visibleCrsEvidence,
    projectedCoordinateEvidence,
    reviewReasons: candidates.reviewReasons,
    diagnostics: Object.freeze({
      ...candidates.diagnostics,
      providerOutputLength: exactRawText.length
    }),
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

function hasCompleteImageAcquisitionEvidence(imageEvidence = null) {
  if (!imageEvidence || typeof imageEvidence !== "object") return false;
  const imageCount = Number(imageEvidence.imageCount || 0);
  const overviewCount = Number(imageEvidence.overviewCount || 0);
  const detailTileCount = Number(imageEvidence.detailTileCount || 0);
  return Number(imageEvidence.sourceWidth || 0) > 0
    && Number(imageEvidence.sourceHeight || 0) > 0
    && Number(imageEvidence.sourceBytes || 0) > 0
    && imageCount > 0
    && overviewCount === 1
    && detailTileCount === imageCount - overviewCount;
}

export function evaluateProjectedCoordinateAuthorizationEvidence(evidence = null) {
  const candidates = Array.isArray(evidence?.candidateCoordinates)
    ? evidence.candidateCoordinates
    : [];
  const applicable = candidates.length > 0
    && candidates.every(candidate => String(candidate?.format || "") === "PROJECTED_XY");
  if (!applicable) {
    return Object.freeze({ applicable: false, eligible: false, reasons: Object.freeze([]) });
  }

  const reasons = new Set();
  const diagnostics = evidence?.diagnostics || {};
  const groups = Array.isArray(evidence?.candidateCoordinateGroups)
    ? evidence.candidateCoordinateGroups
    : [];
  const projectedEvidence = evidence?.projectedCoordinateEvidence;
  const axisOrders = new Set(candidates.map(candidate => String(candidate?.axisOrder || "")));
  const supportedAxisOrders = new Set(["x_y", "y_x"]);
  const crsEvidence = projectedEvidence?.crsEvidence;
  const explicitSupportedCrs = String(crsEvidence?.status || "").toUpperCase() === "EXPLICIT"
    && Boolean(String(crsEvidence?.projection || "").trim());

  if (String(evidence?.providerCompletionState || "") !== "SUCCEEDED") reasons.add("PROJECTED_PROVIDER_NOT_COMPLETED");
  if (String(evidence?.acquisitionStatus || "") !== "COMPLETED") reasons.add("PROJECTED_ACQUISITION_INCOMPLETE");
  if (String(evidence?.normalizationStatus || "") !== "COMPLETED") reasons.add("PROJECTED_NORMALIZATION_INCOMPLETE");
  if (groups.length !== 1 || Number(diagnostics.candidateGroupCount || 0) !== 1) reasons.add("PROJECTED_GROUP_BOUNDARY_NOT_UNIQUE");
  if (Number(diagnostics.boundRowCount || 0) !== candidates.length
    || Number(diagnostics.unboundRowCount || 0) !== 0
    || Number(diagnostics.rejectedRowCount || 0) !== 0) reasons.add("PROJECTED_ROWS_NOT_FULLY_BOUND");
  if (axisOrders.size !== 1 || !supportedAxisOrders.has([...axisOrders][0])) reasons.add("PROJECTED_AXIS_ORDER_UNRESOLVED");
  if (!Array.isArray(evidence?.visibleCrsEvidence) || evidence.visibleCrsEvidence.length === 0
    || projectedEvidence?.status !== LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE
    || !explicitSupportedCrs) reasons.add("PROJECTED_CRS_UNRESOLVED");
  if (Number(projectedEvidence?.rowCount || 0) !== candidates.length) reasons.add("PROJECTED_EVIDENCE_ROW_COUNT_MISMATCH");
  if (!hasCompleteImageAcquisitionEvidence(evidence?.imageEvidence)) reasons.add("PROJECTED_IMAGE_EVIDENCE_INCOMPLETE");
  if (Array.isArray(evidence?.reviewReasons) && evidence.reviewReasons.length > 0) reasons.add("PROJECTED_EVIDENCE_REVIEW_REQUIRED");

  return Object.freeze({
    applicable: true,
    eligible: reasons.size === 0,
    reasons: Object.freeze([...reasons]),
    axisOrder: reasons.has("PROJECTED_AXIS_ORDER_UNRESOLVED") ? null : [...axisOrders][0],
    crsEvidence: explicitSupportedCrs ? crsEvidence : null
  });
}

export function createRecognitionAcquisitionEvidenceStore({
  buildEvidence = buildRecognitionAcquisitionEvidence
} = {}) {
  let evidence = null;
  let buildCount = 0;
  return Object.freeze({
    getOrBuild(input) {
      if (!evidence) {
        evidence = buildEvidence(input);
        buildCount += 1;
      }
      return evidence;
    },
    peek() {
      return evidence;
    },
    getBuildCount() {
      return buildCount;
    }
  });
}

export function evaluateUnifiedRecognitionFinalAuthorization({
  body = {},
  evidence = body?.recognitionAcquisition,
  decision = null,
  conformance = body?.acquisitionContractConformance,
  providerCallCount = body?.providerCallCount
} = {}) {
  const finalized = body?.finalizedCoordinateResult || {};
  const evidenceAcquisitionCompleted = String(evidence?.acquisitionStatus || "").toUpperCase() === "COMPLETED";
  const decisionAcquisitionCompleted = !decision
    || String(decision?.acquisitionStatus || "").toUpperCase() === "COMPLETED";
  const hasUnifiedEvidence = Boolean(evidence && typeof evidence === "object"
    && evidenceAcquisitionCompleted
    && decisionAcquisitionCompleted
    && Array.isArray(evidence.candidateCoordinates)
    && evidence.candidateCoordinates.length > 0
    && Array.isArray(evidence.candidateCoordinateGroups)
    && evidence.candidateCoordinateGroups.length > 0
    && Array.isArray(evidence.visibleCrsEvidence)
    && evidence.imageEvidence && typeof evidence.imageEvidence === "object");
  const projectedEvidenceAuthorization = evaluateProjectedCoordinateAuthorizationEvidence(evidence);
  const projectedDecisionEligible = projectedEvidenceAuthorization.applicable
    && projectedEvidenceAuthorization.eligible
    && decision?.projectedAuthorizationEligible === true;
  const contractRequiresReview = String(conformance?.status || "").toUpperCase() === "REVIEW_REQUIRED"
    && !projectedDecisionEligible;
  const unifiedDecisionRequiresReview = decision?.authorizationStatus === "REVIEW_REQUIRED"
    || decision?.resultStatus === "needs_review";
  const acquisitionIncomplete = !evidenceAcquisitionCompleted || !decisionAcquisitionCompleted;
  const projectedFinalRequirementsFailed = projectedEvidenceAuthorization.applicable && (
    !projectedDecisionEligible
    || Number(providerCallCount || 0) !== 1
    || String(body?.coordinateEngineV2?.source_crs?.id || "").trim() === ""
    || !["easting_northing", "northing_easting"].includes(String(body?.coordinateEngineV2?.source_crs?.axisOrder || ""))
  );
  const finalRequiresReview = body?.requiresReview === true
    || finalized.requiresReview === true
    || finalized.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED
    || body?.authorizationStatus === "REVIEW_REQUIRED"
    || body?.resultStatus === "needs_review"
    || contractRequiresReview
    || unifiedDecisionRequiresReview
    || acquisitionIncomplete
    || projectedFinalRequirementsFailed
    || !hasUnifiedEvidence;
  const finalizerGatePassed = finalized.decisionState === COORDINATE_DECISION_STATE.AUTO_EXPORT
    && finalized.qualityGateStatus === COORDINATE_QUALITY_GATE_STATUS.PASSED
    && finalized.requiresReview === false
    && finalized.kmlReady === true
    && Boolean(finalized.geometry)
    && Boolean(finalized.crs);
  const mapGatePassed = body?.mapReady !== false && finalized.mapReady !== false;
  const kmlGatePassed = body?.kmlReady !== false && finalized.kmlReady === true;
  const authorized = !finalRequiresReview
    && finalizerGatePassed
    && mapGatePassed
    && kmlGatePassed;
  const finalAuthorizationReasons = [...new Set([
    ...projectedEvidenceAuthorization.reasons,
    ...(projectedEvidenceAuthorization.applicable && Number(providerCallCount || 0) !== 1
      ? ["PROJECTED_PROVIDER_CALL_COUNT_INVALID"] : []),
    ...(projectedEvidenceAuthorization.applicable
      && String(body?.coordinateEngineV2?.source_crs?.id || "").trim() === ""
      ? ["PROJECTED_CRS_UNRESOLVED"] : []),
    ...(projectedEvidenceAuthorization.applicable
      && !["easting_northing", "northing_easting"].includes(String(body?.coordinateEngineV2?.source_crs?.axisOrder || ""))
      ? ["PROJECTED_AXIS_ORDER_UNRESOLVED"] : []),
    ...(projectedEvidenceAuthorization.applicable && !finalizerGatePassed
      ? ["PROJECTED_FINALIZER_GEOMETRY_CRS_VALIDATION_FAILED"] : []),
    ...(contractRequiresReview ? ["ACQUISITION_CONTRACT_NOT_CONFORMANT"] : []),
    ...(acquisitionIncomplete ? ["UNIFIED_RECOGNITION_ACQUISITION_INCOMPLETE"] : []),
    ...(!hasUnifiedEvidence ? ["UNIFIED_RECOGNITION_EVIDENCE_INCOMPLETE"] : [])
  ])];
  return Object.freeze({
    authorized,
    finalRequiresReview,
    hasUnifiedEvidence,
    acquisitionIncomplete,
    contractRequiresReview,
    unifiedDecisionRequiresReview,
    projectedEvidenceAuthorization,
    projectedFinalRequirementsFailed,
    finalAuthorizationReasons: Object.freeze(finalAuthorizationReasons),
    finalizerGatePassed,
    mapGatePassed,
    kmlGatePassed,
    mapReady: authorized && mapGatePassed,
    kmlReady: authorized && kmlGatePassed
  });
}

const AUTHORIZATION_ELIGIBLE_FORMATS = Object.freeze(new Set([
  "DMS",
  "WGS84_DECIMAL",
  "PROJECTED_XY"
]));

export function evaluateUnifiedRecognitionAcquisition({
  evidence,
  contractStatus = "UNKNOWN",
  contractReason = null
} = {}) {
  const acquisitionStatus = String(evidence?.acquisitionStatus || "EMPTY");
  const providerCompletionState = String(evidence?.providerCompletionState || "UNKNOWN");
  const candidates = Array.isArray(evidence?.candidateCoordinates)
    ? evidence.candidateCoordinates
    : [];
  const reviewReasons = Array.isArray(evidence?.reviewReasons)
    ? evidence.reviewReasons.map(reason => String(reason || "").trim()).filter(Boolean)
    : [];
  const normalizedContractStatus = String(contractStatus || "UNKNOWN");
  const normalizedContractReason = String(contractReason || "").trim();
  const projectedAuthorizationEvidence = evaluateProjectedCoordinateAuthorizationEvidence(evidence);
  const projectedAuthorizationEligible = projectedAuthorizationEvidence.eligible
    && (normalizedContractStatus === "CONFORMANT" || normalizedContractReason === "GENERIC_REVIEW_ONLY");
  const contractConformant = normalizedContractStatus === "CONFORMANT"
    || projectedAuthorizationEligible;
  const contractReasonSet = new Set([
    ...(contractConformant ? [] : [normalizedContractReason]),
    ...projectedAuthorizationEvidence.reasons,
    ...reviewReasons
  ].filter(Boolean));
  const completed = providerCompletionState === "SUCCEEDED"
    && acquisitionStatus === "COMPLETED"
    && candidates.length > 0;
  const normalizationComplete = evidence?.normalizationStatus === "COMPLETED";
  const allRowsBound = Number(evidence?.diagnostics?.unboundRowCount || 0) === 0;
  const candidateGroupCount = Number(evidence?.diagnostics?.candidateGroupCount || 0);
  const hasUniqueGroups = projectedAuthorizationEvidence.applicable
    ? candidateGroupCount === 1
    : candidateGroupCount > 0;
  const formatsEligible = candidates.length > 0
    && candidates.every(candidate => AUTHORIZATION_ELIGIBLE_FORMATS.has(String(candidate?.format || "")))
    && (!projectedAuthorizationEvidence.applicable || projectedAuthorizationEvidence.eligible);
  if (completed && !contractConformant) contractReasonSet.add("ACQUISITION_CONTRACT_NOT_CONFORMANT");
  if (completed && !normalizationComplete) contractReasonSet.add("CANDIDATE_NORMALIZATION_INCOMPLETE");
  if (completed && !allRowsBound) contractReasonSet.add("COORDINATE_ROW_UNBOUND");
  if (completed && !hasUniqueGroups) contractReasonSet.add("GROUP_BOUNDARY_NOT_ESTABLISHED");
  if (completed && !formatsEligible && !projectedAuthorizationEvidence.applicable) {
    contractReasonSet.add("COORDINATE_FORMAT_REQUIRES_VALIDATION");
  }
  const contractReasons = [...contractReasonSet];
  const mayProceedToGeometryValidation = completed
    && contractConformant
    && normalizationComplete
    && allRowsBound
    && hasUniqueGroups
    && formatsEligible
    && reviewReasons.length === 0;

  if (!completed) {
    return Object.freeze({
      providerCompletionState,
      acquisitionStatus,
      authorizationStatus: "NOT_ESTABLISHED",
      resultStatus: "failed",
      finalState: "FAILED_NO_COORDINATE_EVIDENCE",
      mapStatus: "CLOSED",
      kmlStatus: "CLOSED",
      contractReasons: Object.freeze(contractReasons),
      mayProceedToGeometryValidation: false,
      shouldReturnReview: false,
      shouldReturnFailure: true,
      projectedAuthorizationEligible
    });
  }

  if (!mayProceedToGeometryValidation) {
    return Object.freeze({
      providerCompletionState,
      acquisitionStatus: "COMPLETED",
      authorizationStatus: "REVIEW_REQUIRED",
      resultStatus: "needs_review",
      finalState: "COMPLETED_REVIEW_REQUIRED",
      mapStatus: "CLOSED",
      kmlStatus: "CLOSED",
      contractReasons: Object.freeze(contractReasons),
      mayProceedToGeometryValidation: false,
      shouldReturnReview: true,
      shouldReturnFailure: false,
      projectedAuthorizationEligible
    });
  }

  return Object.freeze({
    providerCompletionState,
    acquisitionStatus: "COMPLETED",
    authorizationStatus: "VALIDATION_PENDING",
    resultStatus: "validation_pending",
    finalState: "VALIDATION_PENDING",
    mapStatus: "CLOSED",
    kmlStatus: "CLOSED",
    contractReasons: Object.freeze(contractReasons),
    mayProceedToGeometryValidation: true,
    shouldReturnReview: false,
    shouldReturnFailure: false,
    projectedAuthorizationEligible
  });
}
