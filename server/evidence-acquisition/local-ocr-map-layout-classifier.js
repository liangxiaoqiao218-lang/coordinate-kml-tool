import { createHash } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "../recognition/coordinate-image-safety.js";
import {
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION
} from "./observation-schema.js";

export const LOCAL_OCR_STRUCTURED_LAYOUT_SOURCE_TYPE = "LOCAL_OCR_STRUCTURED_LAYOUT_V1";
export const LOCAL_OCR_MAP_LAYOUT_CLASSIFIER_VERSION = "local_ocr_map_layout_classifier_v2";
export const LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY = Symbol("LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY");

const TRUSTED_ATTESTOR = "SERVER_LAYOUT_CLASSIFIER_V2";
const ROLE_REGION = Object.freeze({
  MAP_SEARCH_BOX: "MAP_SEARCH_BOX_REGION",
  MAP_PLACE_DETAILS: "MAP_PLACE_DETAILS_REGION"
});

function text(value) {
  return String(value ?? "").trim();
}

function sha256(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}

function finiteBbox(value, imageIdentity) {
  if (!Array.isArray(value) || value.length !== 4) return null;
  const bbox = value.map(Number);
  if (!bbox.every(Number.isFinite)) return null;
  const [x1, y1, x2, y2] = bbox;
  if (x1 < 0 || y1 < 0 || x2 <= x1 || y2 <= y1
    || x2 > imageIdentity.width || y2 > imageIdentity.height) return null;
  return bbox;
}

function normalizeBbox(value = {}, imageIdentity) {
  if (Array.isArray(value)) return finiteBbox(value, imageIdentity);
  const direct = finiteBbox([
    value?.x0 ?? value?.left,
    value?.y0 ?? value?.top,
    value?.x1 ?? (Number(value?.left) + Number(value?.width)),
    value?.y1 ?? (Number(value?.top) + Number(value?.height))
  ], imageIdentity);
  return direct;
}

function flattenTesseractLines(data = {}) {
  const blockLines = (Array.isArray(data.blocks) ? data.blocks : []).flatMap(block => (
    (Array.isArray(block?.paragraphs) ? block.paragraphs : []).flatMap(paragraph => (
      Array.isArray(paragraph?.lines) ? paragraph.lines : []
    ))
  ));
  return blockLines.length > 0 ? blockLines : (Array.isArray(data.lines) ? data.lines : []);
}

function normalizeWords(line = {}, imageIdentity) {
  if (!Array.isArray(line?.words) || line.words.length === 0) return null;
  const words = line.words.map(word => {
    const bbox = normalizeBbox(word?.bbox || word, imageIdentity);
    const wordText = text(word?.text);
    const confidence = Number(word?.confidence ?? word?.conf ?? 0);
    if (!bbox || !wordText || !Number.isFinite(confidence)) return null;
    return Object.freeze({ text: wordText, bbox, confidence });
  });
  return words.some(word => !word) ? null : Object.freeze(words);
}

export function extractLocalOcrLayoutLines(result = {}, imageIdentity) {
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)) return Object.freeze([]);
  const lines = flattenTesseractLines(result?.data || result)
    .map((line, index) => {
      const bbox = normalizeBbox(line?.bbox || line, imageIdentity);
      const lineText = text(line?.text);
      const confidence = Number(line?.confidence ?? line?.conf ?? 0);
      if (!bbox || !lineText || !Number.isFinite(confidence)) return null;
      const words = normalizeWords(line, imageIdentity);
      return Object.freeze({
        text: lineText,
        bbox,
        confidence,
        words: words || Object.freeze([]),
        word_structure_valid: Boolean(words),
        local_line_index: index
      });
    })
    .filter(Boolean);
  return Object.freeze(lines);
}

function decimalToken(value) {
  const match = text(value).match(/^([+-]?)(\d+)(?:\.(\d+))?$/);
  if (!match) return null;
  const fraction = match[3] || "";
  const sign = match[1] === "-" ? -1n : 1n;
  return Object.freeze({
    source: `${match[1] === "-" ? "-" : ""}${BigInt(match[2]).toString()}${fraction ? `.${fraction}` : ""}`,
    scale: fraction.length,
    units: sign * BigInt(`${match[2]}${fraction}`)
  });
}

function extractDecimalPairs(value) {
  const pairs = [];
  const source = text(value).replace(/[，]/g, ",").replace(/[−–—]/g, "-");
  const pattern = /([+-]?\d{1,3}(?:\.\d{4,12})?)\s*,\s*([+-]?\d{1,3}(?:\.\d{4,12})?)/g;
  let match;
  while ((match = pattern.exec(source)) !== null) {
    const latitude = decimalToken(match[1]);
    const longitude = decimalToken(match[2]);
    if (!latitude || !longitude) continue;
    const lat = Number(latitude.source);
    const lon = Number(longitude.source);
    if (Number.isFinite(lat) && Number.isFinite(lon)
      && Math.abs(lat) <= 90 && Math.abs(lon) <= 180) {
      pairs.push(Object.freeze({ latitude, longitude }));
    }
  }
  return Object.freeze(pairs);
}

function flattenPoints(coordinateEngineV2 = {}) {
  return (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : []).flatMap((group, groupIndex) => (
    (Array.isArray(group?.points) ? group.points : []).map((point, pointIndex) => ({
      group_id: text(group?.group_id || `group_${groupIndex + 1}`),
      point_id: text(point?.label || pointIndex + 1),
      raw: text(point?.raw)
    }))
  ));
}

function pairIdentity(pair) {
  return pair ? `${pair.latitude.source},${pair.longitude.source}` : null;
}

function pow10(count) {
  return 10n ** BigInt(count);
}

function roundToScale(value, targetScale) {
  if (!value || targetScale > value.scale) return null;
  if (targetScale === value.scale) return value.units;
  const divisor = pow10(value.scale - targetScale);
  const absolute = value.units < 0n ? -value.units : value.units;
  const quotient = absolute / divisor;
  const remainder = absolute % divisor;
  const rounded = quotient + (remainder * 2n >= divisor ? 1n : 0n);
  return value.units < 0n ? -rounded : rounded;
}

function strictRoundingContains(lower, higher) {
  return Boolean(lower && higher && higher.scale > lower.scale
    && roundToScale(higher, lower.scale) === lower.units);
}

function pairIsStrictRoundingRelated(left, right) {
  return Boolean(left && right && (
    (strictRoundingContains(left.latitude, right.latitude)
      && strictRoundingContains(left.longitude, right.longitude))
    || (strictRoundingContains(right.latitude, left.latitude)
      && strictRoundingContains(right.longitude, left.longitude))
  ));
}

export function isLocalOcrMapLayoutCandidate({ coordinateEngineV2 = {} } = {}) {
  if (coordinateEngineV2.coordinate_type !== "decimal_latlon"
    || coordinateEngineV2.precision_mode !== "wgs84-table-coordinates") return false;
  const points = flattenPoints(coordinateEngineV2);
  if (points.length !== 2) return false;
  const pairs = points.map(point => extractDecimalPairs(point.raw));
  return pairs.every(value => value.length === 1)
    && pairIsStrictRoundingRelated(pairs[0][0], pairs[1][0]);
}

function lineGeometryIsPlausible(line, imageIdentity) {
  const [x1, y1, x2, y2] = line.bbox;
  const width = x2 - x1;
  const height = y2 - y1;
  return line.confidence >= 40
    && width >= imageIdentity.width * 0.12
    && height >= imageIdentity.height * 0.006
    && height <= imageIdentity.height * 0.12;
}

const OPEN_LOCATION_CODE_ALPHABET = "23456789CFGHJMPQRVWX";
const OPEN_LOCATION_CODE_PATTERN = /(?:^|[^23456789CFGHJMPQRVWX])([23456789CFGHJMPQRVWX]{8}\+[23456789CFGHJMPQRVWX]{2,7})(?:$|[^23456789CFGHJMPQRVWX])/i;

function isValidFullOpenLocationCode(value) {
  const code = text(value).toUpperCase();
  if (!/^[23456789CFGHJMPQRVWX]{8}\+[23456789CFGHJMPQRVWX]{2,7}$/.test(code)) return false;
  const firstLatitudeIndex = OPEN_LOCATION_CODE_ALPHABET.indexOf(code[0]);
  const firstLongitudeIndex = OPEN_LOCATION_CODE_ALPHABET.indexOf(code[1]);
  return firstLatitudeIndex >= 0 && firstLatitudeIndex < 9
    && firstLongitudeIndex >= 0 && firstLongitudeIndex < 18;
}

function hasTrailingSearchCloseControl(line, imageIdentity) {
  const source = text(line?.lineText).replace(/[，]/g, ",");
  const pair = /[+-]?\d{1,3}(?:\.\d{4,12})?\s*,\s*[+-]?\d{1,3}(?:\.\d{4,12})?/.exec(source);
  if (!pair || line?.wordStructureValid !== true || !Array.isArray(line?.words) || line.words.length < 2) return false;
  const prefix = source.slice(0, pair.index).trim();
  const suffix = source.slice(pair.index + pair[0].length).replace(/\s+/g, "");
  const compactLineText = source.replace(/\s+/g, "");
  const compactWordText = line.words.map(word => text(word?.text)).join("").replace(/\s+/g, "");
  const coordinateWordIndexes = line.words
    .map((word, index) => extractDecimalPairs(word?.text).length === 1 ? index : -1)
    .filter(index => index >= 0);
  if (coordinateWordIndexes.length !== 1 || coordinateWordIndexes[0] !== 0) return false;
  const coordinateWord = line.words[0];
  const coordinateWordPairs = extractDecimalPairs(coordinateWord?.text);
  if (coordinateWordPairs.length !== 1 || pairIdentity(coordinateWordPairs[0]) !== pairIdentity(line?.pair)) return false;
  const trailingWords = line.words.slice(1);
  if (trailingWords.length < 1 || trailingWords.length > 2) return false;
  const controlText = trailingWords.map(word => text(word?.text)).join("").replace(/\s+/g, "");
  const primaryControl = trailingWords[0];
  const [x1, y1, x2, y2] = primaryControl?.bbox || [];
  const [parentX1, parentY1, parentX2, parentY2] = line.bbox || [];
  const [coordinateX1, coordinateY1, coordinateX2, coordinateY2] = coordinateWord?.bbox || [];
  const primaryVerticalOverlap = Math.max(0, Math.min(y2, coordinateY2) - Math.max(y1, coordinateY1));
  const primaryMinHeight = Math.min(y2 - y1, coordinateY2 - coordinateY1);
  const everyControlWordValid = trailingWords.every((word, index) => {
    const [wordX1, wordY1, wordX2, wordY2] = word?.bbox || [];
    const minWidth = index === 0 ? imageIdentity.width * 0.01 : imageIdentity.width * 0.005;
    const minHeight = index === 0 ? imageIdentity.height * 0.008 : imageIdentity.height * 0.006;
    return Number(word?.confidence) >= 30
      && wordX1 >= parentX1 && wordY1 >= parentY1 && wordX2 <= parentX2 && wordY2 <= parentY2
      && wordX1 >= coordinateX2
      && (wordX2 - wordX1) >= minWidth
      && (wordY2 - wordY1) >= minHeight;
  });
  const tokenOrderValid = trailingWords.length === 1
    || trailingWords[1].bbox[0] >= trailingWords[0].bbox[2];
  return /^(?:[xX×✕✖]{1,2})[)\]}〉》]?$/.test(controlText)
    && prefix === ""
    && suffix === controlText
    && compactLineText === compactWordText
    && Number.isFinite(x1) && Number.isFinite(y1) && Number.isFinite(x2) && Number.isFinite(y2)
    && everyControlWordValid
    && tokenOrderValid
    && x1 >= imageIdentity.width * 0.75
    && (x2 - x1) <= imageIdentity.width * 0.12
    && (y2 - y1) <= imageIdentity.height * 0.08
    && primaryMinHeight > 0
    && primaryVerticalOverlap >= primaryMinHeight * 0.5
    && Number(coordinateWord?.confidence) >= 40
    && (coordinateX2 - coordinateX1) >= imageIdentity.width * 0.12
    && (coordinateY2 - coordinateY1) >= imageIdentity.height * 0.006
    && (coordinateY2 - coordinateY1) <= imageIdentity.height * 0.12
    && coordinateX1 >= parentX1 && coordinateY1 >= parentY1
    && coordinateX2 <= parentX2 && coordinateY2 <= parentY2;
}

function hasClosedDetailCoordinateWord(line, imageIdentity) {
  if (line?.wordStructureValid !== true || !Array.isArray(line?.words)
    || line.words.length < 1 || line.words.length > 2) return false;
  const combinedText = line.words.map(word => text(word?.text)).join("");
  const compactCombinedText = combinedText.replace(/\s+/g, "");
  if (!/^[+-]?\d{1,3}(?:\.\d{4,12})?,[+-]?\d{1,3}(?:\.\d{4,12})?$/.test(compactCombinedText)) return false;
  if (line.words.length === 2 && (
    !/^[+-]?\d{1,3}(?:\.\d{4,12})?,$/.test(text(line.words[0].text).replace(/\s+/g, ""))
    || !/^[+-]?\d{1,3}(?:\.\d{4,12})?$/.test(text(line.words[1].text).replace(/\s+/g, ""))
  )) return false;
  const pairs = extractDecimalPairs(combinedText);
  if (pairs.length !== 1 || pairIdentity(pairs[0]) !== pairIdentity(line?.pair)) return false;
  const [parentX1, parentY1, parentX2, parentY2] = line.bbox || [];
  const first = line.words[0];
  const last = line.words[line.words.length - 1];
  const combinedWidth = last.bbox[2] - first.bbox[0];
  const wordsAreVisible = line.words.every(word => {
    const [x1, y1, x2, y2] = word.bbox || [];
    return Number(word.confidence) >= 40
      && x1 >= parentX1 && y1 >= parentY1 && x2 <= parentX2 && y2 <= parentY2
      && (x2 - x1) >= imageIdentity.width * 0.06
      && (y2 - y1) >= imageIdentity.height * 0.006
      && (y2 - y1) <= imageIdentity.height * 0.12;
  });
  const wordOrderValid = line.words.length === 1 || line.words[1].bbox[0] >= line.words[0].bbox[2];
  const horizontalGap = line.words.length === 1 ? 0 : line.words[1].bbox[0] - line.words[0].bbox[2];
  const verticalOverlap = line.words.length === 1 ? 1 : Math.max(0,
    Math.min(line.words[0].bbox[3], line.words[1].bbox[3])
      - Math.max(line.words[0].bbox[1], line.words[1].bbox[1]));
  const minimumWordHeight = line.words.length === 1 ? 1 : Math.min(
    line.words[0].bbox[3] - line.words[0].bbox[1],
    line.words[1].bbox[3] - line.words[1].bbox[1]
  );
  return text(line.lineText).replace(/\s+/g, "") === combinedText.replace(/\s+/g, "")
    && wordsAreVisible
    && wordOrderValid
    && horizontalGap <= Math.max(imageIdentity.width * 0.05, minimumWordHeight * 2)
    && combinedWidth >= imageIdentity.width * 0.12
    && verticalOverlap >= minimumWordHeight * 0.5;
}

function findMapUiAnchors(lines, coordinateLines, imageIdentity) {
  const plusCodeLines = lines.map((line, index) => {
    const bbox = finiteBbox(line?.bbox, imageIdentity);
    const lineText = text(line?.text);
    const confidence = Number(line?.confidence ?? 0);
    const match = OPEN_LOCATION_CODE_PATTERN.exec(lineText);
    const words = normalizeWords(line, imageIdentity);
    const width = bbox ? bbox[2] - bbox[0] : 0;
    const height = bbox ? bbox[3] - bbox[1] : 0;
    const codeWordWidth = words ? words[0].bbox[2] - words[0].bbox[0] : 0;
    const codeWordHeight = words ? words[0].bbox[3] - words[0].bbox[1] : 0;
    const helpWord = words?.[1] || null;
    if (!bbox || !match || !isValidFullOpenLocationCode(match[1])
      || !words || words.length > 2
      || text(words[0]?.text).toUpperCase() !== match[1].toUpperCase()
      || Number(words[0]?.confidence) < 45
      || codeWordWidth < imageIdentity.width * 0.12
      || codeWordHeight < imageIdentity.height * 0.006
      || codeWordHeight > imageIdentity.height * 0.12
      || lineText.replace(/\s+/g, "") !== words.map(word => text(word?.text)).join("").replace(/\s+/g, "")
      || (words.length === 2 && !/^\((?:\?|7)\)$|^\?$/.test(text(words[1]?.text)))
      || (helpWord && (Number(helpWord.confidence) < 30
        || (helpWord.bbox[2] - helpWord.bbox[0]) < imageIdentity.width * 0.005
        || (helpWord.bbox[3] - helpWord.bbox[1]) < imageIdentity.height * 0.006))
      || words.some(word => word.bbox[0] < bbox[0] || word.bbox[1] < bbox[1]
        || word.bbox[2] > bbox[2] || word.bbox[3] > bbox[3])
      || (words.length === 2 && words[1].bbox[0] < words[0].bbox[2])
      || width < imageIdentity.width * 0.12
      || height < imageIdentity.height * 0.006
      || height > imageIdentity.height * 0.12
      || !Number.isFinite(confidence) || confidence < 45) return null;
    return Object.freeze({ bbox, confidence, lineText, openLocationCode: match[1].toUpperCase(), index });
  }).filter(Boolean);
  if (plusCodeLines.length !== 1) return null;

  const searchCandidates = coordinateLines.filter(line => {
    const [x1, y1, x2, y2] = line.bbox;
    const centerY = (y1 + y2) / 2;
    return centerY <= imageIdentity.height * 0.33
      && (x2 - x1) >= imageIdentity.width * 0.55
      && x2 >= imageIdentity.width * 0.8
      && hasTrailingSearchCloseControl(line, imageIdentity);
  });
  if (searchCandidates.length !== 1) return null;

  const plusCode = plusCodeLines[0];
  const detailCandidates = coordinateLines.filter(line => {
    const [x1, y1, x2, y2] = line.bbox;
    const centerY = (y1 + y2) / 2;
    const verticalGap = y1 - plusCode.bbox[3];
    return centerY >= imageIdentity.height * 0.62
      && hasClosedDetailCoordinateWord(line, imageIdentity)
      && verticalGap >= 0
      && verticalGap <= imageIdentity.height * 0.12
      && Math.abs(x1 - plusCode.bbox[0]) <= imageIdentity.width * 0.15
      && x2 <= plusCode.bbox[2] + imageIdentity.width * 0.05;
  });
  if (detailCandidates.length !== 1 || detailCandidates[0].index === searchCandidates[0].index) return null;
  return Object.freeze({
    searchLineIndex: searchCandidates[0].index,
    detailLineIndex: detailCandidates[0].index,
    openLocationCodeLineIndex: plusCode.index
  });
}

export function createLocalOcrMapLayoutRows({
  lines = [],
  imageIdentity,
  coordinateEngineV2 = {},
  resultRevision = 1
} = {}) {
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)
    || !isLocalOcrMapLayoutCandidate({ coordinateEngineV2 })
    || !Number.isSafeInteger(resultRevision)
    || resultRevision <= 0
    || !Array.isArray(lines)) return null;

  const points = flattenPoints(coordinateEngineV2);
  const pointPairs = points.map(point => extractDecimalPairs(point.raw)[0]);
  const coordinateLines = lines.map((line, index) => {
    const bbox = finiteBbox(line?.bbox, imageIdentity);
    const lineText = text(line?.text);
    const confidence = Number(line?.confidence ?? 0);
    const pairs = extractDecimalPairs(lineText);
    if (!bbox || pairs.length !== 1) return null;
    const rawWords = Array.isArray(line?.words) ? line.words : [];
    const words = rawWords.map(word => ({
      text: text(word?.text),
      bbox: finiteBbox(word?.bbox, imageIdentity),
      confidence: Number(word?.confidence ?? 0)
    }));
    const wordStructureValid = line?.word_structure_valid !== false
      && rawWords.length > 0
      && words.length === rawWords.length
      && words.every(word => word.text && word.bbox && Number.isFinite(word.confidence));
    return Object.freeze({ lineText, bbox, confidence, pair: pairs[0], words, wordStructureValid, index });
  }).filter(Boolean);
  if (coordinateLines.length !== 2) return null;
  const mapUiAnchors = findMapUiAnchors(lines, coordinateLines, imageIdentity);
  if (!mapUiAnchors) return null;

  const matched = points.map((point, pointIndex) => {
    const identity = pairIdentity(pointPairs[pointIndex]);
    const candidates = coordinateLines.filter(line => pairIdentity(line.pair) === identity);
    return candidates.length === 1 ? { point, line: candidates[0] } : null;
  });
  if (matched.some(value => !value)
    || new Set(matched.map(value => value.line.index)).size !== 2
    || matched.some(value => !lineGeometryIsPlausible(value.line, imageIdentity))) return null;

  const classified = matched.map(value => ({
    ...value,
    role: value.line.index === mapUiAnchors.searchLineIndex
      ? "MAP_SEARCH_BOX"
      : (value.line.index === mapUiAnchors.detailLineIndex ? "MAP_PLACE_DETAILS" : null)
  }));
  if (classified.some(value => !value.role)
    || new Set(classified.map(value => value.role)).size !== 2) return null;
  const search = classified.find(value => value.role === "MAP_SEARCH_BOX");
  const details = classified.find(value => value.role === "MAP_PLACE_DETAILS");
  if (!search || !details
    || (details.line.bbox[1] - search.line.bbox[3]) < imageIdentity.height * 0.25) return null;

  const rows = classified.map(({ point, line, role }) => {
    const lineIdentity = sha256(JSON.stringify({
      image_sha256: imageIdentity.image_sha256,
      bbox: line.bbox,
      observed_text_sha256: sha256(line.lineText),
      point: point.raw,
      result_revision: resultRevision
    }));
    const row = {
      text: point.raw,
      bbox: line.bbox,
      coordinate_space: ORIGINAL_IMAGE_PIXEL_SPACE,
      source: "localOcrStructuredLayout",
      source_type: LOCAL_OCR_STRUCTURED_LAYOUT_SOURCE_TYPE,
      source_ref: `local_ocr_${lineIdentity.slice(0, 24)}`,
      source_line_id: `local_ocr_line_${lineIdentity.slice(0, 24)}`,
      source_role: role,
      source_region_id: ROLE_REGION[role],
      provenance_trust: "SERVER_ATTESTED",
      provenance_attestor: TRUSTED_ATTESTOR,
      group_id: point.group_id,
      point_id: point.point_id,
      image_sha256: imageIdentity.image_sha256,
      request_asset_id: imageIdentity.request_asset_id,
      image_id: imageIdentity.image_id,
      page: imageIdentity.page,
      image_width: imageIdentity.width,
      image_height: imageIdentity.height,
      local_ocr_classifier_version: LOCAL_OCR_MAP_LAYOUT_CLASSIFIER_VERSION
    };
    Object.defineProperty(row, SERVER_PROVENANCE_ATTESTATION, { value: true, enumerable: false });
    Object.defineProperty(row, LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY, { value: true, enumerable: false });
    return Object.freeze(row);
  });
  return Object.freeze(rows);
}

export function hasLocalOcrStructuredLayoutCapability(value) {
  return value?.[LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY] === true;
}
