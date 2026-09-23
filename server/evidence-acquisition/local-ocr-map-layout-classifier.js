import { createHash } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "../recognition/coordinate-image-safety.js";
import {
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION
} from "./observation-schema.js";

export const LOCAL_OCR_STRUCTURED_LAYOUT_SOURCE_TYPE = "LOCAL_OCR_STRUCTURED_LAYOUT_V1";
export const LOCAL_OCR_MAP_LAYOUT_CLASSIFIER_VERSION = "local_ocr_map_layout_classifier_v2";
export const LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY = Symbol("LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY");

export const LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  INCOMPLETE: "INCOMPLETE"
});

export const LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON = Object.freeze({
  COMPLETE_COVERAGE: "COMPLETE_COVERAGE",
  TRUSTED_DECIMAL_ROWS_ONLY: "TRUSTED_DECIMAL_ROWS_ONLY",
  TRUSTED_PROJECTED_ROWS_ONLY: "TRUSTED_PROJECTED_ROWS_ONLY",
  SOURCE_LAYOUT_COVERAGE_MISMATCH: "SOURCE_LAYOUT_COVERAGE_MISMATCH",
  INVALID_READING_ORDER: "INVALID_READING_ORDER",
  INVALID_LINE_REGION: "INVALID_LINE_REGION",
  UNTRUSTED_WORD_STRUCTURE: "UNTRUSTED_WORD_STRUCTURE",
  WORD_TEXT_MISMATCH: "WORD_TEXT_MISMATCH"
});

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

const STRUCTURED_FIELD_DEFINITIONS = Object.freeze([
  Object.freeze({ kind: "label", phrases: Object.freeze(["point", "no", "no.", "number", "row", "id", "label", "name"]) }),
  Object.freeze({ kind: "longitude", phrases: Object.freeze(["longitude", "longitude dms", "lon"]) }),
  Object.freeze({ kind: "latitude", phrases: Object.freeze(["latitude", "latitude dms", "lat"]) }),
  Object.freeze({ kind: "easting", phrases: Object.freeze(["easting", "x"]) }),
  Object.freeze({ kind: "northing", phrases: Object.freeze(["northing", "y"]) })
]);

function normalizedWordText(value) {
  return text(value).replace(/[：]/gu, ":").replace(/\s+/gu, " ").toLowerCase();
}

function finiteWordBox(word) {
  const box = Array.isArray(word?.bbox) ? word.bbox.map(Number) : [];
  return box.length === 4 && box.every(Number.isFinite) && box[2] > box[0] && box[3] > box[1]
    ? box
    : null;
}

function finiteLineBox(line) {
  const box = Array.isArray(line?.bbox) ? line.bbox.map(Number) : [];
  return box.length === 4 && box.every(Number.isFinite) && box[2] > box[0] && box[3] > box[1]
    ? box
    : null;
}

function boxesOverlap(left, right) {
  return Math.max(left[0], right[0]) < Math.min(left[2], right[2])
    && Math.max(left[1], right[1]) < Math.min(left[3], right[3]);
}

function normalizedCoverageText(value) {
  return text(value).replace(/\s+/gu, " ");
}

function canonicalCoverageText(value) {
  return normalizedCoverageText(value)
    .replace(/\s*([|:;,/°º'′’"″”=+()])\s*/gu, "$1");
}

function canonicalCoverageStream(values = []) {
  return canonicalCoverageText(values.map(value => normalizedCoverageText(value)).filter(Boolean).join(" "));
}

function boundedCount(value) {
  return Math.max(0, Math.min(256, Number(value) || 0));
}

function incompleteStructuredEvidence(reason, sourceLineCount, layoutLineCount) {
  return Object.freeze({
    text: "",
    layoutLines: Object.freeze([]),
    status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
    reason,
    sourceLineCount: boundedCount(sourceLineCount),
    layoutLineCount: boundedCount(layoutLineCount)
  });
}

function trustedLineWords(line) {
  if (line?.word_structure_valid !== true || !Array.isArray(line?.words) || line.words.length === 0) return null;
  const words = line.words.map(word => {
    const bbox = finiteWordBox(word);
    const value = text(word?.text);
    const confidence = Number(word?.confidence);
    return bbox && value && Number.isFinite(confidence) && confidence >= 30
      ? Object.freeze({ text: value, bbox, confidence })
      : null;
  });
  if (words.some(word => !word)) return null;
  const ordered = [...words].sort((left, right) => left.bbox[0] - right.bbox[0] || left.bbox[1] - right.bbox[1]);
  if (ordered.some((word, index) => index > 0 && word.bbox[0] < ordered[index - 1].bbox[2])) return null;
  return Object.freeze(ordered);
}

function semanticFieldForPhrase(value) {
  const normalized = normalizedWordText(value);
  return STRUCTURED_FIELD_DEFINITIONS.find(definition => definition.phrases.includes(normalized))?.kind || "";
}

function partitionSemanticHeader(words) {
  if (!Array.isArray(words) || words.length < 2 || words.length > 8) return null;
  const partitions = [];
  const visit = (offset, fields) => {
    if (offset === words.length) {
      partitions.push(fields);
      return;
    }
    for (let length = 1; length <= 2 && offset + length <= words.length; length += 1) {
      const selected = words.slice(offset, offset + length);
      const kind = semanticFieldForPhrase(selected.map(word => word.text).join(" "));
      if (!kind) continue;
      visit(offset + length, [...fields, Object.freeze({
        kind,
        text: selected.map(word => word.text).join(" "),
        x0: selected[0].bbox[0],
        x1: selected[selected.length - 1].bbox[2]
      })]);
    }
  };
  visit(0, []);
  const valid = partitions.filter(fields => {
    const kinds = fields.map(field => field.kind);
    const geographic = kinds.filter(kind => kind === "longitude").length === 1
      && kinds.filter(kind => kind === "latitude").length === 1;
    const projected = kinds.filter(kind => kind === "easting").length === 1
      && kinds.filter(kind => kind === "northing").length === 1;
    return (geographic !== projected)
      && kinds.filter(kind => kind === "label").length <= 1
      && fields.length === 2 + kinds.filter(kind => kind === "label").length;
  });
  return valid.length === 1 ? Object.freeze(valid[0]) : null;
}

function coordinateLikeField(value, projected) {
  const source = text(value);
  if (!source || /[A-Za-z]{4,}/u.test(source.replace(/(?:longitude|latitude|easting|northing)/giu, ""))) return false;
  if (projected) return /^[-+]?\d{4,9}(?:[.,]\d+)?$/u.test(source.replace(/\s+/gu, ""));
  return /\d/u.test(source) && (/[-+]?\d{1,3}(?:[.,]\d{3,12})?/u.test(source)
    || /\d{1,3}\s*[°º]/u.test(source));
}

function canonicalizeRowByHeader(words, header) {
  const centers = header.map(field => (field.x0 + field.x1) / 2);
  const boundaries = centers.slice(0, -1).map((center, index) => (center + centers[index + 1]) / 2);
  const columns = header.map(() => []);
  for (const word of words) {
    const center = (word.bbox[0] + word.bbox[2]) / 2;
    const columnIndex = boundaries.findIndex(boundary => center < boundary);
    columns[columnIndex < 0 ? columns.length - 1 : columnIndex].push(word);
  }
  if (columns.some(column => column.length === 0)) return "";
  const values = columns.map(column => column.map(word => word.text).join(" ").trim());
  const projected = header.some(field => field.kind === "easting" || field.kind === "northing");
  const labelIndex = header.findIndex(field => field.kind === "label");
  if (labelIndex >= 0 && !/^(?:[A-Z]|\d{1,3})$/iu.test(values[labelIndex])) return "";
  for (let index = 0; index < header.length; index += 1) {
    if (index === labelIndex || !coordinateLikeField(values[index], projected)) continue;
    const otherAxisIndex = header.findIndex((field, candidateIndex) => candidateIndex !== index && field.kind !== "label");
    if (otherAxisIndex >= 0 && coordinateLikeField(values[otherAxisIndex], projected)) return values.join(" | ");
  }
  return "";
}

function unionLayoutLines(lines, canonicalText) {
  const boxes = lines.map(line => Array.isArray(line?.bbox) ? line.bbox.map(Number) : null);
  if (boxes.some(box => !box || box.length !== 4 || !box.every(Number.isFinite))) return null;
  const indexes = lines.flatMap(line => Array.isArray(line?.source_line_indexes)
    ? line.source_line_indexes
    : [Number(line?.local_line_index)]);
  if (indexes.some(index => !Number.isSafeInteger(index) || index < 0)) return null;
  return Object.freeze({
    ...lines[0],
    text: canonicalText,
    bbox: Object.freeze([
      Math.min(...boxes.map(box => box[0])),
      Math.min(...boxes.map(box => box[1])),
      Math.max(...boxes.map(box => box[2])),
      Math.max(...boxes.map(box => box[3]))
    ]),
    confidence: Math.min(...lines.map(line => Number(line?.confidence) || 0)),
    words: Object.freeze(lines.flatMap(line => Array.isArray(line?.words) ? line.words : [])),
    word_structure_valid: true,
    local_line_index: Math.min(...indexes),
    source_line_indexes: Object.freeze([...new Set(indexes)].sort((left, right) => left - right))
  });
}

function layoutLinesAreAdjacent(labelLine, valueLine) {
  const left = Array.isArray(labelLine?.bbox) ? labelLine.bbox.map(Number) : [];
  const right = Array.isArray(valueLine?.bbox) ? valueLine.bbox.map(Number) : [];
  if (left.length !== 4 || right.length !== 4 || ![...left, ...right].every(Number.isFinite)) return false;
  const leftHeight = left[3] - left[1];
  const rightHeight = right[3] - right[1];
  const gap = right[1] - left[3];
  const overlap = Math.max(0, Math.min(left[2], right[2]) - Math.max(left[0], right[0]));
  const minimumWidth = Math.min(left[2] - left[0], right[2] - right[0]);
  return right[1] >= left[1]
    && gap <= Math.max(leftHeight, rightHeight) * 3
    && gap >= 0
    && (overlap >= minimumWidth * 0.2
      || Math.abs((left[0] + left[2]) - (right[0] + right[2])) <= Math.max(left[2] - left[0], right[2] - right[0]) * 2);
}

function roleLabel(value) {
  const normalized = normalizedWordText(value);
  if (/^(?:search|rechercher|buscar|搜索|搜尋)$/iu.test(normalized)) return "Search";
  if (/^(?:place\s+details?|location\s+details?|details?|place|location|address|directions?|地点|地點|位置|地址|路线|路線)$/iu.test(normalized)) return "Place details";
  if (/^plus\s*code$/iu.test(normalized)) return "Plus Code";
  return "";
}

function axisLabel(value) {
  const normalized = normalizedWordText(value);
  const kind = semanticFieldForPhrase(normalized);
  if (kind === "longitude") return "Longitude";
  if (kind === "latitude") return "Latitude";
  if (kind === "easting") return "Easting";
  if (kind === "northing") return "Northing";
  return "";
}

function projectedPointLabel(value) {
  return text(value).match(/^Point\s*[:#-]?\s*([A-Z]|\d{1,3})$/iu)?.[1]?.toUpperCase() || "";
}

function coordinateOnlyText(value, projected = false) {
  const source = text(value);
  if (projected) return /^[-+]?\d{4,9}(?:[.,]\d+)?$/u.test(source.replace(/\s+/gu, "")) ? source : "";
  const decimalPair = /^[-+]?\d{1,3}(?:[.,]\d{3,12})?\s*,\s*[-+]?\d{1,3}(?:[.,]\d{3,12})?$/u;
  const singleDecimal = /^[-+]?\d{1,3}(?:[.,]\d{3,12})?$/u;
  const dms = /^\d{1,3}\s*[°º]\s*\d{1,2}(?:\s*['′’])?\s*\d{1,2}(?:[.,]\d+)?(?:\s*["″”])?\s*[NSEWO]$/iu;
  return decimalPair.test(source) || singleDecimal.test(source) || dms.test(source) ? source : "";
}

function decimalCoordinatePairFromWords(words) {
  if (!Array.isArray(words) || words.length !== 2) return "";
  const values = words.map(word => text(word?.text).replace(/[，,]$/u, ""));
  if (!values.every(value => /^[-+]?\d{1,3}(?:\.\d{4,12})?$/u.test(value))) return "";
  const numbers = values.map(Number);
  if (!numbers.every(Number.isFinite)
    || !numbers.some(value => Math.abs(value) <= 90)
    || numbers.some(value => Math.abs(value) > 180)) return "";
  return `${values[0]}, ${values[1]}`;
}

// Reconstructs only structures already supported by trustworthy OCR word boxes.
// It never supplies a missing label, value, CRS field, row or map role.
export function normalizeLocalOcrStructuredEvidence({ sourceText = "", layoutLines = [] } = {}) {
  const sourceLines = String(sourceText || "").split(/\r?\n/u)
    .map(normalizedCoverageText)
    .filter(Boolean);
  if (!Array.isArray(layoutLines) || sourceLines.length === 0 || sourceLines.length > 256
    || layoutLines.length === 0 || layoutLines.length > 256) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      sourceLines.length,
      Array.isArray(layoutLines) ? layoutLines.length : 0
    );
  }
  const ordered = [...layoutLines].sort((left, right) => (
    Number(left?.local_line_index) - Number(right?.local_line_index)
  ));
  const indexes = ordered.map(line => Number(line?.local_line_index));
  if (indexes.some(index => !Number.isSafeInteger(index) || index < 0)
    || new Set(indexes).size !== indexes.length) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.INVALID_READING_ORDER,
      sourceLines.length,
      ordered.length
    );
  }
  const lineBoxes = ordered.map(finiteLineBox);
  if (lineBoxes.some(box => !box)
    || lineBoxes.some((box, index) => lineBoxes.slice(index + 1).some(other => boxesOverlap(box, other)))) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.INVALID_LINE_REGION,
      sourceLines.length,
      ordered.length
    );
  }
  const orderedWords = ordered.map(trustedLineWords);
  if (orderedWords.some(words => !words)) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.UNTRUSTED_WORD_STRUCTURE,
      sourceLines.length,
      ordered.length
    );
  }
  const wordCoverageMatches = ordered.every((line, index) => {
    const rawText = normalizedCoverageText(line?.text);
    const wordsText = orderedWords[index].map(word => word.text).join(" ");
    const lineBox = lineBoxes[index];
    const wordsInsideLine = orderedWords[index].every(word => (
      word.bbox[0] >= lineBox[0] && word.bbox[1] >= lineBox[1]
      && word.bbox[2] <= lineBox[2] && word.bbox[3] <= lineBox[3]
    ));
    return canonicalCoverageText(rawText) === canonicalCoverageText(wordsText)
      && wordsInsideLine;
  });
  if (!wordCoverageMatches) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.WORD_TEXT_MISMATCH,
      sourceLines.length,
      ordered.length
    );
  }
  const sourceCoverage = canonicalCoverageStream(sourceLines);
  const layoutCoverage = canonicalCoverageStream(ordered.map(line => line?.text));
  if (!sourceCoverage || sourceCoverage !== layoutCoverage) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      sourceLines.length,
      ordered.length
    );
  }
  const logical = [];
  for (let index = 0; index < ordered.length; index += 1) {
    const line = ordered[index];
    const words = orderedWords[index];
    const rawText = text(line?.text);
    const joinedWords = words.map(word => word.text).join(" ").trim();
    const compactWords = words.map(word => word.text).join("").replace(/\s+/gu, "");
    const next = ordered[index + 1];
    const nextWords = trustedLineWords(next);
    const nextText = nextWords ? nextWords.map(word => word.text).join(" ").trim() : "";
    const projectedLabel = projectedPointLabel(joinedWords);
    if (projectedLabel && index + 4 < ordered.length) {
      const candidateLines = ordered.slice(index, index + 5);
      const candidateTexts = candidateLines.map(candidate => {
        const candidateWords = trustedLineWords(candidate);
        return candidateWords ? candidateWords.map(word => word.text).join(" ").trim() : "";
      });
      const firstAxis = axisLabel(candidateTexts[1]);
      const firstValue = coordinateOnlyText(candidateTexts[2], true);
      const secondAxis = axisLabel(candidateTexts[3]);
      const secondValue = coordinateOnlyText(candidateTexts[4], true);
      const adjacent = candidateLines.slice(0, -1).every((candidate, candidateIndex) => (
        layoutLinesAreAdjacent(candidate, candidateLines[candidateIndex + 1])
      ));
      if (adjacent && [firstAxis, secondAxis].sort().join("|") === "Easting|Northing"
        && firstValue && secondValue) {
        const merged = unionLayoutLines(candidateLines,
          `POINT | ${projectedLabel} | ${firstAxis.toUpperCase()}=${firstValue} | ${secondAxis.toUpperCase()}=${secondValue}`);
        if (merged) {
          logical.push(merged);
          index += 4;
          continue;
        }
      }
    }
    const explicitAxis = axisLabel(joinedWords);
    const explicitRole = roleLabel(joinedWords);
    const nextProjectedValue = coordinateOnlyText(nextText, ["Easting", "Northing"].includes(explicitAxis));
    const nextCoordinateValue = coordinateOnlyText(nextText, false);
    const splitProjectionMethod = /^\s*(?:Projection|Projected\s+CRS|Projected\s+Coordinate\s+System)\s*[:|]?\s*$/iu.test(joinedWords)
      && /^[\p{L}][\p{L}\s_-]{1,60}$/u.test(nextText);
    if (nextWords && layoutLinesAreAdjacent(line, next) && splitProjectionMethod) {
      const merged = unionLayoutLines([line, next], `Projection | ${nextText}`);
      if (merged) {
        logical.push(merged);
        index += 1;
        continue;
      }
    }
    if (nextWords && layoutLinesAreAdjacent(line, next) && explicitAxis
      && (nextProjectedValue || nextCoordinateValue)) {
      const merged = unionLayoutLines([line, next], `${explicitAxis}: ${nextProjectedValue || nextCoordinateValue}`);
      if (merged) {
        logical.push(merged);
        index += 1;
        continue;
      }
    }
    const nextMapCoordinate = coordinateOnlyText(nextText, false)
      || decimalCoordinatePairFromWords(nextWords);
    const nextPlusCode = explicitRole === "Plus Code"
      && /^[A-Z0-9]{4,12}\+[A-Z0-9]{2,}$/iu.test(nextText) ? nextText : "";
    if (nextWords && layoutLinesAreAdjacent(line, next) && nextPlusCode) {
      const merged = unionLayoutLines([line, next], `${explicitRole} ${nextPlusCode}`);
      if (merged) {
        logical.push(merged);
        index += 1;
        continue;
      }
    }
    if (nextWords && layoutLinesAreAdjacent(line, next) && explicitRole
      && /^[-+]?\d{1,3}(?:\.\d{4,12})?\s*,\s*[-+]?\d{1,3}(?:\.\d{4,12})?$/u.test(nextMapCoordinate)) {
      const merged = unionLayoutLines([line, next], `${explicitRole} ${nextMapCoordinate}`);
      if (merged) {
        logical.push(merged);
        index += 1;
        continue;
      }
    }
    const inlineAxis = axisLabel(words[0]?.text);
    const inlineValue = coordinateOnlyText(words.slice(1).map(word => word.text).join(" "), ["Easting", "Northing"].includes(inlineAxis));
    if (inlineAxis && inlineValue) {
      logical.push(Object.freeze({ ...line, text: `${inlineAxis}: ${inlineValue}` }));
      continue;
    }
    const header = partitionSemanticHeader(words);
    if (header) {
      logical.push(Object.freeze({
        ...line,
        text: header.map(field => field.text).join(" | "),
        structured_header: header
      }));
      continue;
    }
    const mgrs = compactWords.match(/^MGRS([1-9]|[1-5]\d|60)([C-HJ-NP-X])([A-HJ-NP-Z]{2})(\d{1,5})(\d{1,5})$/iu);
    if (mgrs && mgrs[4].length === mgrs[5].length) {
      logical.push(Object.freeze({ ...line, text: `MGRS | ${mgrs[1]}${mgrs[2]} | ${mgrs[3]} | ${mgrs[4]} | ${mgrs[5]}` }));
      continue;
    }
    logical.push(Object.freeze({ ...line, text: rawText }));
  }

  let activeHeader = null;
  const normalized = logical.map(line => {
    const header = Array.isArray(line?.structured_header) ? line.structured_header : null;
    if (header) {
      activeHeader = header;
      const { structured_header: ignored, ...publicLine } = line;
      return Object.freeze(publicLine);
    }
    const words = trustedLineWords(line);
    if (activeHeader && words) {
      const canonicalRow = canonicalizeRowByHeader(words, activeHeader);
      if (canonicalRow) return Object.freeze({ ...line, text: canonicalRow });
    }
    if (/^(?:location\s+group|group|site|sites|area|mining\s+area|矿区|礦區)\b/iu.test(text(line?.text))) {
      activeHeader = null;
    } else if (!/\d/u.test(text(line?.text))) {
      activeHeader = null;
    }
    return line;
  });
  return Object.freeze({
    text: normalized.map(line => text(line?.text)).filter(Boolean).join("\n"),
    layoutLines: Object.freeze(normalized),
    status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE,
    reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.COMPLETE_COVERAGE,
    sourceLineCount: boundedCount(sourceLines.length),
    layoutLineCount: boundedCount(ordered.length)
  });
}

function decimalCoordinatePairObservation(value) {
  const normalized = text(value).replace(/[，]/gu, ",").replace(/[−–—]/gu, "-");
  const pattern = /([+-]?\d{1,3}(?:\.\d{4,12})?)\s*,\s*([+-]?\d{1,3}(?:\.\d{4,12})?)/gu;
  const matches = [...normalized.matchAll(pattern)];
  if (matches.length !== 1) return null;

  const first = Number(matches[0][1]);
  const second = Number(matches[0][2]);
  if (!Number.isFinite(first) || !Number.isFinite(second)
    || Math.abs(first) > 180 || Math.abs(second) > 180
    || (Math.abs(first) > 90 && Math.abs(second) > 90)) return null;

  const prefix = normalized.slice(0, matches[0].index);
  const suffix = normalized.slice((matches[0].index || 0) + matches[0][0].length);
  const outsidePair = `${prefix}${suffix}`;
  if (/\d|[°º'′’"″”]/u.test(outsidePair)) return null;
  return Object.freeze({
    pair: `${matches[0][1]},${matches[0][2]}`,
    prefix,
    suffix,
    compactPrefix: prefix.trim().match(/^[A-Za-z]$/u)?.[0]?.toLowerCase() || "",
    compactSuffix: suffix.trim()
  });
}

function decimalCoordinatePairText(value) {
  return decimalCoordinatePairObservation(value)?.pair || "";
}

// A noisy app screenshot may contain status bars and buttons whose OCR is not
// trustworthy while its coordinate rows are clear. Keep only independently
// high-confidence decimal pairs that are present in both OCR text and bound
// layout lines. This never assigns axis order or grants Map/KML authority.
export function extractTrustedLocalOcrDecimalCoordinateEvidence({
  sourceText = "",
  layoutLines = [],
  minimumRows = 3
} = {}) {
  const sourceObservations = String(sourceText || "").split(/\r?\n/u)
    .map(decimalCoordinatePairObservation)
    .filter(Boolean);
  const sourceRows = sourceObservations.map(observation => observation.pair);
  if (!Array.isArray(layoutLines) || sourceRows.length < minimumRows) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      sourceRows.length,
      Array.isArray(layoutLines) ? layoutLines.length : 0
    );
  }

  const selected = layoutLines.flatMap(line => {
    const words = trustedLineWords(line);
    const lineConfidence = Number(line?.confidence);
    const observation = decimalCoordinatePairObservation(line?.text);
    if (!observation || !words || !Number.isFinite(lineConfidence) || lineConfidence < 70
      || words.some(word => word.confidence < 70) || !finiteLineBox(line)) return [];
    return [Object.freeze({
      line: Object.freeze({ ...line, text: observation.pair }),
      observation
    })];
  });
  const layoutRows = selected.map(item => item.observation.pair);
  if (layoutRows.length < minimumRows
    || layoutRows.length !== sourceRows.length
    || layoutRows.some((row, index) => row !== sourceRows[index])) {
    return incompleteStructuredEvidence(
      LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      sourceRows.length,
      layoutRows.length
    );
  }

  const repeatedCompactPrefix = sourceObservations[0]?.compactPrefix || "";
  const hasRepeatedCompactPrefix = Boolean(
    repeatedCompactPrefix
    && sourceObservations.every(observation => (
      observation.compactPrefix === repeatedCompactPrefix
      && observation.compactSuffix === ""
    ))
    && selected.every(item => (
      item.observation.compactPrefix === repeatedCompactPrefix
      && item.observation.compactSuffix === ""
    ))
  );

  return Object.freeze({
    text: sourceRows.join("\n"),
    layoutLines: Object.freeze(selected.map(item => item.line)),
    status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE,
    reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.TRUSTED_DECIMAL_ROWS_ONLY,
    sourceLineCount: boundedCount(sourceRows.length),
    layoutLineCount: boundedCount(layoutRows.length),
    axisOrderEvidence: hasRepeatedCompactPrefix
      ? Object.freeze({
        status: "FORMAT_RESOLVED",
        axisOrder: "longitude_latitude",
        interpretation: "first_is_lon_second_is_lat",
        confidence: 0.9,
        reason: "repeated_compact_platform_prefix_before_decimal_pair"
      })
      : null
  });
}

// Provider transcription is the fallback evidence source when bounded local
// OCR cannot finish in time. Accept only a closed set of independently valid
// decimal-pair rows; unrelated UI prose, timestamps and toolbar text are
// ignored. Axis order remains unresolved unless every row carries the same
// compact platform prefix already used by the source application.
export function extractProviderDecimalCoordinateEvidence({
  sourceText = "",
  minimumRows = 3
} = {}) {
  const sourceLines = String(sourceText || "").split(/\r?\n/u);
  const coordinateBearingLines = sourceLines.filter(line => (
    /[+-]?\d{1,3}(?:\.\d+)?\s*[,，]\s*[+-]?\d{1,3}(?:\.\d+)?/u.test(line)
  ));
  const observations = sourceLines
    .map(decimalCoordinatePairObservation)
    .filter(Boolean);
  if (observations.length < minimumRows || observations.length !== coordinateBearingLines.length) {
    return Object.freeze({
      text: "",
      rows: Object.freeze([]),
      rowCount: 0,
      status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
      reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      axisOrderEvidence: null
    });
  }

  const repeatedCompactPrefix = observations[0]?.compactPrefix || "";
  const hasRepeatedCompactPrefix = Boolean(
    repeatedCompactPrefix
    && observations.every(observation => (
      observation.compactPrefix === repeatedCompactPrefix
      && observation.compactSuffix === ""
    ))
  );
  const rows = Object.freeze(observations.map(observation => observation.pair));
  return Object.freeze({
    text: rows.join("\n"),
    rows,
    rowCount: rows.length,
    status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE,
    reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.TRUSTED_DECIMAL_ROWS_ONLY,
    axisOrderEvidence: hasRepeatedCompactPrefix
      ? Object.freeze({
        status: "FORMAT_RESOLVED",
        axisOrder: "longitude_latitude",
        interpretation: "first_is_lon_second_is_lat",
        confidence: 0.9,
        reason: "repeated_compact_platform_prefix_before_decimal_pair"
      })
      : null
  });
}

function normalizeProjectedProviderNumber(value) {
  const source = text(value).replace(/\u00a0/gu, " ").replace(/[−–—]/gu, "-");
  if (!source) return null;
  const compact = source.replace(/\s+/gu, "");
  const normalized = /^[-+]?\d{1,3}(?:,\d{3}){1,3}(?:\.\d+)?$/u.test(compact)
    ? compact.replace(/,/gu, "")
    : compact.replace(/,/gu, ".");
  if (!/^[-+]?\d{4,10}(?:\.\d+)?$/u.test(normalized)) return null;
  const numeric = Number(normalized);
  return Number.isFinite(numeric) && Math.abs(numeric) >= 10000
    ? normalized
    : null;
}

function groupedProjectedProviderNumber(tokens = []) {
  if (!Array.isArray(tokens) || tokens.length === 0) return null;
  if (tokens.length === 1) return normalizeProjectedProviderNumber(tokens[0]);
  const unsigned = tokens.map(token => String(token || "").trim());
  const sign = /^[+-]/u.test(unsigned[0]) ? unsigned[0][0] : "";
  unsigned[0] = unsigned[0].replace(/^[+-]/u, "");
  if (!/^\d{1,3}$/u.test(unsigned[0]) || unsigned.slice(1).some(token => !/^\d{3}$/u.test(token))) return null;
  return normalizeProjectedProviderNumber(`${sign}${unsigned.join("")}`);
}

function parseProviderProjectedRow(line, { labelColumnVisible = false } = {}) {
  const raw = text(line).replace(/[｜]/gu, "|");
  if (!raw) return null;
  const labelledCommaPair = raw.match(
    /^\s*(?:POINT\s+)?([A-Z][A-Z0-9_.-]{0,15}|\d{1,3})\s*(?:[|:]\s*)?([+-]?\d{4,10}(?:\.\d+)?)\s*,\s*([+-]?\d{4,10}(?:\.\d+)?)\s*$/iu
  );
  if (labelledCommaPair) {
    const x = normalizeProjectedProviderNumber(labelledCommaPair[2]);
    const y = normalizeProjectedProviderNumber(labelledCommaPair[3]);
    return x && y
      ? Object.freeze({ label: labelledCommaPair[1], x, y, sourceText: raw })
      : null;
  }
  const pipeParts = raw.split("|").map(part => part.trim()).filter(Boolean);
  if (pipeParts.length === 4 && /^POINT$/iu.test(pipeParts[0])) {
    const x = normalizeProjectedProviderNumber(pipeParts[2]);
    const y = normalizeProjectedProviderNumber(pipeParts[3]);
    if (x && y) return Object.freeze({ label: pipeParts[1], x, y, sourceText: raw });
  }
  if (pipeParts.length === 3) {
    const x = normalizeProjectedProviderNumber(pipeParts[1]);
    const y = normalizeProjectedProviderNumber(pipeParts[2]);
    if (x && y) return Object.freeze({ label: pipeParts[0], x, y, sourceText: raw });
  }
  if (pipeParts.length >= 5
    && pipeParts.slice(3).every(part => /\d\s*°[^|]*[NSEWO]\b/iu.test(part))) {
    const x = normalizeProjectedProviderNumber(pipeParts[1]);
    const y = normalizeProjectedProviderNumber(pipeParts[2]);
    if (x && y) return Object.freeze({ label: pipeParts[0], x, y, sourceText: raw });
  }

  const tokens = raw.match(/[+-]?\d+(?:[.,]\d+)?/gu) || [];
  let label = raw.match(/^\s*(?:POINT\s+)?([A-Z][A-Z0-9_.-]{0,15})\b/iu)?.[1] || "";
  let coordinateTokens = tokens;
  if (!label && labelColumnVisible && tokens.length >= 3 && /^\d{1,3}$/u.test(tokens[0])) {
    label = tokens[0];
    coordinateTokens = tokens.slice(1);
  }
  if (!label && !labelColumnVisible) return null;
  if (coordinateTokens.length === 2) {
    const x = normalizeProjectedProviderNumber(coordinateTokens[0]);
    const y = normalizeProjectedProviderNumber(coordinateTokens[1]);
    return x && y ? Object.freeze({ label, x, y, sourceText: raw }) : null;
  }
  const candidates = [];
  for (let split = 1; split < coordinateTokens.length; split += 1) {
    const x = groupedProjectedProviderNumber(coordinateTokens.slice(0, split));
    const y = groupedProjectedProviderNumber(coordinateTokens.slice(split));
    if (x && y) candidates.push({ x, y });
  }
  const unique = Array.from(new Map(candidates.map(candidate => [`${candidate.x}|${candidate.y}`, candidate])).values());
  return unique.length === 1
    ? Object.freeze({ label, ...unique[0], sourceText: raw })
    : null;
}

function parseProviderProjectedRows(line, { labelColumnVisible = false } = {}) {
  const raw = text(line).replace(/[｜]/gu, "|");
  if (!raw) return Object.freeze({ rows: Object.freeze([]), multiRecord: false });

  const pipeParts = raw.split("|").map(part => part.trim()).filter(Boolean);
  if (pipeParts.length >= 6 && pipeParts.length % 3 === 0) {
    const groupedRows = [];
    for (let index = 0; index < pipeParts.length; index += 3) {
      const label = pipeParts[index];
      const x = normalizeProjectedProviderNumber(pipeParts[index + 1]);
      const y = normalizeProjectedProviderNumber(pipeParts[index + 2]);
      if (!/^\d{1,3}$/u.test(label) || !x || !y) {
        return Object.freeze({ rows: Object.freeze([]), multiRecord: true });
      }
      groupedRows.push(Object.freeze({
        label,
        x,
        y,
        sourceText: raw
      }));
    }
    return Object.freeze({ rows: Object.freeze(groupedRows), multiRecord: true });
  }

  // Some Providers preserve parallel projected-table columns as whitespace or
  // tabs rather than pipes. Accept that layout only when the entire physical
  // line is an exact sequence of two or more numeric-label/X/Y triples. A
  // valid first triple followed by any incomplete or extra material is treated
  // as a rejected multi-record line instead of falling back to a lossy single
  // row parse. Thousand-spaced single rows keep using the existing ambiguity
  // checked parser below because their first X/Y tokens are not complete
  // projected numbers on their own.
  if (!raw.includes("|")) {
    const whitespaceParts = raw.split(/\s+/u).filter(Boolean);
    const firstLabel = whitespaceParts[0] || "";
    const firstX = normalizeProjectedProviderNumber(whitespaceParts[1]);
    const firstY = normalizeProjectedProviderNumber(whitespaceParts[2]);
    const beginsWithCompleteTriple = /^\d{1,3}$/u.test(firstLabel) && firstX && firstY;
    const continuesWithNumericLabel = /^\d{1,3}$/u.test(whitespaceParts[3] || "");

    if (whitespaceParts.length >= 6 && whitespaceParts.length % 3 === 0) {
      const groupedRows = [];
      for (let index = 0; index < whitespaceParts.length; index += 3) {
        const label = whitespaceParts[index];
        const x = normalizeProjectedProviderNumber(whitespaceParts[index + 1]);
        const y = normalizeProjectedProviderNumber(whitespaceParts[index + 2]);
        if (!/^\d{1,3}$/u.test(label) || !x || !y) {
          if (beginsWithCompleteTriple && continuesWithNumericLabel) {
            return Object.freeze({ rows: Object.freeze([]), multiRecord: true });
          }
          break;
        }
        groupedRows.push(Object.freeze({ label, x, y, sourceText: raw }));
      }
      if (groupedRows.length * 3 === whitespaceParts.length) {
        return Object.freeze({ rows: Object.freeze(groupedRows), multiRecord: true });
      }
    } else if (beginsWithCompleteTriple && continuesWithNumericLabel) {
      return Object.freeze({ rows: Object.freeze([]), multiRecord: true });
    }
  }

  const row = parseProviderProjectedRow(raw, { labelColumnVisible });
  return Object.freeze({
    rows: Object.freeze(row ? [row] : []),
    multiRecord: false
  });
}

// A generic Provider may faithfully transcribe a projected X/Y table even
// when local OCR could not establish a private one-shot family binding. Keep
// those rows available for review, but never infer a CRS from filenames,
// countries, or numeric ranges and never grant Map/KML authority here.
export function extractProviderProjectedCoordinateEvidence({
  sourceText = "",
  minimumRows = 3
} = {}) {
  const lines = String(sourceText || "").split(/\r?\n/u).map(line => line.trim()).filter(Boolean);
  const unclassifiedTitleIndex = lines.findIndex(line => /UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE/iu.test(line));
  const headerIndex = lines.findIndex(line => (
    /\b(?:POINT|SOMMETS?|LABEL|ID|N[°O])\b[^\r\n]*(?:\bX\b|\bEASTING\b)[^\r\n]*(?:\bY\b|\bNORTHING\b)/iu.test(line)
      || /(?:\bX\b|\bEASTING\b)[^\r\n]+(?:\bY\b|\bNORTHING\b)/iu.test(line)
  ));
  // Generic acquisition sometimes preserves all labelled rows and visible CRS
  // text but omits a separate X/Y header. The server-issued unclassified title
  // is then the required contract boundary; ordinary headerless number lists
  // remain ineligible.
  if (headerIndex < 0 && unclassifiedTitleIndex < 0) {
    return Object.freeze({
      text: "", rows: Object.freeze([]), rowCount: 0,
      status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
      reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      crsEvidence: null
    });
  }
  const labelColumnVisible = unclassifiedTitleIndex >= 0
    || /\b(?:POINT|SOMMETS?|LABEL|ID|N[°O])\b/iu.test(lines[headerIndex]);
  const rowLines = lines.slice((headerIndex >= 0 ? headerIndex : unclassifiedTitleIndex) + 1);
  const rows = [];
  let projectedCandidateLineCount = 0;
  let rejectedProjectedCandidateLineCount = 0;
  let multiRecordLineCount = 0;
  for (const line of rowLines) {
    const parsedLine = parseProviderProjectedRows(line, { labelColumnVisible });
    if (parsedLine.rows.length > 0) {
      rows.push(...parsedLine.rows);
      projectedCandidateLineCount += 1;
      if (parsedLine.multiRecord) multiRecordLineCount += 1;
      continue;
    }
    const numericTokens = line.match(/[+-]?\d+(?:[.,]\d+)?/gu) || [];
    if (numericTokens.length >= 2) {
      projectedCandidateLineCount += 1;
      rejectedProjectedCandidateLineCount += 1;
    }
  }
  if (rows.length < minimumRows || rejectedProjectedCandidateLineCount > 0) {
    return Object.freeze({
      text: "", rows: Object.freeze([]), rowCount: 0,
      status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
      reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      crsEvidence: null,
      diagnostics: Object.freeze({
        contractTitlePresent: unclassifiedTitleIndex >= 0,
        headerPresent: headerIndex >= 0,
        projectedCandidateLineCount,
        parsedProjectedRowCount: rows.length,
        rejectedProjectedCandidateLineCount,
        multiRecordLineCount
      })
    });
  }
  const labels = rows.map(row => text(row.label));
  if (labels.some(label => !label) || new Set(labels).size !== labels.length) {
    return Object.freeze({
      text: "", rows: Object.freeze([]), rowCount: 0,
      status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
      reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
      crsEvidence: null
    });
  }
  let orderedRows = rows;
  if (multiRecordLineCount > 0) {
    const numericLabels = labels.map(label => /^\d{1,3}$/u.test(label) ? Number(label) : null);
    if (numericLabels.some(label => !Number.isInteger(label))) {
      return Object.freeze({
        text: "", rows: Object.freeze([]), rowCount: 0,
        status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
        reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
        crsEvidence: null
      });
    }
    orderedRows = rows.slice().sort((left, right) => Number(left.label) - Number(right.label));
    if (orderedRows.some((row, index) => Number(row.label) !== index + 1)) {
      return Object.freeze({
        text: "", rows: Object.freeze([]), rowCount: 0,
        status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE,
        reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH,
        crsEvidence: null
      });
    }
  }
  const explicitUtm = String(sourceText || "").match(/\bUTM\s*(?:ZONE\s*)?(\d{1,2})\s*([NS])\b/iu)
    || String(sourceText || "").match(/\bUTM\b[^\r\n]{0,80}\b(?:ZONE|ZONA)\s*(\d{1,2})\s*([NS])\b/iu);
  const explicitBftm = /\bBFTM\b/iu.test(String(sourceText || ""));
  const crsEvidence = explicitBftm
    ? Object.freeze({ status: "EXPLICIT", projection: "bftm", zone: null, hemisphere: "" })
    : explicitUtm
      ? Object.freeze({ status: "EXPLICIT", projection: "utm", zone: Number(explicitUtm[1]), hemisphere: explicitUtm[2].toUpperCase() })
      : Object.freeze({ status: "UNCONFIRMED", projection: "", zone: null, hemisphere: "" });
  const frozenRows = Object.freeze(orderedRows.map((row, index) => Object.freeze({
    ...row,
    label: text(row.label) || String(index + 1)
  })));
  return Object.freeze({
    text: frozenRows.map(row => `${row.label} | ${row.x} | ${row.y}`).join("\n"),
    rows: frozenRows,
    rowCount: frozenRows.length,
    status: LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE,
    reason: LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.TRUSTED_PROJECTED_ROWS_ONLY,
    crsEvidence,
    diagnostics: Object.freeze({
      contractTitlePresent: unclassifiedTitleIndex >= 0,
      headerPresent: headerIndex >= 0,
      projectedCandidateLineCount,
      parsedProjectedRowCount: frozenRows.length,
      rejectedProjectedCandidateLineCount,
      multiRecordLineCount
    })
  });
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
