import assert from "node:assert/strict";
import {
  LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON,
  LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS,
  extractTrustedLocalOcrDecimalCoordinateEvidence
} from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";

const rows = [
  "g-8.09722200,10.62611100",
  "g-8.06027800,10.62611100",
  "g-8.06027800,10.52944400",
  "g-8.09722200,10.52944400",
  "g-8.09722200,10.62611100"
];
const sourceText = ["10:32 94 RNY |", "untrusted toolbar text", ...rows, "footer"].join("\n");
const layoutLines = [
  { text: "10:32 94 RNY |", confidence: 50, bbox: [0, 0, 100, 20], local_line_index: 0, word_structure_valid: true, words: [{ text: "10:32", confidence: 50, bbox: [0, 0, 40, 20] }] },
  { text: "untrusted toolbar text", confidence: 17, bbox: [0, 30, 100, 50], local_line_index: 1, word_structure_valid: true, words: [{ text: "untrusted", confidence: 17, bbox: [0, 30, 50, 50] }] },
  ...rows.map((text, index) => ({
    text,
    confidence: 91,
    bbox: [0, 60 + index * 30, 300, 80 + index * 30],
    local_line_index: index + 2,
    word_structure_valid: true,
    words: [{ text, confidence: 91, bbox: [0, 60 + index * 30, 300, 80 + index * 30] }]
  })),
  { text: "footer", confidence: 33, bbox: [0, 240, 100, 260], local_line_index: 7, word_structure_valid: true, words: [{ text: "footer", confidence: 33, bbox: [0, 240, 100, 260] }] }
];

const result = extractTrustedLocalOcrDecimalCoordinateEvidence({ sourceText, layoutLines });
assert.equal(result.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
assert.equal(result.reason, LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.TRUSTED_DECIMAL_ROWS_ONLY);
assert.equal(result.layoutLines.length, 5);
assert.equal(result.text.split("\n").length, 5);
assert.equal(result.text.split("\n")[0], "-8.09722200,10.62611100");
assert.equal(result.text.split("\n").at(-1), "-8.09722200,10.62611100");
assert.doesNotMatch(result.text, /toolbar|footer|10:32|g-/u);
assert.equal(result.axisOrderEvidence?.status, "FORMAT_RESOLVED");
assert.equal(result.axisOrderEvidence?.axisOrder, "longitude_latitude");
assert.equal(result.axisOrderEvidence?.interpretation, "first_is_lon_second_is_lat");

const lowConfidenceCoordinate = structuredClone(layoutLines);
lowConfidenceCoordinate[2].confidence = 40;
lowConfidenceCoordinate[2].words[0].confidence = 40;
const rejected = extractTrustedLocalOcrDecimalCoordinateEvidence({ sourceText, layoutLines: lowConfidenceCoordinate });
assert.equal(rejected.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
assert.equal(rejected.text, "");

const unprefixedRows = rows.map(row => row.slice(1));
const unprefixedResult = extractTrustedLocalOcrDecimalCoordinateEvidence({
  sourceText: unprefixedRows.join("\n"),
  layoutLines: unprefixedRows.map((text, index) => ({
    text,
    confidence: 91,
    bbox: [0, 60 + index * 30, 300, 80 + index * 30],
    local_line_index: index,
    word_structure_valid: true,
    words: [{ text, confidence: 91, bbox: [0, 60 + index * 30, 300, 80 + index * 30] }]
  }))
});
assert.equal(unprefixedResult.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
assert.equal(unprefixedResult.axisOrderEvidence, null, "plain ambiguous pairs must not receive automatic axis authority");

console.log(JSON.stringify({
  suite: "platform-prefixed-decimal-local-ocr-regression",
  passed: 13,
  cases: [
    "UI_NOISE_IGNORED",
    "SHORT_PLATFORM_PREFIX_REMOVED",
    "SOURCE_PRECISION_PRESERVED",
    "SOURCE_ORDER_PRESERVED",
    "SOURCE_CLOSURE_PRESERVED",
    "FIVE_ROWS_RECOVERED",
    "LAYOUT_BINDING_REQUIRED",
    "HIGH_CONFIDENCE_REQUIRED",
    "LOW_CONFIDENCE_COORDINATE_REJECTED",
    "REPEATED_PLATFORM_PREFIX_RESOLVES_AXIS_ORDER",
    "LONGITUDE_LATITUDE_ORDER_SELECTED",
    "PLAIN_PAIRS_REMAIN_AMBIGUOUS",
    "AXIS_ORDER_AUTHORITY_IS_FORMAT_BOUND"
  ]
}, null, 2));
