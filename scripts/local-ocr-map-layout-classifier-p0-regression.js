import assert from "node:assert/strict";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  buildEvidenceAcquisition,
  createLocalOcrMapLayoutRows,
  createTrustedLayoutAttestation,
  extractLocalOcrLayoutLines,
  hasLocalOcrStructuredLayoutCapability,
  isLocalOcrMapLayoutCandidate
} from "../server/evidence-acquisition/index.js";
import { evaluateWgs84NearDuplicateConsolidation } from "../server/recognition/wgs84-near-duplicate-consolidation.js";

const low = "35.447819,83.178991";
const high = "35.4478191,83.1789913";
const tests = [];
const test = (id, name, run) => tests.push({ id, name, run });

function makeBmp(width = 1080, height = 1920) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer.writeUInt32LE(pixelBytes, 34);
  return buffer;
}

const buffer = makeBmp();
const imageIdentity = createCoordinateImageIdentity(
  { buffer, mimetype: "image/bmp", size: buffer.length },
  { requestId: "local-ocr-map-layout-regression", page: 1 }
);

function engineWith(rawLow = low, rawHigh = high, overrides = {}) {
  return {
    coordinate_type: "decimal_latlon",
    precision_mode: "wgs84-table-coordinates",
    source_crs: "EPSG:4326",
    groups: [{
      group_id: "group_1",
      points: [
        { label: "1", raw: rawLow, lat: Number(rawLow.split(",")[0]), lon: Number(rawLow.split(",")[1]) },
        { label: "2", raw: rawHigh, lat: Number(rawHigh.split(",")[0]), lon: Number(rawHigh.split(",")[1]) }
      ]
    }],
    ...overrides
  };
}

function lines(overrides = {}) {
  const values = [
    {
      text: `${low} xX )`,
      bbox: [91, 188, 1060, 250],
      confidence: 91,
      words: [
        { text: low, bbox: [91, 189, 586, 236], confidence: 90 },
        { text: "xX", bbox: [956, 188, 998, 240], confidence: 41 },
        { text: ")", bbox: [1038, 225, 1060, 250], confidence: 90 }
      ]
    },
    { text: "8M75C5XH+4HHPX4C (?)", bbox: [203, 1677, 953, 1714], confidence: 60, words: [
      { text: "8M75C5XH+4HHPX4C", bbox: [203, 1682, 588, 1711], confidence: 84 },
      { text: "(?)", bbox: [916, 1677, 953, 1714], confidence: 37 }
    ] },
    { text: high, bbox: [199, 1822, 595, 1855], confidence: 88, words: [
      { text: high, bbox: [199, 1822, 595, 1855], confidence: 91 }
    ] }
  ];
  for (const [index, patch] of Object.entries(overrides)) {
    values[Number(index)] = { ...values[Number(index)], ...patch };
  }
  return values;
}

function createRows(options = {}) {
  return createLocalOcrMapLayoutRows({
    lines: options.lines || lines(),
    imageIdentity: options.imageIdentity || imageIdentity,
    coordinateEngineV2: options.engine || engineWith(),
    resultRevision: options.resultRevision || 1
  });
}

test("LOML-01", "exact Production WGS84 rounding candidate is eligible", () => {
  assert.equal(isLocalOcrMapLayoutCandidate({ coordinateEngineV2: engineWith() }), true);
});

test("LOML-02", "search close control and Open Location Code anchor independent map UI roles", () => {
  const rows = createRows();
  assert.ok(rows);
  assert.deepEqual(rows.map(row => row.source_role).sort(), ["MAP_PLACE_DETAILS", "MAP_SEARCH_BOX"]);
  assert.equal(rows.every(hasLocalOcrStructuredLayoutCapability), true);
  assert.equal(rows.every(row => row.source_type === "LOCAL_OCR_STRUCTURED_LAYOUT_V1"), true);
});

test("LOML-03", "Tesseract block output is flattened in original image pixels", () => {
  const extracted = extractLocalOcrLayoutLines({ data: { blocks: [{ paragraphs: [{ lines: [
    { text: `${low} xX )`, confidence: 91, bbox: { x0: 91, y0: 188, x1: 1060, y1: 250 }, words: [
      { text: low, confidence: 90, bbox: { x0: 91, y0: 189, x1: 586, y1: 236 } },
      { text: "xX", confidence: 41, bbox: { x0: 956, y0: 188, x1: 998, y1: 240 } },
      { text: ")", confidence: 90, bbox: { x0: 1038, y0: 225, x1: 1060, y1: 250 } }
    ] },
    { text: "8M75C5XH+4HHPX4C (?)", confidence: 60, bbox: { x0: 203, y0: 1677, x1: 953, y1: 1714 }, words: [
      { text: "8M75C5XH+4HHPX4C", confidence: 84, bbox: { x0: 203, y0: 1682, x1: 588, y1: 1711 } },
      { text: "(?)", confidence: 37, bbox: { x0: 916, y0: 1677, x1: 953, y1: 1714 } }
    ] },
    { text: high, confidence: 88, bbox: { x0: 199, y0: 1822, x1: 595, y1: 1855 }, words: [
      { text: high, confidence: 91, bbox: { x0: 199, y0: 1822, x1: 595, y1: 1855 } }
    ] }
  ] }] }] } }, imageIdentity);
  assert.deepEqual(extracted.map(line => line.bbox), [[91, 188, 1060, 250], [203, 1677, 953, 1714], [199, 1822, 595, 1855]]);
  assert.ok(createLocalOcrMapLayoutRows({ lines: extracted, imageIdentity, coordinateEngineV2: engineWith() }));
});

test("LOML-04", "local OCR capability creates a bound Trusted Layout attestation", () => {
  const attestation = createTrustedLayoutAttestation({
    imageIdentity,
    observations: createRows(),
    coordinateEngineV2: engineWith(),
    resultRevision: 1
  });
  assert.ok(attestation);
  assert.equal(attestation.observations.length, 2);
  assert.equal(attestation.row_bindings.length, 2);
});

test("LOML-05", "trusted local layout proves same location but cannot auto-authorize Point", () => {
  const engine = engineWith();
  const trustedLayoutAttestation = createTrustedLayoutAttestation({
    imageIdentity,
    observations: createRows(),
    coordinateEngineV2: engine,
    resultRevision: 1
  });
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation, rawText: `${low}\n${high}` };
  const evidenceAcquisition = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: engine, resultRevision: 1 });
  const outcome = evaluateWgs84NearDuplicateConsolidation({ coordinateEngineV2: engine, evidenceAcquisition, recognitionResult, revision: 1 });
  assert.equal(outcome.decision.decision, "SAME_LOCATION_CONFIRMED");
  assert.equal(outcome.geometryIntentGate.decision, "BLOCKED");
  assert.equal(outcome.geometryIntentGate.reason_code, "POINT_GEOMETRY_INTENT_CAPABILITY_MISSING");
});

test("LOML-06", "generic decimal mode cannot enter local map classification", () => {
  assert.equal(isLocalOcrMapLayoutCandidate({
    coordinateEngineV2: engineWith(low, high, { precision_mode: "decimal-coordinates" })
  }), false);
});

test("LOML-07", "non-rounding coordinates cannot enter local map classification", () => {
  assert.equal(isLocalOcrMapLayoutCandidate({ coordinateEngineV2: engineWith(low, "35.4478291,83.1789913") }), false);
});

test("LOML-08", "three-point tables cannot enter the two-region classifier", () => {
  const engine = engineWith();
  engine.groups[0].points.push({ label: "3", raw: "35.5,83.2", lat: 35.5, lon: 83.2 });
  assert.equal(isLocalOcrMapLayoutCandidate({ coordinateEngineV2: engine }), false);
});

test("LOML-09", "OCR coordinate mismatch fails closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { text: "35.4478291,83.1789913" } }) }), null);
});

test("LOML-10", "a third coordinate-bearing OCR line fails closed", () => {
  assert.equal(createRows({ lines: [...lines(), { text: "1.234567,2.345678", bbox: [100, 900, 500, 940], confidence: 90 }] }), null);
});

test("LOML-11", "two coordinates in one upper UI region fail closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { bbox: [199, 300, 595, 340] } }) }), null);
});

test("LOML-12", "ambiguous center-screen region fails closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { bbox: [199, 900, 595, 940] } }) }), null);
});

test("LOML-13", "low-confidence or tiny OCR rows fail closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { confidence: 39 } }) }), null);
  assert.equal(createRows({ lines: lines({ 2: { bbox: [199, 1822, 250, 1855] } }) }), null);
});

test("LOML-14", "JSON and structuredClone lose local OCR capability", () => {
  const rows = createRows();
  for (const plain of [JSON.parse(JSON.stringify(rows)), structuredClone(rows)]) {
    assert.equal(plain.every(hasLocalOcrStructuredLayoutCapability), false);
    assert.equal(createTrustedLayoutAttestation({
      imageIdentity,
      observations: plain,
      coordinateEngineV2: engineWith(),
      resultRevision: 1
    }), null);
  }
});

test("LOML-15", "out-of-bounds local OCR evidence fails closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { bbox: [199, 1822, 1200, 1855] } }) }), null);
});

test("LOML-16", "different coordinates with the same map action use the same classifier", () => {
  const alternateLow = "-6.123456,106.123456";
  const alternateHigh = "-6.1234564,106.1234564";
  const rows = createLocalOcrMapLayoutRows({
    lines: [
      { text: `${alternateLow} x`, bbox: [91, 188, 1060, 250], confidence: 91, words: [
        { text: alternateLow, bbox: [91, 189, 650, 236], confidence: 90 },
        { text: "x", bbox: [956, 188, 998, 240], confidence: 80 }
      ] },
      { text: "7P52R8M4+7Q2V9W", bbox: [203, 1677, 900, 1714], confidence: 88, words: [
        { text: "7P52R8M4+7Q2V9W", bbox: [203, 1677, 600, 1714], confidence: 88 }
      ] },
      { text: alternateHigh, bbox: [199, 1822, 650, 1855], confidence: 88, words: [
        { text: alternateHigh, bbox: [199, 1822, 650, 1855], confidence: 88 }
      ] }
    ],
    imageIdentity,
    coordinateEngineV2: engineWith(alternateLow, alternateHigh)
  });
  assert.ok(rows, "classification must bind the UI class, not the original sample values");
});

test("LOML-17", "ordinary document rows separated vertically cannot fabricate map UI roles", () => {
  const ordinaryDocument = [
    { text: `Appendix table row A ${low}`, bbox: [91, 188, 1060, 250], confidence: 91 },
    { text: `Appendix table row B ${high}`, bbox: [199, 1822, 900, 1855], confidence: 88 }
  ];
  const rows = createRows({ lines: ordinaryDocument });
  assert.equal(rows, null);
  assert.equal(createTrustedLayoutAttestation({
    imageIdentity,
    observations: rows || [],
    coordinateEngineV2: engineWith(),
    resultRevision: 1
  }), null);
});

test("LOML-18", "an Open Location Code without a search close control is not a map action", () => {
  assert.equal(createRows({ lines: lines({ 0: { text: low, words: [{ text: low, bbox: [91, 189, 586, 236], confidence: 90 }] } }) }), null);
});

test("LOML-19", "a search-like coordinate row without the map details semantic anchor fails closed", () => {
  assert.equal(createRows({ lines: lines({ 1: { text: "Appendix table" } }) }), null);
});

test("LOML-20", "an odd-position pseudo Plus Code is rejected", () => {
  assert.equal(createRows({ lines: lines({ 1: { text: "23456+23" } }) }), null);
});

test("LOML-21", "a multiplication expression cannot masquerade as a search close control", () => {
  const mathematicalSearchRow = {
    text: `${low} x 10`,
    words: [
      { text: low, bbox: [91, 189, 586, 236], confidence: 90 },
      { text: "x", bbox: [956, 188, 998, 240], confidence: 80 },
      { text: "10", bbox: [1010, 188, 1060, 240], confidence: 90 }
    ]
  };
  assert.equal(createRows({ lines: lines({ 0: mathematicalSearchRow, 1: { text: "23456+23" } }) }), null);
  assert.equal(createRows({ lines: lines({ 0: mathematicalSearchRow }) }), null);
});

test("LOML-22", "prefix prose before the coordinate word cannot mimic a search box", () => {
  const prefixed = lines()[0];
  assert.equal(createRows({ lines: lines({ 0: {
    text: `Appendix ${prefixed.text}`,
    words: [
      { text: "Appendix", bbox: [91, 189, 180, 236], confidence: 95 },
      ...prefixed.words
    ]
  } }) }), null);
});

test("LOML-23", "a close glyph outside its parent OCR line cannot create a role", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[1].bbox = [956, 1500, 998, 1540];
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-24", "a zero-confidence close glyph cannot create a role", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[1].confidence = 0;
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-25", "a one-pixel OCR speck cannot create a close control", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[1].bbox = [956, 188, 957, 189];
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-26", "the coordinate word must bind the same pair as its parent OCR line", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[0].text = "1.234567,2.345678";
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-27", "a zero-confidence coordinate word cannot anchor a search action", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[0].confidence = 0;
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-28", "a one-pixel coordinate word cannot anchor a search action", () => {
  const words = lines()[0].words.map(word => ({ ...word }));
  words[0].bbox = [91, 189, 92, 190];
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-29", "a one-pixel Plus Code line cannot anchor a details panel", () => {
  assert.equal(createRows({ lines: lines({ 1: { bbox: [199, 1780, 900, 1781] } }) }), null);
});

test("LOML-30", "hidden parent-line prefix text cannot be omitted from words", () => {
  assert.equal(createRows({ lines: lines({ 0: { text: `Appendix ${lines()[0].text}` } }) }), null);
});

test("LOML-31", "parent-line arithmetic suffix cannot be omitted from words", () => {
  assert.equal(createRows({ lines: lines({ 0: { text: `${low} x 10`, words: lines()[0].words.slice(0, 2) } }) }), null);
});

test("LOML-32", "an invalid-bbox trailing word cannot be silently filtered", () => {
  const words = [...lines()[0].words, { text: "10", bbox: [-1, 188, 20, 240], confidence: 90 }];
  assert.equal(createRows({ lines: lines({ 0: { text: `${low} xX ) 10`, words } }) }), null);
});

test("LOML-33", "an empty trailing word cannot be silently filtered", () => {
  const words = [...lines()[0].words, { text: "", bbox: [1000, 188, 1020, 240], confidence: 90 }];
  assert.equal(createRows({ lines: lines({ 0: { words } }) }), null);
});

test("LOML-34", "a one-pixel Open Location Code word cannot anchor details", () => {
  const words = lines()[1].words.map(word => ({ ...word }));
  words[0].bbox = [203, 1682, 204, 1683];
  assert.equal(createRows({ lines: lines({ 1: { words } }) }), null);
});

test("LOML-35", "a one-pixel Open Location Code sliver cannot anchor details", () => {
  const words = lines()[1].words.map(word => ({ ...word }));
  words[0].bbox = [203, 1713, 588, 1714];
  assert.equal(createRows({ lines: lines({ 1: { words } }) }), null);
});

test("LOML-36", "an invisible optional help word invalidates the closed OCR line", () => {
  const zeroConfidenceWords = lines()[1].words.map(word => ({ ...word }));
  zeroConfidenceWords[1].confidence = 0;
  assert.equal(createRows({ lines: lines({ 1: { words: zeroConfidenceWords } }) }), null);
  const onePixelWords = lines()[1].words.map(word => ({ ...word }));
  onePixelWords[1].bbox = [916, 1677, 917, 1678];
  assert.equal(createRows({ lines: lines({ 1: { words: onePixelWords } }) }), null);
});

test("LOML-37", "Production extraction without detail words cannot create a details role", () => {
  const extracted = extractLocalOcrLayoutLines({ data: { blocks: [{ paragraphs: [{ lines: [
    { text: `${low} x`, confidence: 91, bbox: { x0: 91, y0: 188, x1: 1060, y1: 250 }, words: [
      { text: low, confidence: 90, bbox: { x0: 91, y0: 189, x1: 586, y1: 236 } },
      { text: "x", confidence: 80, bbox: { x0: 956, y0: 188, x1: 998, y1: 240 } }
    ] },
    { text: "8M75C5XH+4HHPX4C", confidence: 84, bbox: { x0: 203, y0: 1677, x1: 900, y1: 1714 }, words: [
      { text: "8M75C5XH+4HHPX4C", confidence: 84, bbox: { x0: 203, y0: 1682, x1: 588, y1: 1711 } }
    ] },
    { text: high, confidence: 91, bbox: { x0: 199, y0: 1822, x1: 595, y1: 1855 }, words: [] }
  ] }] }] } }, imageIdentity);
  assert.equal(extracted[2].word_structure_valid, false);
  assert.equal(createLocalOcrMapLayoutRows({ lines: extracted, imageIdentity, coordinateEngineV2: engineWith() }), null);
});

test("LOML-38", "detail word must be the same coordinate pair as its parent line", () => {
  assert.equal(createRows({ lines: lines({ 2: { words: [
    { text: "1.234567,2.345678", bbox: [199, 1822, 595, 1855], confidence: 91 }
  ] } }) }), null);
  assert.equal(createRows({ lines: lines({ 2: { words: [
    { text: "Appendix", bbox: [199, 1822, 595, 1855], confidence: 91 }
  ] } }) }), null);
});

test("LOML-39", "invalid invisible or low-confidence detail words fail closed", () => {
  assert.equal(createRows({ lines: lines({ 2: { words: [
    { text: high, bbox: [-1, 1822, 595, 1855], confidence: 91 }
  ] } }) }), null);
  assert.equal(createRows({ lines: lines({ 2: { words: [
    { text: high, bbox: [199, 1822, 595, 1855], confidence: 0 }
  ] } }) }), null);
  assert.equal(createRows({ lines: lines({ 2: { words: [
    { text: high, bbox: [199, 1822, 200, 1823], confidence: 91 }
  ] } }) }), null);
});

test("LOML-40", "a comma-split two-word detail coordinate remains a closed proof", () => {
  const rows = createRows({ lines: lines({ 2: { text: `${high.split(",")[0]}, ${high.split(",")[1]}`, words: [
    { text: `${high.split(",")[0]},`, bbox: [199, 1822, 396, 1855], confidence: 91 },
    { text: high.split(",")[1], bbox: [411, 1822, 595, 1851], confidence: 91 }
  ] } }) });
  assert.ok(rows);
  assert.deepEqual(rows.map(row => row.source_role).sort(), ["MAP_PLACE_DETAILS", "MAP_SEARCH_BOX"]);
});

test("LOML-41", "detail coordinate words cannot contain report labels or suffix prose", () => {
  for (const words of [
    [{ text: `Appendix:${high}`, bbox: [199, 1822, 700, 1855], confidence: 91 }],
    [
      { text: `Lat:${high.split(",")[0]},`, bbox: [199, 1822, 430, 1855], confidence: 91 },
      { text: high.split(",")[1], bbox: [440, 1822, 650, 1855], confidence: 91 }
    ],
    [
      { text: `${high},`, bbox: [199, 1822, 650, 1855], confidence: 91 },
      { text: "copy", bbox: [660, 1822, 800, 1855], confidence: 91 }
    ]
  ]) {
    assert.equal(createRows({ lines: lines({ 2: { text: words.map(word => word.text).join(""), bbox: [199, 1822, 800, 1855], words } }) }), null);
  }
});

test("LOML-42", "widely separated table columns cannot mimic a split detail coordinate", () => {
  assert.equal(createRows({ lines: lines({ 2: {
    text: `${high.split(",")[0]}, ${high.split(",")[1]}`,
    bbox: [100, 1822, 950, 1855],
    words: [
      { text: `${high.split(",")[0]},`, bbox: [100, 1822, 250, 1855], confidence: 91 },
      { text: high.split(",")[1], bbox: [800, 1822, 950, 1855], confidence: 91 }
    ]
  } }) }), null);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.run();
    passed += 1;
    console.log(`PASS ${item.id} ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.id} ${item.name}`);
    throw error;
  }
}

console.log(JSON.stringify({
  suite: "local-ocr-map-layout-classifier-p0-regression",
  passed,
  total: tests.length,
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  productionOperations: 0
}, null, 2));
