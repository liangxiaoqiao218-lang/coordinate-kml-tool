import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  ONE_SHOT_ACQUISITION_CONFORMANCE_REASON,
  ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS,
  ONE_SHOT_STRUCTURED_FAMILY,
  buildOneShotStructuredFamilyPrompt,
  buildPrimaryRouteDecision,
  classifyOneShotStructuredFamily,
  createOneShotAcquisitionContract,
  validateOneShotAcquisitionContract
} from "../server/recognition/family-primary-routing.js";
import {
  RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE,
  RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE,
  RecognitionBudget
} from "../server/coordinate-finalizer/recognition-deadline.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import {
  LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON,
  LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS,
  normalizeLocalOcrStructuredEvidence
} from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";

const tests = [];
const test = (id, name, fn) => tests.push({ id, name, fn });
const route = (text, layoutLines = []) => classifyOneShotStructuredFamily({ text, layoutLines });

function makeBmp(width = 1200, height = 2400, marker = 0) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes, marker);
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

const syntheticImageBuffer = makeBmp();
const syntheticImageIdentity = createCoordinateImageIdentity(
  { buffer: syntheticImageBuffer, mimetype: "image/bmp", size: syntheticImageBuffer.length },
  { requestId: "one-shot-spatial-source-provenance", page: 1 }
);

function syntheticLayoutLines(text, { page = 1, resultRevision = 1, overlap = false, reverseVisualOrder = false } = {}) {
  return String(text).split("\n").filter(Boolean).map((lineText, index) => {
    const visualIndex = reverseVisualOrder ? Math.max(0, String(text).split("\n").filter(Boolean).length - index - 1) : index;
    const top = overlap ? 20 : 20 + (visualIndex * 42);
    return {
      text: lineText,
      bbox: [20, top, 1120, top + 24],
      confidence: 0.99,
      local_line_index: index,
      page,
      resultRevision
    };
  });
}

function syntheticWordLayout(rows, { overlap = false, missingWordBox = false } = {}) {
  return rows.map((fields, lineIndex) => {
    const values = Array.isArray(fields) ? fields : [fields];
    const top = overlap ? 40 + lineIndex : 40 + (lineIndex * 48);
    const columnWidth = Math.floor(1020 / Math.max(1, values.length));
    const words = values.flatMap((field, fieldIndex) => String(field).split(/\s+/u).filter(Boolean).map((word, wordIndex) => {
      const x0 = 30 + (fieldIndex * columnWidth) + (wordIndex * 128);
      return {
        text: word,
        bbox: missingWordBox && lineIndex === 0 && fieldIndex === 0 && wordIndex === 0
          ? null
          : [x0, top, x0 + Math.max(42, word.length * 11), top + 26],
        confidence: 96
      };
    }));
    return {
      text: values.join(" "),
      bbox: [20, top, 1120, top + 28],
      confidence: 96,
      words,
      word_structure_valid: true,
      local_line_index: lineIndex,
      page: 1,
      resultRevision: 1
    };
  });
}

function normalizedWordEvidence(rows, options = {}) {
  return normalizeLocalOcrStructuredEvidence({
    sourceText: rows.map(fields => (Array.isArray(fields) ? fields.join(" ") : fields)).join("\n"),
    layoutLines: syntheticWordLayout(rows, options)
  });
}

test("R01", "labelled decimal single point selects one-shot single-point route", () => {
  const result = route("Longitude: 73.418205\nLatitude: 18.672914");
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  assert.equal(result.matched, true);
});

test("R02", "labelled DMS single point selects one-shot single-point route", () => {
  const result = route(`Longitude: 73°25'05.54\"E\nLatitude: 18°40'22.49\"N`);
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  assert.equal(result.matched, true);
  assert.match(buildOneShotStructuredFamilyPrompt({ family: result.family }), /original visible longitude value/);
});

test("R02B", "one labelled header plus one coordinate row is a complete single-point structure", () => {
  const decimal = route("Longitude | Latitude\n73.418205 | 18.672914");
  const dms = route(`Longitude | Latitude\n73°25'05.54\"E | 18°40'22.49\"N`);
  assert.equal(decimal.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  assert.equal(dms.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
});

test("R02C", "an extra unlabelled coordinate pair prevents single-point routing", () => {
  const decimal = route("Longitude: 73.418205\nLatitude: 18.672914\n74.1000 | 19.2000");
  const dms = route(`Longitude: 73°25'05.54\"E\nLatitude: 18°40'22.49\"N\n74°06'00.00\"E | 19°12'00.00\"N`);
  assert.equal(decimal.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(decimal.matched, false);
  assert.equal(dms.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(dms.matched, false);
});

test("R02D", "multiple coordinate pairs compressed into one OCR line never select single-point routing", () => {
  const repeatedLabels = route("Longitude: 73.418205 Latitude: 18.672914 Longitude: 74.1000 Latitude: 19.2000");
  const decimalHeader = route("Longitude | Latitude\n73.418205 | 18.672914 | 74.1000 | 19.2000");
  const dmsHeader = route(`Longitude | Latitude\n73°25'05.54\"E | 18°40'22.49\"N | 74°06'00.00\"E | 19°12'00.00\"N`);
  for (const result of [repeatedLabels, decimalHeader, dmsHeader]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.evidence.ambiguousMultiPairLineCount, 1);
  }
});

test("R02E", "mixed decimal and DMS values on one OCR row are ambiguous and review-only", () => {
  const mixed = route(`Longitude | Latitude\n73.418205 | 18°40'22.49\"N`);
  assert.equal(mixed.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(mixed.matched, false);
  assert.equal(mixed.evidence.ambiguousMultiPairLineCount, 1);
});

test("R02F", "a valid single-point structure plus any unbound coordinate component fails closed", () => {
  const candidates = [
    route("Longitude: 73.418205\nLatitude: 18.672914\nunbound 74.1000"),
    route(`Longitude: 73°25'05.54"E\nLatitude: 18°40'22.49"N\nunbound 74°06'00.00"E`),
    route("Longitude | Latitude\n73.418205 | 18.672914\nunbound 74.1000"),
    route(`Longitude | Latitude\n73°25'05.54"E | 18°40'22.49"N\nunbound 74°06'00.00"E`),
    route(`Longitude: 73°25'05.54"E\nLatitude: 18°40'22.49"N\nunbound 74.1000`)
  ];
  for (const result of candidates) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.ok(result.evidence.conflictingCoordinateLineCount >= 1);
  }
});

test("R02G", "one dual-axis label line cannot bind a second unlabelled value as a single point", () => {
  for (const result of [
    route("Longitude / Latitude: 73.418205\nunbound 18.672914"),
    route(`Longitude / Latitude: 73°25'05.54"E\nunbound 18°40'22.49"N`),
    route(`Longitude / Latitude: 73.418205\nunbound 18°40'22.49"N`),
    route("经度 / 纬度: 73.418205\n未绑定 18.672914")
  ]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R02H", "split-axis single point requires closed labels values and compatible DMS directions", () => {
  for (const result of [
    route("Longitude report version 1.2\nLatitude report version 2.3"),
    route("Longitude precision tolerance 0.01\nLatitude precision tolerance 0.02"),
    route(`Longitude: 18°40'01.10"N\nLatitude: 73°25'01.10"E`),
    route("East Longitude: -73.4101\nNorth Latitude: -18.6701")
  ]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R03", "four-row WGS84 table selects table primary route", () => {
  const result = route([
    "Point | Longitude | Latitude",
    "A | 73.4101 | 18.6701",
    "B | 73.4202 | 18.6702",
    "C | 73.4203 | 18.6803",
    "D | 73.4104 | 18.6804"
  ].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE);
  assert.equal(result.evidence.coordinateRowCount, 4);
});

test("R04", "twenty-row table is classified by header and labelled row continuity", () => {
  const rows = Array.from({ length: 20 }, (_, index) => (
    `${index + 1} | ${(70 + index / 100).toFixed(4)} | ${(20 + index / 100).toFixed(4)}`
  ));
  const result = route(["No | Longitude | Latitude", ...rows].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE);
  assert.equal(result.evidence.coordinateRowCount, 20);
});

test("R05", "repeated DMS headers and restarted labels preserve grouped family", () => {
  const result = route([
    "GROUP A",
    "Point | Latitude | Longitude",
    `A | 18°40'01.10\"N | 73°25'01.10\"E`,
    `B | 18°40'02.20\"N | 73°25'02.20\"E`,
    "GROUP B",
    "Point | Latitude | Longitude",
    `A | 19°41'03.30\"N | 74°26'03.30\"E`,
    `B | 19°41'04.40\"N | 74°26'04.40\"E`
  ].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED);
  assert.equal(result.evidence.groupHeadingCount, 2);
  assert.match(buildOneShotStructuredFamilyPrompt({ family: result.family }), /never connect points across groups/);
});

test("R05B", "duplicate OCR text and layout representations cannot fabricate group boundaries", () => {
  const lines = [
    "Point | Latitude | Longitude",
    `A | 18°40'01.10\"N | 73°25'01.10\"E`,
    `B | 18°40'02.20\"N | 73°25'02.20\"E`,
    `C | 18°40'03.30\"N | 73°25'03.30\"E`
  ];
  const result = route(lines.join("\n"), lines.map((text, index) => ({
    text,
    bbox: [5, index * 20 + 5, 300, index * 20 + 20]
  })));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE);
  assert.equal(result.evidence.repeatedHeaderCount, 1);
});

test("R05C", "clean table rows plus one conflicting row fail closed to generic review", () => {
  const decimalMultiPair = route([
    "Point | Longitude | Latitude",
    "A | 73.4101 | 18.6701",
    "B | 73.4202 | 18.6702",
    "C | 73.4303 | 18.6803",
    "D | 73.4404 | 18.6904 | 74.4404 | 19.6904"
  ].join("\n"));
  const decimalMixed = route([
    "Point | Longitude | Latitude",
    "A | 73.4101 | 18.6701",
    "B | 73.4202 | 18.6702",
    "C | 73.4303 | 18.6803",
    `D | 73.4404 | 18°40'22.49"N`
  ].join("\n"));
  const dmsMultiPair = route([
    "Point | Longitude | Latitude",
    `A | 73°25'01.10"E | 18°40'01.10"N`,
    `B | 73°25'02.20"E | 18°40'02.20"N`,
    `C | 73°25'03.30"E | 18°40'03.30"N`,
    `D | 73°25'04.40"E | 18°40'04.40"N | 74°26'04.40"E | 19°41'04.40"N`
  ].join("\n"));
  const dmsWithForeignDecimal = route([
    "Point | Longitude | Latitude",
    `A | 73°25'01.10"E | 18°40'01.10"N`,
    `B | 73°25'02.20"E | 18°40'02.20"N`,
    `C | 73°25'03.30"E | 18°40'03.30"N`,
    "unbound 73.4404"
  ].join("\n"));
  for (const result of [decimalMultiPair, decimalMixed, dmsMultiPair, dmsWithForeignDecimal]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.ok(result.evidence.conflictingCoordinateLineCount >= 1);
  }
  assert.equal(decimalMultiPair.evidence.ambiguousMultiPairLineCount, 1);
  assert.equal(decimalMixed.evidence.ambiguousMultiPairLineCount, 1);
  assert.equal(dmsMultiPair.evidence.ambiguousMultiPairLineCount, 1);
});

test("R05D", "report prose and unrelated labels cannot fabricate a bound coordinate table", () => {
  const decimalSeparateProse = route([
    "This report discusses longitude accuracy.",
    "A later paragraph discusses latitude accuracy.",
    "73.4101 | 18.6701",
    "74.4202 | 19.6702",
    "75.4303 | 20.6803"
  ].join("\n"));
  const decimalCombinedProse = route([
    "This report compares longitude and latitude terminology.",
    "73.4101 | 18.6701",
    "74.4202 | 19.6702",
    "75.4303 | 20.6803"
  ].join("\n"));
  const dmsWithUnrelatedAppendixLabels = route([
    "This report compares longitude and latitude terminology.",
    `73°25'01.10"E | 18°40'01.10"N`,
    `73°25'02.20"E | 18°40'02.20"N`,
    `73°25'03.30"E | 18°40'03.30"N`,
    "A appendix",
    "B appendix",
    "C appendix"
  ].join("\n"));
  for (const result of [decimalSeparateProse, decimalCombinedProse, dmsWithUnrelatedAppendixLabels]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R05E", "axis prose punctuation and numeric pseudo-headers cannot fabricate a table", () => {
  const candidates = [
    route("This note compares longitude and latitude, not a table.\n73.4101 | 18.6701"),
    route([
      "This note compares longitude and latitude; see appendix.",
      "A | 73.4101 | 18.6701",
      "B | 74.4202 | 19.6702",
      "C | 75.4303 | 20.6803"
    ].join("\n")),
    route([
      "Note, longitude and latitude terminology",
      "A | 73.4101 | 18.6701",
      "B | 74.4202 | 19.6702",
      "C | 75.4303 | 20.6803"
    ].join("\n")),
    route("Longitude | Latitude | 73 | 18\n73.4101 | 18.6701"),
    route([
      "Point | Longitude | Latitude",
      "A | 73.4101 | 18.6701",
      "C | 74.4202 | 19.6702",
      "D | 75.4303 | 20.6803"
    ].join("\n"))
  ];
  for (const result of candidates) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R05F", "table rows must match declared axis and label columns exactly", () => {
  const candidates = [
    route([
      "Longitude | Latitude | Point",
      "A | 73.4101 | 18.6701",
      "B | 74.4202 | 19.6702",
      "C | 75.4303 | 20.6803"
    ].join("\n")),
    route([
      "Point | Longitude | Latitude",
      "A | 999 | 73.4101 | 18.6701",
      "B | 1000 | 74.4202 | 19.6702",
      "C | 1001 | 75.4303 | 20.6803"
    ].join("\n")),
    route("Longitude | Latitude\n73.4101 | 18.6701 | 999"),
    route([
      "Point | Longitude | Latitude",
      `A | 999 | 73°25'01.10"E | 18°40'01.10"N`,
      `B | 1000 | 73°25'02.20"E | 18°40'02.20"N`,
      `C | 1001 | 73°25'03.30"E | 18°40'03.30"N`
    ].join("\n"))
  ];
  for (const result of candidates) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R06", "map search, details, and Plus Code require separate layout regions", () => {
  const text = [
    "Search 73.41820, 18.67291",
    "Place details 73.418205, 18.672914",
    "Plus Code 7JCPMC9C+5P"
  ].join("\n");
  const layoutLines = [
    { text: "Search 73.41820, 18.67291", bbox: [10, 10, 260, 50] },
    { text: "Place details 73.418205, 18.672914", bbox: [10, 210, 300, 260] }
  ];
  const result = route(text, layoutLines);
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT);
  assert.equal(result.evidence.layoutRegionCount, 2);
  assert.match(buildOneShotStructuredFamilyPrompt({ family: result.family }), /Do not merge values/);
});

test("R06B", "map evidence with an additional ambiguous source region fails closed", () => {
  const text = [
    "Search 73.41820, 18.67291",
    "Place details 73.418205, 18.672914",
    "Unknown region 73.51, 18.71, 74.52, 19.72",
    "Plus Code 7JCPMC9C+5P"
  ].join("\n");
  const layoutLines = [
    { text: "Search 73.41820, 18.67291", bbox: [10, 10, 260, 50] },
    { text: "Place details 73.418205, 18.672914", bbox: [10, 210, 300, 260] },
    { text: "Unknown region 73.51, 18.71, 74.52, 19.72", bbox: [10, 310, 360, 360] }
  ];
  const result = route(text, layoutLines);
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(result.matched, false);
  assert.equal(result.evidence.ambiguousMultiPairLineCount, 1);
  assert.equal(result.evidence.conflictingCoordinateLineCount, 1);
});

test("R06C", "map route requires distinct non-overlapping Search and Details regions bound to OCR text", () => {
  const text = [
    "Search 73.41820, 18.67291",
    "Place details 73.418205, 18.672914",
    "Plus Code 7JCPMC9C+5P"
  ].join("\n");
  const duplicateSearch = route(text, [
    { text: "Search 73.41820, 18.67291", bbox: [10, 10, 260, 50] },
    { text: "Search 73.41820, 18.67291", bbox: [10, 10, 260, 50] }
  ]);
  const overlappingRoles = route(text, [
    { text: "Search 73.41820, 18.67291", bbox: [10, 10, 260, 50] },
    { text: "Place details 73.418205, 18.672914", bbox: [10, 10, 260, 50] }
  ]);
  const unrelatedLayout = route(text, [
    { text: "Unbound 73.41820, 18.67291", bbox: [10, 10, 260, 50] },
    { text: "Unbound 73.418205, 18.672914", bbox: [10, 210, 300, 260] }
  ]);
  for (const result of [duplicateSearch, overlappingRoles, unrelatedLayout]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R06D", "projected evidence mixed into a valid map structure fails closed before map routing", () => {
  const baseLines = [
    "Search 73.41820, 18.67291",
    "Place details 73.418205, 18.672914",
    "Plus Code 7JCPMC9C+5P"
  ];
  const layoutLines = [
    { text: baseLines[0], bbox: [10, 10, 260, 50] },
    { text: baseLines[1], bbox: [10, 210, 300, 260] }
  ];
  for (const suffix of [
    ["Easting: 500000"],
    ["X: 500000"],
    ["UTM coordinates", "Easting: 500000", "Northing: 2065000"]
  ]) {
    const result = route([...baseLines, ...suffix].join("\n"), layoutLines);
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.match(result.reason, /^mixed_map_and_projected_structure$/u);
  }
});

test("R07", "complete explicit projected CRS evidence selects projected primary", () => {
  const result = route([
    "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100",
    "B | 500200 | 2065200"
  ].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(result.evidence.projectedEvidenceComplete, true);
});

test("R07B", "MGRS requires explicit datum zone hemisphere and axis order", () => {
  const complete = route("MGRS | Datum WGS 84 | Zone 43N | Hemisphere N\nAxis order X | Y\nMGRS | 43Q | AB | 12345 | 67890");
  const incomplete = route("MGRS | Zone 43\nPOINT | A | synthetic-grid-cell");
  assert.equal(complete.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(complete.matched, true);
  assert.equal(incomplete.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(incomplete.matched, false);
});

test("R07C", "projected tables require visible row labels and never synthesize provenance", () => {
  const unlabeled = route([
    "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N",
    "Easting | Northing",
    "500100 | 2065100"
  ].join("\n"));
  assert.equal(unlabeled.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(unlabeled.matched, false);
  assert.equal(unlabeled.evidence.projectedEvidenceComplete, false);
});

test("R08", "incomplete projected CRS evidence remains generic review", () => {
  const result = route("UTM coordinates\nPoint | X | Y\nA | 500100 | 2065100");
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(result.matched, false);
  assert.equal(result.reason, "projected_crs_evidence_incomplete");
});

test("R08B", "foreign integer projected axes block WGS84 and DMS primary routing", () => {
  const candidates = [
    route("Longitude: 73.4101\nLatitude: 18.6701\nEasting: 500000"),
    route([
      "Point | Longitude | Latitude",
      "A | 73.4101 | 18.6701",
      "B | 74.4202 | 19.6702",
      "C | 75.4303 | 20.6803",
      "Easting: 500000"
    ].join("\n")),
    route([
      "Point | Longitude | Latitude",
      `A | 73°25'01.10"E | 18°40'01.10"N`,
      `B | 73°25'02.20"E | 18°40'02.20"N`,
      `C | 73°25'03.30"E | 18°40'03.30"N`,
      "Northing: 2065000"
    ].join("\n")),
    route("Longitude: 73.4101\nLatitude: 18.6701\nX: 500000")
  ];
  for (const result of candidates) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
  }
});

test("R08C", "out-of-range decimal table coordinates cannot select a WGS84 primary route", () => {
  const result = route([
    "Point | Longitude | Latitude",
    "A | 999.1000 | 18.6701",
    "B | 74.4202 | 190.6702",
    "C | 75.4303 | 20.6803"
  ].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(result.matched, false);
});

test("R08D", "projected metadata without a bound projected or MGRS coordinate row remains review-only", () => {
  for (const text of [
    "Projection UTM\nDatum WGS 84\nZone 43N\nHemisphere N\nAxis order X | Y",
    "Projection UTM documentation\nDatum WGS 84\nZone 43N\nNorthern hemisphere\nThe report discusses Easting accuracy before Northing accuracy.",
    "MGRS\nDatum WGS 84\nZone 43N\nHemisphere N\nAxis order X | Y"
  ]) {
    const result = route(text);
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_incomplete");
    assert.equal(result.evidence.projectedCoordinateRowCount, 0);
  }
});

test("R08E", "projected CRS zone hemisphere and MGRS precision must be valid and consistent", () => {
  const inconsistentHemisphere = route([
    "Projection UTM",
    "Datum WGS 84",
    "Zone 43N",
    "Hemisphere S",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ].join("\n"));
  const invalidZone = route([
    "Projection UTM",
    "Datum WGS 84",
    "Zone 99N",
    "Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ].join("\n"));
  const mgrsMetadataMismatch = route([
    "MGRS | Datum WGS 84 | Zone 44N | Hemisphere S",
    "Axis order X | Y",
    "MGRS | 43Q | AB | 12345 | 67890"
  ].join("\n"));
  const invalidMgrsZone = route([
    "MGRS | Datum WGS 84 | Zone 43N | Hemisphere N",
    "Axis order X | Y",
    "MGRS | 0Q | AB | 12345 | 67890"
  ].join("\n"));
  const unequalMgrsPrecision = route([
    "MGRS | Datum WGS 84 | Zone 43N | Hemisphere N",
    "Axis order X | Y",
    "MGRS | 43Q | AB | 1234 | 67890"
  ].join("\n"));
  for (const result of [inconsistentHemisphere, invalidZone, mgrsMetadataMismatch, invalidMgrsZone, unequalMgrsPrecision]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R08F", "a bound X Y table cannot be ignored by a WGS84 table route", () => {
  const result = route([
    "Point | Longitude | Latitude",
    "A | 73.4101 | 18.6701",
    "B | 74.4202 | 19.6702",
    "C | 75.4303 | 20.6803",
    "Point | X | Y",
    "1 | 500000 | 2065000"
  ].join("\n"));
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(result.matched, false);
});

test("R08G", "Datum EPSG zone and hemisphere identities must be unique and compatible", () => {
  const baseRows = ["Point | Easting | Northing", "A | 500100 | 2065100"];
  const candidates = [
    ["Projection UTM", "Datum WGS84", "Datum NAD83", "Zone 43N", "Hemisphere N", ...baseRows],
    ["Projection UTM", "EPSG:32643", "Zone 44S", "Hemisphere S", ...baseRows],
    ["Projection UTM", "EPSG:4326", "Zone 43N", "Hemisphere N", ...baseRows],
    ["Projection UTM", "EPSG:32643", "EPSG:32744", "Zone 43N", "Hemisphere N", ...baseRows],
    ["Projection UTM", "Datum WGS84", "Zone 43Z", "Hemisphere N", ...baseRows]
  ];
  for (const lines of candidates) {
    const result = route(lines.join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
  }
});

test("R08H", "a CRS metadata field cannot hide a second candidate after the first value", () => {
  const baseRows = ["Point | Easting | Northing", "A | 500100 | 2065100"];
  const candidates = [
    ["Projection UTM", "Datum: WGS84 / NAD83", "Zone 43N", "Hemisphere N", ...baseRows],
    ["Projection UTM", "Datum WGS84", "Zone: 43N / 44S", "Hemisphere N", ...baseRows],
    ["Projection UTM", "Datum WGS84", "Zone 43N", "Hemisphere: N / S", ...baseRows],
    ["Projection UTM", "EPSG:32643 / 32744", "Zone 43N", "Hemisphere N", ...baseRows]
  ];
  for (const lines of candidates) {
    const result = route(lines.join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
  }
});

test("R08I", "all projected and MGRS coordinate evidence must be valid bound and complete", () => {
  const malformedProjectedRow = route([
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100",
    "B | 500200 | 2065200 | 999"
  ].join("\n"));
  const unpairedAxis = route([
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100",
    "Easting: 500200"
  ].join("\n"));
  const malformedMgrs = route([
    "MGRS | 43Q | AA | 12345 | 12345",
    "MGRS | 43Q | AA | 123 | 12345",
    "Datum WGS84 | Zone 43Q | Hemisphere N",
    "Axis order: X | Y"
  ].join("\n"));
  for (const result of [malformedProjectedRow, unpairedAxis, malformedMgrs]) {
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
    assert.ok(result.evidence.projectedConflictLineCount >= 1);
  }
});

test("R08J", "a projected row with one malformed or missing declared axis is a conflict", () => {
  const prefix = [
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ];
  for (const badRow of [
    "B | 500200 | malformed",
    "B | 500200 |",
    "B | 500200 | 20"
  ]) {
    const result = route([...prefix, badRow].join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
    assert.ok(result.evidence.projectedConflictLineCount >= 1);
  }
});

test("R08K", "pure-text axis values inside a projected table are conflicts", () => {
  const labelledPrefix = [
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ];
  const unlabelledPrefix = [
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Easting | Northing",
    "500100 | 2065100"
  ];
  for (const lines of [
    [...labelledPrefix, "B | malformed | missing"],
    [...labelledPrefix, "B | east-value | north-value"],
    [...unlabelledPrefix, "malformed | missing"]
  ]) {
    const result = route(lines.join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
    assert.ok(result.evidence.projectedConflictLineCount >= 1);
  }
});

test("R08L", "a started projected segment cannot hide truncated rows behind invalid labels", () => {
  const labelledPrefix = [
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ];
  const unlabelledPrefix = [
    "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N",
    "Easting | Northing",
    "500100 | 2065100"
  ];
  for (const lines of [
    [...labelledPrefix, "LongLabel | 500200 | malformed"],
    [...labelledPrefix, " | 500200 | malformed"],
    [...labelledPrefix, "B | 500200"],
    [...unlabelledPrefix, "500200"]
  ]) {
    const result = route(lines.join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
    assert.ok(result.evidence.projectedConflictLineCount >= 1);
  }
});

test("R08M", "an invalid first projected row cannot be hidden by a later valid table segment", () => {
  const metadata = "Projection UTM | Datum WGS84 | Zone 43N | Hemisphere N";
  const firstHeader = "Point | Easting | Northing";
  const secondSegment = ["Point | Easting | Northing", "A | 500100 | 2065100"];
  const unlabelledSecondSegment = ["Easting | Northing", "500100 | 2065100"];
  const cases = [
    [metadata, firstHeader, "B | 500200", ...secondSegment],
    [metadata, firstHeader, "LongLabel | 500200 | malformed", ...secondSegment],
    [metadata, firstHeader, " | 500200 | malformed", ...secondSegment],
    [metadata, "Easting | Northing", "500200", ...unlabelledSecondSegment],
    [metadata, firstHeader, "٥٠٠٢٠٠", ...secondSegment],
    [metadata, firstHeader, "५००२००", ...secondSegment]
  ];
  for (const lines of cases) {
    const result = route(lines.join("\n"));
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
    assert.equal(result.reason, "projected_crs_evidence_inconsistent");
    assert.ok(result.evidence.projectedConflictLineCount >= 1);
  }
});

test("R09", "ordinary report, mathematics, columns, and pseudo-coordinates fail closed", () => {
  for (const text of [
    "Quarterly report 2026\nRevenue 73.41\nMargin 18.67",
    "f(x)=73.41 + 18.67\nlongitude of a wave is not a coordinate",
    "Column A 73.41\nColumn B 18.67",
    "73.4101 18.6701"
  ]) {
    assert.equal(route(text).family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(route(text).matched, false);
  }
});

test("R09A", "word-box normalization classifies the complete eleven-category synthetic matrix", () => {
  const decimalSingle = normalizedWordEvidence([
    ["Longitude"], ["61.234567"], ["Latitude"], ["14.765432"]
  ]);
  const dmsSingle = normalizedWordEvidence([
    ["Longitude"], [`61°14'04.44\"E`], ["Latitude"], [`14°45'55.56\"N`]
  ]);
  const fourRows = normalizedWordEvidence([
    ["Point", "Longitude", "Latitude"],
    ...Array.from({ length: 4 }, (_, index) => [
      String.fromCharCode(65 + index),
      (61.1 + index / 100).toFixed(4),
      (14.2 + index / 100).toFixed(4)
    ])
  ]);
  const longRows = normalizedWordEvidence([
    ["No", "Longitude", "Latitude"],
    ...Array.from({ length: 10 }, (_, index) => [String(index + 1), (62 + index / 100).toFixed(4), (15 + index / 100).toFixed(4)]),
    ["No", "Longitude", "Latitude"],
    ...Array.from({ length: 10 }, (_, index) => [String(index + 1), (63 + index / 100).toFixed(4), (16 + index / 100).toFixed(4)])
  ]);
  const groupedDms = normalizedWordEvidence([
    ["Location Group A"],
    ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `14°01'01.00\"N`, `61°01'01.00\"E`],
    ["B", `14°01'02.00\"N`, `61°01'02.00\"E`],
    ["Location Group B"],
    ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `15°01'01.00\"N`, `62°01'01.00\"E`],
    ["B", `15°01'02.00\"N`, `62°01'02.00\"E`]
  ]);
  const mapRoles = normalizedWordEvidence([
    ["Search"], ["14.7654, 61.2345"],
    ["Place details"], ["14.765432, 61.234567"],
    ["Plus Code 8FVC9G8F+5W"]
  ]);
  const utm = normalizedWordEvidence([
    ["CRS: WGS 84 / UTM zone 34N"],
    ["Point A"], ["Easting"], ["512340"], ["Northing"], ["1634560"]
  ]);
  const mgrs = normalizedWordEvidence([
    ["MGRS Datum WGS 84 Zone 34N Hemisphere N"],
    ["Axis order X | Y"],
    ["MGRS 34N AB 12345 67890"]
  ]);
  const otherProjected = normalizedWordEvidence([
    ["Projected CRS EPSG:3857"],
    ["Point", "Easting", "Northing"],
    ["A", "512340", "1634560"]
  ]);
  const ambiguous = normalizedWordEvidence([["Coordinate candidate 14.2 maybe 61.3"]]);
  const negative = normalizedWordEvidence([["Report 2026 area 61.3 km2 margin 14.2 percent"]]);

  const expectations = [
    [decimalSingle, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT, true],
    [dmsSingle, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT, true],
    [fourRows, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE, true],
    [longRows, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE, true],
    [groupedDms, ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED, true],
    [mapRoles, ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT, true],
    [utm, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, true],
    [mgrs, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, true],
    [otherProjected, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, true],
    [ambiguous, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW, false],
    [negative, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW, false]
  ];
  assert.equal(expectations.length, 11);
  for (const [evidence, family, matched] of expectations) {
    const result = route(evidence.text, evidence.layoutLines);
    assert.equal(result.family, family);
    assert.equal(result.matched, matched);
  }
});

test("R09B", "word-box reconstruction fails closed for missing overlapping or uncertain structure", () => {
  const missing = normalizedWordEvidence([
    ["Point", "Longitude", "Latitude"],
    ["A", "61.1001", "14.1001"],
    ["B", "61.1002", "14.1002"],
    ["C", "61.1003", "14.1003"]
  ], { missingWordBox: true });
  const overlap = normalizedWordEvidence([
    ["Longitude"], ["61.234567"], ["Latitude"], ["14.765432"]
  ], { overlap: true });
  const duplicateIdentityRows = [
    ["Point", "Longitude", "Latitude"],
    ["D", "71.6101", "25.7101"],
    ["E", "71.6102", "25.7102"],
    ["F", "71.6103", "25.7103"]
  ];
  const duplicateIdentity = normalizeLocalOcrStructuredEvidence({
    sourceText: duplicateIdentityRows.map(row => row.join(" ")).join("\n"),
    layoutLines: syntheticWordLayout(duplicateIdentityRows).map((line, index) => ({
      ...line,
      local_line_index: index === 2 ? 1 : line.local_line_index
    }))
  });
  const outsideRegionRows = [["Longitude"], ["71.612345"], ["Latitude"], ["25.712345"]];
  const outsideRegion = normalizeLocalOcrStructuredEvidence({
    sourceText: outsideRegionRows.map(row => row.join(" ")).join("\n"),
    layoutLines: syntheticWordLayout(outsideRegionRows).map((line, index) => ({
      ...line,
      words: index === 1
        ? line.words.map(word => ({ ...word, bbox: [word.bbox[0], word.bbox[1], 1180, word.bbox[3]] }))
        : line.words
    }))
  });
  for (const evidence of [missing, overlap, duplicateIdentity, outsideRegion]) {
    assert.equal(evidence.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
    const result = route(evidence.text, evidence.layoutLines);
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R09D", "source-layout coverage gaps and delimiter fallback fail closed", () => {
  const visibleRows = [
    ["Point", "Longitude", "Latitude"],
    ["K", "67.3101", "21.4101"],
    ["L", "67.3102", "21.4102"],
    ["M", "67.3103", "21.4103"]
  ];
  const sourceOnlyCandidate = "Z 68.9901 22.8801";
  const missingLayoutLine = normalizeLocalOcrStructuredEvidence({
    sourceText: `${visibleRows.map(row => row.join(" ")).join("\n")}\n${sourceOnlyCandidate}`,
    layoutLines: syntheticWordLayout(visibleRows)
  });
  assert.equal(missingLayoutLine.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
  assert.equal(missingLayoutLine.reason, LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.SOURCE_LAYOUT_COVERAGE_MISMATCH);
  assert.equal(missingLayoutLine.text, "");
  assert.equal(missingLayoutLine.layoutLines.length, 0);
  assert.equal(route(missingLayoutLine.text, missingLayoutLine.layoutLines).family,
    ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);

  const pipeRows = [
    ["Point", "Longitude", "Latitude"],
    ["K", "67.3101", "21.4101"],
    ["L", "67.3102", "21.4102"],
    ["M", "67.3103", "21.4103"]
  ];
  const pipeText = pipeRows.map(row => row.join(" | "));
  for (const invalidLineIndex of [0, 2]) {
    const layoutLines = syntheticWordLayout(pipeRows).map((line, index) => ({
      ...line,
      text: pipeText[index],
      words: index === invalidLineIndex ? [] : line.words,
      word_structure_valid: index !== invalidLineIndex
    }));
    const invalid = normalizeLocalOcrStructuredEvidence({
      sourceText: pipeText.join("\n"),
      layoutLines
    });
    assert.equal(invalid.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
    assert.equal(invalid.reason, LOCAL_OCR_STRUCTURE_NORMALIZATION_REASON.UNTRUSTED_WORD_STRUCTURE);
    assert.equal(invalid.text, "");
    const result = route(invalid.text, invalid.layoutLines);
    assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(result.matched, false);
  }
});

test("R09C", "normalized word-box evidence retains exact private observation coverage", () => {
  const splitDms = normalizedWordEvidence([
    ["Longitude"], [`61°14'04.44\"E`], ["Latitude"], [`14°45'55.56\"N`]
  ]);
  const splitDmsResult = assertConformant(
    contractFor(splitDms.text, splitDms.layoutLines),
    splitDms.text
  );
  assert.equal(splitDmsResult.counts.observedCandidateCount, 2);
  assert.equal(splitDmsResult.counts.boundCandidateCount, 2);
  assert.equal(splitDmsResult.counts.unassignedCandidateCount, 0);

  const decimalTable = normalizedWordEvidence([
    ["Point", "Longitude", "Latitude"],
    ["A", "61.1001", "14.1001"],
    ["B", "61.1002", "14.1002"],
    ["C", "61.1003", "14.1003"]
  ]);
  const tableResult = assertConformant(
    contractFor(decimalTable.text, decimalTable.layoutLines),
    decimalTable.text
  );
  assert.equal(tableResult.counts.observedCandidateCount, 4);
  assert.equal(tableResult.counts.boundCandidateCount, 4);
  assert.equal(tableResult.counts.unassignedCandidateCount, 0);

  const map = normalizedWordEvidence([
    ["Search"], ["14.7654, 61.2345"],
    ["Details"], ["14.765432, 61.234567"],
    ["Plus Code 8FVC9G8F+5W"]
  ]);
  const mapResult = assertConformant(
    contractFor(map.text, map.layoutLines),
    "MAP_SEARCH_BOX | 14.7654, 61.2345\nMAP_PLACE_DETAILS | 14.765432, 61.234567\nPLUS_CODE | 8FVC9G8F+5W"
  );
  assert.equal(mapResult.counts.unassignedCandidateCount, 0);

  const utm = normalizedWordEvidence([
    ["CRS: WGS 84 / UTM zone 34N"],
    ["Point A"], ["Easting"], ["512340"], ["Northing"], ["1634560"]
  ]);
  const projectedResult = assertConformant(
    contractFor(utm.text, utm.layoutLines),
    "CRS | WGS 84\nZONE | 34N\nHEMISPHERE | N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 512340 | 1634560"
  );
  assert.equal(projectedResult.counts.unassignedCandidateCount, 0);

  const otherProjected = normalizedWordEvidence([
    ["Projected CRS EPSG:3857"],
    ["Point", "Easting", "Northing"],
    ["A", "512340", "1634560"]
  ]);
  const otherProjectedResult = assertConformant(
    contractFor(otherProjected.text, otherProjected.layoutLines),
    "CRS | EPSG:3857\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 512340 | 1634560"
  );
  assert.equal(otherProjectedResult.counts.unassignedCandidateCount, 0);
});

test("R10", "country, filename, and fixed-value metadata cannot influence classifier", () => {
  const result = classifyOneShotStructuredFamily({
    text: "unclassified image",
    layoutLines: [],
    fileName: "country-fixed-coordinate.png",
    country: "Exampleland",
    knownCoordinate: "73.418205,18.672914"
  });
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(result.matched, false);
  const genericPrompt = buildOneShotStructuredFamilyPrompt({ family: result.family });
  assert.match(genericPrompt, /family is not reliably classified/);
  assert.doesNotMatch(genericPrompt, /Kyrgyz|Madagascar|Mozambique|country-fixed-coordinate|73\.418205/iu);
});

test("R11", "selected primary route is fail-closed and cannot fall through", () => {
  const evidence = route("Longitude: 73.418205\nLatitude: 18.672914");
  const decision = buildPrimaryRouteDecision({ family: evidence.family, evidence });
  assert.equal(decision.selected, true);
  assert.equal(decision.genericProviderAllowed, false);
  assert.equal(decision.failClosedOnSpecializedFailure, true);
});

test("R12", "request budget enforces one Provider call", () => {
  const budget = new RecognitionBudget({ startedAt: 0, deadlineMs: 55_000, now: () => 100, trace: false });
  budget.beginExecutionPhase();
  budget.assertCanStartProvider({ stageName: "pre_route" });
  budget.markProviderAttempted();
  assert.throws(
    () => budget.assertCanStartProvider({ stageName: "family_retry" }),
    error => error?.code === RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE
  );
});

test("R13", "request budget enforces one local OCR use across classification and fallback", () => {
  const budget = new RecognitionBudget({ startedAt: 0, deadlineMs: 55_000, now: () => 100, trace: false });
  budget.beginExecutionPhase();
  budget.assertCanStartLocalOcr({ stageName: "local_ocr" });
  assert.throws(
    () => budget.assertCanStartLocalOcr({ stageName: "local_ocr_fallback" }),
    error => error?.code === RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE
  );
});

test("R14", "candidate evidence without confirmation remains blocked from Map and KML", () => {
  const finalized = finalizeCoordinateResult({
    resultId: "sr08f7-one-shot",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "decimal_latlon",
    precisionMode: "wgs84-single-point",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: { type: "Point", coordinates: [73.418205, 18.672914] },
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    warnings: []
  });
  assert.notEqual(finalized.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.equal(finalized.requiresReview, true);
  assert.equal(finalized.kmlReady, false);
});

test("R15", "server integrates classification before selected one-shot Provider prompt", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const classifyIndex = source.indexOf("await runLocalOcrFamilyClassification");
  const promptIndex = source.indexOf("prompt: selectedProviderPrompt");
  assert.ok(classifyIndex >= 0 && promptIndex > classifyIndex);
  assert.match(source, /const useKyrgyzGkPromptFirst = false/);
  assert.match(source, /const useMozambiqueGeographicPromptFirst = false/);
  assert.match(source, /oneShotLocalOcrAttempted/);
  assert.match(source, /materializeMapLayoutRowsFromFamilyEvidence/);
  assert.match(source, /oneShotStructuredFamilyRoute\.reviewRequired/);
});

test("R16", "sanitized diagnostics contain only bounded route evidence", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const start = source.indexOf('console.log("One-shot structured family route:"');
  const diagnostic = source.slice(start, source.indexOf("const prompt =", start));
  assert.doesNotMatch(diagnostic, /rawText|imageDataUrl|providerResponse|cookie|apiKey|secret/iu);
  assert.match(diagnostic, /coordinateRowCount/);
  assert.match(diagnostic, /localOcrCallCount/);
});

function contractFor(text, layoutLines = null, options = {}) {
  const effectiveLayoutLines = Array.isArray(layoutLines) && layoutLines.length > 0
    ? layoutLines
    : syntheticLayoutLines(text, options);
  const selectedRoute = route(text, effectiveLayoutLines);
  return createOneShotAcquisitionContract({
    route: selectedRoute,
    sourceText: text,
    layoutLines: effectiveLayoutLines,
    imageIdentity: options.imageIdentity || syntheticImageIdentity,
    resultRevision: options.resultRevision || 1
  });
}

function assertConformant(contract, providerText) {
  const result = validateOneShotAcquisitionContract({ contract, providerText });
  assert.equal(result.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT);
  assert.equal(result.reason, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONFORMANT);
  return result;
}

function assertReview(contract, providerText, reason) {
  const result = validateOneShotAcquisitionContract({ contract, providerText });
  assert.equal(result.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  if (reason) assert.equal(result.reason, reason);
  return result;
}

test("R17", "request contract is bounded immutable and rejects digest tampering", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const contract = contractFor(source);
  assert.equal(Object.isFrozen(contract), true);
  assert.equal(Object.isFrozen(contract.structure), true);
  assert.doesNotMatch(JSON.stringify(contract), /64\.125001|12\.875002|Longitude:/u);
  assert.equal(Object.getOwnPropertySymbols(contract).length, 0);
  assert.equal(Object.hasOwn(contract, "privateBinding"), false);
  assertReview(
    { ...contract },
    source,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PRIVATE_BINDING_UNAVAILABLE
  );
  const tampered = { ...contract, family: ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE };
  assertReview(tampered, source, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONTRACT_INVALID);
});

test("R18", "decimal and DMS single-point evidence conforms only as one closed pair", () => {
  const decimalSource = "Longitude: 64.125001\nLatitude: 12.875002";
  const dmsSource = `Longitude: 64°07'30.00\"E\nLatitude: 12°52'30.00\"N`;
  const decimal = contractFor(decimalSource);
  const dms = contractFor(dmsSource);
  assertConformant(decimal, decimalSource);
  assertConformant(dms, dmsSource);
  assertReview(
    decimal,
    "Longitude: 63.500001\nLatitude: 11.500002",
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.VALUE_FIDELITY_MISMATCH
  );
  assertReview(
    dms,
    `Longitude: 63°07'30.00\"E\nLatitude: 11°52'30.00\"N`,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.VALUE_FIDELITY_MISMATCH
  );
  for (const providerText of [
    "Longitude: 63.500001\nLatitude: 11.500002\n64.1000 | 12.2000",
    "Longitude: 63.500001\nLongitude: 64.1000\nLatitude: 11.500002",
    `Longitude: 63°07'30.00\"E\nLatitude: 11.500002`,
    `Longitude: 11°30'00.00\"N\nLatitude: 63°30'00.00\"E`
  ]) assertReview(decimal, providerText);
});

test("R19", "WGS84 table contract detects header row and continuity loss", () => {
  const source = [
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.8001",
    "B | 64.2002 | 12.8002",
    "C | 64.2003 | 12.9003",
    "D | 64.1004 | 12.9004"
  ].join("\n");
  const contract = contractFor(source);
  assertConformant(contract, source);
  assertReview(
    contract,
    source.replace("B | 64.2002 | 12.8002", "B | 63.2002 | 11.8002"),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH
  );
  assertReview(contract, source.split("\n").slice(1).join("\n"));
  assertReview(contract, source.replace("C |", "B |"));
  assertReview(contract, source.split("\n").filter(line => !line.startsWith("C |")).join("\n"));
});

test("R20", "twenty-row table contract preserves exact row count and header", () => {
  const rows = Array.from({ length: 20 }, (_, index) => `${index + 1} | ${(61 + index / 1000).toFixed(4)} | ${(14 + index / 1000).toFixed(4)}`);
  const source = ["No | Longitude | Latitude", ...rows].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.structure.coordinateRowCount, 20);
  assertConformant(contract, source);
  assertReview(
    contract,
    ["No | Longitude | Latitude", rows[1], rows[0], ...rows.slice(2)].join("\n")
  );
  assertReview(contract, ["No | Longitude | Latitude", ...rows.slice(0, 19)].join("\n"));
});

test("R20B", "DMS table contract preserves original format header and exact rows", () => {
  const source = [
    "Point | Latitude | Longitude",
    `A | 12°01'01.00\"N | 64°01'01.00\"E`,
    `B | 12°01'02.00\"N | 64°01'02.00\"E`,
    `C | 12°01'03.00\"N | 64°01'03.00\"E`,
    `D | 12°01'04.00\"N | 64°01'04.00\"E`
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE);
  assertConformant(contract, source);
  assertReview(
    contract,
    source.replace(`B | 12°01'02.00\"N | 64°01'02.00\"E`, `B | 12°01'22.00\"N | 64°01'22.00\"E`),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH
  );
  assertReview(contract, source.split("\n").slice(1).join("\n"));
  assertReview(contract, source.replace(`C | 12°01'03.00\"N | 64°01'03.00\"E\n`, ""));
});

test("R20C", "DMS table binding accepts exact visible omitted markers and rejects value drift", () => {
  const source = [
    "Point | Latitude DMS | Longitude DMS",
    "A | 12°01 01.00N | 64°01 01.00E",
    "B | 12°01 02.00N | 64°01 02.00E",
    "C | 12°01 03.00N | 64°01 03.00E"
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_TABLE);
  assertConformant(contract, source);
  assertReview(
    contract,
    source.replace("B | 12°01 02.00N | 64°01 02.00E", "B | 12°01 22.00N | 64°01 02.00E"),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH
  );
});

test("R21", "grouped DMS contract preserves repeated headers and group boundaries", () => {
  const source = [
    "GROUP A",
    "Point | Latitude | Longitude",
    `A | 12°01'01.00\"N | 64°01'01.00\"E`,
    `B | 12°01'02.00\"N | 64°01'02.00\"E`,
    "GROUP B",
    "Point | Latitude | Longitude",
    `A | 13°01'01.00\"N | 65°01'01.00\"E`,
    `B | 13°01'02.00\"N | 65°01'02.00\"E`
  ].join("\n");
  const contract = contractFor(source);
  const provider = [
    "GROUP | A", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 12°01'01.00\"N | 64°01'01.00\"E`,
    `POINT | B | 12°01'02.00\"N | 64°01'02.00\"E`,
    "GROUP | B", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 13°01'01.00\"N | 65°01'01.00\"E`,
    `POINT | B | 13°01'02.00\"N | 65°01'02.00\"E`
  ].join("\n");
  assertConformant(contract, provider);
  assertReview(
    contract,
    provider.replace(`POINT | B | 13°01'02.00\"N | 65°01'02.00\"E`, `POINT | B | 14°01'02.00\"N | 66°01'02.00\"E`),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH
  );
  assertReview(contract, provider.replace("GROUP | B\n", ""), ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GROUP_BOUNDARY_MISMATCH);
  assertReview(contract, provider.replace("POINT | B | 13°01'02.00\"N", "POINT | A | 13°01'02.00\"N"));
  const movedAcrossGroups = provider
    .replace(`POINT | B | 12°01'02.00\"N | 64°01'02.00\"E\n`, "")
    .replace("GROUP | B\n", `GROUP | B\nPOINT | C | 15°01'03.00\"N | 67°01'03.00\"E\n`);
  assertReview(contract, movedAcrossGroups, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GROUP_BOUNDARY_MISMATCH);
});

test("R22", "map roles require pre-bound local layout and cannot be forged by Provider text", () => {
  const source = "Search 64.1250,12.8750\nPlace details 64.125001,12.875002\nPlus Code 7JCPTEST+5P";
  const layoutLines = syntheticLayoutLines(source);
  const provider = "MAP_SEARCH_BOX | 64.1250,12.8750\nMAP_PLACE_DETAILS | 64.125001,12.875002\nPLUS_CODE | 7JCPTEST+5P";
  assertConformant(contractFor(source, layoutLines), provider);
  assertReview(
    contractFor(source, layoutLines),
    provider.replace("64.125001,12.875002", "63.125001,11.875002"),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.MAP_ROLE_VALUE_MISMATCH
  );
  assertReview(contractFor("unclassified evidence"), provider, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GENERIC_REVIEW_ONLY);
});

test("R21B", "repeated DMS headers bind group rows even without visible group titles", () => {
  const source = [
    "Point | Latitude | Longitude",
    `A | 12°01'01.00\"N | 64°01'01.00\"E`,
    `B | 12°01'02.00\"N | 64°01'02.00\"E`,
    "Point | Latitude | Longitude",
    `A | 13°01'01.00\"N | 65°01'01.00\"E`,
    `B | 13°01'02.00\"N | 65°01'02.00\"E`
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED);
  assert.equal(contract.structure.groupBoundaryMode, "REPEATED_HEADERS");
  assert.deepEqual(contract.structure.groupRowCounts, [2, 2]);
  const provider = [
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 12°01'01.00\"N | 64°01'01.00\"E`,
    `POINT | B | 12°01'02.00\"N | 64°01'02.00\"E`,
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 13°01'01.00\"N | 65°01'01.00\"E`,
    `POINT | B | 13°01'02.00\"N | 65°01'02.00\"E`
  ].join("\n");
  assertConformant(contract, provider);
});

test("R21C", "grouped DMS binding accepts exact curved marker source representation", () => {
  const source = [
    "GROUP A",
    "Point | Latitude | Longitude",
    "A | 12°01’01.00”N | 64°01’01.00”E",
    "B | 12°01’02.00”N | 64°01’02.00”E",
    "GROUP B",
    "Point | Latitude | Longitude",
    "A | 13°01’01.00”N | 65°01’01.00”E",
    "B | 13°01’02.00”N | 65°01’02.00”E"
  ].join("\n");
  const provider = [
    "GROUP | A", "HEADER | Point | Latitude | Longitude",
    "POINT | A | 12°01’01.00”N | 64°01’01.00”E",
    "POINT | B | 12°01’02.00”N | 64°01’02.00”E",
    "GROUP | B", "HEADER | Point | Latitude | Longitude",
    "POINT | A | 13°01’01.00”N | 65°01’01.00”E",
    "POINT | B | 13°01’02.00”N | 65°01’02.00”E"
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED);
  assertConformant(contract, provider);
});

test("R23", "projected contract requires exact complete CRS identity", () => {
  const source = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100";
  const contract = contractFor(source);
  const provider = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 500100 | 2065100";
  assertConformant(contract, provider);
  assertReview(
    contract,
    provider.replace("500100 | 2065100", "500200 | 2065200"),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_VALUE_MISMATCH
  );
  assertReview(contract, provider.replace("43N", "44N"), ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_CRS_MISMATCH);
  assertReview(contract, provider.replace("Zone 43N | Hemisphere N", "Zone 43"), ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_CRS_MISMATCH);
  assertReview(contract, provider.replace("EASTING | NORTHING", "NORTHING | EASTING"), ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_CRS_MISMATCH);
});

test("R23B", "MGRS contract requires the same explicit CRS and bounded row count", () => {
  const source = "MGRS | Datum WGS 84 | Zone 43N | Hemisphere N\nAxis order X | Y\nMGRS | 43Q | AB | 12345 | 67890";
  const contract = contractFor(source);
  const provider = "CRS | Datum WGS 84\nZONE | 43N\nHEMISPHERE | N\nAXIS_ORDER | X | Y\nMGRS | 43Q | AB | 12345 | 67890";
  assertConformant(contract, provider);
  assertReview(
    contract,
    provider.replace("AB | 12345 | 67890", "CD | 23456 | 78901"),
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_VALUE_MISMATCH
  );
  assertReview(contract, provider.replace("43Q", "44Q"), ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.PROJECTED_CRS_MISMATCH);
});

test("R24", "generic review contract cannot be upgraded by authoritative-looking Provider output", () => {
  const contract = contractFor("ordinary report with no closed coordinate structure");
  const result = assertReview(contract, "Longitude: 63.500001\nLatitude: 11.500002");
  assert.equal(result.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
});

test("R25", "post-Provider conformance gate runs before parsing and geometry inference", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const conformanceIndex = source.indexOf("validateOneShotAcquisitionContract");
  const rawDmsIndex = source.indexOf("formatHandwrittenDmsRawRows", conformanceIndex);
  const parserIndex = source.indexOf("extractCoordinateLines", conformanceIndex);
  assert.ok(conformanceIndex >= 0 && rawDmsIndex > conformanceIndex && parserIndex > conformanceIndex);
  assert.match(source, /ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED/);
  assert.match(source, /rawText:\s*""/);
  assert.match(source, /coordinates:\s*""/);
});

test("R26", "contract diagnostics expose bounded enums and counts only", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const start = source.indexOf('console.log("One-shot acquisition conformance:"');
  const diagnostic = source.slice(start, source.indexOf("if (oneShotAcquisitionConformance.status", start));
  assert.ok(start >= 0);
  assert.match(diagnostic, /family|status|reason|counts|providerCallCount|localOcrCallCount|terminalState/);
  assert.doesNotMatch(diagnostic, /rawText|providerRawText|imageDataUrl|authorization|cookie|apiKey|secret|headers/iu);
});

test("R27", "selected family without canonical OCR spatial capability fails closed", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const selectedRoute = route(source);
  const contract = createOneShotAcquisitionContract({ route: selectedRoute, sourceText: source });
  const result = assertReview(
    contract,
    source,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE
  );
  assert.equal(result.counts.sourceRegionCount, 0);
});

test("R28", "decimal and DMS single points bind canonical image page revision rows and regions", () => {
  for (const source of [
    "Longitude: 64.125001\nLatitude: 12.875002",
    `Longitude: 64°07'30.00"E\nLatitude: 12°52'30.00"N`
  ]) {
    const result = assertConformant(contractFor(source), source);
    assert.ok(result.counts.sourceRegionCount >= 1);
  }
});

test("R29", "page revision overlap bounds and reading-order ambiguity fail spatial provenance closed", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const invalidLayouts = [
    syntheticLayoutLines(source, { page: 2 }),
    syntheticLayoutLines(source, { resultRevision: 2 }),
    syntheticLayoutLines(source, { overlap: true }),
    syntheticLayoutLines(source, { reverseVisualOrder: true }),
    syntheticLayoutLines(source).map((line, index) => index === 0 ? { ...line, bbox: [-1, 20, 100, 40] } : line),
    syntheticLayoutLines(source).map((line, index, lines) => ({
      ...line,
      text: lines[lines.length - index - 1].text
    }))
  ];
  for (const layoutLines of invalidLayouts) {
    const contract = contractFor(source, layoutLines);
    assertReview(contract, source, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE);
  }
});

test("R30", "identical visible values in distinct rows retain distinct spatial identities", () => {
  const source = [
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.8001",
    "B | 64.1001 | 12.8001",
    "C | 64.2003 | 12.9003"
  ].join("\n");
  const contract = contractFor(source);
  const result = assertConformant(contract, source);
  assert.equal(result.counts.coordinateRowCount, 3);
  assert.equal(result.counts.sourceRegionCount, 4);
  assertReview(contract, source.replace("B |", "A |"));
});

test("R31", "repeated headers keep independent source segments without resetting row provenance", () => {
  const source = [
    "Point | Latitude | Longitude",
    `A | 12°01'01.00"N | 64°01'01.00"E`,
    `B | 12°01'02.00"N | 64°01'02.00"E`,
    "Point | Latitude | Longitude",
    `A | 13°01'01.00"N | 65°01'01.00"E`,
    `B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  const provider = [
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 12°01'01.00"N | 64°01'01.00"E`,
    `POINT | B | 12°01'02.00"N | 64°01'02.00"E`,
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 13°01'01.00"N | 65°01'01.00"E`,
    `POINT | B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  const result = assertConformant(contractFor(source), provider);
  assert.equal(result.counts.sourceRegionCount, 6);
});

test("R31B", "grouped DMS rows cannot retain authority after crossing source regions", () => {
  const source = [
    "GROUP A", "Point | Latitude | Longitude",
    `A | 12°01'01.00"N | 64°01'01.00"E`,
    `B | 12°01'02.00"N | 64°01'02.00"E`,
    "GROUP B", "Point | Latitude | Longitude",
    `A | 13°01'01.00"N | 65°01'01.00"E`,
    `B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  const layoutLines = syntheticLayoutLines(source);
  [layoutLines[2].text, layoutLines[6].text] = [layoutLines[6].text, layoutLines[2].text];
  const contract = contractFor(source, layoutLines);
  const provider = [
    "GROUP | A", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 12°01'01.00"N | 64°01'01.00"E`, `POINT | B | 12°01'02.00"N | 64°01'02.00"E`,
    "GROUP | B", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 13°01'01.00"N | 65°01'01.00"E`, `POINT | B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  assertReview(contract, provider, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE);
});

test("R32", "map and projected families require private source regions without exposing them publicly", () => {
  const mapSource = "Search 64.1250,12.8750\nPlace details 64.125001,12.875002\nPlus Code 7JCPTEST+5P";
  const mapProvider = "MAP_SEARCH_BOX | 64.1250,12.8750\nMAP_PLACE_DETAILS | 64.125001,12.875002\nPLUS_CODE | 7JCPTEST+5P";
  const projectedSource = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100";
  const projectedProvider = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 500100 | 2065100";
  for (const [source, provider] of [[mapSource, mapProvider], [projectedSource, projectedProvider]]) {
    const contract = contractFor(source);
    assertConformant(contract, provider);
    const serialized = JSON.stringify(contract);
    assert.doesNotMatch(serialized, /asset_|image_sha256|bbox|sourceRegion|64\.1250|500100/u);
  }
});

test("R32B", "map role names and values cannot retain authority after source regions are exchanged", () => {
  const source = "Search 64.1250,12.8750\nPlace details 64.125001,12.875002\nPlus Code 7JCPTEST+5P";
  const provider = "MAP_SEARCH_BOX | 64.1250,12.8750\nMAP_PLACE_DETAILS | 64.125001,12.875002\nPLUS_CODE | 7JCPTEST+5P";
  const layoutLines = syntheticLayoutLines(source);
  [layoutLines[0].text, layoutLines[1].text] = [layoutLines[1].text, layoutLines[0].text];
  assertReview(
    contractFor(source, layoutLines),
    provider,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE
  );
});

test("R32C", "projected CRS and values cannot retain authority after row regions are exchanged", () => {
  const source = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100";
  const provider = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 500100 | 2065100";
  const layoutLines = syntheticLayoutLines(source);
  [layoutLines[1].text, layoutLines[2].text] = [layoutLines[2].text, layoutLines[1].text];
  assertReview(
    contractFor(source, layoutLines),
    provider,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE
  );
});

test("R33", "server binds spatial capability before Provider and emits only a bounded region count", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const classification = source.slice(
    source.indexOf("async function runLocalOcrFamilyClassification"),
    source.indexOf("function materializeMapLayoutRowsFromFamilyEvidence")
  );
  assert.match(classification, /createOneShotAcquisitionContract\(\{[\s\S]*layoutLines,[\s\S]*imageIdentity,[\s\S]*resultRevision:\s*1/);
  const start = source.indexOf('console.log("One-shot acquisition conformance:"');
  const diagnostic = source.slice(start, source.indexOf("if (oneShotAcquisitionConformance.status", start));
  assert.match(diagnostic, /sourceRegionCount/);
  assert.doesNotMatch(diagnostic, /image_sha256|request_asset_id|bbox|spatialIdentity|privateBinding/iu);
});

test("R34", "single-point observation set binds every locally observed candidate exactly once", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const result = assertConformant(contractFor(source), source);
  assert.equal(result.counts.observedCandidateCount, 2);
  assert.equal(result.counts.boundCandidateCount, 2);
  assert.equal(result.counts.unassignedCandidateCount, 0);
});

test("R35", "an extra locally observed coordinate candidate cannot remain unassigned", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const layoutLines = syntheticLayoutLines(source);
  layoutLines.push({
    text: "Z | 65.5000 | 13.5000",
    bbox: [20, 104, 1120, 128],
    confidence: 0.99,
    local_line_index: 2,
    page: 1,
    resultRevision: 1
  });
  const result = assertReview(
    contractFor(source, layoutLines),
    source,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE
  );
  assert.equal(result.counts.observedCandidateCount, 3);
  assert.equal(result.counts.boundCandidateCount, 2);
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("R36", "a duplicate local observation cannot be assigned twice", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const layoutLines = syntheticLayoutLines(source);
  layoutLines.push({
    ...layoutLines[1],
    bbox: [20, 104, 1120, 128],
    local_line_index: 2
  });
  const result = assertReview(
    contractFor(source, layoutLines),
    source,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE
  );
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("R37", "cross-family local observations make a selected map family ambiguous", () => {
  const source = "Search 64.1250,12.8750\nPlace details 64.125001,12.875002\nPlus Code 7JCPTEST+5P";
  const provider = "MAP_SEARCH_BOX | 64.1250,12.8750\nMAP_PLACE_DETAILS | 64.125001,12.875002\nPLUS_CODE | 7JCPTEST+5P";
  const layoutLines = syntheticLayoutLines(source);
  layoutLines.push({
    text: "A | 500100 | 2065100",
    bbox: [20, 146, 1120, 170],
    confidence: 0.99,
    local_line_index: 3,
    page: 1,
    resultRevision: 1
  });
  const result = assertReview(
    contractFor(source, layoutLines),
    provider,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_AMBIGUOUS
  );
  assert.equal(result.counts.observedCandidateCount, 4);
  assert.equal(result.counts.boundCandidateCount, 3);
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("R38", "twenty-row tables expose exact complete observation coverage", () => {
  const rows = Array.from({ length: 20 }, (_, index) => `${index + 1} | ${(61 + index / 1000).toFixed(4)} | ${(14 + index / 1000).toFixed(4)}`);
  const source = ["No | Longitude | Latitude", ...rows].join("\n");
  const result = assertConformant(contractFor(source), source);
  assert.equal(result.counts.observedCandidateCount, 21);
  assert.equal(result.counts.boundCandidateCount, 21);
  assert.equal(result.counts.unassignedCandidateCount, 0);
});

test("R39", "repeated headers preserve independent segments in the complete observation set", () => {
  const source = [
    "Point | Latitude | Longitude",
    `A | 12°01'01.00"N | 64°01'01.00"E`,
    `B | 12°01'02.00"N | 64°01'02.00"E`,
    "Point | Latitude | Longitude",
    `A | 13°01'01.00"N | 65°01'01.00"E`,
    `B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  const provider = [
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 12°01'01.00"N | 64°01'01.00"E`,
    `POINT | B | 12°01'02.00"N | 64°01'02.00"E`,
    "GROUP | REPEATED_HEADER_BOUNDARY", "HEADER | Point | Latitude | Longitude",
    `POINT | A | 13°01'01.00"N | 65°01'01.00"E`,
    `POINT | B | 13°01'02.00"N | 65°01'02.00"E`
  ].join("\n");
  const result = assertConformant(contractFor(source), provider);
  assert.equal(result.counts.observedCandidateCount, 6);
  assert.equal(result.counts.boundCandidateCount, 6);
  assert.equal(result.counts.unassignedCandidateCount, 0);
});

test("R40", "projected observations reject an additional unassigned projected row", () => {
  const source = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100";
  const provider = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 500100 | 2065100";
  const exact = assertConformant(contractFor(source), provider);
  assert.equal(exact.counts.observedCandidateCount, 5);
  assert.equal(exact.counts.unassignedCandidateCount, 0);
  const layoutLines = syntheticLayoutLines(source);
  layoutLines.push({
    text: "B | 500200 | 2065200",
    bbox: [20, 146, 1120, 170],
    confidence: 0.99,
    local_line_index: 3,
    page: 1,
    resultRevision: 1
  });
  const mismatch = assertReview(
    contractFor(source, layoutLines),
    provider,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE
  );
  assert.equal(mismatch.counts.unassignedCandidateCount, 1);
});

test("R41", "observation diagnostics expose bounded counts without candidate content", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const start = source.indexOf('console.log("One-shot acquisition conformance:"');
  const diagnostic = source.slice(start, source.indexOf("if (oneShotAcquisitionConformance.status", start));
  assert.match(diagnostic, /observedCandidateCount/);
  assert.match(diagnostic, /boundCandidateCount/);
  assert.match(diagnostic, /unassignedCandidateCount/);
  assert.doesNotMatch(diagnostic, /candidateText|observationIdentity|sourceRegionIdentity|bbox|imageDataUrl/iu);
});

test("R42", "a locally observed duplicate map role cannot be omitted by Provider output", () => {
  const source = [
    "Search 64.1250,12.8750",
    "Place details 64.125001,12.875002",
    "Plus Code 7JCPTEST+5P",
    "Plus Code 7JCPTEST+5P"
  ].join("\n");
  const provider = [
    "MAP_SEARCH_BOX | 64.1250,12.8750",
    "MAP_PLACE_DETAILS | 64.125001,12.875002",
    "PLUS_CODE | 7JCPTEST+5P"
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT);
  const result = assertReview(contract, provider);
  assert.notEqual(result.reason, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONFORMANT);
});

test("R43", "a duplicate local CRS observation cannot be compressed into one Provider field", () => {
  const source = [
    "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N",
    "Datum WGS 84",
    "Point | Easting | Northing",
    "A | 500100 | 2065100"
  ].join("\n");
  const provider = [
    "CRS | WGS 84",
    "ZONE | 43N",
    "HEMISPHERE | N",
    "AXIS_ORDER | EASTING | NORTHING",
    "POINT | A | 500100 | 2065100"
  ].join("\n");
  const result = assertReview(
    contractFor(source),
    provider,
    ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE
  );
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("R44", "WGS84 specialized primary validates the contract before coordinate parsing or usage consumption", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const blockStart = source.indexOf("if (wgs84PrimaryRoute.selected)");
  const blockEnd = source.indexOf("if (madagascarPrimaryRoute.selected)", blockStart);
  const block = source.slice(blockStart, blockEnd);
  const validationIndex = block.indexOf("validateOneShotAcquisitionContract");
  const parserIndex = block.indexOf("getWgs84TableCoordinatesInfo");
  const usageIndex = block.indexOf("consumeCoordinateUsage");
  assert.ok(validationIndex >= 0);
  assert.ok(parserIndex > validationIndex);
  assert.ok(usageIndex > validationIndex);
  assert.match(block, /ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED/);
});

test("R45", "moving a repeated WGS84 header boundary changes row segment identity", () => {
  const source = [
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.1001",
    "B | 64.1002 | 12.1002",
    "Point | Longitude | Latitude",
    "C | 65.1001 | 13.1001",
    "D | 65.1002 | 13.1002"
  ].join("\n");
  const provider = [
    "WGS84 Longitude Latitude Table",
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.1001",
    "Point | Longitude | Latitude",
    "B | 64.1002 | 12.1002",
    "C | 65.1001 | 13.1001",
    "D | 65.1002 | 13.1002"
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE);
  assert.equal(contract.structure.repeatedHeaderCount, 2);
  assertReview(contract, provider, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH);
});

test("R46", "an empty DMS group fails closed before a Provider can replace its heading", () => {
  const source = [
    "GROUP A",
    "Point | Latitude | Longitude",
    "GROUP B",
    "Point | Latitude | Longitude",
    `B1 | 13°01'01.00"N | 65°01'01.00"E`
  ].join("\n");
  const provider = [
    "GROUP | X",
    "HEADER | Point | Latitude | Longitude",
    "GROUP | B",
    "HEADER | Point | Latitude | Longitude",
    `POINT | B1 | 13°01'01.00"N | 65°01'01.00"E`
  ].join("\n");
  const contract = contractFor(source);
  assert.equal(contract.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assertReview(contract, provider, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GENERIC_REVIEW_ONLY);
});

test("R47", "fresh production-classification recovery matrix preserves complete trusted word-box authority", () => {
  const normalizeRows = (rows, sourceLines = null) => normalizeLocalOcrStructuredEvidence({
    sourceText: (sourceLines || rows.map(row => row.join(" "))).join("\n"),
    layoutLines: syntheticWordLayout(rows)
  });

  const splitDmsRows = [
    ["Longitude"], [`112°36'19.87\"W`], ["Latitude"], [`28°17'43.21\"N`]
  ];
  const splitDms = normalizeRows(splitDmsRows, [
    "Longitude", `112° 36' 19.87\" W`, "Latitude", `28° 17' 43.21\" N`
  ]);
  const splitRoute = route(splitDms.text, splitDms.layoutLines);
  const splitContract = contractFor(splitDms.text, splitDms.layoutLines);
  assert.equal(splitDms.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.equal(splitRoute.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  assert.equal(splitContract.format, "DMS_SINGLE_POINT");
  assert.match(buildOneShotStructuredFamilyPrompt({
    family: splitRoute.family,
    format: splitContract.format
  }), /requires the original visible DMS notation/u);
  assertConformant(splitContract, splitDms.text);
  const uncertainLineIdentity = normalizeRows(splitDmsRows, [
    "Longi tude", `112° 36' 19.87\" W`, "Latitude", `28° 17' 43.21\" N`
  ]);
  assert.equal(uncertainLineIdentity.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
  assert.equal(route(uncertainLineIdentity.text, uncertainLineIdentity.layoutLines).family,
    ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);

  const tableRows = [];
  for (let index = 0; index < 20; index += 1) {
    if (index === 0 || index === 10) tableRows.push(["Point", "Longitude", "Latitude"]);
    tableRows.push([
      String(index + 1),
      (101.7301 + (index * 0.0011)).toFixed(4),
      (-33.2401 - (index * 0.0011)).toFixed(4)
    ]);
  }
  const longTable = normalizeRows(tableRows);
  const longRoute = route(longTable.text, longTable.layoutLines);
  assert.equal(longRoute.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE);
  assert.equal(longRoute.evidence.coordinateRowCount, 20);
  assert.equal(longRoute.evidence.repeatedHeaderCount, 2);
  const longResult = assertConformant(contractFor(longTable.text, longTable.layoutLines), longTable.text);
  assert.equal(longResult.counts.observedCandidateCount, 22);
  assert.equal(longResult.counts.unassignedCandidateCount, 0);

  const groupedRows = [
    ["Location Group Kappa"], ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `31°11'21.11\"S`, `118°22'32.22\"E`],
    ["B", `31°11'22.22\"S`, `118°22'33.33\"E`],
    ["Location Group Lambda"], ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `32°12'23.33\"S`, `119°23'34.44\"E`],
    ["B", `32°12'24.44\"S`, `119°23'35.55\"E`]
  ];
  const grouped = normalizeRows(groupedRows);
  const groupedRoute = route(grouped.text, grouped.layoutLines);
  assert.equal(groupedRoute.family, ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED);
  assert.equal(groupedRoute.evidence.groupHeadingCount, 2);
  assert.equal(groupedRoute.evidence.repeatedHeaderCount, 2);

  const map = normalizeRows([
    ["Search"], ["31.4821", "-108.7314"],
    ["Place details"], ["31.482167", "-108.731467"],
    ["Plus Code"], ["75XHF7JF+V8"]
  ]);
  const mapRoute = route(map.text, map.layoutLines);
  assert.equal(mapRoute.family, ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT);
  assert.equal(mapRoute.evidence.layoutRegionCount, 2);
  assertConformant(
    contractFor(map.text, map.layoutLines),
    "MAP_SEARCH_BOX | 31.4821, -108.7314\nMAP_PLACE_DETAILS | 31.482167, -108.731467\nPLUS_CODE | 75XHF7JF+V8"
  );

  const utm = normalizeRows([
    ["WGS 84 / UTM zone 52S / EPSG:32752"],
    ["Point G"], ["Easting"], ["246810"], ["Northing"], ["6357913"]
  ]);
  const utmRoute = route(utm.text, utm.layoutLines);
  assert.equal(utmRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(utmRoute.evidence.projectedEvidenceComplete, true);
  assertConformant(
    contractFor(utm.text, utm.layoutLines),
    "CRS | WGS 84 EPSG:32752\nZONE | 52S\nHEMISPHERE | S\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 6357913"
  );

  const mgrs = normalizeRows([
    ["MGRS", "52H", "KV", "24680", "13579"],
    ["MGRS", "52H", "KW", "97531", "24680"]
  ]);
  const mgrsRoute = route(mgrs.text, mgrs.layoutLines);
  assert.equal(mgrsRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(mgrsRoute.reason, "explicit_projected_crs_structure");
  assertConformant(
    contractFor(mgrs.text, mgrs.layoutLines),
    "CRS | MGRS\nZONE | 52H\nHEMISPHERE | S\nAXIS_ORDER | EASTING | NORTHING\nMGRS | 52H | KV | 24680 | 13579\nMGRS | 52H | KW | 97531 | 24680"
  );

  const otherProjectedRows = [
    ["Projected CRS WGS 84 / Pseudo-Mercator EPSG:3857"],
    ["Point", "Easting", "Northing"],
    ["G", "246810", "135790"]
  ];
  const otherProjectedLayout = syntheticWordLayout(otherProjectedRows);
  let projectedTitleX = 30;
  otherProjectedLayout[0].words = otherProjectedLayout[0].words.map(word => {
    const width = Math.max(42, word.text.length * 11);
    const positioned = { ...word, bbox: [projectedTitleX, 40, projectedTitleX + width, 66] };
    projectedTitleX += width + 14;
    return positioned;
  });
  const otherProjected = normalizeLocalOcrStructuredEvidence({
    sourceText: otherProjectedRows.map(row => row.join(" ")).join("\n"),
    layoutLines: otherProjectedLayout
  });
  const otherRoute = route(otherProjected.text, otherProjected.layoutLines);
  assert.equal(otherRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(otherRoute.evidence.projectedEvidenceComplete, true);
  assertConformant(
    contractFor(otherProjected.text, otherProjected.layoutLines),
    "CRS | EPSG:3857\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 135790"
  );

  const nad83Projected = normalizeRows([
    ["Projected CRS NAD83 / EPSG:26915"],
    ["Point", "Easting", "Northing"],
    ["G", "246810", "135790"]
  ]);
  const nad83Route = route(nad83Projected.text, nad83Projected.layoutLines);
  assert.equal(nad83Route.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(nad83Route.evidence.projectedEvidenceComplete, false);

  const nad83Utm = normalizeRows([
    ["Projection NAD83 UTM zone 15N EPSG:26915"],
    ["Point", "Easting", "Northing"],
    ["G", "246810", "135790"]
  ]);
  const nad83UtmRoute = route(nad83Utm.text, nad83Utm.layoutLines);
  assert.equal(nad83UtmRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(nad83UtmRoute.evidence.projectedEvidenceComplete, true);
  assertConformant(
    contractFor(nad83Utm.text, nad83Utm.layoutLines),
    "CRS | EPSG:26915\nZONE | 15N\nHEMISPHERE | N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 135790"
  );

  for (const [crsTitle, providerCrs, zone, hemisphere] of [
    ["Projection WGS84 Transverse Mercator UTM zone 47S EPSG:32747", "WGS84 EPSG:32747", "47S", "S"],
    ["Projection NAD83 Transverse Mercator UTM zone 15N EPSG:26915", "EPSG:26915", "15N", "N"]
  ]) {
    const transverseUtm = normalizeRows([
      [crsTitle],
      ["Point", "Easting", "Northing"],
      ["G", "246810", "135790"]
    ]);
    const transverseRoute = route(transverseUtm.text, transverseUtm.layoutLines);
    assert.equal(transverseRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
    assert.equal(transverseRoute.evidence.projectedEvidenceComplete, true);
    assertConformant(
      contractFor(transverseUtm.text, transverseUtm.layoutLines),
      `CRS | ${providerCrs}\nZONE | ${zone}\nHEMISPHERE | ${hemisphere}\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 135790`
    );
  }

  const splitMethodConflictRows = [
    ["Projected CRS WGS84"], ["Projection"], ["Mollweide"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ];
  const splitMethodConflict = normalizeRows(splitMethodConflictRows);
  assert.equal(splitMethodConflict.status, "COMPLETE");
  const splitMethodConflictRoute = route(splitMethodConflict.text, splitMethodConflict.layoutLines);
  assert.equal(splitMethodConflictRoute.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(splitMethodConflictRoute.evidence.projectedEvidenceComplete, false);

  const splitMethodValid = normalizeRows([
    ["Projected CRS WGS84"], ["Projection"], ["Pseudo-Mercator"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ]);
  const splitMethodValidRoute = route(splitMethodValid.text, splitMethodValid.layoutLines);
  assert.equal(splitMethodValidRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(splitMethodValidRoute.evidence.projectedEvidenceComplete, true);

  const explicitMethodConflict = normalizeRows([
    ["Projected CRS WGS84"], ["Method: Mollweide"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ]);
  const explicitMethodConflictRoute = route(explicitMethodConflict.text, explicitMethodConflict.layoutLines);
  assert.equal(explicitMethodConflictRoute.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(explicitMethodConflictRoute.evidence.projectedEvidenceComplete, false);

  const explicitProjectionMethod = normalizeRows([
    ["Projected CRS WGS84"], ["Projection Method: Pseudo-Mercator"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ]);
  const explicitProjectionMethodRoute = route(explicitProjectionMethod.text, explicitProjectionMethod.layoutLines);
  assert.equal(explicitProjectionMethodRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(explicitProjectionMethodRoute.evidence.projectedEvidenceComplete, true);
  const explicitProjectionMethodResult = assertConformant(
    contractFor(explicitProjectionMethod.text, explicitProjectionMethod.layoutLines),
    "CRS | EPSG:3857\nMETHOD | Pseudo-Mercator\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 135790"
  );
  assert.equal(explicitProjectionMethodResult.counts.unassignedCandidateCount, 0);

  const delimiterlessMethod = normalizeRows([
    ["Projected CRS WGS84"], ["Method Pseudo-Mercator"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ]);
  const delimiterlessMethodRoute = route(delimiterlessMethod.text, delimiterlessMethod.layoutLines);
  assert.equal(delimiterlessMethodRoute.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  const delimiterlessMethodResult = assertConformant(
    contractFor(delimiterlessMethod.text, delimiterlessMethod.layoutLines),
    "CRS | EPSG:3857\nMETHOD | Pseudo-Mercator\nAXIS_ORDER | EASTING | NORTHING\nPOINT | G | 246810 | 135790"
  );
  assert.equal(delimiterlessMethodResult.counts.observedCandidateCount, 5);
  assert.equal(delimiterlessMethodResult.counts.boundCandidateCount, 5);
  assert.equal(delimiterlessMethodResult.counts.unassignedCandidateCount, 0);

  const nonAdjacentSplitLayout = syntheticWordLayout([
    ["Projected CRS WGS84"], ["Projection"], ["Pseudo-Mercator"], ["EPSG:3857"],
    ["Point", "Easting", "Northing"], ["G", "246810", "135790"]
  ]).map((line, index) => {
    if (index < 2) return line;
    return {
      ...line,
      bbox: [line.bbox[0], line.bbox[1] + 200, line.bbox[2], line.bbox[3] + 200],
      words: line.words.map(word => ({
        ...word,
        bbox: [word.bbox[0], word.bbox[1] + 200, word.bbox[2], word.bbox[3] + 200]
      }))
    };
  });
  const nonAdjacentSplit = normalizeLocalOcrStructuredEvidence({
    sourceText: [
      "Projected CRS WGS84", "Projection", "Pseudo-Mercator", "EPSG:3857",
      "Point Easting Northing", "G 246810 135790"
    ].join("\n"),
    layoutLines: nonAdjacentSplitLayout
  });
  assert.equal(nonAdjacentSplit.status, "COMPLETE");
  const nonAdjacentSplitRoute = route(nonAdjacentSplit.text, nonAdjacentSplit.layoutLines);
  assert.equal(nonAdjacentSplitRoute.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(nonAdjacentSplitRoute.evidence.projectedEvidenceComplete, false);

  const untrustedSplitMethodLayout = syntheticWordLayout(splitMethodConflictRows);
  untrustedSplitMethodLayout[2] = { ...untrustedSplitMethodLayout[2], words: [], word_structure_valid: false };
  const untrustedSplitMethod = normalizeLocalOcrStructuredEvidence({
    sourceText: splitMethodConflictRows.map(row => row.join(" ")).join("\n"),
    layoutLines: untrustedSplitMethodLayout
  });
  assert.equal(untrustedSplitMethod.status, "INCOMPLETE");
  assert.equal(route(untrustedSplitMethod.text, untrustedSplitMethod.layoutLines).family,
    ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);

  for (const conflictingCrsTitle of [
    "Projection NAD83 PseudoMercator EPSG:3857",
    "Projection WGS84 EPSG:26915",
    "Projection WGS84 EPSG:22222",
    "Projection WGS84 UTM zone 47S EPSG:3857",
    "Projection NAD83 UTM zone 16N EPSG:26915",
    "Projection WGS84 Transverse Mercator EPSG:3857",
    "Projection WGS84 Lambert Conformal Conic EPSG:3857",
    "Projection: Mollweide / Datum WGS84 / EPSG:3857",
    "Projection WGS84 Lambert Azimuthal Equal Area EPSG:3857",
    "Projection WGS84 Transverse-Mercator EPSG:3857"
  ]) {
    const conflict = normalizeRows([
      [conflictingCrsTitle],
      ["Point", "Easting", "Northing"],
      ["G", "246810", "135790"]
    ]);
    const conflictRoute = route(conflict.text, conflict.layoutLines);
    assert.equal(conflictRoute.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.ok(["projected_crs_evidence_inconsistent", "strong_structure_not_established"]
      .includes(conflictRoute.reason));
    assert.equal(conflictRoute.evidence.projectedEvidenceComplete, false);
  }
});

let passed = 0;
for (const entry of tests) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}
console.log(`SR-08F.7 one-shot structured family regression: ${passed}/${tests.length} PASS`);
