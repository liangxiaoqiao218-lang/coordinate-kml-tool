import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
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

const tests = [];
const test = (id, name, fn) => tests.push({ id, name, fn });
const route = (text, layoutLines = []) => classifyOneShotStructuredFamily({ text, layoutLines });

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

function contractFor(text, layoutLines = []) {
  const selectedRoute = route(text, layoutLines);
  return createOneShotAcquisitionContract({ route: selectedRoute, sourceText: text });
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
  const layoutLines = [
    { text: "Search 64.1250,12.8750", bbox: [1, 1, 101, 21] },
    { text: "Place details 64.125001,12.875002", bbox: [1, 101, 151, 121] }
  ];
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
