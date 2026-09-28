import assert from "node:assert/strict";
import {
  DMS_PARSE_STATUS,
  parseDmsField,
  parseLooseDmsLine
} from "../server/recognition/dms-evidence-parser.js";
import {
  evaluateImageDmsAcquisitionCompleteness,
  extractDmsSourceStructure,
  IMAGE_DMS_SELECTED_ROUTE
} from "../server/recognition/dms-source-structure.js";
import {
  ACQUISITION_REVIEW_STATUS,
  formatProviderDmsReviewCoordinates,
  normalizeProviderDmsReviewResult
} from "../server/recognition/recognition-review-result.js";

let passed = 0;
function test(name, run) {
  run();
  passed += 1;
  console.log(`PASS ${name}`);
}

const coteRows = [
  `1 | 05° 34' 42,00"N | 2° 47' 05,00"W`,
  `2 | 05° 34' 42,00"N | 2° 46' 19,00"W`,
  `3 | 05° 34' 21,00"N | 2° 46' 19,00"W`,
  `4 | 05° 34' 21,00"N | 2° 47' 05,00"W`
];
const indonesiaRows = [
  `1 | 2° 31' 2,794" S | 119° 30' 31,553" E`,
  `2 | 2° 31' 2,783" S | 119° 30' 35,279" E`,
  `3 | 2° 31' 14,694" S | 119° 30' 35,302" E`,
  `4 | 2° 31' 14,708" S | 119° 30' 28,050" E`
];

test("decimal-comma DMS field keeps exact seconds semantics", () => {
  const parsed = parseDmsField(`2° 31' 2,794" S`);
  assert.equal(parsed.parseStatus, DMS_PARSE_STATUS.VALID_STANDARD);
  assert.equal(parsed.exactSeconds, "2.794");
  assert.ok(parsed.normalizedNumericValue < 0);
});

test("decimal comma is never treated as a coordinate-column delimiter", () => {
  const parsed = parseLooseDmsLine(coteRows[0]);
  assert.deepEqual(parsed, {
    latitude: 5.578333333333333,
    longitude: -2.7847222222222223
  });
});

test("four decimal-comma rows remain four ordered source rows", () => {
  const structure = extractDmsSourceStructure([
    "Point | Latitude N | Longitude W",
    ...coteRows
  ].join("\n"));
  assert.equal(structure.rowCount, 4);
  assert.equal(structure.groupCount, 1);
  assert.deepEqual(structure.groups[0].parsedRows.map(row => row.label), ["1", "2", "3", "4"]);
  assert.equal(structure.displayText, coteRows.join("\n"));
});

test("generic DMS completeness binds source rows to canonical lon-lat rows", () => {
  const rawText = ["Point | Latitude N | Longitude W", ...coteRows].join("\n");
  const normalized = coteRows.map(row => {
    const point = parseLooseDmsLine(row);
    return `${point.longitude},${point.latitude}`;
  }).join("\n");
  const result = evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText,
    normalizedCoordinates: normalized,
    selectedRoute: IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(result.allowed, true);
  assert.equal(result.sourceRowCount, 4);
  assert.equal(result.normalizedCoordinateRowCount, 4);
});

test("Provider review removes protocol title but keeps all source rows", () => {
  const review = normalizeProviderDmsReviewResult([
    "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
    "Point | Latitude N | Longitude W",
    ...coteRows
  ].join("\n"));
  assert.equal(review.candidatePointCount, 4);
  assert.equal(review.candidateGroupCount, 1);
  assert.deepEqual(review.candidateGroups[0].titlePath, []);
  assert.equal(review.status, ACQUISITION_REVIEW_STATUS.REVIEW_REQUIRED);
});

test("bracketed generic protocol title cannot become a user-visible group", () => {
  const review = normalizeProviderDmsReviewResult([
    "[UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE]",
    "Point | Latitude N | Longitude W",
    ...coteRows
  ].join("\n"));
  assert.equal(review.candidatePointCount, 4);
  assert.deepEqual(review.candidateGroups[0].titlePath, []);
});

test("Indonesia decimal-comma DMS produces four finite distinct lon-lat rows", () => {
  const review = normalizeProviderDmsReviewResult([
    "DMS Coordinate Table",
    ...indonesiaRows
  ].join("\n"));
  const rows = formatProviderDmsReviewCoordinates(review).split("\n").filter(Boolean);
  assert.equal(rows.length, 4);
  assert.equal(new Set(rows).size, 4);
  for (const row of rows) {
    const [longitude, latitude] = row.split(",").map(Number);
    assert.ok(Number.isFinite(longitude) && Number.isFinite(latitude));
    assert.ok(longitude > 100 && latitude < 0);
  }
});

test("source order stays latitude-longitude while canonical order stays lon-lat", () => {
  const structure = extractDmsSourceStructure(indonesiaRows.join("\n"));
  assert.ok(structure.groups[0].parsedRows.every(row => row.axisOrder === "latitude_longitude"));
  const first = structure.groups[0].parsedRows[0];
  assert.ok(first.longitude > 100);
  assert.ok(first.latitude < 0);
});

console.log(`Coordinate authority matrix regression: ${passed}/${passed} PASS`);
