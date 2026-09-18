import assert from "node:assert/strict";
import {
  RECOGNITION_COMPLETENESS_DECISION,
  RECOGNITION_COMPLETENESS_NEXT_ACTION,
  assessRecognitionCompleteness
} from "../server/recognition/recognition-completeness.js";
import {
  ONE_SHOT_ACQUISITION_CONFORMANCE_REASON,
  ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS,
  ONE_SHOT_STRUCTURED_FAMILY,
  classifyOneShotStructuredFamily,
  createOneShotAcquisitionContract,
  validateOneShotAcquisitionContract
} from "../server/recognition/family-primary-routing.js";

let passed = 0;

function verifyAuthorityBoundary(result) {
  assert.equal(result.allowGenericProviderRetry, false);
  assert.equal(result.geometryInferenceAllowed, false);
  assert.equal(result.pointCreationAllowed, false);
  assert.equal(result.mapCreationAllowed, false);
  assert.equal(result.kmlCreationAllowed, false);
  assert.equal(result.preserveSourceRepresentation, true);
}

function test(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS ${passed}: ${name}`);
}

function assess(input) {
  const result = assessRecognitionCompleteness(input);
  verifyAuthorityBoundary(result);
  return result;
}

test("complete single WGS84 decimal candidate does not retry because it has fewer than four rows", () => {
  const result = assess({ coordinates: "98.67370605,26.34265281", coordinateFormat: "WGS84_DECIMAL" });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
  assert.equal(result.allowLocalOcrEvidence, false);
});

test("production family evidence can authorize only its named bounded family retry", () => {
  const result = assess({
    rawText: "candidate rows",
    coordinates: "98.1,26.1\n98.2,26.2",
    family: "wgs84_table",
    structure: { incomplete: true, familyEvidencePresent: true }
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.FAMILY_RETRY_ONLY);
  assert.equal(result.allowFamilyProviderRetry, true);
  assert.equal(result.retryFamily, "wgs84_table");
});

test("complete single DMS candidate preserves source representation without retry", () => {
  const result = assess({
    rawText: `A. 11°00'00.00\"N, 08°00'00.00\"W`,
    coordinates: `A. 11°00'00.00\"N, 08°00'00.00\"W`,
    coordinateRowCount: 1,
    coordinateFormat: "DMS"
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
});

test("one-shot classifier routes labelled single DMS before completeness without family retry", () => {
  const primary = classifyOneShotStructuredFamily({
    text: `Longitude: 73°25'05.54\"E\nLatitude: 18°40'22.49\"N`
  });
  assert.equal(primary.family, ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  const result = assess({
    coordinates: `73°25'05.54\"E,18°40'22.49\"N`,
    coordinateRowCount: 1,
    coordinateFormat: "DMS",
    family: primary.family
  });
  assert.equal(result.allowFamilyProviderRetry, false);
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
});

test("map search and place detail precision variants route to Point Review", () => {
  const result = assess({
    coordinates: "35.447819,83.178991\n35.4478191,83.1789913",
    coordinateRowCount: 2,
    sourceRoles: ["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS", "MAP_PLUS_CODE"],
    samePlaceNearDuplicate: true
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.POINT_REVIEW_REQUIRED);
  assert.equal(result.nextAction, RECOGNITION_COMPLETENESS_NEXT_ACTION.POINT_REVIEW);
  assert.equal(result.forcePointReview, true);
});

test("map screenshot family preserves region roles for near-duplicate Point Review", () => {
  const primary = classifyOneShotStructuredFamily({
    text: "Search 73.41820,18.67291\nPlace details 73.418205,18.672914\nPlus Code 7JCPMC9C+5P",
    layoutLines: [
      { text: "Search 73.41820,18.67291", bbox: [1, 1, 101, 21] },
      { text: "Place details 73.418205,18.672914", bbox: [1, 101, 151, 121] }
    ]
  });
  assert.equal(primary.family, ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT);
  const result = assess({
    coordinates: "73.41820,18.67291\n73.418205,18.672914",
    coordinateRowCount: 2,
    sourceRoles: ["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS", "MAP_PLUS_CODE"],
    samePlaceNearDuplicate: true,
    family: primary.family
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.POINT_REVIEW_REQUIRED);
  assert.equal(result.geometryInferenceAllowed, false);
});

test("distinct map locations never become an automatic merge or line", () => {
  const result = assess({
    coordinates: "35.447819,83.178991\n36.447819,84.178991",
    coordinateRowCount: 2,
    sourceRoles: ["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS"],
    samePlaceNearDuplicate: false
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
  assert.equal(result.forcePointReview, false);
});

test("two unbound coordinate rows require review instead of Provider retry", () => {
  const result = assess({ coordinates: "35.4,83.1\n36.4,84.1", coordinateRowCount: 2 });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
});

test("complete four-point table uses header and continuous row labels", () => {
  const rows = [1, 2, 3, 4].map(index => `${index}. ${10 + index}.123,${20 + index}.456`).join("\n");
  const result = assess({
    rawText: `Point | Longitude | Latitude\n${rows}`,
    coordinates: rows,
    coordinateRowCount: 4,
    family: "wgs84_table",
    structure: { expectedRowCount: 4 }
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
});

test("missing row in four-point table permits only its typed family retry", () => {
  const rows = "1. 11.1,21.1\n2. 12.1,22.1\n4. 14.1,24.1";
  const result = assess({
    rawText: `Point | Longitude | Latitude\n${rows}`,
    coordinates: rows,
    coordinateRowCount: 3,
    family: "wgs84_table",
    structure: { expectedRowCount: 4, missingRowCount: 1 }
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.FAMILY_RETRY_ONLY);
  assert.equal(result.retryFamily, "wgs84_table");
  assert.equal(result.allowFamilyProviderRetry, true);
});

test("incomplete table without a trusted family fails to review", () => {
  const result = assess({
    rawText: "Point | Longitude | Latitude\n1. 11.1,21.1\n3. 13.1,23.1",
    coordinates: "1. 11.1,21.1\n3. 13.1,23.1",
    coordinateRowCount: 2,
    family: "generic",
    structure: { expectedRowCount: 4, missingRowCount: 2 }
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
  assert.equal(result.allowFamilyProviderRetry, false);
});

test("twenty-row long table is complete by header and row sequence, not fixed coordinate values", () => {
  const rows = Array.from({ length: 20 }, (_, index) => `${index + 1}. ${400000 + index * 7.25},${800000 + index * 3.5}`).join("\n");
  const result = assess({
    rawText: `Point | X | Y\n${rows}`,
    coordinates: rows,
    coordinateRowCount: 20,
    coordinateFormat: "PROJECTED",
    family: "bftm",
    structure: { expectedRowCount: 20 },
    crsEvidence: { datum: "WGS84", zone: "30", hemisphere: "N", axisOrder: "EASTING_NORTHING" }
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
});

test("complete multi-site DMS preserves group boundaries and still requires review", () => {
  const result = assess({
    rawText: "Point | Longitude | Latitude",
    coordinates: Array.from({ length: 16 }, (_, index) => `${index + 1}. 11°00'00.00\"N, 08°00'00.00\"W`).join("\n"),
    coordinateRowCount: 16,
    coordinateFormat: "DMS",
    layoutGroups: [
      { rowCount: 8, expectedRowCount: 8 },
      { rowCount: 4, expectedRowCount: 4 },
      { rowCount: 4, expectedRowCount: 4 }
    ]
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
  assert.equal(result.preserveGroupBoundaries, true);
});

test("incomplete group evidence permits only the typed DMS family retry", () => {
  const result = assess({
    rawText: "Point | Longitude | Latitude",
    coordinates: "1. 11°00'00.00\"N, 08°00'00.00\"W",
    coordinateRowCount: 1,
    coordinateFormat: "DMS",
    family: "dms_grouped",
    layoutGroups: [{ rowCount: 7, expectedRowCount: 8 }, { rowCount: 4, expectedRowCount: 4 }]
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.FAMILY_RETRY_ONLY);
  assert.equal(result.retryFamily, "dms_grouped");
});

test("handwritten DMS conflict remains human review", () => {
  const result = assess({
    coordinates: `A. 11°00'00.00\"N, 08°00'00.00\"W`,
    coordinateRowCount: 1,
    coordinateFormat: "DMS",
    handwrittenConflict: true
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
});

test("empty successful Provider result allows one local OCR evidence read", () => {
  const result = assess({ coordinates: "", providerStatus: "SUCCESS", localOcrAttempted: false });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.LOCAL_OCR_EVIDENCE_ONLY);
  assert.equal(result.allowLocalOcrEvidence, true);
});

test("empty result after local OCR fails closed", () => {
  const result = assess({ coordinates: "", providerStatus: "SUCCESS", localOcrAttempted: true });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.FAILED_CLOSED);
  assert.equal(result.allowLocalOcrEvidence, false);
});

for (const terminalState of ["FAILED", "TIMEOUT", "UNCERTAIN"]) {
  test(`Provider ${terminalState.toLowerCase()} allows no Provider retry and only one local OCR`, () => {
    const first = assess({ providerStatus: terminalState, localOcrAttempted: false });
    assert.equal(first.allowFamilyProviderRetry, false);
    assert.equal(first.allowLocalOcrEvidence, true);
    const second = assess({ providerStatus: terminalState, localOcrAttempted: true });
    assert.equal(second.decision, RECOGNITION_COMPLETENESS_DECISION.FAILED_CLOSED);
  });
}

for (const sourceKind of ["ORDINARY_REPORT", "MATH_EXPRESSION", "PSEUDO_COORDINATE", "SPLIT_NUMERIC_COLUMNS"]) {
  test(`${sourceKind.toLowerCase()} fails closed as non-coordinate content`, () => {
    const result = assess({
      rawText: "10.2 20.3 30.4",
      coordinates: "10.2,20.3",
      coordinateRowCount: 1,
      sourceKind
    });
    assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.NON_COORDINATE_REJECTED);
  });
}

test("projected XY without datum zone hemisphere and axis order remains review-only", () => {
  const result = assess({
    rawText: "Point | X | Y\n1. 500000,7200000",
    coordinates: "1. 500000,7200000",
    coordinateRowCount: 1,
    coordinateFormat: "PROJECTED"
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED);
  assert.ok(result.reasons.includes("PROJECTED_CRS_EVIDENCE_INCOMPLETE"));
});

test("projected primary route requires datum zone hemisphere and axis-order structure", () => {
  const complete = classifyOneShotStructuredFamily({
    text: "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100"
  });
  const incomplete = classifyOneShotStructuredFamily({
    text: "Projection UTM\nPoint | X | Y\nA | 500100 | 2065100"
  });
  assert.equal(complete.family, ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(incomplete.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(incomplete.matched, false);
});

test("fixed decision output contains no raw Provider response or sensitive request material", () => {
  const result = assess({
    rawText: "secret-provider-text",
    coordinates: "98.1,26.2",
    coordinateRowCount: 1,
    headers: { authorization: "Bearer secret" },
    cookie: "secret-cookie",
    apiKey: "secret-key"
  });
  const serialized = JSON.stringify(result);
  assert.doesNotMatch(serialized, /secret-provider-text|Bearer secret|secret-cookie|secret-key/);
  assert.equal(Object.hasOwn(result, "rawText"), false);
  assert.equal(Object.hasOwn(result, "coordinates"), false);
});

test("only contract-conformant single-point evidence reaches completeness", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const primary = classifyOneShotStructuredFamily({ text: source });
  const contract = createOneShotAcquisitionContract({ route: primary, sourceText: source });
  const conformant = validateOneShotAcquisitionContract({
    contract,
    providerText: source
  });
  assert.equal(conformant.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT);
  const result = assess({
    coordinates: "64.125001,12.875002",
    coordinateRowCount: conformant.counts.coordinateRowCount,
    coordinateFormat: "WGS84_DECIMAL",
    family: conformant.family
  });
  assert.equal(result.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
});

test("value-fidelity mismatch remains review-only before completeness", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const primary = classifyOneShotStructuredFamily({ text: source });
  const contract = createOneShotAcquisitionContract({ route: primary, sourceText: source });
  const mismatch = validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002"
  });
  assert.equal(mismatch.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(mismatch.reason, ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.VALUE_FIDELITY_MISMATCH);
  assert.equal(mismatch.conformant, false);
});

test("contract mismatch is review-only before completeness and geometry", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const primary = classifyOneShotStructuredFamily({ text: source });
  const contract = createOneShotAcquisitionContract({ route: primary, sourceText: source });
  const mismatch = validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002\n64.1000 | 12.2000"
  });
  assert.equal(mismatch.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(mismatch.conformant, false);
  assert.equal(Object.hasOwn(mismatch, "geometryInferenceAllowed"), false);
});

test("generic review contract cannot become complete from Provider text alone", () => {
  const primary = classifyOneShotStructuredFamily({ text: "ordinary report" });
  const contract = createOneShotAcquisitionContract({ route: primary, sourceText: "ordinary report" });
  const mismatch = validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002"
  });
  assert.equal(mismatch.status, ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(mismatch.family, ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
});

console.log(`Recognition completeness P0 regression: ${passed}/${passed} PASS`);
