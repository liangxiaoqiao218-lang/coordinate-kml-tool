import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import path from "node:path";

import { buildCoordinateEvidenceShadowModel } from "../server/coordinate-evidence/index.js";
import {
  buildStructuredUtmPriority,
  getStructuredUtmVerificationMismatches,
  mergeProjectedXyRows,
  mergeSelectiveDmsReferenceRows,
  mergeSelectiveProjectedXyRows,
  mergeStructuredUtmReferenceRows,
  mergeStructuredUtmTableRows,
  parseSelectiveDmsReferenceModelText,
  parseStructuredUtmTableModelText
} from "../server/utm-intent/structured-projected-priority.js";
import { transformUtmWgs84Point } from "../server/utm-intent/utm-wgs84-transform.js";

const fixtureRoot = path.resolve("artifacts", "fixtures");
const shadowIntent = Object.freeze({
  projection: "utm",
  datum: "WGS84",
  zone: 50,
  hemisphere: "south",
  epsg: "EPSG:32750",
  confidence: "confirmed",
  conflicts: []
});

const cases = [
  {
    name: "Indonesia UTM50S #001",
    fixture: "indonesia-utm50s-real-001.jpg",
    recoveryExpected: false,
    initial: "complete",
    rows: [
      [1, 779271.176, 9720912.526],
      [2, 779554.165, 9720912.526],
      [3, 779554.165, 9720734.464],
      [4, 779271.176, 9720734.464]
    ]
  },
  {
    name: "Indonesia UTM50S #002",
    fixture: "indonesia-utm50s-real-002.jpg",
    recoveryExpected: false,
    initial: "complete",
    rows: [
      [1, 778984.492, 9721476.737],
      [2, 779099.680, 9721476.848],
      [3, 779099.680, 9721110.798],
      [4, 778875.519, 9721110.798],
      [5, 778875.519, 9721180.576],
      [6, 778984.492, 9721180.576]
    ]
  },
  {
    name: "Indonesia UTM50S #003",
    fixture: "indonesia-utm50s-real-003.jpg",
    recoveryExpected: false,
    selectiveRereadExpected: true,
    expectedInitialCorrectRows: 13,
    expectedMismatchLabels: ["3", "4", "6", "13"],
    expectedXyAcceptedLabels: ["3", "4", "6"],
    expectedPostXyMismatchLabels: ["13"],
    dmsReferenceRereadExpected: true,
    expectedDmsAcceptedLabels: ["13"],
    initial: "mismatched",
    rows: [
      [1, 778807.293, 9721476.737, -2.517445833, 119.507172222],
      [2, 778981.768, 9721477.288, -2.517437778, 119.508740278],
      [3, 778982.700, 9721182.351, -2.520103611, 119.508753611],
      [4, 778855.308, 9721181.948, -2.520109444, 119.507608889],
      [5, 778855.543, 9721107.284, -2.520784167, 119.507612222],
      [6, 778980.724, 9721107.010, -2.520784444, 119.508737222],
      [7, 778980.920, 9720910.990, -2.522556111, 119.508742500],
      [8, 779100.477, 9720911.109, -2.522553056, 119.509816667],
      [9, 779100.599, 9720788.271, -2.523663333, 119.509820000],
      [10, 778950.926, 9720787.948, -2.523668889, 119.508475000],
      [11, 778950.926, 9720833.787, -2.523254444, 119.508474167],
      [12, 778927.907, 9720833.787, -2.523255000, 119.508267222],
      [13, 778927.907, 9720922.219, -2.522455556, 119.508265833],
      [14, 778906.895, 9720922.219, -2.522456111, 119.508076944],
      [15, 778906.895, 9721078.633, -2.521042222, 119.508074167],
      [16, 778807.082, 9721078.633, -2.521043889, 119.507177222]
    ],
    initialRows: [
      [1, 778807.293, 9721476.737, -2.517445833, 119.507172222],
      [2, 778981.768, 9721477.288, -2.517437778, 119.508740278],
      [3, 778982.700, 9721188.351, -2.520103611, 119.508753611],
      [4, 778855.308, 9721188.194, -2.520109444, 119.507608889],
      [5, 778855.543, 9721107.284, -2.520784167, 119.507612222],
      [6, 778880.724, 9721107.010, -2.520784444, 119.508737222],
      [7, 778980.920, 9720910.990, -2.522556111, 119.508742500],
      [8, 779100.477, 9720911.109, -2.522553056, 119.509816667],
      [9, 779100.599, 9720788.271, -2.523663333, 119.509820000],
      [10, 778950.926, 9720787.948, -2.523668889, 119.508475000],
      [11, 778950.926, 9720833.787, -2.523254444, 119.508474167],
      [12, 778927.907, 9720833.787, -2.523255000, 119.508267222],
      [13, 778927.907, 9720922.219, -2.522455556, 119.508265833],
      [14, 778906.895, 9720922.219, -2.522456111, 119.508076944],
      [15, 778906.895, 9721078.633, -2.521042222, 119.508074167],
      [16, 778807.082, 9721078.633, -2.521043889, 119.507177222]
    ],
    selectiveRows: [
      [3, 778982.700, 9721182.351],
      [4, 778855.308, 9721181.948],
      [6, 778980.724, 9721107.010]
    ],
    initialReferenceRows: [
      [1, 778807.293, 9721476.737, -2.517445833, 119.507172222],
      [2, 778981.768, 9721477.288, -2.517437778, 119.508740278],
      [3, 778982.700, 9721182.351, -2.520103611, 119.508753611],
      [4, 778855.308, 9721181.948, -2.520109444, 119.507608889],
      [5, 778855.543, 9721107.284, -2.520784167, 119.507612222],
      [6, 778980.724, 9721107.010, -2.520784444, 119.508737222],
      [7, 778980.920, 9720910.990, -2.522556111, 119.508742500],
      [8, 779100.477, 9720911.109, -2.522553056, 119.509816667],
      [9, 779100.599, 9720788.271, -2.523663333, 119.509820000],
      [10, 778950.926, 9720787.948, -2.523668889, 119.508475000],
      [11, 778950.926, 9720833.787, -2.523254444, 119.508474167],
      [12, 778927.907, 9720833.787, -2.523255000, 119.508267222],
      [13, 778927.907, 9720922.219, -2.522455556, 119 + 30 / 60 + 29.795 / 3600],
      [14, 778906.895, 9720922.219, -2.522456111, 119.508076944],
      [15, 778906.895, 9721078.633, -2.521042222, 119.508074167],
      [16, 778807.082, 9721078.633, -2.521043889, 119.507177222]
    ],
    selectiveReferenceRows: [
      ["13", "2°31'20.840\" S", "119°30'29.757\" E"]
    ]
  }
];

function makeModelText(rows, { includeReference = true } = {}) {
  return JSON.stringify({
    status: rows.length >= 3 ? "observed" : "none",
    rows: rows.map(([point, x, y, lat, lon]) => {
      const reference = Number.isFinite(lat) && Number.isFinite(lon)
        ? { lat, lon }
        : transformUtmWgs84Point({ easting: x, northing: y, zone: 50, hemisphere: "south" });
      return {
        point: String(point),
        x: String(x),
        y: String(y),
        latitude: includeReference ? String(reference.lat ?? reference.latitude) : "",
        longitude: includeReference ? String(reference.lon ?? reference.longitude) : ""
      };
    })
  });
}

function makeReferenceTable(rows) {
  return {
    status: "observed",
    source: "frozen_real_fixture_reference",
    orderPreserved: true,
    rows: rows.map(([point, x, y, lat, lon]) => {
      const reference = Number.isFinite(lat) && Number.isFinite(lon)
        ? { latitude: lat, longitude: lon }
        : transformUtmWgs84Point({ easting: x, northing: y, zone: 50, hemisphere: "south" });
      return {
        point: String(point),
        latitude: String(reference.latitude),
        longitude: String(reference.longitude)
      };
    })
  };
}

function makeSelectiveReferenceModelText(rows = []) {
  return JSON.stringify({
    status: rows.length > 0 ? "observed" : "none",
    rows: rows.map(([point, latitude, longitude]) => ({
      point: String(point),
      latitude: String(latitude),
      longitude: String(longitude)
    }))
  });
}

function hasCoverageGap(priority, referenceTable) {
  const expectedRows = referenceTable.rows.length;
  const observedRows = priority?.table?.rows?.length || 0;
  return expectedRows >= 3 && observedRows < expectedRows;
}

function shouldRecover(priority, table, referenceTable) {
  if (priority?.accepted && !hasCoverageGap(priority, referenceTable)) return false;
  const rowCount = table?.rows?.length || 0;
  return !priority || rowCount < 3 || hasCoverageGap({ table }, referenceTable);
}

function countCorrectRows(rows, expectedRows) {
  return rows.reduce((count, row, index) => {
    const expected = expectedRows[index];
    return row.easting === expected[1] && row.northing === expected[2] ? count + 1 : count;
  }, 0);
}

function buildPriority(table, referenceTable) {
  const priority = buildStructuredUtmPriority({ shadowIntent, table });
  if (priority?.accepted && hasCoverageGap(priority, referenceTable)) {
    return {
      ...priority,
      accepted: false,
      reason: "structured_utm_row_coverage_incomplete",
      transformationVerification: {
        ...priority.transformationVerification,
        status: "coverage_incomplete"
      }
    };
  }
  return priority;
}

function assertPointCoverage(priority, expectedRows) {
  const rows = priority.table.rows;
  assert.equal(rows.length, expectedRows.length);
  assert.deepEqual(rows.map(row => row.point), expectedRows.map(row => String(row[0])));
  assert.equal(new Set(rows.map(row => row.point)).size, expectedRows.length);
  rows.forEach((row, index) => {
    assert.equal(row.easting, expectedRows[index][1], `point ${index + 1} X mismatch`);
    assert.equal(row.northing, expectedRows[index][2], `point ${index + 1} Y mismatch`);
  });
}

let passed = 0;
for (const testCase of cases) {
  await access(path.join(fixtureRoot, testCase.fixture));
  const referenceTable = makeReferenceTable(testCase.initialReferenceRows || testCase.rows);
  const initialRows = testCase.initialRows || testCase.rows;
  const initialStructured = parseStructuredUtmTableModelText(
    testCase.initial === "complete"
      ? makeModelText(testCase.rows, { includeReference: true })
      : testCase.initial === "mismatched"
        ? makeModelText(initialRows, { includeReference: true })
      : JSON.stringify({ status: "none", rows: [] })
  );
  const initialXyOnly = parseStructuredUtmTableModelText(
    testCase.initial === "complete"
      ? makeModelText(testCase.rows, { includeReference: false })
      : testCase.initial === "mismatched"
        ? makeModelText(initialRows, { includeReference: false })
      : JSON.stringify({ status: "none", rows: [] })
  );
  let table = mergeProjectedXyRows(initialStructured, initialXyOnly, { shadowIntent });
  table = mergeStructuredUtmReferenceRows(table, referenceTable, { allowVerifiedIndexMerge: true });
  let priority = buildPriority(table, referenceTable);
  const recoveryTriggered = shouldRecover(priority, table, referenceTable);
  const initialCorrectRows = countCorrectRows(table.rows || [], testCase.rows);

  if (recoveryTriggered) {
    const recoveredStructured = parseStructuredUtmTableModelText(JSON.stringify({ status: "none", rows: [] }));
    const recoveredXyOnly = parseStructuredUtmTableModelText(makeModelText(testCase.rows, { includeReference: false }));
    table = mergeStructuredUtmTableRows(table, recoveredStructured, { shadowIntent });
    table = mergeProjectedXyRows(table, recoveredXyOnly, { shadowIntent });
    table = mergeStructuredUtmReferenceRows(table, referenceTable, { allowVerifiedIndexMerge: true });
    priority = buildPriority(table, referenceTable);
  }

  const mismatchLabels = getStructuredUtmVerificationMismatches(priority).map(item => item.point);
  const selectiveRereadTriggered = priority?.reason === "transformation_verification_failed"
    && !hasCoverageGap(priority, referenceTable)
    && mismatchLabels.length > 0;

  let acceptedReplacementLabels = [];
  let dmsReferenceRereadTriggered = false;
  let dmsAcceptedLabels = [];
  let postXyMismatchLabels = [];
  if (selectiveRereadTriggered) {
    const selectiveRows = parseStructuredUtmTableModelText(makeModelText(testCase.selectiveRows || [], { includeReference: false }));
    table = mergeSelectiveProjectedXyRows(priority.table, selectiveRows, { shadowIntent });
    acceptedReplacementLabels = (table.selectiveReread?.replacements || [])
      .filter(item => item.accepted)
      .map(item => item.point);
    table = mergeStructuredUtmReferenceRows(table, referenceTable, { allowVerifiedIndexMerge: true });
    priority = buildPriority(table, referenceTable);
    postXyMismatchLabels = getStructuredUtmVerificationMismatches(priority).map(item => item.point);
  }

  dmsReferenceRereadTriggered = priority?.reason === "transformation_verification_failed"
    && !hasCoverageGap(priority, referenceTable)
    && getStructuredUtmVerificationMismatches(priority).length > 0;

  if (dmsReferenceRereadTriggered) {
    const selectiveReferenceRows = parseSelectiveDmsReferenceModelText(makeSelectiveReferenceModelText(testCase.selectiveReferenceRows || []));
    table = mergeSelectiveDmsReferenceRows(priority.table, selectiveReferenceRows, { shadowIntent });
    dmsAcceptedLabels = (table.selectiveDmsReferenceReread?.replacements || [])
      .filter(item => item.accepted)
      .map(item => item.point);
    priority = buildPriority(table, referenceTable);
  }

  assert.equal(recoveryTriggered, testCase.recoveryExpected, `${testCase.name} recovery trigger mismatch`);
  assert.equal(selectiveRereadTriggered, Boolean(testCase.selectiveRereadExpected), `${testCase.name} selective reread trigger mismatch`);
  assert.equal(dmsReferenceRereadTriggered, Boolean(testCase.dmsReferenceRereadExpected), `${testCase.name} DMS reference reread trigger mismatch`);
  if (testCase.expectedInitialCorrectRows) {
    assert.equal(initialCorrectRows, testCase.expectedInitialCorrectRows, `${testCase.name} initial correct row count mismatch`);
  }
  if (testCase.expectedMismatchLabels) {
    assert.deepEqual(mismatchLabels, testCase.expectedMismatchLabels, `${testCase.name} mismatch labels mismatch`);
    assert.deepEqual(
      acceptedReplacementLabels,
      testCase.expectedXyAcceptedLabels || testCase.expectedMismatchLabels,
      `${testCase.name} accepted labels mismatch`
    );
  }
  if (testCase.expectedPostXyMismatchLabels) {
    assert.deepEqual(postXyMismatchLabels, testCase.expectedPostXyMismatchLabels, `${testCase.name} post-X/Y mismatch labels mismatch`);
  }
  if (testCase.expectedDmsAcceptedLabels) {
    assert.deepEqual(dmsAcceptedLabels, testCase.expectedDmsAcceptedLabels, `${testCase.name} DMS accepted labels mismatch`);
  }
  assert.equal(priority?.accepted, true, `${testCase.name} priority must be accepted`);
  assert.equal(priority.typedUtmIntent.zone, 50);
  assert.equal(priority.typedUtmIntent.hemisphere, "south");
  assert.equal(priority.typedUtmIntent.epsg, "EPSG:32750");
  assert.equal(priority.transformationVerification.status, "match");
  assert.equal(priority.transformationVerification.comparedRows, testCase.rows.length);
  assertPointCoverage(priority, testCase.rows);

  const { coordinateEvidenceCandidates, shadowEvidenceDecision } = buildCoordinateEvidenceShadowModel({
    crsEvidenceShadow: { shadowIntent },
    structuredUtmPriority: priority
  }, {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review"
  });
  assert.ok(coordinateEvidenceCandidates.find(candidate => candidate.evidenceType === "verified_utm_transformation"));
  assert.ok(coordinateEvidenceCandidates.find(candidate => candidate.evidenceType === "utm_crs_text"));
  assert.equal(shadowEvidenceDecision.winnerEvidenceType, "verified_utm_transformation");

  passed += 1;
  console.log(JSON.stringify({
    sample: testCase.name,
    fixture: testCase.fixture,
    rowCount: priority.table.rows.length,
    zone: priority.typedUtmIntent.zone,
    hemisphere: priority.typedUtmIntent.hemisphere,
    epsg: priority.typedUtmIntent.epsg,
    transformVerified: priority.transformationVerification.status === "match",
    maximumDifference: priority.transformationVerification.maximumDifference,
    recoveryTriggered,
    selectiveRereadTriggered,
    dmsReferenceRereadTriggered,
    initialCorrectRows,
    mismatchedLabels: mismatchLabels,
    acceptedReplacementLabels,
    postXyMismatchLabels,
    dmsAcceptedLabels,
    candidate: "verified_utm_transformation",
    shadowWinner: shadowEvidenceDecision.winnerEvidenceType
  }));
}

console.log(`Indonesia Complex UTM Acquisition Regression: ${passed}/${cases.length} PASS`);
