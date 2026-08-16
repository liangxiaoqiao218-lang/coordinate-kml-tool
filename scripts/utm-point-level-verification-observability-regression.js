import assert from "node:assert/strict";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";
import { snapshotPreSuppressionCandidates } from "../server/coordinate-evidence/index.js";
import {
  buildStructuredUtmPriority,
  mergeSelectiveDmsReferenceRows,
  mergeStructuredUtmReferenceRows,
  parseSelectiveDmsReferenceModelText,
  summarizeStructuredUtmTransformationVerification
} from "../server/utm-intent/structured-projected-priority.js";

const shadowIntent = Object.freeze({
  projection: "utm",
  datum: "WGS84",
  zone: 50,
  hemisphere: "south",
  epsg: "EPSG:32750",
  confidence: "confirmed",
  conflicts: []
});

const projectedRows = Object.freeze([
  [1, 778807.293, 9721476.737],
  [2, 778981.768, 9721477.288],
  [3, 778982.700, 9721182.351],
  [4, 778855.308, 9721181.948],
  [5, 778855.543, 9721107.284],
  [6, 778980.724, 9721107.010],
  [7, 778980.920, 9720910.990],
  [8, 779100.477, 9720911.109],
  [9, 779100.599, 9720788.271],
  [10, 778950.926, 9720787.948],
  [11, 778950.926, 9720833.787],
  [12, 778927.907, 9720833.787],
  [13, 778927.907, 9720922.219],
  [14, 778906.895, 9720922.219],
  [15, 778906.895, 9721078.633],
  [16, 778807.082, 9721078.633]
]);

const referenceRows = Object.freeze([
  [1, -2.517445833, 119.507172222],
  [2, -2.517437778, 119.508740278],
  [3, -2.520103611, 119.508753611],
  [4, -2.520109444, 119.507608889],
  [5, -2.520784167, 119.507612222],
  [6, -2.520784444, 119.508737222],
  [7, -2.522556111, 119.508742500],
  [8, -2.522553056, 119.509816667],
  [9, -2.523663333, 119.509820000],
  [10, -2.523668889, 119.508475000],
  [11, -2.523254444, 119.508474167],
  [12, -2.523255000, 119.508267222],
  [13, -2.522455556, 119.508265833],
  [14, -2.522456111, 119.508076944],
  [15, -2.521042222, 119.508074167],
  [16, -2.521043889, 119.507177222]
]);

function buildProjectedTable() {
  return {
    status: "observed",
    rows: projectedRows.map(([point, easting, northing]) => ({
      point: String(point),
      easting,
      northing,
      xText: String(easting),
      yText: String(northing)
    }))
  };
}

function buildReferenceTable(mutator = rows => rows) {
  const rows = referenceRows.map(([point, latitude, longitude]) => ({ point: String(point), latitude, longitude }));
  return {
    status: "observed",
    source: "raw_dms_rows",
    orderPreserved: true,
    rows: mutator(rows)
  };
}

function buildPriority(mutator) {
  const table = mergeStructuredUtmReferenceRows(buildProjectedTable(), buildReferenceTable(mutator), {
    allowVerifiedIndexMerge: true
  });
  return buildStructuredUtmPriority({ shadowIntent, table });
}

function findPoint(verification, point) {
  return verification.pointLevelVerification.find(row => row.point === String(point));
}

function makeResponse(priority, includeDebug) {
  const precisionMode = priority.accepted ? "utm-projected-x-y" : "utm-projected-x-y-review";
  const coordinateEvidenceContext = snapshotPreSuppressionCandidates({
    crsEvidenceShadow: { shadowIntent },
    structuredUtmPriority: priority,
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: [],
    reason: "utm_evidence_lock_final_verification_only"
  });

  return buildFinalizedCoordinateVerificationResponse({
    model: "regression",
    rawText: "",
    coordinates: priority.coordinates,
    coordinateType: "utm_projected_xy",
    precisionMode,
    confirmationStatus: priority.accepted ? "awaiting_confirmation" : "blocked",
    qualityGateStatus: priority.accepted ? "passed" : "blocked",
    kml_ready: false,
    crsEvidence: { shadowIntent },
    structuredUtmTable: {
      accepted: priority.accepted,
      reason: priority.reason,
      rowCount: priority.table.rows.length,
      transformationVerification: summarizeStructuredUtmTransformationVerification(priority.transformationVerification)
    },
    _coordinateEvidenceContext: coordinateEvidenceContext
  }, {}, {
    includeCoordinateEvidenceDebug: includeDebug
  });
}

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

test("16 rows all match and mismatched labels are empty", () => {
  const priority = buildPriority();
  assert.equal(priority.accepted, true);
  assert.equal(priority.transformationVerification.status, "match");
  assert.equal(priority.transformationVerification.comparedRows, 16);
  assert.equal(priority.transformationVerification.matchedRows, 16);
  assert.equal(priority.transformationVerification.mismatchedRows, 0);
  assert.deepEqual(priority.transformationVerification.mismatchedPointLabels, []);
});

test("one DMS reference error reports point 13 mismatch", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: 119 + 30 / 60 + 29.795 / 3600 }
    : row));
  assert.equal(priority.accepted, false);
  assert.equal(priority.reason, "transformation_verification_failed");
  assert.deepEqual(priority.transformationVerification.mismatchedPointLabels, ["13"]);
});

test("multiple mismatches preserve labels", () => {
  const priority = buildPriority(rows => rows.map(row => {
    if (row.point === "3") return { ...row, latitude: row.latitude + 0.00001 };
    if (row.point === "13") return { ...row, longitude: row.longitude + 0.00001 };
    return row;
  }));
  assert.deepEqual(priority.transformationVerification.mismatchedPointLabels, ["3", "13"]);
  assert.equal(priority.transformationVerification.mismatchedRows, 2);
});

test("delta fields are correct", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: 119 + 30 / 60 + 29.795 / 3600 }
    : row));
  const point13 = findPoint(priority.transformationVerification, 13);
  assert.ok(point13.longitudeDifference > 0.00001);
  assert.equal(point13.maximumDifference, Math.max(point13.latitudeDifference, point13.longitudeDifference));
});

test("projected X/Y are retained per point", () => {
  const priority = buildPriority();
  const point1 = findPoint(priority.transformationVerification, 1);
  assert.equal(point1.projected.x, 778807.293);
  assert.equal(point1.projected.y, 9721476.737);
});

test("transformed and reference lat/lon are retained per point", () => {
  const priority = buildPriority();
  const point1 = findPoint(priority.transformationVerification, 1);
  assert.ok(Math.abs(point1.transformed.latitude - point1.reference.latitude) < 1e-6);
  assert.ok(Math.abs(point1.transformed.longitude - point1.reference.longitude) < 1e-6);
  assert.equal(point1.reference.latitude, -2.517445833);
  assert.equal(point1.reference.longitude, 119.507172222);
});

test("reference provenance is sanitized and retained", () => {
  const priority = buildPriority();
  const point1 = findPoint(priority.transformationVerification, 1);
  assert.equal(point1.referenceSource, "raw_dms_rows");
  assert.equal(point1.referenceMergeMode, "point_label");
});

test("debug=true exposes detailed point-level rows", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: row.longitude + 0.00001 }
    : row));
  const response = makeResponse(priority, true);
  const verification = response.debugEvidenceContext.utm.structuredUtmTable.transformationVerification;
  assert.deepEqual(verification.mismatchedPointLabels, ["13"]);
  assert.equal(verification.pointLevelVerification.length, 16);
  assert.equal(findPoint(verification, 13).status, "mismatch");
});

test("debug=false does not expose detailed point-level rows", () => {
  const priority = buildPriority();
  const response = makeResponse(priority, false);
  assert.equal("debugEvidenceContext" in response, false);
  assert.equal("pointLevelVerification" in response.structuredUtmTable.transformationVerification, false);
});

test("security sanitization excludes raw/model/prompt/image secrets", () => {
  const priority = buildPriority();
  const response = makeResponse(priority, true);
  const serialized = JSON.stringify(response);
  assert.equal(serialized.includes("raw OCR"), false);
  assert.equal(serialized.includes("prompt"), false);
  assert.equal(serialized.includes("model response"), false);
  assert.equal(serialized.includes("sk-secret"), false);
  assert.equal(serialized.includes("base64"), false);
  assert.equal(serialized.includes("C:\\Users"), false);
});

test("accepted logic remains unchanged", () => {
  const priority = buildPriority();
  assert.equal(priority.accepted, true);
  assert.equal(priority.reason, "explicit_crs_structured_projected_xy");
  assert.equal(priority.transformationVerification.status, "match");
});

test("fail-closed logic remains unchanged", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: row.longitude + 0.00001 }
    : row));
  const response = makeResponse(priority, true);
  assert.equal(priority.accepted, false);
  assert.equal(priority.reason, "transformation_verification_failed");
  assert.equal(response.precisionMode, "utm-projected-x-y-review");
  assert.equal(response.confirmationStatus, "blocked");
  assert.equal(response.qualityGateStatus, "blocked");
  assert.equal(response.kml_ready, false);
  assert.equal(response.coordinateResult.state, "BLOCKED_REVIEW");
});

test("selective DMS reference reread accepts only verification-improving rows", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: 119 + 30 / 60 + 29.795 / 3600 }
    : row));
  const selectiveReferenceRows = parseSelectiveDmsReferenceModelText(JSON.stringify({
    status: "observed",
    rows: [{ point: "13", latitude: "2°31'20.840\" S", longitude: "119°30'29.757\" E" }]
  }));
  const table = mergeSelectiveDmsReferenceRows(priority.table, selectiveReferenceRows, { shadowIntent });
  const replacement = table.selectiveDmsReferenceReread.replacements[0];
  assert.equal(replacement.point, "13");
  assert.equal(replacement.accepted, true);
  assert.equal(replacement.reason, "verification_improved");
  const repaired = buildStructuredUtmPriority({ shadowIntent, table });
  assert.equal(repaired.accepted, true);
  assert.equal(repaired.transformationVerification.status, "match");
});

test("selective DMS reference reread rejects non-improving rows and stays fail-closed", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: 119 + 30 / 60 + 29.795 / 3600 }
    : row));
  const selectiveReferenceRows = parseSelectiveDmsReferenceModelText(JSON.stringify({
    status: "observed",
    rows: [{ point: "13", latitude: "2°31'20.840\" S", longitude: "119°30'29.895\" E" }]
  }));
  const table = mergeSelectiveDmsReferenceRows(priority.table, selectiveReferenceRows, { shadowIntent });
  const replacement = table.selectiveDmsReferenceReread.replacements[0];
  assert.equal(replacement.accepted, false);
  assert.equal(replacement.reason, "verification_not_improved");
  const repaired = buildStructuredUtmPriority({ shadowIntent, table });
  assert.equal(repaired.accepted, false);
  assert.equal(repaired.reason, "transformation_verification_failed");
});

console.log(`UTM Point-level Verification Observability Regression: ${passed}/14 PASS`);
