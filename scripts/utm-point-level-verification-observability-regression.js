import assert from "node:assert/strict";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";
import { snapshotPreSuppressionCandidates } from "../server/coordinate-evidence/index.js";
import {
  buildStructuredUtmPriority,
  mergeSelectiveDmsReferenceRows,
  mergeSelectiveProjectedXyRows,
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

function buildProjectedTableWith(mutator = rows => rows) {
  const rows = mutator(projectedRows.map(([point, easting, northing]) => ({ point, easting, northing })));
  return {
    status: "observed",
    rows: rows.map(({ point, easting, northing }) => ({
      point: String(point),
      easting,
      northing,
      xText: String(easting),
      yText: String(northing)
    }))
  };
}

function buildSelectiveXyTable(rows = []) {
  return {
    status: rows.length > 0 ? "observed" : "none",
    rows: rows.map(([point, easting, northing]) => ({
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

function buildPriorityFromProjected(projectedTable) {
  const table = mergeStructuredUtmReferenceRows(projectedTable, buildReferenceTable(), {
    allowVerifiedIndexMerge: true
  });
  return buildStructuredUtmPriority({ shadowIntent, table });
}

function buildXyRecoveredPriority({ badRows, selectiveRows, requestedLabels } = {}) {
  const badPriority = buildPriorityFromProjected(buildProjectedTableWith(badRows));
  const selectiveTable = mergeSelectiveProjectedXyRows(
    badPriority.table,
    buildSelectiveXyTable(selectiveRows),
    { shadowIntent }
  );
  const replacements = selectiveTable.selectiveReread.replacements;
  const repaired = buildStructuredUtmPriority({ shadowIntent, table: selectiveTable });
  return {
    badPriority,
    selectiveTable,
    priority: {
      ...repaired,
      selectiveReread: {
        status: selectiveTable.selectiveReread.status,
        requestedLabels,
        attempts: 1,
        acceptedLabels: replacements.filter(item => item.accepted).map(item => item.point),
        rejectedLabels: replacements.filter(item => !item.accepted).map(item => item.point),
        replacements
      }
    }
  };
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

test("X/Y selective reread metadata is not triggered when no mismatch exists", () => {
  const response = makeResponse(buildPriority(), true);
  assert.equal(response.debugEvidenceContext.utm.xySelectiveReread.triggered, false);
  assert.deepEqual(response.debugEvidenceContext.utm.xySelectiveReread.requestedLabels, []);
  assert.deepEqual(response.debugEvidenceContext.utm.xySelectiveReread.replacements, []);
});

test("one X/Y mismatch records requested label and accepted replacement metadata", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const xy = makeResponse(priority, true).debugEvidenceContext.utm.xySelectiveReread;
  assert.equal(xy.triggered, true);
  assert.deepEqual(xy.requestedLabels, ["4"]);
  assert.deepEqual(xy.acceptedLabels, ["4"]);
  assert.equal(xy.replacements[0].point, "4");
  assert.equal(xy.replacements[0].suspectedField, "Y");
  assert.equal(xy.replacements[0].beforeY, 9721188.194);
  assert.equal(xy.replacements[0].afterY, 9721181.948);
  assert.equal(xy.replacements[0].accepted, true);
});

test("multiple X/Y mismatch labels are observable", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => {
      if (row.point === 3) return { ...row, northing: 9721188.351 };
      if (row.point === 4) return { ...row, northing: 9721188.194 };
      return row;
    }),
    selectiveRows: [[3, 778982.700, 9721182.351], [4, 778855.308, 9721181.948]],
    requestedLabels: ["3", "4"]
  });
  const xy = makeResponse(priority, true).debugEvidenceContext.utm.xySelectiveReread;
  assert.deepEqual(xy.requestedLabels, ["3", "4"]);
  assert.deepEqual(xy.acceptedLabels, ["3", "4"]);
  assert.equal(xy.replacements.length, 2);
});

test("rejected X/Y replacement metadata is observable and fail-closed", () => {
  const { badPriority, selectiveTable } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721189.194]],
    requestedLabels: ["4"]
  });
  const replacements = selectiveTable.selectiveReread.replacements;
  const priority = {
    ...buildStructuredUtmPriority({ shadowIntent, table: selectiveTable }),
    selectiveReread: {
      status: selectiveTable.selectiveReread.status,
      requestedLabels: ["4"],
      attempts: 1,
      acceptedLabels: replacements.filter(item => item.accepted).map(item => item.point),
      rejectedLabels: replacements.filter(item => !item.accepted).map(item => item.point),
      replacements
    }
  };
  const xy = makeResponse(priority, true).debugEvidenceContext.utm.xySelectiveReread;
  assert.equal(badPriority.reason, "transformation_verification_failed");
  assert.deepEqual(xy.acceptedLabels, []);
  assert.deepEqual(xy.rejectedLabels, ["4"]);
  assert.equal(xy.replacements[0].accepted, false);
  assert.equal(xy.replacements[0].reason, "verification_not_improved");
  assert.equal(priority.accepted, false);
});

test("X/Y before and after differences are observable", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const replacement = makeResponse(priority, true).debugEvidenceContext.utm.xySelectiveReread.replacements[0];
  assert.ok(replacement.beforeDifference > replacement.afterDifference);
  assert.ok(replacement.afterDifference < 1e-6);
});

test("debug=true exposes X/Y selective reread metadata", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const response = makeResponse(priority, true);
  assert.equal(response.debugEvidenceContext.utm.xySelectiveReread.triggered, true);
  assert.equal(response.debugEvidenceContext.utm.xySelectiveReread.replacements[0].point, "4");
});

test("debug=false does not expose X/Y selective reread detail", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const response = makeResponse(priority, false);
  assert.equal("debugEvidenceContext" in response, false);
  assert.equal("xySelectiveReread" in response, false);
});

test("X/Y selective reread metadata security excludes raw/provider payload markers", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const serialized = JSON.stringify(makeResponse(priority, true).debugEvidenceContext.utm.xySelectiveReread);
  assert.equal(serialized.includes("raw OCR"), false);
  assert.equal(serialized.includes("prompt"), false);
  assert.equal(serialized.includes("model response"), false);
  assert.equal(serialized.includes("base64"), false);
  assert.equal(serialized.includes("C:\\Users"), false);
  assert.equal(serialized.includes("sk-secret"), false);
});

test("adding X/Y selective reread metadata does not change public behavior", () => {
  const { priority } = buildXyRecoveredPriority({
    badRows: rows => rows.map(row => row.point === 4 ? { ...row, northing: 9721188.194 } : row),
    selectiveRows: [[4, 778855.308, 9721181.948]],
    requestedLabels: ["4"]
  });
  const debugResponse = makeResponse(priority, true);
  const publicResponse = makeResponse(priority, false);
  assert.equal(debugResponse.coordinates, publicResponse.coordinates);
  assert.equal(debugResponse.coordinateType, publicResponse.coordinateType);
  assert.equal(debugResponse.precisionMode, publicResponse.precisionMode);
  assert.equal(debugResponse.confirmationStatus, publicResponse.confirmationStatus);
  assert.equal(debugResponse.qualityGateStatus, publicResponse.qualityGateStatus);
  assert.equal(debugResponse.kml_ready, publicResponse.kml_ready);
  assert.equal(priority.transformationVerification.status, "match");
});

test("X/Y selective reread metadata does not affect DMS reference reread behavior", () => {
  const priority = buildPriority(rows => rows.map(row => row.point === "13"
    ? { ...row, longitude: 119 + 30 / 60 + 29.795 / 3600 }
    : row));
  const selectiveReferenceRows = parseSelectiveDmsReferenceModelText(JSON.stringify({
    status: "observed",
    rows: [{ point: "13", latitude: "2°31'20.840\" S", longitude: "119°30'29.757\" E" }]
  }));
  const table = mergeSelectiveDmsReferenceRows(priority.table, selectiveReferenceRows, { shadowIntent });
  const repaired = buildStructuredUtmPriority({ shadowIntent, table });
  assert.equal(repaired.accepted, true);
  assert.equal(repaired.transformationVerification.status, "match");
});

console.log(`UTM Point-level Verification Observability Regression: ${passed}/24 PASS`);
