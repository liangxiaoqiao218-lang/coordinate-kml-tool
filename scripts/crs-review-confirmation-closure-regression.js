import assert from "node:assert/strict";
import fs from "node:fs";

import { buildCoordinateResultV1 } from "../server/coordinate-type/coordinate-result.js";
import {
  buildConfirmedCoordinateResponse,
  buildReverifiedCoordinateResponse
} from "../server/coordinate-type/review-confirmation.js";
import { transformUtmWgs84Point } from "../server/utm-intent/utm-wgs84-transform.js";

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

const crs = {
  zone: 50,
  hemisphere: "south"
};

const projectedRows = [
  { point: "1", x: 778807.293, y: 9721224.771 },
  { point: "2", x: 778826.031, y: 9721204.457 },
  { point: "3", x: 778855.308, y: 9721181.948 }
];

const referenceRows = projectedRows.map(row => {
  const transformed = transformUtmWgs84Point({
    easting: row.x,
    northing: row.y,
    zone: crs.zone,
    hemisphere: crs.hemisphere
  });
  return {
    point: row.point,
    referenceLatitude: transformed.latitude,
    referenceLongitude: transformed.longitude
  };
});

const badRows = projectedRows.map(row => row.point === "3"
  ? { ...row, y: 9721188.194 }
  : row);

function responseFor(rows) {
  return buildReverifiedCoordinateResponse({
    projectedRows: rows,
    crs,
    referenceRows
  });
}

test("verification FAIL has review summary and no confirmation bypass", () => {
  const response = responseFor(badRows);
  assert.equal(response.precisionMode, "utm-projected-x-y-review");
  assert.equal(response.confirmationStatus, "blocked");
  assert.equal(response.qualityGateStatus, "blocked");
  assert.equal(response.kml_ready, false);
  assert.equal(response.coordinateResult.state, "BLOCKED_REVIEW");
  assert.equal(response.coordinateResult.review.canUserConfirm, false);
  assert.equal(response.reviewSummary.status, "review_required");
  assert.deepEqual(response.reviewSummary.mismatchedPointLabels, ["3"]);
  assert.equal(response.reviewSummary.points[0].point, "3");
  assert.equal(response.reviewSummary.points[0].observed.y, 9721188.194);
});

test("failed verification cannot be confirmed", () => {
  const response = buildConfirmedCoordinateResponse({
    projectedRows: badRows,
    crs,
    referenceRows
  });
  assert.equal(response.success, false);
  assert.equal(response.kml_ready, false);
  assert.notEqual(response.confirmationStatus, "accepted");
});

test("reverify PASS returns awaiting confirmation, not KML ready", () => {
  const response = responseFor(projectedRows);
  assert.equal(response.precisionMode, "utm-projected-x-y");
  assert.equal(response.confirmationStatus, "awaiting_confirmation");
  assert.equal(response.qualityGateStatus, "passed");
  assert.equal(response.kml_ready, false);
  assert.equal(response.coordinateResult.state, "CONFIRM_REQUIRED");
  assert.equal(response.coordinateResult.kml.ready, false);
});

test("server confirmation after PASS allows KML", () => {
  const response = buildConfirmedCoordinateResponse({
    projectedRows,
    crs,
    referenceRows
  });
  assert.equal(response.success, true);
  assert.equal(response.confirmationStatus, "accepted");
  assert.equal(response.qualityGateStatus, "passed");
  assert.equal(response.kml_ready, true);
  assert.equal(response.coordinateResult.state, "AUTO_EXPORT");
  assert.equal(response.coordinateResult.kml.ready, true);
});

test("verified UTM AUTO_EXPORT contract mismatch is resolved", () => {
  const result = buildCoordinateResultV1({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    confirmationStatus: "awaiting_confirmation",
    qualityGateStatus: "passed",
    kml_ready: false,
    requires_review: false,
    coordinateArbitration: {
      reason: "explicit_utm_crs_and_structured_xy"
    }
  });
  assert.equal(result.state, "CONFIRM_REQUIRED");
  assert.equal(result.kml.ready, false);
});

test("review summary is sanitized", () => {
  const response = responseFor(badRows);
  const serialized = JSON.stringify(response.reviewSummary);
  assert.doesNotMatch(serialized, /raw OCR|prompt|model response|base64|api[_-]?key|secret|C:\\/i);
  assert.equal(response.reviewSummary.referenceRows.length, 3);
});

test("frontend wires review, reverify, and server confirmation", () => {
  const source = fs.readFileSync(new URL("../index.html", import.meta.url), "utf8");
  assert.match(source, /reviewSummary/);
  assert.match(source, /reverificationContext/);
  assert.match(source, /\/api\/reverify-coordinate-result/);
  assert.match(source, /\/api\/confirm-coordinate-result/);
  assert.match(source, /修改异常坐标/);
  assert.match(source, /重新验证/);
  assert.match(source, /CONFIRMATION_REQUIRES_VERIFICATION_PASS/);
});

test("frontend edit invalidates old confirmation state", () => {
  const source = fs.readFileSync(new URL("../index.html", import.meta.url), "utf8");
  assert.match(source, /function markCoordinateTextChanged\(\)[\s\S]*activeCoordinateResult = null/);
  assert.match(source, /function markCoordinateTextChanged\(\)[\s\S]*kml_ready:\s*false/);
  assert.match(source, /function markCoordinateTextChanged\(\)[\s\S]*requiresReview:\s*true/);
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("CRS Review Confirmation Closure Regression: 8/8 PASS");
