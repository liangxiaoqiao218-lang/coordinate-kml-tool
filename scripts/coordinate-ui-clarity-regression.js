import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = await readFile(path.join(repoRoot, "index.html"), "utf8");

function extractFunctionSource(source, functionName) {
  const start = source.indexOf(`function ${functionName}(`);
  assert.notEqual(start, -1, `${functionName} must exist`);
  let parameterDepth = 0;
  let bodyStart = -1;
  for (let index = source.indexOf("(", start); index < source.length; index += 1) {
    if (source[index] === "(") parameterDepth += 1;
    if (source[index] === ")") {
      parameterDepth -= 1;
      if (parameterDepth === 0) {
        bodyStart = source.indexOf("{", index);
        break;
      }
    }
  }
  assert.notEqual(bodyStart, -1, `${functionName} body must start`);
  let depth = 0;
  for (let index = bodyStart; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${functionName} body is not closed`);
}

const createReviewHarness = new Function(`
  ${extractFunctionSource(html, "spatialReviewRequired")}
  ${extractFunctionSource(html, "spatialHasBoundaryReviewWarning")}
  ${extractFunctionSource(html, "spatialWarningText")}
  return { spatialReviewRequired, spatialHasBoundaryReviewWarning, spatialWarningText };
`);
const review = createReviewHarness();

const createCoordinateDisplayHarness = new Function(`
  ${extractFunctionSource(html, "compactRecognizedCoordinateDisplayText")}
  return { compactRecognizedCoordinateDisplayText };
`);
const coordinateDisplay = createCoordinateDisplayHarness();

const clearPayload = {
  mapPreviewObject: {
    previewEligibility: { allowed: true, warning: true },
    previewWarnings: ["possible swapped lat/lon", "source_coordinates_preserved"]
  },
  kmlEligibility: {
    allowed: true,
    confirmationStatus: "NOT_REQUIRED",
    qualityGateStatus: "PASS",
    decisionState: "USABLE"
  }
};
assert.equal(review.spatialReviewRequired(clearPayload), false);
assert.equal(review.spatialWarningText(clearPayload), "");

const uncertainPayload = {
  mapPreviewObject: {
    previewEligibility: { allowed: true, warning: true },
    previewWarnings: ["REVIEW_REQUIRED"]
  },
  kmlEligibility: {
    allowed: true,
    confirmationStatus: "REVIEW_REQUIRED",
    qualityGateStatus: "REVIEW",
    decisionState: "NEEDS_REVIEW"
  }
};
assert.equal(review.spatialReviewRequired(uncertainPayload), true);
assert.match(review.spatialWarningText(uncertainPayload), /不确定信息/);

const selfIntersectionPayload = {
  mapPreviewObject: {
    previewEligibility: { allowed: true, warning: true },
    previewWarnings: ["SELF_INTERSECTION"]
  },
  kmlEligibility: { allowed: false }
};
assert.equal(review.spatialReviewRequired(selfIntersectionPayload), true);
assert.match(review.spatialWarningText(selfIntersectionPayload), /自相交/);

const boundaryReviewPayload = {
  mapPreviewObject: {
    geometryType: "Polygon",
    previewWarnings: ["矿区轮廓待核对：地图已按原图点号顺序连接"]
  },
  kmlEligibility: { allowed: false }
};
assert.equal(review.spatialHasBoundaryReviewWarning(boundaryReviewPayload.mapPreviewObject), true);
assert.match(review.spatialWarningText(boundaryReviewPayload), /按原图点号顺序连接为待核对轮廓/);

const genericPointReviewPayload = {
  mapPreviewObject: { geometryType: "MultiPoint", previewWarnings: ["REVIEW_REQUIRED"] },
  kmlEligibility: { allowed: false }
};
assert.doesNotMatch(review.spatialWarningText(genericPointReviewPayload), /交叉/);

const spacedDms = [
  "1     11°   43’   16.45’’          09°   01’   13.67’’",
  "2     11°   43’   09.20’’          09°   00’   56.03’’"
].join("\n");
assert.equal(
  coordinateDisplay.compactRecognizedCoordinateDisplayText(spacedDms),
  [
    "1 11° 43’ 16.45’’ 09° 01’ 13.67’’",
    "2 11° 43’ 09.20’’ 09° 00’ 56.03’’"
  ].join("\n")
);
assert.equal(
  coordinateDisplay.compactRecognizedCoordinateDisplayText("A | 727250 | 1219700"),
  "A | 727250 | 1219700",
  "projected source rows must not be reformatted"
);

assert.doesNotMatch(html, /验证并查看地图|验证并下载 KML/);
assert.match(html, /id="coordinateCopyAction"[^>]*data-state="blocked"[^>]*aria-disabled="true"[^>]*disabled/);
assert.match(html, /\.coordinate-result-actions \.coordinate-kml-action,\s*\.coordinate-result-actions \.coordinate-copy-action/);
assert.match(html, /coordinateCopyAction\.dataset\.state = copyEnabled \? "enabled" : "blocked"/);

console.log("Coordinate UI clarity regression: 15/15 PASS");
console.log("PROVIDER_CALLS=0");
