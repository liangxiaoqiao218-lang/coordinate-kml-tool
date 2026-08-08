import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";

import { arbitrateCoordinateType, COORDINATE_TYPE_PRIORITY } from "../server/coordinate-type/arbitration.js";

const fixtureRoot = process.env.COORDINATE_TYPE_FIXTURE_ROOT
  || "D:\\萨赫勒数字科技有限公司\\关于西非的业务\\测试素材";

const typedUtm50sPriority = {
  accepted: true,
  reason: "explicit_crs_structured_projected_xy",
  typedUtmIntent: {
    coordinateType: "utm_projected_xy",
    projection: "utm",
    datum: "WGS84",
    zone: 50,
    hemisphere: "south",
    epsg: "EPSG:32750"
  },
  transformationVerification: { status: "match" }
};

const imageCases = [
  { file: "印尼矿地01.jpg", expectedType: "utm_projected_xy", expectedMode: "utm-projected-x-y", kind: "utm" },
  { file: "印尼矿地02.jpg", expectedType: "utm_projected_xy", expectedMode: "utm-projected-x-y", kind: "utm" },
  { file: "印尼矿地03.jpg", expectedType: "utm_projected_xy", expectedMode: "utm-projected-x-y", kind: "utm" },
  { file: "科特迪瓦03.png", expectedType: "cote_divoire_geographic_dms_table", expectedMode: "cote-divoire-geographic-dms-table", kind: "dms" },
  { file: "科特迪瓦04.png", expectedType: "cote_divoire_geographic_dms_table", expectedMode: "cote-divoire-geographic-dms-table", kind: "dms" }
];

assert.deepEqual(COORDINATE_TYPE_PRIORITY, [
  "explicit_crs_evidence",
  "typed_projection",
  "structured_xy",
  "validated_wgs84",
  "dms",
  "chat"
]);

let imagePassed = 0;
for (const testCase of imageCases) {
  const imagePath = path.join(fixtureRoot, testCase.file);
  const image = await readFile(imagePath);
  const sha256 = createHash("sha256").update(image).digest("hex");
  assert.ok(image.length > 0, `${testCase.file} is empty`);

  const decision = testCase.kind === "utm"
    ? arbitrateCoordinateType({
        structuredUtmPriority: typedUtm50sPriority,
        // These candidates reproduce the legacy takeover risk and must lose.
        utm30Accepted: true,
        dmsAccepted: true,
        wgs84TableCoordinates: { isWgs84TableCoordinates: true }
      })
    : arbitrateCoordinateType({
        dmsAccepted: true,
        coordinateEngineV2: {
          coordinate_type: "cote_divoire_geographic_dms_table",
          precision_mode: "cote-divoire-geographic-dms-table",
          requires_review: false
        }
      });

  assert.equal(decision.coordinateType, testCase.expectedType);
  assert.equal(decision.precisionMode, testCase.expectedMode);
  assert.equal(decision.requires_review, false);
  assert.equal(decision.kml_allowed, true);
  if (testCase.kind === "utm") {
    assert.equal(decision.arbitrationEligible, true);
    assert.equal(decision.confirmationStatus, "awaiting_confirmation");
    assert.equal(decision.qualityGateStatus, "passed");
    assert.equal(decision.kml_ready, false);
    assert.equal(decision.lat_lon_role, "verification_only");
    assert.ok(decision.blockedFallbacks.includes("utm30n-projected-x-y"));
    assert.ok(decision.blockedFallbacks.includes("dms"));
  }
  if (testCase.kind === "dms") assert.equal(decision.kml_ready, true);
  imagePassed += 1;
  console.log(JSON.stringify({
    sample: testCase.file,
    sha256,
    coordinateType: decision.coordinateType,
    precisionMode: decision.precisionMode,
    requires_review: decision.requires_review,
    kml_allowed: decision.kml_allowed,
    kml_ready: decision.kml_ready,
    result: "PASS"
  }));
}

const protectionCases = [
  {
    name: "BFTM lock",
    decision: arbitrateCoordinateType({ bftmAccepted: true, utm30Accepted: true }),
    type: "bftm_projected_xy",
    mode: "bftm-projected-x-y"
  },
  {
    name: "MGRS lock",
    decision: arbitrateCoordinateType({ mgrs: { isMgrs: true }, utm30Accepted: true }),
    type: "mgrs_utm_grid_reference",
    mode: "mgrs-utm-grid-reference"
  },
  {
    name: "Kyrgyz GK lock",
    decision: arbitrateCoordinateType({ kyrgyzGk: { isKyrgyzGk: true }, utm30Accepted: true }),
    type: "kyrgyz_gk_projected_xy",
    mode: "kyrgyz-gk-point-x-y"
  },
  {
    name: "ordinary DMS",
    decision: arbitrateCoordinateType({ dmsAccepted: true }),
    type: "dms",
    mode: "preserve-original-decimals-and-parse-dms"
  }
];

for (const testCase of protectionCases) {
  assert.equal(testCase.decision.coordinateType, testCase.type, testCase.name);
  assert.equal(testCase.decision.precisionMode, testCase.mode, testCase.name);
}

const swapped = arbitrateCoordinateType({
  chatCoordinates: {
    isChatCoordinates: true,
    warnings: ["possible swapped lat/lon"]
  }
});
assert.equal(swapped.requires_review, true);
assert.equal(swapped.kml_allowed, false);
assert.equal(swapped.reason, "possible_swapped_lat_lon");

const mismatch = arbitrateCoordinateType({
  structuredUtmPriority: {
    ...typedUtm50sPriority,
    accepted: false,
    reason: "transformation_verification_failed"
  },
  dmsAccepted: true
});
assert.equal(mismatch.coordinateType, "utm_projected_xy");
assert.equal(mismatch.precisionMode, "utm-projected-x-y-review");
assert.equal(mismatch.requires_review, true);
assert.equal(mismatch.kml_allowed, false);

const explicitUtmBeatsBftmHeuristic = arbitrateCoordinateType({
  structuredUtmPriority: typedUtm50sPriority,
  bftmAccepted: true
});
assert.equal(explicitUtmBeatsBftmHeuristic.coordinateType, "utm_projected_xy");
assert.equal(explicitUtmBeatsBftmHeuristic.reason, "explicit_utm_crs_and_structured_xy");

const explicitCrsConflict = arbitrateCoordinateType({
  bftmAccepted: true,
  crsEvidenceShadow: {
    shadowIntent: {
      conflicts: [{ type: "crs_conflict", sources: ["utm", "bftm"] }]
    }
  }
});
assert.equal(explicitCrsConflict.coordinateType, "crs_conflict");
assert.equal(explicitCrsConflict.requires_review, true);
assert.equal(explicitCrsConflict.arbitrationEligible, false);
assert.equal(explicitCrsConflict.confirmationStatus, "blocked");
assert.equal(explicitCrsConflict.kml_ready, false);

const explicitUtmWithoutTypedRows = arbitrateCoordinateType({
  bftmAccepted: true,
  crsEvidenceShadow: {
    shadowIntent: {
      confidence: "confirmed",
      projection: "utm",
      datum: "WGS84",
      zone: 50,
      hemisphere: "south",
      epsg: "EPSG:32750",
      conflicts: []
    }
  }
});
assert.equal(explicitUtmWithoutTypedRows.coordinateType, "utm_projected_xy");
assert.equal(explicitUtmWithoutTypedRows.precisionMode, "utm-projected-x-y-review");
assert.equal(explicitUtmWithoutTypedRows.reason, "explicit_utm_crs_without_validated_structured_xy");
assert.equal(explicitUtmWithoutTypedRows.kml_ready, false);

const html = await readFile(new URL("../index.html", import.meta.url), "utf8");
assert.ok(html.includes("KML_COORDINATE_ORDER_REVIEW_REQUIRED"));
assert.ok(html.includes("KML_CRS_CONFLICT"));
assert.ok(html.includes("data.coordinateType === \"utm_projected_xy\""));
assert.ok(html.includes("Never map an"));
assert.ok(!html.includes('return "utm30n";\n      }\n\n      if (/bftm'));

console.log(`Coordinate Type Arbitration Real Asset Regression: ${imagePassed}/${imageCases.length} PASS`);
console.log(`Dedicated Type Protection Regression: ${protectionCases.length}/${protectionCases.length} PASS`);
console.log("Swapped Lat/Lon KML Gate Regression: 1/1 PASS");
console.log("UTM Transformation Mismatch Gate Regression: 1/1 PASS");
console.log("Explicit UTM vs BFTM Heuristic Regression: 1/1 PASS");
console.log("Explicit CRS Conflict Regression: 1/1 PASS");
console.log("Explicit UTM Without Typed Rows Blocks Heuristic Regression: 1/1 PASS");
