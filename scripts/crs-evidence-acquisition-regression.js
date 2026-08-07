import assert from "node:assert/strict";

import { normalizeCrsVisionOutput, parseCrsVisionModelText } from "../server/crs-evidence/crs-vision-pass.js";
import { collectCrsEvidence } from "../server/crs-evidence/evidence-collector.js";
import { buildShadowIntentFromCrsVision } from "../server/crs-evidence/shadow-pipeline.js";

const cases = [
  {
    name: "UTM50S confirmed",
    acquisition: {
      status: "observed",
      observations: [{ field: "crs_label", rawText: "UTM WGS 1984 ZONA 50S", source: "crs_vision", region: "bottom_footer" }]
    },
    verify(result) {
      assert.equal(result.crsEvidence.projection, "utm");
      assert.equal(result.crsEvidence.datum, "WGS84");
      assert.equal(result.crsEvidence.zone, 50);
      assert.equal(result.crsEvidence.hemisphere, "south");
      assert.equal(result.shadowIntent.confidence, "confirmed");
      assert.equal(result.shadowIntent.epsg, "EPSG:32750");
    }
  },
  {
    name: "UTM30 confirmed",
    acquisition: {
      status: "observed",
      observations: [{ field: "crs_label", rawText: "UTM WGS 1984 ZONE 30N", source: "crs_vision", region: "document_body" }]
    },
    verify(result) {
      assert.equal(result.shadowIntent.zone, 30);
      assert.equal(result.shadowIntent.hemisphere, "north");
      assert.equal(result.shadowIntent.epsg, "EPSG:32630");
      assert.equal(result.shadowIntent.confidence, "confirmed");
    }
  },
  {
    name: "BFTM excluded",
    acquisition: {
      status: "observed",
      observations: [{ field: "projection_label", rawText: "Projection BFTM / Systeme de Reference ITRF 2008", source: "crs_vision", region: "bottom_footer" }]
    },
    verify(result) {
      assert.equal(result.crsEvidence.projection, null);
      assert.deepEqual(result.crsEvidence.exclusions, ["bftm"]);
      assert.notEqual(result.shadowIntent.confidence, "confirmed");
    }
  },
  {
    name: "MGRS blocks UTM projected XY",
    acquisition: {
      status: "observed",
      observations: [{ field: "grid_reference_label", rawText: "47RLH", source: "crs_vision", region: "document_body" }]
    },
    verify(result) {
      assert.equal(result.crsEvidence.projection, null);
      assert.ok(result.crsEvidence.exclusions.includes("mgrs"));
      assert.deepEqual(result.shadowIntent.blockedFallbacks, ["utm_projected_xy"]);
      assert.notEqual(result.shadowIntent.confidence, "confirmed");
    }
  },
  {
    name: "Kyrgyz Gauss-Kruger excluded",
    acquisition: {
      status: "observed",
      observations: [{ field: "projection_label", rawText: "Gauss-Kruger rectangular coordinate system", source: "crs_vision", region: "table_caption" }]
    },
    verify(result) {
      assert.equal(result.crsEvidence.projection, null);
      assert.ok(result.crsEvidence.exclusions.includes("kyrgyzstan_gk"));
      assert.notEqual(result.shadowIntent.confidence, "confirmed");
    }
  },
  {
    name: "Unknown XY is not evidence",
    acquisition: normalizeCrsVisionOutput({
      observations: [{ field: "crs_label", rawText: "778500,9720912", source: "crs_vision", region: "table_caption" }]
    }),
    verify(result) {
      assert.equal(result.crsVision.status, "none");
      assert.equal(result.shadowIntent.confidence, "unknown");
      assert.equal(result.shadowIntent.zone, null);
    }
  }
];

let passed = 0;
for (const testCase of cases) {
  const result = buildShadowIntentFromCrsVision(testCase.acquisition);
  testCase.verify(result);
  passed += 1;
  console.log(`PASS ${testCase.name}`);
}

const invalid = parseCrsVisionModelText("not json");
assert.deepEqual(invalid, { status: "invalid", observations: [] });
console.log("PASS invalid model response is quarantined");
console.log(`\nCRS Evidence Acquisition Regression: ${passed + 1}/${cases.length + 1} PASS`);
