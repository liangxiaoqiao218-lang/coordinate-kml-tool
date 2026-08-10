import assert from "node:assert/strict";

import { arbitrateCoordinateType } from "../server/coordinate-type/arbitration.js";
import { buildCoordinateResultV1 } from "../server/coordinate-type/coordinate-result.js";

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

function chatCandidate({ hemisphereEvidence = "signed", hemisphereAmbiguous = false, warning = "", warnings = [] } = {}) {
  return {
    isChatCoordinates: true,
    hemisphereEvidence,
    coordinateOrderEvidence: "default_latlon",
    ambiguity: {
      hemisphere: hemisphereAmbiguous,
      order: false,
      reason: hemisphereAmbiguous ? "decimal_pair_without_hemisphere" : null
    },
    warning,
    warnings
  };
}

function assertDecisionState(name, decision, expectedState) {
  const coordinateResult = buildCoordinateResultV1(decision);
  assert.equal(coordinateResult.state, expectedState, `${name} coordinateResult.state`);
  return coordinateResult;
}

test("Case A unsigned WGS84 chat requires confirmation", () => {
  const decision = arbitrateCoordinateType({
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "absent",
      hemisphereAmbiguous: true
    })
  });

  assert.equal(decision.coordinateType, "wgs84_chat_coordinates");
  assert.equal(decision.precisionMode, "wgs84-chat-coordinates");
  assert.equal(decision.reason, "hemisphere_ambiguous");
  assert.equal(decision.confirmationStatus, "required");
  assert.equal(decision.qualityGateStatus, "passed");
  assert.equal(decision.kml_ready, false);
  assert.equal(decision.kml_allowed, false);
  assertDecisionState("Case A", decision, "CONFIRM_REQUIRED");
});

test("Case B signed WGS84 chat remains AUTO_EXPORT", () => {
  const decision = arbitrateCoordinateType({
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "signed",
      hemisphereAmbiguous: false
    })
  });

  assert.equal(decision.reason, "wgs84_chat_coordinates");
  assert.equal(decision.confirmationStatus, "not_required");
  assert.equal(decision.qualityGateStatus, "passed");
  assert.equal(decision.kml_ready, true);
  assertDecisionState("Case B", decision, "AUTO_EXPORT");
});

test("Case C equal unsigned decimal pair cannot AUTO_EXPORT", () => {
  const decision = arbitrateCoordinateType({
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "absent",
      hemisphereAmbiguous: true
    })
  });

  const coordinateResult = assertDecisionState("Case C", decision, "CONFIRM_REQUIRED");
  assert.notEqual(coordinateResult.state, "AUTO_EXPORT");
  assert.equal(decision.reason, "hemisphere_ambiguous");
});

test("Case D explicit signed WGS84 chat remains AUTO_EXPORT", () => {
  const decision = arbitrateCoordinateType({
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "signed",
      hemisphereAmbiguous: false
    })
  });

  assert.equal(decision.kml_ready, true);
  assertDecisionState("Case D", decision, "AUTO_EXPORT");
});

test("Case E Indonesia02 verified UTM remains AUTO_EXPORT", () => {
  const decision = arbitrateCoordinateType({
    structuredUtmPriority: {
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
    },
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "absent",
      hemisphereAmbiguous: true
    })
  });

  assert.equal(decision.coordinateType, "utm_projected_xy");
  assert.equal(decision.precisionMode, "utm-projected-x-y");
  assert.equal(decision.reason, "explicit_utm_crs_and_structured_xy");
  assertDecisionState("Case E", decision, "AUTO_EXPORT");
});

test("MGRS type lock remains unaffected by chat ambiguity", () => {
  const decision = arbitrateCoordinateType({
    mgrs: { isMgrs: true },
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "absent",
      hemisphereAmbiguous: true
    })
  });

  assert.equal(decision.coordinateType, "mgrs_utm_grid_reference");
  assert.equal(decision.reason, "mgrs_type_lock");
  assertDecisionState("MGRS", decision, "AUTO_EXPORT");
});

test("Handwritten DMS remains CONFIRM_REQUIRED", () => {
  const decision = arbitrateCoordinateType({
    handwrittenDms: { isHandwrittenDms: true },
    chatCoordinates: chatCandidate({
      hemisphereEvidence: "absent",
      hemisphereAmbiguous: true
    })
  });

  assert.equal(decision.precisionMode, "handwritten-dms-coordinates");
  assert.equal(decision.reason, "handwritten_dms_requires_review");
  assertDecisionState("Handwritten DMS", decision, "CONFIRM_REQUIRED");
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Geographic Hemisphere Decision Regression: 7/7 PASS");
