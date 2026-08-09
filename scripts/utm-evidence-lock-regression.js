import assert from "node:assert/strict";
import {
  hasExplicitUtmEvidenceLock,
  suppressGeographicFallbacksForUtmEvidenceLock
} from "../server/utm-intent/evidence-lock.js";
import { arbitrateCoordinateType } from "../server/coordinate-type/arbitration.js";

function explicitUtmShadow(overrides = {}) {
  return {
    shadowIntent: {
      confidence: "confirmed",
      projection: "utm",
      datum: "WGS84",
      zone: 50,
      hemisphere: "south",
      epsg: "EPSG:32750",
      conflicts: [],
      ...overrides
    }
  };
}

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

test("explicit UTM CRS evidence activates lock", () => {
  assert.equal(hasExplicitUtmEvidenceLock(explicitUtmShadow()), true);
});

test("conflicted UTM CRS evidence does not activate lock", () => {
  assert.equal(hasExplicitUtmEvidenceLock(explicitUtmShadow({ conflicts: ["BFTM"] })), false);
});

test("explicit UTM lock suppresses WGS84 table and chat final winners", () => {
  const locked = suppressGeographicFallbacksForUtmEvidenceLock({
    wgs84TableCoordinates: {
      isWgs84TableCoordinates: true,
      points: [{ lon: 119.5, lat: -2.5 }]
    },
    chatCoordinates: {
      isChatCoordinates: true,
      points: [{ lon: 119.5, lat: -2.5 }]
    }
  }, { enabled: true });

  assert.equal(locked.wgs84TableCoordinates.isWgs84TableCoordinates, false);
  assert.equal(locked.wgs84TableCoordinates.verificationOnly, true);
  assert.equal(locked.chatCoordinates.isChatCoordinates, false);
  assert.equal(locked.chatCoordinates.verificationOnly, true);
  assert.deepEqual(locked.suppressedFallbacks, ["WGS84_TABLE", "WGS84_CHAT"]);
});

test("explicit UTM lock suppresses DMS fallback final winners", () => {
  const locked = suppressGeographicFallbacksForUtmEvidenceLock({
    dmsGroupedAccepted: true,
    frenchPerimeterDms: { isFrenchPerimeterDms: true },
    pointAzDmsTableAccepted: true,
    handwrittenDms: { isHandwrittenDms: true },
    dmsAccepted: true
  }, { enabled: true });

  assert.equal(locked.dmsGroupedAccepted, false);
  assert.equal(locked.frenchPerimeterDms.isFrenchPerimeterDms, false);
  assert.equal(locked.frenchPerimeterDms.verificationOnly, true);
  assert.equal(locked.pointAzDmsTableAccepted, false);
  assert.equal(locked.handwrittenDms.isHandwrittenDms, false);
  assert.equal(locked.handwrittenDms.verificationOnly, true);
  assert.equal(locked.dmsAccepted, false);
  assert.deepEqual(locked.suppressedFallbacks, [
    "DMS_GROUPED",
    "FRENCH_PERIMETER_DMS",
    "POINT_AZ_DMS_TABLE",
    "HANDWRITTEN_DMS",
    "DMS"
  ]);
});

test("explicit UTM wins over WGS84-looking candidates after suppression", () => {
  const candidates = suppressGeographicFallbacksForUtmEvidenceLock({
    wgs84TableCoordinates: { isWgs84TableCoordinates: true, points: [{ lon: 119.5, lat: -2.5 }] },
    chatCoordinates: { isChatCoordinates: true, points: [{ lon: 119.5, lat: -2.5 }] }
  }, { enabled: true });
  const decision = arbitrateCoordinateType({
    crsEvidenceShadow: explicitUtmShadow(),
    structuredUtmPriority: {
      accepted: true,
      reason: "explicit_crs_structured_projected_xy"
    },
    ...candidates
  });

  assert.equal(decision.coordinateType, "utm_projected_xy");
  assert.equal(decision.precisionMode, "utm-projected-x-y");
  assert.equal(decision.confirmationStatus, "awaiting_confirmation");
  assert.equal(decision.reason, "explicit_utm_crs_and_structured_xy");
});

test("explicit UTM mismatch remains blocked review instead of WGS84 fallback", () => {
  const candidates = suppressGeographicFallbacksForUtmEvidenceLock({
    wgs84TableCoordinates: { isWgs84TableCoordinates: true, points: [{ lon: 119.5, lat: -2.5 }] },
    dmsAccepted: true
  }, { enabled: true });
  const decision = arbitrateCoordinateType({
    crsEvidenceShadow: explicitUtmShadow(),
    structuredUtmPriority: {
      accepted: false,
      reason: "transformation_verification_failed"
    },
    ...candidates
  });

  assert.equal(decision.coordinateType, "utm_projected_xy");
  assert.equal(decision.precisionMode, "utm-projected-x-y-review");
  assert.equal(decision.confirmationStatus, "blocked");
  assert.equal(decision.qualityGateStatus, "blocked");
  assert.equal(decision.kml_ready, false);
  assert.equal(decision.reason, "utm_transformation_verification_failed");
});

test("Indonesia 03 repeated parser competition stays UTM under explicit evidence lock", () => {
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const candidates = suppressGeographicFallbacksForUtmEvidenceLock({
      wgs84TableCoordinates: { isWgs84TableCoordinates: true, points: [{ lon: 119.623, lat: -2.738 }] },
      chatCoordinates: { isChatCoordinates: true, points: [{ lon: 119.623, lat: -2.738 }] },
      dmsAccepted: attempt === 2,
      handwrittenDms: { isHandwrittenDms: attempt === 3 }
    }, { enabled: true });
    const decision = arbitrateCoordinateType({
      crsEvidenceShadow: explicitUtmShadow(),
      structuredUtmPriority: {
        accepted: attempt !== 2,
        reason: attempt === 2 ? "transformation_verification_failed" : "explicit_crs_structured_projected_xy"
      },
      ...candidates
    });

    assert.equal(decision.coordinateType, "utm_projected_xy");
    assert.notEqual(decision.coordinateType, "wgs84_geographic_table");
    assert.notEqual(decision.precisionMode, "wgs84-table-coordinates");
  }
});

test("ordinary WGS84 table remains valid when UTM lock is disabled", () => {
  const candidates = suppressGeographicFallbacksForUtmEvidenceLock({
    wgs84TableCoordinates: { isWgs84TableCoordinates: true, points: [{ lon: 16.032, lat: 3.7638 }] }
  }, { enabled: false });
  const decision = arbitrateCoordinateType(candidates);

  assert.equal(decision.coordinateType, "wgs84_geographic_table");
  assert.equal(decision.precisionMode, "wgs84-table-coordinates");
  assert.equal(decision.reason, "validated_wgs84_table");
});

if (process.exitCode) {
  process.exit(process.exitCode);
}
console.log("UTM Evidence Lock Regression: 8/8 PASS");
