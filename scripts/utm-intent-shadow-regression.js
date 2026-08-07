import assert from "node:assert/strict";
import { resolveShadowUtmIntent } from "../server/utm-intent/shadow-resolver.js";

const confirmed50s = {
  projection: "utm",
  datum: "WGS84",
  zone: 50,
  hemisphere: "south",
  epsg: "EPSG:32750",
  confidence: "confirmed"
};

const utmBlockedFallbacks = [
  "bftm_xy",
  "generic_projected_xy",
  "wgs84_chat_coordinates"
];

const cases = [
  {
    group: "Indonesia UTM50S",
    id: "indonesia_01",
    rawText: "SISTEM KOORDINAT\nUTM WGS 1984 ZONA 50S\nX Y Latitude Longitude\n778000 9720000",
    expected: { ...confirmed50s, blockedFallbacks: utmBlockedFallbacks }
  },
  {
    group: "Indonesia UTM50S",
    id: "indonesia_02",
    rawText: "SISTEM KOORDINAT: WGS84 / UTM ZONE 50 S\n778125 9720410",
    expected: { ...confirmed50s, blockedFallbacks: utmBlockedFallbacks }
  },
  {
    group: "Indonesia UTM50S",
    id: "indonesia_03",
    rawText: "World Geodetic System 1984\nUniversal Transverse Mercator Zone 50 South\nX 778300 Y 9720900",
    expected: { ...confirmed50s, blockedFallbacks: utmBlockedFallbacks }
  },
  {
    group: "Burkina UTM30",
    id: "utm30_burkina_003",
    rawText: "SYSTEME DE COORDONNEES\nWGS 1984 UTM ZONE 30N\nSOMMETS X Y\n1 727250 1219700",
    expected: {
      projection: "utm",
      datum: "WGS84",
      zone: 30,
      hemisphere: "north",
      epsg: "EPSG:32630",
      confidence: "confirmed",
      blockedFallbacks: utmBlockedFallbacks
    }
  },
  {
    group: "BFTM",
    id: "bftm_burkina_002",
    rawText: "Projection BFTM / ITRF 2008\nSOMMETS X(m) Y(m)\n1 655000 1333600",
    expected: { projection: null, confidence: "unknown", excluded: "bftm", blockedFallbacks: [] }
  },
  {
    group: "MGRS",
    id: "mgrs_myanmar_001",
    rawText: "MGRS / UTM Grid Reference\nA 47RLH 24123 42905\nB 47RLH 24200 43000",
    expected: {
      projection: null,
      confidence: "unknown",
      excluded: "mgrs",
      blockedFallbacks: ["utm_projected_xy"]
    }
  },
  {
    group: "Kyrgyz GK",
    id: "kyrgyz_gk_001",
    rawText: "Kyrgyzstan Gauss-Kruger EPSG:28413\npoint | X | Y\n1 | 13261341 | 4607777",
    expected: { projection: null, confidence: "unknown", excluded: "kyrgyzstan_gk", blockedFallbacks: [] }
  },
  {
    group: "Unknown projected XY",
    id: "xy_only",
    rawText: "X Y\n1 778000 9720000\n2 778100 9720100",
    expected: { projection: null, confidence: "unknown", blockedFallbacks: [] }
  },
  {
    group: "Country-only context",
    id: "indonesia_country_only",
    rawText: "Indonesia mining concession\nX Y\n1 778000 9720000",
    expected: { projection: null, confidence: "unknown", blockedFallbacks: [] }
  },
  {
    group: "Incomplete UTM evidence",
    id: "utm_without_hemisphere",
    rawText: "WGS 1984 / UTM Zone 50\nX Y\n1 778000 9720000",
    expected: {
      projection: "utm",
      zone: 50,
      confidence: "candidate",
      blockedFallbacks: utmBlockedFallbacks
    }
  },
  {
    group: "CRS conflict",
    id: "utm50s_with_mgrs_token",
    rawText: "UTM WGS 1984 ZONA 50S\n50S AB 12345 67890",
    expected: {
      projection: null,
      confidence: "unknown",
      conflictSources: ["utm", "mgrs"],
      blockedFallbacks: ["utm_projected_xy"]
    }
  },
  {
    group: "CRS conflict",
    id: "utm50s_with_bftm",
    rawText: "UTM WGS 1984 ZONA 50S\nProjection BFTM",
    expected: {
      projection: null,
      confidence: "unknown",
      conflictSources: ["utm", "bftm"],
      blockedFallbacks: utmBlockedFallbacks
    }
  },
  {
    group: "CRS conflict",
    id: "utm50s_with_conflicting_epsg",
    rawText: "UTM WGS 1984 ZONA 50S\nEPSG:32650",
    expected: {
      projection: null,
      confidence: "unknown",
      conflictSources: ["utm_zone_hemisphere", "epsg"],
      blockedFallbacks: utmBlockedFallbacks
    }
  }
];

function selectComparable(intent) {
  return {
    projection: intent.projection,
    datum: intent.datum,
    zone: intent.zone,
    hemisphere: intent.hemisphere,
    epsg: intent.epsg,
    confidence: intent.confidence
  };
}

const results = cases.map(testCase => {
  const { shadowIntent } = resolveShadowUtmIntent({ rawText: testCase.rawText });
  assert.deepEqual(
    Object.keys(shadowIntent),
    [
      "projection",
      "datum",
      "zone",
      "hemisphere",
      "epsg",
      "confidence",
      "evidence",
      "conflicts",
      "blockedFallbacks"
    ],
    `${testCase.id} must expose the complete Phase 1 schema`
  );
  assert.deepEqual(shadowIntent.blockedFallbacks, testCase.expected.blockedFallbacks, testCase.id);

  if (testCase.expected.projection === "utm" && testCase.expected.confidence === "confirmed") {
    assert.deepEqual(
      selectComparable(shadowIntent),
      selectComparable(testCase.expected),
      testCase.id
    );
    assert.deepEqual(shadowIntent.conflicts, [], `${testCase.id} must not report a conflict`);
  } else if (testCase.expected.projection === "utm") {
    assert.equal(shadowIntent.projection, "utm", testCase.id);
    assert.equal(shadowIntent.confidence, testCase.expected.confidence, testCase.id);
    assert.equal(shadowIntent.zone, testCase.expected.zone ?? null, testCase.id);
    assert.equal(shadowIntent.epsg, null, `${testCase.id} must not derive EPSG from incomplete evidence`);
  } else {
    assert.equal(shadowIntent.projection, null, `${testCase.id} must not produce UTM intent`);
    assert.equal(shadowIntent.confidence, testCase.expected.confidence, testCase.id);
    if (testCase.expected.excluded) {
      assert.ok(
        shadowIntent.evidence.some(item => item.value === testCase.expected.excluded),
        `${testCase.id} must record ${testCase.expected.excluded} exclusion evidence`
      );
    }
    if (testCase.expected.conflictSources) {
      assert.deepEqual(shadowIntent.conflicts, [{
        type: "crs_conflict",
        sources: testCase.expected.conflictSources
      }], `${testCase.id} must retain the CRS conflict`);
      assert.equal(shadowIntent.epsg, null, `${testCase.id} conflict must not retain an EPSG`);
    } else {
      assert.deepEqual(shadowIntent.conflicts, [], `${testCase.id} must not report a conflict`);
    }
  }

  return {
    group: testCase.group,
    id: testCase.id,
    shadowIntent
  };
});

const legacyPayload = {
  rawText: cases[1].rawText,
  coordinates: "778125,9720410\n778225,9720410\n778225,9720510",
  precisionMode: "preserve-original-decimals-and-parse-dms",
  parserTrace: ["OCR", "BFTM:rejected"],
  projection: undefined,
  kml: "legacy-kml-unchanged"
};
const legacySnapshot = structuredClone(legacyPayload);
const shadowObservation = resolveShadowUtmIntent({
  rawText: legacyPayload.rawText,
  coordinateContext: {
    coordinates: legacyPayload.coordinates,
    projectionLabel: legacyPayload.projection
  }
});

assert.deepEqual(legacyPayload, legacySnapshot, "shadow resolution must not mutate its legacy inputs");
assert.equal(legacyPayload.precisionMode, legacySnapshot.precisionMode, "precisionMode must remain unchanged");
assert.deepEqual(legacyPayload.parserTrace, legacySnapshot.parserTrace, "parserTrace must remain unchanged");
assert.equal(legacyPayload.kml, legacySnapshot.kml, "KML payload must remain unchanged");
assert.deepEqual(
  selectComparable(shadowObservation.shadowIntent),
  confirmed50s,
  "standalone shadow observation must resolve the UTM evidence"
);
assert.deepEqual(shadowObservation.shadowIntent.conflicts, [], "confirmed observation must have no conflicts");
assert.deepEqual(
  shadowObservation.shadowIntent.blockedFallbacks,
  utmBlockedFallbacks,
  "confirmed UTM observation must block legacy fallbacks"
);

console.log(JSON.stringify({
  status: "PASS",
  cases: results,
  invariants: {
    precisionModeUnchanged: true,
    parserTraceUnchanged: true,
    kmlUnchanged: true
  }
}, null, 2));
