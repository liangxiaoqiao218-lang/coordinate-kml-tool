import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  ACQUISITION_ADAPTER_STATUS,
  buildCoordinateEngineV3ProductionShadow,
  createNormalizedCoordinateResult,
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function point(label, latitude, longitude) {
  return { label, lat: latitude, lon: longitude, confidence: 0.95, requires_review: false, warnings: [] };
}

function engine({
  coordinateType,
  precisionMode,
  points,
  groups = null,
  warnings = [],
} = {}) {
  return {
    schema_version: "coordinate_engine_v2",
    coordinate_type: coordinateType,
    precision_mode: precisionMode,
    requires_review: warnings.length > 0,
    warnings,
    groups: groups || [{
      group_id: "group_1",
      group_name: "矿地1",
      geometry: points.length === 1 ? "point" : points.length === 2 ? "line" : "polygon",
      requires_review: warnings.length > 0,
      kml_ready: points.length > 0,
      warnings,
      points,
    }],
  };
}

function shadowFromEngine(coordinateEngineV2, payload = {}) {
  return buildCoordinateEngineV3ProductionShadow({
    payload: {
      rawText: "legacy raw text remains outside the compact V3 production shadow",
      coordinates: "legacy coordinates remain source of truth",
      precisionMode: coordinateEngineV2.precision_mode,
      ...payload,
    },
    coordinateEngineV2,
  });
}

function assertShadowSuccess(result) {
  assert.equal(result.schemaVersion, "coordinate_engine_v3_production_shadow_v1");
  assert.equal(result.shadowOnly, true);
  assert.equal(result.status, V3_PRODUCTION_STATUS.SUCCESS);
  assert.equal(result.reasonCode, null);
  assert.equal(result.productionSupported, true);
  assert.equal(result.technicalKmlReady, true);
}

function assertShadowReview(result, reasonCode) {
  assert.equal(result.shadowOnly, true);
  assert.equal(result.status, V3_PRODUCTION_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reasonCode, reasonCode);
  assert.equal(result.productionSupported, false);
}

function assertShadowUnsupported(result, reasonCode) {
  assert.equal(result.shadowOnly, true);
  assert.equal(result.status, V3_PRODUCTION_STATUS.UNSUPPORTED);
  assert.equal(result.reasonCode, reasonCode);
  assert.equal(result.productionSupported, false);
  assert.equal(result.technicalKmlReady, false);
}

function normalized(overrides = {}) {
  return createNormalizedCoordinateResult({
    coordinateType: "wgs84_decimal",
    recognizerId: "wgs84_decimal",
    coordinates: [
      { label: "1", latitude: 12.319572, longitude: -11.178174 },
      { label: "2", latitude: 12.32, longitude: -11.18 },
      { label: "3", latitude: 12.318, longitude: -11.182 },
    ],
    geometryType: "polygon",
    crs: "EPSG:4326",
    precisionMode: "decimal-degree",
    ...overrides,
  });
}

test("SUCCESS shadow for Côte d’Ivoire", () => {
  const result = shadowFromEngine(engine({
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    points: [
      point("1", 11.869980556, -8.892405556),
      point("2", 11.871447222, -8.89255),
      point("3", 11.870158333, -8.898341667),
    ],
  }));
  assertShadowSuccess(result);
});

test("SUCCESS shadow for Indonesia UTM", () => {
  const result = shadowFromEngine(engine({
    coordinateType: "indonesia_utm",
    precisionMode: "indonesia-utm-wgs84-zone-50s",
    points: [
      point("1", -2.517445833, 119.507172222),
      point("2", -2.517437778, 119.508740278),
      point("3", -2.520103611, 119.508753611),
    ],
  }));
  assertShadowSuccess(result);
});

test("SUCCESS shadow for grouped DMS", () => {
  const result = shadowFromEngine(engine({
    coordinateType: "standard_dms_table",
    precisionMode: "dms-grouped-coordinates",
    points: [],
    groups: [
      {
        group_id: "group_1",
        group_name: "Mining Area 1",
        geometry: "polygon",
        points: [
          point("1", 11.873811111, -8.887052778),
          point("2", 11.872575, -8.886605556),
          point("3", 11.872222222, -8.891111111),
        ],
      },
      {
        group_id: "group_2",
        group_name: "Mining Area 2",
        geometry: "polygon",
        points: [
          point("1", 11.869980556, -8.892405556),
          point("2", 11.871447222, -8.89255),
          point("3", 11.868791667, -8.898211111),
        ],
      },
    ],
  }));
  assertShadowSuccess(result);
});

test("REVIEW_REQUIRED shadow for experimental path", () => {
  const result = buildCoordinateEngineV3ProductionShadow({
    productionInput: {
      normalized: normalized({ recognizerId: "indonesia_utm", coordinateType: "indonesia_utm" }),
      productionMetadata: { experimental: true, inputMode: "table_context_composite" },
    },
  });
  assertShadowReview(result, V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED);
  assert.equal(result.technicalKmlReady, true);
});

test("REVIEW_REQUIRED shadow for recognizer unavailable", () => {
  const result = shadowFromEngine(engine({
    coordinateType: "handwritten_dms_experimental",
    precisionMode: "handwritten-dms-coordinates",
    points: [
      point("1", 11.1, -8.1),
      point("2", 11.2, -8.2),
    ],
  }));
  assertShadowReview(result, V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE);
});

test("REVIEW_REQUIRED shadow for candidate conflict", () => {
  const result = buildCoordinateEngineV3ProductionShadow({
    productionInput: {
      adapterResult: {
        status: ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT,
        recognizerId: "generic_dms",
        coordinateType: "generic_dms",
        normalized: normalized({ recognizerId: "generic_dms", coordinateType: "generic_dms" }),
      },
    },
  });
  assertShadowReview(result, V3_PRODUCTION_REASON_CODE.CANDIDATE_CONFLICT);
});

test("UNSUPPORTED shadow for no candidate", () => {
  const result = buildCoordinateEngineV3ProductionShadow({
    payload: { rawText: "", coordinates: "" },
    coordinateEngineV2: { groups: [] },
  });
  assertShadowUnsupported(result, V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE);
});

test("UNSUPPORTED shadow for invalid geometry", () => {
  const result = buildCoordinateEngineV3ProductionShadow({
    productionInput: {
      normalized: normalized({
        geometryType: "polygon",
        coordinates: [
          { label: "1", latitude: 1, longitude: 2 },
          { label: "2", latitude: 3, longitude: 4 },
        ],
      }),
    },
  });
  assertShadowUnsupported(result, V3_PRODUCTION_REASON_CODE.INVALID_GEOMETRY);
});

test("server response wrapper is shadow-only and non-authoritative", () => {
  const source = readFileSync("server.js", "utf8");
  assert.match(source, /coordinateEngineV3Production:\s*buildCoordinateEngineV3ProductionShadow/);
  assert.doesNotMatch(source, /coordinateEngineV3Production\.[\s\S]{0,80}(coordinates|precisionMode|kml_ready|verification)\s*=/);
});

test("frontend does not consume V3 shadow field", () => {
  const source = readFileSync("index.html", "utf8");
  assert.equal(source.includes("coordinateEngineV3Production"), false);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.fn();
    passed += 1;
    console.log(`PASS ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.name}`);
    throw error;
  }
}

console.log(`Coordinate Engine V3 Production Shadow Integration Regression: ${passed}/${tests.length} PASS`);
