import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  buildV3FamilyCanarySelection,
  buildV3ShadowEvaluationMetric,
  canUseV3Canary,
  V3_CANARY_SELECTED_ENGINE,
  V3_CANARY_SELECTION_REASON,
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_SCOPE_STATUS,
  V3_PRODUCTION_STATUS,
  WGS84_DECIMAL_CANARY_FLAG,
  WGS84_DECIMAL_CANARY_FAMILY,
  WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function production({
  family = WGS84_DECIMAL_CANARY_FAMILY,
  status = V3_PRODUCTION_STATUS.SUCCESS,
  productionSupported = status === V3_PRODUCTION_STATUS.SUCCESS,
  technicalKmlReady = status !== V3_PRODUCTION_STATUS.UNSUPPORTED,
  productionScopeStatus = V3_PRODUCTION_SCOPE_STATUS.SUPPORTED,
  reasonCode = null,
  reasonCodes = [],
} = {}) {
  return Object.freeze({
    status,
    reasonCode,
    recognizerId: family,
    coordinateType: family,
    productionSupported,
    technicalKmlReady,
    productionScopeStatus,
    reasonCodes,
  });
}

function select(options = {}) {
  return buildV3FamilyCanarySelection({
    v3Production: production(options),
    env: options.env || {},
    visitorId: options.visitorId || "",
    userId: options.userId || "",
    rollbackActive: options.rollbackActive === true,
  });
}

test("flag OFF keeps legacy authoritative even when visitor is allowlisted", () => {
  const result = select({
    visitorId: "internal-1",
    env: { [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1" },
  });
  assert.equal(result.family, WGS84_DECIMAL_CANARY_FAMILY);
  assert.equal(result.flag, WGS84_DECIMAL_CANARY_FLAG);
  assert.equal(result.flagEnabled, false);
  assert.equal(result.canaryUser, false);
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.FLAG_OFF);
  assert.equal(result.canaryEligible, false);
});

test("flag ON but visitor not allowlisted keeps legacy authoritative", () => {
  const result = select({
    visitorId: "external-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
  });
  assert.equal(result.flagEnabled, true);
  assert.equal(result.canaryUser, false);
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.USER_NOT_ALLOWED);
});

test("flag ON and allowlisted V3 SUCCESS selects V3 for wgs84_decimal only", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
  });
  assert.equal(result.flagEnabled, true);
  assert.equal(result.canaryUser, true);
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.V3);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.V3_SUCCESS_SELECTED);
  assert.equal(result.canaryEligible, true);
});

test("flag ON with V3 REVIEW_REQUIRED falls back to legacy", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
    status: V3_PRODUCTION_STATUS.REVIEW_REQUIRED,
    productionSupported: false,
    reasonCode: V3_PRODUCTION_REASON_CODE.INCOMPLETE_EXTRACTION,
  });
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.V3_REVIEW_FALLBACK_LEGACY);
  assert.equal(result.canaryEligible, false);
});

test("flag ON with V3 UNSUPPORTED falls back to legacy", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
    status: V3_PRODUCTION_STATUS.UNSUPPORTED,
    productionSupported: false,
    technicalKmlReady: false,
    reasonCode: V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE,
  });
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.V3_UNSUPPORTED_FALLBACK_LEGACY);
});

test("other family is never selected by wgs84 flag", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
    family: "cote_divoire_dms",
  });
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.FAMILY_NOT_ENABLED);
});

test("rollback active restores legacy", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
    rollbackActive: true,
  });
  assert.equal(result.rollbackActive, true);
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.ROLLBACK_ACTIVE);
});

test("experimental signal cannot be selected", () => {
  const result = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
    productionScopeStatus: V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL,
    reasonCodes: [V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED],
  });
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.V3_REVIEW_FALLBACK_LEGACY);
  assert.equal(result.experimentalSignal, true);
});

test("shadow metrics include sanitized canary selection fields", () => {
  const coordinateEngineV3Canary = select({
    visitorId: "internal-1",
    env: {
      [WGS84_DECIMAL_CANARY_FLAG]: "true",
      [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1",
    },
  });
  const event = buildV3ShadowEvaluationMetric({
    response: {
      coordinates: "-8,11",
      coordinateEngineV2: {
        groups: [{ kml_ready: true, points: [{ lat: 11, lon: -8 }] }],
      },
      coordinateEngineV3Production: production(),
      coordinateEngineV3Canary,
    },
  });
  assert.equal(event.family, WGS84_DECIMAL_CANARY_FAMILY);
  assert.equal(event.canaryUser, true);
  assert.equal(event.selectedEngine, V3_CANARY_SELECTED_ENGINE.V3);
  assert.equal(event.selectionReason, V3_CANARY_SELECTION_REASON.V3_SUCCESS_SELECTED);
  assert.equal(event.rollbackActive, false);
  assert.equal(JSON.stringify(event).includes("provider"), false);
  assert.equal(JSON.stringify(event).includes("internal-1"), false);
});

test("empty allowlist keeps legacy even when flag is on", () => {
  const result = select({
    visitorId: "internal-1",
    env: { [WGS84_DECIMAL_CANARY_FLAG]: "true" },
  });
  assert.equal(result.selectedEngine, V3_CANARY_SELECTED_ENGINE.LEGACY);
  assert.equal(result.selectionReason, V3_CANARY_SELECTION_REASON.USER_NOT_ALLOWED);
  assert.equal(result.canaryUser, false);
});

test("canUseV3Canary requires family flag and allowlisted visitor", () => {
  assert.equal(canUseV3Canary({
    family: WGS84_DECIMAL_CANARY_FAMILY,
    visitorId: "internal-1",
    flag: true,
    env: { [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1,internal-2" },
  }), true);
  assert.equal(canUseV3Canary({
    family: WGS84_DECIMAL_CANARY_FAMILY,
    visitorId: "external-1",
    flag: true,
    env: { [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1" },
  }), false);
  assert.equal(canUseV3Canary({
    family: "generic_dms",
    visitorId: "internal-1",
    flag: true,
    env: { [WGS84_DECIMAL_CANARY_VISITOR_ALLOWLIST]: "internal-1" },
  }), false);
});

test("server wrapper attaches canary metadata without public coordinate override", () => {
  const source = readFileSync("server.js", "utf8");
  assert.match(source, /coordinateEngineV3Canary\s*=\s*buildV3FamilyCanarySelection/);
  assert.match(source, /coordinateEngineV3Canary/);
  assert.doesNotMatch(source, /selectedEngine[\s\S]{0,240}coordinates\s*=/);
});

test("frontend remains unchanged and does not consume canary metadata", () => {
  const source = readFileSync("index.html", "utf8");
  assert.equal(source.includes("coordinateEngineV3Canary"), false);
  assert.equal(source.includes("v3_enable_wgs84_decimal"), false);
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

console.log(`Coordinate Engine V3 WGS84 Canary Regression: ${passed}/${tests.length} PASS`);
