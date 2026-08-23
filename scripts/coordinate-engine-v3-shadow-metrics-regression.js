import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  buildV3ShadowEvaluationMetric,
  recordV3ShadowEvaluationMetric,
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_SCOPE_STATUS,
  V3_PRODUCTION_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function baseResponse({
  status = V3_PRODUCTION_STATUS.SUCCESS,
  reasonCode = null,
  recognizerId = "generic_dms",
  coordinateType = "generic_dms",
  productionSupported = status === V3_PRODUCTION_STATUS.SUCCESS,
  technicalKmlReady = true,
  productionScopeStatus = V3_PRODUCTION_SCOPE_STATUS.SUPPORTED,
  reasonCodes = [],
  legacy = "success",
} = {}) {
  const legacyGroups = legacy === "fail"
    ? []
    : [{
        group_id: "group_1",
        requires_review: legacy === "review",
        kml_ready: legacy !== "fail",
        points: [{ label: "1", lat: 11, lon: -8 }],
      }];
  return {
    rawText: "SHOULD_NOT_BE_LOGGED",
    coordinates: legacy === "fail" ? "" : "-8,11",
    providerResponse: "SHOULD_NOT_BE_LOGGED",
    prompt: "SHOULD_NOT_BE_LOGGED",
    imageBase64: "SHOULD_NOT_BE_LOGGED",
    coordinateEngineV2: {
      requires_review: legacy === "review",
      groups: legacyGroups,
    },
    verification: {
      status: legacy === "review" ? "REVIEW" : (legacy === "fail" ? "BLOCK" : "PASS"),
    },
    coordinateEngineV3Production: {
      status,
      reasonCode,
      recognizerId,
      coordinateType,
      productionSupported,
      technicalKmlReady,
      productionScopeStatus,
      reasonCodes,
    },
  };
}

function assertNoSensitivePayload(value) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes("SHOULD_NOT_BE_LOGGED"), false);
  assert.equal(/"rawText"|"coordinates"|"providerResponse"|"prompt"|"imageBase64"|"Authorization"|Bearer|"apiKey"|"credential"/i.test(serialized), false);
}

test("SUCCESS event", () => {
  const event = buildV3ShadowEvaluationMetric({ response: baseResponse(), route: "recognize-coordinates" });
  assert.equal(event.event, "v3_shadow_evaluation");
  assert.equal(event.status, V3_PRODUCTION_STATUS.SUCCESS);
  assert.equal(event.reasonCode, "OTHER");
  assert.equal(event.productionSupported, true);
  assert.equal(event.technicalKmlReady, true);
  assert.equal(event.recognizerId, "generic_dms");
  assert.equal(event.coordinateType, "generic_dms");
  assert.equal(event.legacySuccess, true);
  assert.equal(event.legacyRequiresReview, false);
  assert.equal(event.legacyKmlReady, true);
  assert.equal(event.comparisonBucket, "legacy_success_v3_success");
  assertNoSensitivePayload(event);
});

test("REVIEW_REQUIRED event", () => {
  const event = buildV3ShadowEvaluationMetric({
    response: baseResponse({
      status: V3_PRODUCTION_STATUS.REVIEW_REQUIRED,
      reasonCode: V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE,
      productionSupported: false,
      legacy: "review",
    }),
  });
  assert.equal(event.status, V3_PRODUCTION_STATUS.REVIEW_REQUIRED);
  assert.equal(event.reasonCode, V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE);
  assert.equal(event.legacyRequiresReview, true);
  assert.equal(event.comparisonBucket, "legacy_review_v3_review");
});

test("UNSUPPORTED event", () => {
  const event = buildV3ShadowEvaluationMetric({
    response: baseResponse({
      status: V3_PRODUCTION_STATUS.UNSUPPORTED,
      reasonCode: V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE,
      productionSupported: false,
      technicalKmlReady: false,
      legacy: "fail",
    }),
  });
  assert.equal(event.status, V3_PRODUCTION_STATUS.UNSUPPORTED);
  assert.equal(event.reasonCode, V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE);
  assert.equal(event.legacySuccess, false);
  assert.equal(event.legacyKmlReady, false);
  assert.equal(event.comparisonBucket, "legacy_fail_v3_unsupported");
});

test("experimental violation detection", () => {
  const event = buildV3ShadowEvaluationMetric({
    response: baseResponse({
      status: V3_PRODUCTION_STATUS.SUCCESS,
      reasonCode: null,
      productionSupported: true,
      productionScopeStatus: V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL,
      reasonCodes: [V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED],
    }),
  });
  assert.equal(event.experimentalSilentSuccessViolation, true);
  assert.equal(event.v3_shadow_experimental_silent_success_violation, 1);
});

test("normal experimental review is not violation", () => {
  const event = buildV3ShadowEvaluationMetric({
    response: baseResponse({
      status: V3_PRODUCTION_STATUS.REVIEW_REQUIRED,
      reasonCode: V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED,
      productionSupported: false,
      productionScopeStatus: V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL,
      reasonCodes: [V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED],
    }),
  });
  assert.equal(event.experimentalSilentSuccessViolation, false);
  assert.equal(event.v3_shadow_experimental_silent_success_violation, 0);
});

test("legacy/v3 comparison matrix", () => {
  const statuses = [
    [V3_PRODUCTION_STATUS.SUCCESS, "v3_success"],
    [V3_PRODUCTION_STATUS.REVIEW_REQUIRED, "v3_review"],
    [V3_PRODUCTION_STATUS.UNSUPPORTED, "v3_unsupported"],
  ];
  const legacyStates = ["success", "review", "fail"];
  const buckets = [];

  for (const legacy of legacyStates) {
    for (const [status] of statuses) {
      buckets.push(buildV3ShadowEvaluationMetric({
        response: baseResponse({
          status,
          productionSupported: status === V3_PRODUCTION_STATUS.SUCCESS,
          technicalKmlReady: status !== V3_PRODUCTION_STATUS.UNSUPPORTED,
          legacy,
        }),
      }).comparisonBucket);
    }
  }

  assert.deepEqual(buckets, [
    "legacy_success_v3_success",
    "legacy_success_v3_review",
    "legacy_success_v3_unsupported",
    "legacy_review_v3_success",
    "legacy_review_v3_review",
    "legacy_review_v3_unsupported",
    "legacy_fail_v3_success",
    "legacy_fail_v3_review",
    "legacy_fail_v3_unsupported",
  ]);
});

test("duration metadata is sanitized", () => {
  const event = buildV3ShadowEvaluationMetric({
    response: baseResponse(),
    durationMetadata: {
      providerMs: 123,
      totalMs: 456,
      rawText: "SHOULD_NOT_BE_LOGGED",
      filesystemPath: "C:/secret/file.png",
      Authorization: "Bearer secret",
    },
  });
  assert.deepEqual(event.durationMetadata, { providerMs: 123, totalMs: 456 });
  assertNoSensitivePayload(event);
});

test("record helper uses structured event logger", () => {
  const logs = [];
  const logger = { log: (...args) => logs.push(args) };
  const event = recordV3ShadowEvaluationMetric({ response: baseResponse() }, logger);
  assert.equal(logs.length, 1);
  assert.equal(logs[0][0], "[v3_shadow_evaluation]");
  assert.equal(logs[0][1], event);
});

test("server records metric after shadow augmentation", () => {
  const source = readFileSync("server.js", "utf8");
  assert.match(source, /coordinateEngineV3Production\s*=\s*buildCoordinateEngineV3ProductionShadow/);
  assert.match(source, /coordinateEngineV3Canary\s*=\s*buildV3FamilyCanarySelection/);
  assert.match(source, /recordV3ShadowEvaluationMetric\(\{/);
  const metricCall = source.match(/recordV3ShadowEvaluationMetric\(\{([\s\S]*?)\n\s*\}\);/);
  assert.ok(metricCall);
  assert.doesNotMatch(metricCall[1], /\b(rawText|coordinates|imageDataUrl|buffer|providerResponse|prompt|imageBase64)\s*:/);
});

test("frontend remains unchanged and does not consume metrics", () => {
  const source = readFileSync("index.html", "utf8");
  assert.equal(source.includes("v3_shadow_evaluation"), false);
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

console.log(`Coordinate Engine V3 Shadow Metrics Regression: ${passed}/${tests.length} PASS`);
