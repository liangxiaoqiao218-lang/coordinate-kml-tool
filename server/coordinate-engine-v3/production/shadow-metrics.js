import {
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_STATUS,
} from "./contracts.js";
import { V3_PRODUCTION_SCOPE_STATUS } from "./supported-scope.js";

const SHADOW_METRICS_EVENT = "v3_shadow_evaluation";
const LEGACY_STATE = Object.freeze({
  SUCCESS: "legacy_success",
  REVIEW: "legacy_review",
  FAIL: "legacy_fail",
});

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function cleanBoolean(value) {
  return value === true;
}

function hasLegacyGroups(coordinateEngineV2 = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  return groups.some((group) => Array.isArray(group?.points) && group.points.length > 0);
}

function hasLegacyCoordinates(response = {}) {
  return cleanString(response.coordinates) !== "" || hasLegacyGroups(response.coordinateEngineV2 || response.coordinate_engine_v2 || {});
}

function getLegacyKmlReady(coordinateEngineV2 = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  return groups.length > 0 && groups.some((group) => group?.kml_ready === true);
}

function getLegacyRequiresReview(response = {}) {
  const coordinateEngineV2 = response.coordinateEngineV2 || response.coordinate_engine_v2 || {};
  const verificationStatus = cleanString(response.verification?.status);
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  return Boolean(
    coordinateEngineV2.requires_review === true
    || verificationStatus === "REVIEW"
    || groups.some((group) => group?.requires_review === true),
  );
}

function getLegacyState({ legacySuccess, legacyRequiresReview } = {}) {
  if (!legacySuccess) return LEGACY_STATE.FAIL;
  return legacyRequiresReview ? LEGACY_STATE.REVIEW : LEGACY_STATE.SUCCESS;
}

function getV3State(status = "") {
  if (status === V3_PRODUCTION_STATUS.SUCCESS) return "v3_success";
  if (status === V3_PRODUCTION_STATUS.REVIEW_REQUIRED) return "v3_review";
  return "v3_unsupported";
}

function normalizeReasonCode(value = "") {
  const code = cleanString(value, "OTHER");
  return code || "OTHER";
}

function getExperimentalViolation(v3Production = {}) {
  const reasonCodes = Array.isArray(v3Production.reasonCodes) ? v3Production.reasonCodes : [];
  const experimentalSignal = v3Production.productionScopeStatus === V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL
    || v3Production.reasonCode === V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED
    || reasonCodes.includes(V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED);
  return Boolean(experimentalSignal && v3Production.status === V3_PRODUCTION_STATUS.SUCCESS);
}

function sanitizeDurationMetadata(value = {}) {
  const source = value && typeof value === "object" ? value : {};
  const output = {};
  for (const [key, raw] of Object.entries(source)) {
    const normalizedKey = cleanString(key);
    if (!normalizedKey) continue;
    if (/raw|prompt|image|base64|coordinate|provider.*response|authorization|credential|api.*key|path/i.test(normalizedKey)) {
      continue;
    }
    if (typeof raw === "number" && Number.isFinite(raw)) {
      output[normalizedKey] = raw;
    } else if (typeof raw === "boolean") {
      output[normalizedKey] = raw;
    } else if (typeof raw === "string" && raw.length <= 80 && !/[\\/]|data:|Bearer\s+/i.test(raw)) {
      output[normalizedKey] = raw;
    }
  }
  return Object.freeze(output);
}

export function buildV3ShadowEvaluationMetric({
  response = {},
  route = "recognize-coordinates",
  durationMetadata = {},
} = {}) {
  const v3Production = response.coordinateEngineV3Production || {};
  const v3Canary = response.coordinateEngineV3Canary || {};
  const coordinateEngineV2 = response.coordinateEngineV2 || response.coordinate_engine_v2 || {};
  const legacySuccess = hasLegacyCoordinates(response);
  const legacyRequiresReview = getLegacyRequiresReview(response);
  const legacyKmlReady = getLegacyKmlReady(coordinateEngineV2);
  const legacyState = getLegacyState({ legacySuccess, legacyRequiresReview });
  const v3State = getV3State(v3Production.status);
  const comparisonBucket = `${legacyState}_${v3State}`;
  const experimentalSilentSuccessViolation = getExperimentalViolation(v3Production);

  return Object.freeze({
    event: SHADOW_METRICS_EVENT,
    status: cleanString(v3Production.status, V3_PRODUCTION_STATUS.UNSUPPORTED),
    reasonCode: normalizeReasonCode(v3Production.reasonCode),
    productionSupported: cleanBoolean(v3Production.productionSupported),
    technicalKmlReady: cleanBoolean(v3Production.technicalKmlReady),
    recognizerId: cleanString(v3Production.recognizerId),
    coordinateType: cleanString(v3Production.coordinateType),
    family: cleanString(v3Canary.family || v3Production.recognizerId || v3Production.coordinateType),
    canaryUser: cleanBoolean(v3Canary.canaryUser),
    selectedEngine: cleanString(v3Canary.selectedEngine, "legacy"),
    selectionReason: cleanString(v3Canary.selectionReason, "flag_off"),
    rollbackActive: cleanBoolean(v3Canary.rollbackActive),
    legacySuccess,
    legacyRequiresReview,
    legacyKmlReady,
    route: cleanString(route, "recognize-coordinates"),
    comparisonBucket,
    experimentalSilentSuccessViolation,
    v3_shadow_experimental_silent_success_violation: experimentalSilentSuccessViolation ? 1 : 0,
    durationMetadata: sanitizeDurationMetadata(durationMetadata),
  });
}

export function recordV3ShadowEvaluationMetric(options = {}, logger = console) {
  const metric = buildV3ShadowEvaluationMetric(options);
  logger.log("[v3_shadow_evaluation]", metric);
  return metric;
}
