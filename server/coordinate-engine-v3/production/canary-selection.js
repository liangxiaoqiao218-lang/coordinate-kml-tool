import {
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_STATUS,
} from "./contracts.js";
import { V3_PRODUCTION_SCOPE_STATUS } from "./supported-scope.js";

export const V3_CANARY_SELECTION_SCHEMA_VERSION = "coordinate_engine_v3_family_canary_selection_v1";
export const WGS84_DECIMAL_CANARY_FAMILY = "wgs84_decimal";
export const WGS84_DECIMAL_CANARY_FLAG = "v3_enable_wgs84_decimal";

export const V3_CANARY_SELECTED_ENGINE = Object.freeze({
  LEGACY: "legacy",
  V3: "v3",
});

export const V3_CANARY_SELECTION_REASON = Object.freeze({
  FLAG_OFF: "flag_off",
  FAMILY_NOT_ENABLED: "family_not_enabled",
  V3_SUCCESS_SELECTED: "v3_success_selected",
  V3_REVIEW_FALLBACK_LEGACY: "v3_review_fallback_legacy",
  V3_UNSUPPORTED_FALLBACK_LEGACY: "v3_unsupported_fallback_legacy",
  ROLLBACK_ACTIVE: "rollback_active",
});

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function boolFromFlag(value) {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return false;
  return /^(1|true|yes|on)$/i.test(String(value).trim());
}

export function isWgs84DecimalCanaryEnabled(env = process.env) {
  return boolFromFlag(env[WGS84_DECIMAL_CANARY_FLAG] ?? env.V3_ENABLE_WGS84_DECIMAL);
}

function getV3Family(v3Production = {}) {
  return cleanString(v3Production.recognizerId || v3Production.coordinateType);
}

function hasExperimentalSignal(v3Production = {}) {
  const reasonCodes = Array.isArray(v3Production.reasonCodes) ? v3Production.reasonCodes : [];
  return Boolean(
    v3Production.productionScopeStatus === V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL
    || v3Production.reasonCode === V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED
    || reasonCodes.includes(V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED)
    || v3Production.experimentalSilentSuccessViolation === true
    || v3Production.v3_shadow_experimental_silent_success_violation > 0,
  );
}

function legacySelection(reason, details = {}) {
  return Object.freeze({
    selectedEngine: V3_CANARY_SELECTED_ENGINE.LEGACY,
    selectionReason: reason,
    canaryEligible: false,
    ...details,
  });
}

export function buildV3FamilyCanarySelection({
  response = {},
  v3Production = response.coordinateEngineV3Production || {},
  env = process.env,
  family = WGS84_DECIMAL_CANARY_FAMILY,
  rollbackActive = false,
} = {}) {
  const targetFamily = cleanString(family, WGS84_DECIMAL_CANARY_FAMILY);
  const v3Family = getV3Family(v3Production);
  const flagEnabled = targetFamily === WGS84_DECIMAL_CANARY_FAMILY
    ? isWgs84DecimalCanaryEnabled(env)
    : false;
  const experimentalSignal = hasExperimentalSignal(v3Production);
  const common = {
    schemaVersion: V3_CANARY_SELECTION_SCHEMA_VERSION,
    family: targetFamily,
    flag: WGS84_DECIMAL_CANARY_FLAG,
    flagEnabled,
    rollbackActive: rollbackActive === true,
    v3Family,
    v3Status: cleanString(v3Production.status, V3_PRODUCTION_STATUS.UNSUPPORTED),
    v3TechnicalKmlReady: v3Production.technicalKmlReady === true,
    v3ProductionSupported: v3Production.productionSupported === true,
    experimentalSignal,
  };

  if (common.rollbackActive) {
    return Object.freeze({
      ...common,
      ...legacySelection(V3_CANARY_SELECTION_REASON.ROLLBACK_ACTIVE),
    });
  }

  if (!flagEnabled) {
    return Object.freeze({
      ...common,
      ...legacySelection(V3_CANARY_SELECTION_REASON.FLAG_OFF),
    });
  }

  if (v3Family !== targetFamily) {
    return Object.freeze({
      ...common,
      ...legacySelection(V3_CANARY_SELECTION_REASON.FAMILY_NOT_ENABLED),
    });
  }

  if (v3Production.status === V3_PRODUCTION_STATUS.SUCCESS
    && v3Production.technicalKmlReady === true
    && v3Production.productionSupported === true
    && experimentalSignal !== true) {
    return Object.freeze({
      ...common,
      selectedEngine: V3_CANARY_SELECTED_ENGINE.V3,
      selectionReason: V3_CANARY_SELECTION_REASON.V3_SUCCESS_SELECTED,
      canaryEligible: true,
    });
  }

  if (v3Production.status === V3_PRODUCTION_STATUS.REVIEW_REQUIRED
    || v3Production.status === V3_PRODUCTION_STATUS.SUCCESS) {
    return Object.freeze({
      ...common,
      ...legacySelection(V3_CANARY_SELECTION_REASON.V3_REVIEW_FALLBACK_LEGACY),
    });
  }

  return Object.freeze({
    ...common,
    ...legacySelection(V3_CANARY_SELECTION_REASON.V3_UNSUPPORTED_FALLBACK_LEGACY),
  });
}
