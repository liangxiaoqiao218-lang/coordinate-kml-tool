import {
  createNormalizedCoordinateResult,
  createWarningMetadata,
  RECOGNIZER_PORT_STATUS,
  RECOGNIZER_TYPES,
  validateNormalizedCoordinateResult,
} from "./contracts.js";
import { createLatencyBudget, allocateRecognizerBudget } from "./latency-budget.js";
import { createDefaultRecognizerRegistry, getRecognizerRegistrySummary, validateRecognizerRegistry } from "./registry.js";

export const COORDINATE_ENGINE_V3_DISABLED_REASON = "coordinate_engine_v3_not_enabled";

export function isCoordinateEngineV3Enabled(env = process.env) {
  return String(env.ENABLE_COORDINATE_ENGINE_V3 || "").trim().toLowerCase() === "true";
}

export async function recognizeWithIsolatedRecognizers(input = {}, {
  env = process.env,
  registry = createDefaultRecognizerRegistry(),
  latencyBudget = createLatencyBudget(),
} = {}) {
  if (!isCoordinateEngineV3Enabled(env)) {
    return Object.freeze({
      handled: false,
      reason: COORDINATE_ENGINE_V3_DISABLED_REASON,
      registry: getRecognizerRegistrySummary(registry),
    });
  }

  for (const recognizer of registry) {
    if (recognizer.portStatus !== RECOGNIZER_PORT_STATUS.STABLE) continue;
    if (!recognizer.canHandle(input, { latencyBudget })) continue;
    const childBudget = allocateRecognizerBudget(latencyBudget, recognizer.providerBudgetMs || latencyBudget.targetMs);
    const recognized = await recognizer.recognize(input, { latencyBudget: childBudget });
    const normalized = recognizer.normalize(recognized, { input });
    const verification = await recognizer.verify(normalized, { input });
    return Object.freeze({
      handled: true,
      recognizerId: recognizer.recognizerId,
      coordinateType: recognizer.coordinateType,
      normalized,
      verification,
    });
  }

  return Object.freeze({
    handled: false,
    reason: "no_stable_recognizer_matched",
    registry: getRecognizerRegistrySummary(registry),
  });
}

export {
  allocateRecognizerBudget,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  createNormalizedCoordinateResult,
  createWarningMetadata,
  getRecognizerRegistrySummary,
  RECOGNIZER_PORT_STATUS,
  RECOGNIZER_TYPES,
  validateNormalizedCoordinateResult,
  validateRecognizerRegistry,
};

