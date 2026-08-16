import { RECOGNIZER_PORT_STATUS, validateNormalizedCoordinateResult } from "./contracts.js";
import { createLatencyBudget } from "./latency-budget.js";
import { createDefaultRecognizerRegistry, getRecognizerRegistrySummary } from "./registry.js";

export const V3_RUNNER_STATUS = Object.freeze({
  MATCHED: "MATCHED",
  NO_MATCH: "NO_MATCH",
  AMBIGUOUS: "AMBIGUOUS",
  DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
  RECOGNIZER_ERROR: "RECOGNIZER_ERROR",
});

const DISPATCHABLE_STATUS = new Set([
  RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  RECOGNIZER_PORT_STATUS.STABLE,
]);

function sanitizeError(error) {
  return Object.freeze({
    name: String(error?.name || "Error"),
    message: String(error?.message || "recognizer_error").slice(0, 240),
  });
}

function makeBaseResult(status, extras = {}) {
  return Object.freeze({
    schemaVersion: "coordinate_engine_v3_runner_v1",
    status,
    recognizerId: null,
    coordinateType: null,
    normalized: null,
    verification: null,
    technicalKmlReady: false,
    warnings: Object.freeze([]),
    suspectedPoints: Object.freeze([]),
    candidates: Object.freeze([]),
    errors: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
    ...extras,
  });
}

function getDispatchableRecognizers(registry = []) {
  return registry.filter((recognizer) => DISPATCHABLE_STATUS.has(recognizer?.portStatus));
}

function summarizeRecognizer(recognizer = {}) {
  return Object.freeze({
    recognizerId: recognizer.recognizerId || null,
    coordinateType: recognizer.coordinateType || null,
    portStatus: recognizer.portStatus || null,
  });
}

function sumCallCounts(...values) {
  return values.reduce((sum, value) => sum + (Number(value) || 0), 0);
}

export async function runCoordinateEngineV3(input = {}, {
  registry = createDefaultRecognizerRegistry(),
  latencyBudget = createLatencyBudget(),
} = {}) {
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return makeBaseResult(V3_RUNNER_STATUS.DEADLINE_EXCEEDED, {
      reason: "runner_deadline_exceeded_before_dispatch",
      registry: getRecognizerRegistrySummary(registry),
    });
  }

  const matches = [];
  const errors = [];
  for (const recognizer of getDispatchableRecognizers(registry)) {
    try {
      if (recognizer.canHandle(input, {
        latencyBudget,
        remainingBudgetMs: latencyBudget.remainingMs?.(),
        deadlineExceeded: latencyBudget.deadlineExceeded?.(),
      })) {
        matches.push(recognizer);
      }
    } catch (error) {
      errors.push(Object.freeze({
        recognizerId: recognizer.recognizerId,
        phase: "canHandle",
        error: sanitizeError(error),
      }));
    }
  }

  if (matches.length === 0) {
    return makeBaseResult(V3_RUNNER_STATUS.NO_MATCH, {
      reason: errors.length ? "no_match_after_recognizer_error" : "no_recognizer_match",
      errors: Object.freeze(errors),
      registry: getRecognizerRegistrySummary(registry),
    });
  }

  if (matches.length > 1) {
    return makeBaseResult(V3_RUNNER_STATUS.AMBIGUOUS, {
      reason: "multiple_recognizers_matched",
      candidates: Object.freeze(matches.map(summarizeRecognizer)),
      errors: Object.freeze(errors),
    });
  }

  const recognizer = matches[0];
  try {
    const recognized = await recognizer.recognize(input, {
      latencyBudget,
      remainingBudgetMs: latencyBudget.remainingMs?.(),
      deadlineExceeded: latencyBudget.deadlineExceeded?.(),
    });
    const normalized = recognizer.normalize(recognized, { input, latencyBudget });
    const verification = await recognizer.verify(normalized, { input, latencyBudget });
    const contract = validateNormalizedCoordinateResult(normalized);
    return makeBaseResult(V3_RUNNER_STATUS.MATCHED, {
      recognizerId: recognizer.recognizerId,
      coordinateType: recognizer.coordinateType,
      normalized,
      verification,
      technicalKmlReady: normalized.technicalKmlReady === true,
      warnings: normalized.warnings || Object.freeze([]),
      suspectedPoints: normalized.suspectedPoints || Object.freeze([]),
      candidates: Object.freeze([summarizeRecognizer(recognizer)]),
      errors: Object.freeze(errors),
      normalizedContract: contract,
      providerCalls: sumCallCounts(recognized?.providerCalls, verification?.providerCalls),
      visionCalls: sumCallCounts(recognized?.visionCalls, verification?.visionCalls),
      ocrCalls: sumCallCounts(recognized?.ocrCalls, verification?.ocrCalls),
    });
  } catch (error) {
    return makeBaseResult(V3_RUNNER_STATUS.RECOGNIZER_ERROR, {
      recognizerId: recognizer.recognizerId,
      coordinateType: recognizer.coordinateType,
      reason: "selected_recognizer_error",
      errors: Object.freeze([
        ...errors,
        Object.freeze({
          recognizerId: recognizer.recognizerId,
          phase: "recognize_normalize_verify",
          error: sanitizeError(error),
        }),
      ]),
      candidates: Object.freeze([summarizeRecognizer(recognizer)]),
    });
  }
}

