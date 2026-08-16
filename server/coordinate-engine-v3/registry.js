import { RECOGNIZER_PORT_STATUS, RECOGNIZER_TYPES } from "./contracts.js";
import { assertRecognizerIsolation, createRecognizerContract } from "./recognizer-contract.js";
import { mgrsRecognizer } from "./recognizers/mgrs/index.js";
import { wgs84DecimalRecognizer } from "./recognizers/wgs84-decimal/index.js";

export const COORDINATE_ENGINE_V3_REGISTRY_VERSION = "coordinate_engine_v3_registry_v1";

export function createDefaultRecognizerRegistry() {
  const implementedRecognizers = new Map([
    [mgrsRecognizer.coordinateType, mgrsRecognizer],
    [wgs84DecimalRecognizer.coordinateType, wgs84DecimalRecognizer],
  ]);
  return Object.freeze(RECOGNIZER_TYPES.map((coordinateType) => implementedRecognizers.get(coordinateType)
    || createRecognizerContract({
      recognizerId: `${coordinateType}_recognizer`,
      coordinateType,
      portStatus: RECOGNIZER_PORT_STATUS.NOT_PORTED,
    })));
}

export function validateRecognizerRegistry(registry = []) {
  const errors = [];
  if (!Array.isArray(registry)) {
    return Object.freeze({ valid: false, errors: Object.freeze(["registry_not_array"]) });
  }
  const seen = new Set();
  for (const recognizer of registry) {
    const isolation = assertRecognizerIsolation(recognizer);
    if (!isolation.valid) errors.push(...isolation.errors.map((error) => `${recognizer?.recognizerId || "unknown"}:${error}`));
    if (seen.has(recognizer.coordinateType)) errors.push(`duplicate_coordinate_type:${recognizer.coordinateType}`);
    seen.add(recognizer.coordinateType);
  }
  for (const coordinateType of RECOGNIZER_TYPES) {
    if (!seen.has(coordinateType)) errors.push(`missing_coordinate_type:${coordinateType}`);
  }
  return Object.freeze({
    valid: errors.length === 0,
    errors: Object.freeze(errors),
  });
}

export function getRecognizerRegistrySummary(registry = createDefaultRecognizerRegistry()) {
  return Object.freeze({
    schemaVersion: COORDINATE_ENGINE_V3_REGISTRY_VERSION,
    recognizers: Object.freeze(registry.map((recognizer) => Object.freeze({
      recognizerId: recognizer.recognizerId,
      coordinateType: recognizer.coordinateType,
      portStatus: recognizer.portStatus,
    }))),
  });
}
