import { RECOGNIZER_PORT_STATUS, RECOGNIZER_TYPES } from "./contracts.js";

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function createNoopAsync(value) {
  return async () => value;
}

export function createRecognizerContract(value = {}) {
  const coordinateType = cleanString(value.coordinateType);
  if (!RECOGNIZER_TYPES.includes(coordinateType)) {
    throw new Error(`Unsupported recognizer coordinateType: ${coordinateType || "(empty)"}`);
  }
  const recognizerId = cleanString(value.recognizerId, coordinateType);
  const portStatus = cleanString(value.portStatus, RECOGNIZER_PORT_STATUS.NOT_PORTED);
  return Object.freeze({
    recognizerId,
    coordinateType,
    portStatus,
    canHandle: typeof value.canHandle === "function" ? value.canHandle : () => false,
    recognize: typeof value.recognize === "function"
      ? value.recognize
      : createNoopAsync({ handled: false, reason: "recognizer_not_ported" }),
    normalize: typeof value.normalize === "function" ? value.normalize : (result) => result,
    verify: typeof value.verify === "function"
      ? value.verify
      : createNoopAsync({ verified: false, status: "verification_not_ported" }),
  });
}

export function assertRecognizerIsolation(recognizer = {}) {
  const errors = [];
  if (typeof recognizer.canHandle !== "function") errors.push("canHandle_missing");
  if (typeof recognizer.recognize !== "function") errors.push("recognize_missing");
  if (typeof recognizer.normalize !== "function") errors.push("normalize_missing");
  if (typeof recognizer.verify !== "function") errors.push("verify_missing");
  if (recognizer.portStatus !== RECOGNIZER_PORT_STATUS.NOT_PORTED && recognizer.portStatus !== RECOGNIZER_PORT_STATUS.STABLE) {
    errors.push("invalid_port_status");
  }
  return Object.freeze({
    valid: errors.length === 0,
    errors: Object.freeze(errors),
  });
}

