import {
  COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION,
  COORDINATE_EVIDENCE_SHADOW_DECISION_SCHEMA_VERSION
} from "./schema.js";

const SECRET_KEY_PATTERN = /api[_-]?key|secret|token|password|authorization|credential|env|raw[_-]?ocr|rawtext|prompt|modelresponse|fullresponse/i;
const SECRET_VALUE_PATTERN = /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

function cleanString(value) {
  return String(value ?? "")
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .trim();
}

function sanitizePlainValue(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") return cleanString(value);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.map(sanitizePlainValue);
  if (typeof value === "object") {
    return Object.fromEntries(Object.entries(value)
      .filter(([key]) => !SECRET_KEY_PATTERN.test(key))
      .map(([key, nestedValue]) => [key, sanitizePlainValue(nestedValue)]));
  }
  return null;
}

function pickObject(value = {}, key, fallback = {}) {
  const selected = value?.[key];
  return selected && typeof selected === "object" && !Array.isArray(selected)
    ? sanitizePlainValue(selected)
    : fallback;
}

export function sanitizeCandidateForResponse(candidate = {}) {
  return Object.freeze({
    schemaVersion: COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION,
    evidenceId: cleanString(candidate.evidenceId),
    evidenceType: cleanString(candidate.evidenceType),
    sourceParser: cleanString(candidate.sourceParser),
    coordinateSource: cleanString(candidate.coordinateSource),
    authority: pickObject(candidate, "authority"),
    confidence: pickObject(candidate, "confidence"),
    attributes: pickObject(candidate, "attributes"),
    coordinateSummary: pickObject(candidate, "coordinateSummary"),
    conflicts: Array.isArray(candidate.conflicts) ? sanitizePlainValue(candidate.conflicts) : [],
    recommendedState: cleanString(candidate.recommendedState),
    canGenerateKml: candidate.canGenerateKml === true,
    reason: cleanString(candidate.reason)
  });
}

export function sanitizeShadowDecisionForResponse(decision = {}) {
  return Object.freeze({
    schemaVersion: COORDINATE_EVIDENCE_SHADOW_DECISION_SCHEMA_VERSION,
    winnerEvidenceId: cleanString(decision.winnerEvidenceId),
    winnerEvidenceType: cleanString(decision.winnerEvidenceType),
    winnerAuthority: pickObject(decision, "winnerAuthority", null),
    currentWinnerType: cleanString(decision.currentWinnerType),
    currentWinnerPrecision: cleanString(decision.currentWinnerPrecision),
    differenceFromCurrentWinner: decision.differenceFromCurrentWinner === true,
    reason: cleanString(decision.reason),
    blockedByShadowOnly: decision.blockedByShadowOnly === true,
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}
