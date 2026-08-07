import { resolveShadowUtmIntent } from "../utm-intent/shadow-resolver.js";
import { collectCrsEvidence } from "./evidence-collector.js";

export function buildShadowIntentFromCrsVision(acquisition = {}) {
  const crsEvidence = collectCrsEvidence(acquisition);
  const evidenceText = (Array.isArray(acquisition.observations) ? acquisition.observations : [])
    .map(observation => observation.rawText)
    .filter(Boolean)
    .join("\n");
  const typedExclusionContext = crsEvidence.exclusions
    .map(value => value === "mgrs" ? "MGRS" : value === "bftm" ? "BFTM" : value === "kyrgyzstan_gk" ? "Gauss-Kruger" : "")
    .filter(Boolean)
    .join(" ");
  const { shadowIntent } = resolveShadowUtmIntent({
    rawText: evidenceText,
    coordinateContext: { projectionLabel: typedExclusionContext }
  });
  return {
    crsVision: acquisition,
    crsEvidence,
    shadowIntent
  };
}
