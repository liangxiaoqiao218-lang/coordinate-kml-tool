import { buildDmsGeographicEvidenceCandidate } from "./dms.js";
import { buildStructuredCadastralEvidenceCandidate } from "./cadastral.js";
import { buildUtmEvidenceCandidates } from "./utm.js";
import { rankCoordinateEvidenceCandidates } from "../shadow-ranking.js";

function compactCandidates(candidates = []) {
  return candidates.filter(Boolean);
}

export function buildCoordinateEvidenceCandidates(value = {}) {
  return compactCandidates([
    buildDmsGeographicEvidenceCandidate(value),
    buildStructuredCadastralEvidenceCandidate(value),
    ...buildUtmEvidenceCandidates(value)
  ]);
}

export function buildCoordinateEvidenceShadowModel(value = {}, currentWinner = {}) {
  const coordinateEvidenceCandidates = buildCoordinateEvidenceCandidates(value);
  return {
    coordinateEvidenceCandidates,
    shadowEvidenceDecision: rankCoordinateEvidenceCandidates(coordinateEvidenceCandidates, currentWinner)
  };
}

export { buildDmsGeographicEvidenceCandidate } from "./dms.js";
export { buildStructuredCadastralEvidenceCandidate } from "./cadastral.js";
export {
  buildUtmCrsTextEvidenceCandidate,
  buildUtmEvidenceCandidates,
  buildVerifiedUtmTransformationEvidenceCandidate
} from "./utm.js";
