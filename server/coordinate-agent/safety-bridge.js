import { assertAgenticCoordinateConsistency } from '../agentic-coordinate-recognition/consistency.js';
import { assertStrictAgenticCandidate } from './schema.js';

export function validateCoordinateCandidate(candidate) {
  if (!candidate) return null;
  return assertAgenticCoordinateConsistency(assertStrictAgenticCandidate(candidate));
}

export function buildFailClosedAuthorization(terminalState) {
  const allowed = terminalState === 'CONFIRMED';
  return Object.freeze({ mapAllowed: allowed, kmlAllowed: allowed });
}
