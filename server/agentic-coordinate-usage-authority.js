import { createHash, timingSafeEqual } from 'node:crypto';

import {
  AGENTIC_RESULT_STATUS,
  normalizeAgenticCoordinateResult,
} from './agentic-coordinate-recognition/index.js';

export const AGENTIC_COORDINATE_USAGE_AUTHORITY_VERSION = 'agentic-coordinate-usage-authority/v1';

const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function equalText(left, right) {
  const a = Buffer.from(String(left || ''));
  const b = Buffer.from(String(right || ''));
  return a.length === b.length && timingSafeEqual(a, b);
}

export function hashAgenticCoordinateResult(result) {
  const normalized = normalizeAgenticCoordinateResult(result);
  return createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
}

function decisionStateFor(resultStatus) {
  return resultStatus === AGENTIC_RESULT_STATUS.USABLE ? 'AUTO_EXPORT' : 'REVIEW_REQUIRED';
}

export function buildAgenticCoordinateUsageAuthority({ recognitionRequestId, result }) {
  const requestId = String(recognitionRequestId || '').trim().toLowerCase();
  if (!REQUEST_ID_PATTERN.test(requestId)) throw new Error('recognitionRequestId must be a UUID');
  const normalized = normalizeAgenticCoordinateResult(result);
  if (!normalized.success || normalized.resultStatus === AGENTIC_RESULT_STATUS.FAILED) {
    throw new Error('failed recognition cannot establish usage authority');
  }
  return Object.freeze({
    schemaVersion: AGENTIC_COORDINATE_USAGE_AUTHORITY_VERSION,
    recognitionRequestId: requestId,
    resultId: `agentic:${requestId}`,
    resultRevision: 1,
    decisionState: decisionStateFor(normalized.resultStatus),
    resultHash: hashAgenticCoordinateResult(normalized),
  });
}

export function evaluateAgenticCoordinateUsageAuthority({ httpStatus = 200, body = null } = {}) {
  const reject = reason => Object.freeze({ eligible: false, reason, identity: null });
  if (!Number.isInteger(Number(httpStatus)) || Number(httpStatus) < 200 || Number(httpStatus) >= 300 || body?.success !== true) {
    return reject('HTTP_OR_BODY_NOT_SUCCESSFUL');
  }
  const authority = body?.agenticCoordinateAuthority;
  if (!authority || authority.schemaVersion !== AGENTIC_COORDINATE_USAGE_AUTHORITY_VERSION) {
    return reject('AGENTIC_AUTHORITY_MISSING');
  }
  const requestId = String(body?.requestId || '').trim().toLowerCase();
  if (!REQUEST_ID_PATTERN.test(requestId)
    || authority.recognitionRequestId !== requestId
    || authority.resultId !== `agentic:${requestId}`
    || authority.resultRevision !== 1) {
    return reject('AGENTIC_AUTHORITY_IDENTITY_INVALID');
  }
  let normalized;
  try {
    normalized = normalizeAgenticCoordinateResult(body?.result);
  } catch {
    return reject('AGENTIC_RESULT_INVALID');
  }
  if (!normalized.success || normalized.resultStatus === AGENTIC_RESULT_STATUS.FAILED) {
    return reject('AGENTIC_RESULT_FAILED');
  }
  const expectedDecision = decisionStateFor(normalized.resultStatus);
  if (authority.decisionState !== expectedDecision) return reject('AGENTIC_DECISION_MISMATCH');
  const resultHash = hashAgenticCoordinateResult(normalized);
  if (!/^[a-f0-9]{64}$/i.test(String(authority.resultHash || ''))
    || !equalText(authority.resultHash, resultHash)) {
    return reject('AGENTIC_RESULT_HASH_MISMATCH');
  }
  return Object.freeze({
    eligible: true,
    reason: 'AGENTIC_SERVER_AUTHORITY_ESTABLISHED',
    identity: Object.freeze({
      resultId: authority.resultId,
      resultRevision: authority.resultRevision,
      decisionState: authority.decisionState,
      geometryHash: resultHash,
    }),
  });
}
