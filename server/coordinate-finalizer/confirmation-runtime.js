import { COORDINATE_CONFIRMATION_STATUS, COORDINATE_GATE_REASON } from "./reason-codes.js";
import { finalizeCoordinateResult } from "./finalized-coordinate-result-v1.js";
import { releaseConfirmedFamilySafetyPolicy } from "./family-safety-policy.js";

export const DEFAULT_CONFIRMATION_TTL_MS = 15 * 60 * 1000;
export const DEFAULT_CONFIRMATION_MAX_RESULTS = 500;

function runtimeFailure(code, httpStatus) {
  return Object.freeze({ ok: false, code, httpStatus });
}

export class CoordinateConfirmationRuntime {
  constructor({ ttlMs = DEFAULT_CONFIRMATION_TTL_MS, maxResults = DEFAULT_CONFIRMATION_MAX_RESULTS, now = () => Date.now() } = {}) {
    if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) throw new TypeError("confirmation_ttl_invalid");
    if (!Number.isSafeInteger(maxResults) || maxResults <= 0) throw new TypeError("confirmation_max_results_invalid");
    this.ttlMs = ttlMs;
    this.maxResults = maxResults;
    this.now = now;
    this.records = new Map();
  }

  cleanup() {
    const now = this.now();
    for (const [resultId, record] of this.records) {
      if (record.expiresAt <= now) this.records.delete(resultId);
    }
    while (this.records.size > this.maxResults) {
      this.records.delete(this.records.keys().next().value);
    }
  }

  register(finalizedResult) {
    if (!finalizedResult?.resultId || !Number.isSafeInteger(finalizedResult.resultRevision)) {
      throw new TypeError("finalized_result_identity_invalid");
    }
    this.cleanup();
    const existing = this.records.get(finalizedResult.resultId);
    if (existing && existing.result.resultRevision > finalizedResult.resultRevision) return existing.result;
    this.records.delete(finalizedResult.resultId);
    this.records.set(finalizedResult.resultId, Object.freeze({
      result: finalizedResult,
      expiresAt: this.now() + this.ttlMs
    }));
    this.cleanup();
    return finalizedResult;
  }

  validateIdentity({ resultId, resultRevision, geometryHash } = {}) {
    const record = this.records.get(String(resultId || ""));
    if (!record) return runtimeFailure(COORDINATE_GATE_REASON.CONFIRMATION_RESULT_NOT_FOUND, 404);
    if (record.expiresAt <= this.now()) {
      this.records.delete(resultId);
      return runtimeFailure(COORDINATE_GATE_REASON.CONFIRMATION_RESULT_EXPIRED, 410);
    }
    this.cleanup();
    if (record.result.resultRevision !== resultRevision) {
      return runtimeFailure(COORDINATE_GATE_REASON.STALE_CONFIRMATION_REVISION, 409);
    }
    if (!geometryHash || record.result.geometryHash !== geometryHash) {
      return runtimeFailure(COORDINATE_GATE_REASON.GEOMETRY_HASH_MISMATCH, 409);
    }
    return Object.freeze({ ok: true, result: record.result });
  }

  confirm(identity = {}) {
    if (identity.action !== "accept") {
      return runtimeFailure(COORDINATE_GATE_REASON.CONFIRMATION_ACTION_INVALID, 400);
    }
    const validated = this.validateIdentity(identity);
    if (!validated.ok) return validated;
    if (validated.result.confirmationStatus === COORDINATE_CONFIRMATION_STATUS.ACCEPTED) {
      return Object.freeze({ ok: true, idempotent: true, finalizedCoordinateResult: validated.result });
    }
    const confirmedPolicy = releaseConfirmedFamilySafetyPolicy(validated.result);
    const updated = finalizeCoordinateResult({
      ...validated.result,
      ...confirmedPolicy,
      currentRevision: validated.result.resultRevision,
      confirmedRevision: validated.result.resultRevision,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED,
      createdAt: validated.result.createdAt
    });
    this.register(updated);
    return Object.freeze({ ok: true, idempotent: false, finalizedCoordinateResult: updated });
  }

  clear() {
    this.records.clear();
  }
}

export const coordinateConfirmationRuntime = new CoordinateConfirmationRuntime();

export function registerFinalizedCoordinateResult(result) {
  return coordinateConfirmationRuntime.register(result);
}

export function confirmFinalizedCoordinateResult(identity) {
  return coordinateConfirmationRuntime.confirm(identity);
}

export function validateFinalizedCoordinateIdentity(identity) {
  return coordinateConfirmationRuntime.validateIdentity(identity);
}
