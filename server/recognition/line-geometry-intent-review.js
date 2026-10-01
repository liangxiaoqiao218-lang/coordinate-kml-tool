import { createHash, randomUUID } from "node:crypto";

export const LINE_GEOMETRY_INTENT_REVIEW_VERSION = "line_geometry_intent_review_v1";

const DEFAULT_TTL_MS = 15 * 60 * 1000;
const DEFAULT_MAX_REVIEWS = 500;

function text(value) {
  return String(value ?? "").trim();
}

function digest(value) {
  return createHash("sha256").update(String(value ?? "")).digest("hex");
}

function reviewBinding(review = {}) {
  return digest(JSON.stringify({
    schemaVersion: review.schemaVersion,
    reviewId: review.reviewId,
    resultId: review.resultId,
    resultRevision: review.resultRevision,
    geometryHash: review.geometryHash,
    sourceCrs: review.sourceCrs,
    coordinateTextSha256: review.coordinateTextSha256,
    evidenceType: review.evidenceType
  }));
}

function failure(code, httpStatus) {
  return Object.freeze({ ok: false, code, httpStatus });
}

export class LineGeometryIntentReviewRuntime {
  constructor({ ttlMs = DEFAULT_TTL_MS, maxReviews = DEFAULT_MAX_REVIEWS, now = () => Date.now() } = {}) {
    this.ttlMs = ttlMs;
    this.maxReviews = maxReviews;
    this.now = now;
    this.records = new Map();
  }

  cleanup() {
    const now = this.now();
    for (const [reviewId, record] of this.records) {
      if (record.expiresAt <= now) this.records.delete(reviewId);
    }
    while (this.records.size > this.maxReviews) this.records.delete(this.records.keys().next().value);
  }

  issue({ result, sourceCrs, coordinateText } = {}) {
    this.cleanup();
    if (!text(result?.resultId) || !Number.isSafeInteger(result?.resultRevision)
      || !text(sourceCrs) || !text(coordinateText)) return null;
    const review = {
      schemaVersion: LINE_GEOMETRY_INTENT_REVIEW_VERSION,
      reviewId: randomUUID(),
      resultId: result.resultId,
      resultRevision: result.resultRevision,
      geometryHash: text(result.geometryHash) || null,
      sourceCrs: text(sourceCrs).toLowerCase(),
      coordinateTextSha256: digest(coordinateText),
      evidenceType: "IDENTITY_BOUND_USER_CONFIRMATION"
    };
    review.reviewBindingSha256 = reviewBinding(review);
    const publicReview = Object.freeze({ ...review });
    this.records.set(review.reviewId, {
      review: publicReview,
      expiresAt: this.now() + this.ttlMs,
      consumed: false
    });
    return publicReview;
  }

  accept({ reviewId, reviewBindingSha256, result, sourceCrs, coordinateText, action } = {}) {
    const record = this.records.get(text(reviewId));
    if (!record) return failure("LINE_GEOMETRY_INTENT_REVIEW_NOT_FOUND", 404);
    if (record.expiresAt <= this.now()) {
      this.records.delete(text(reviewId));
      return failure("LINE_GEOMETRY_INTENT_REVIEW_EXPIRED", 410);
    }
    if (record.consumed) return failure("LINE_GEOMETRY_INTENT_REVIEW_REPLAYED", 409);
    if (text(action) !== "accept_line") return failure("LINE_GEOMETRY_INTENT_ACTION_INVALID", 400);
    const review = record.review;
    if (text(reviewBindingSha256) !== review.reviewBindingSha256
      || text(result?.resultId) !== review.resultId
      || Number(result?.resultRevision) !== review.resultRevision
      || (text(result?.geometryHash) || null) !== review.geometryHash
      || text(sourceCrs).toLowerCase() !== review.sourceCrs
      || digest(coordinateText) !== review.coordinateTextSha256) {
      return failure("LINE_GEOMETRY_INTENT_REVIEW_IDENTITY_MISMATCH", 409);
    }
    record.consumed = true;
    return Object.freeze({ ok: true, review });
  }

  clear() {
    this.records.clear();
  }
}

export const lineGeometryIntentReviewRuntime = new LineGeometryIntentReviewRuntime();
