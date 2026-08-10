const SCHEMA_VERSION = "recognition_metrics_v1";

const PIPELINE_STATUSES = new Set(["success", "degraded", "failed"]);
const ATTEMPT_STATUSES = new Set(["success", "timeout", "error", "skipped"]);
const PARSER_STATUSES = new Set(["success", "rejected", "failed", "skipped"]);
const PROVIDERS = new Set(["vision", "ocr"]);
const FALLBACK_REASONS = new Set([
  "NONE",
  "VISION_TIMEOUT",
  "VISION_ERROR",
  "OCR_TIMEOUT",
  "OCR_EMPTY_RESPONSE",
  "PARSER_NO_VALID_RESULT",
  "MODEL_UNAVAILABLE",
  "MANUAL_DEGRADED_PATH"
]);

const FALLBACK_TYPES = new Set([
  "local_tesseract",
  "vision_retry",
  "ocr_retry",
  "manual",
  "other"
]);

const SPECIAL_PARSERS = [
  "cadastralGrid",
  "utm",
  "dms"
];

const SENSITIVE_KEY_PATTERN = /(api[_-]?key|authorization|secret|token|password|credential|env|supabase|dashscope|aliyun)/i;

function nowMs() {
  return Date.now();
}

function toSafeString(value, fallback = null) {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function toNonNegativeNumber(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.round(number) : fallback;
}

function normalizeAttemptStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  return ATTEMPT_STATUSES.has(status) ? status : "error";
}

function normalizeParserStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  return PARSER_STATUSES.has(status) ? status : "failed";
}

function normalizePipelineStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  return PIPELINE_STATUSES.has(status) ? status : "success";
}

function normalizeProvider(value) {
  const provider = String(value || "").trim().toLowerCase();
  return PROVIDERS.has(provider) ? provider : "vision";
}

function normalizeFallbackReason(value) {
  const reason = String(value || "NONE").trim().toUpperCase();
  return FALLBACK_REASONS.has(reason) ? reason : "MANUAL_DEGRADED_PATH";
}

function normalizeFallbackType(value) {
  const type = String(value || "").trim().toLowerCase();
  return FALLBACK_TYPES.has(type) ? type : null;
}

function sanitizeErrorCode(value) {
  const text = toSafeString(value);
  if (!text) return null;
  if (SENSITIVE_KEY_PATTERN.test(text)) return "REDACTED";
  return text
    .replace(/[^A-Z0-9_:-]/gi, "_")
    .slice(0, 80)
    .toUpperCase();
}

function sanitizeModelName(value) {
  const text = toSafeString(value);
  if (!text) return null;
  if (SENSITIVE_KEY_PATTERN.test(text)) return "REDACTED";
  return text.slice(0, 120);
}

function sanitizeStage(value) {
  const text = toSafeString(value, "unknown");
  if (SENSITIVE_KEY_PATTERN.test(text)) return "redacted_stage";
  return text
    .replace(/[^a-z0-9:_-]/gi, "_")
    .slice(0, 80);
}

function sanitizeReason(value) {
  const text = toSafeString(value);
  if (!text) return null;
  if (SENSITIVE_KEY_PATTERN.test(text)) return "REDACTED";
  return text
    .replace(/[^a-z0-9:_+-]/gi, "_")
    .slice(0, 120);
}

function createSummaryStage() {
  return {
    started: false,
    durationMs: null,
    status: "skipped",
    model: null,
    errorCode: null
  };
}

function createParserStage() {
  return {
    started: false,
    status: "skipped",
    reason: null
  };
}

function ensureMetrics(metrics) {
  if (!metrics || metrics.schemaVersion !== SCHEMA_VERSION) {
    throw new Error("recognitionMetrics must be created with createRecognitionMetrics()");
  }
  return metrics;
}

function recomputePipelineStatus(metrics) {
  const attempts = Array.isArray(metrics.attempts) ? metrics.attempts : [];
  const parserStages = Object.values(metrics.specialParsers || {});
  const failed = attempts.some(attempt => attempt.status === "error")
    || parserStages.some(parser => parser.status === "failed");
  const timeout = attempts.some(attempt => attempt.status === "timeout");
  const fallback = Boolean(metrics.fallback?.used);
  const rejected = parserStages.some(parser => parser.status === "rejected");

  if (failed && !fallback) {
    metrics.pipelineStatus = "failed";
  } else if (failed || timeout || fallback || rejected) {
    metrics.pipelineStatus = "degraded";
  } else {
    metrics.pipelineStatus = "success";
  }
}

function updateProviderSummary(metrics, attempt) {
  const provider = normalizeProvider(attempt.provider);
  const summary = metrics[provider];
  if (!summary) return;

  summary.started = true;
  summary.model = attempt.model || summary.model;
  summary.errorCode = attempt.errorCode || summary.errorCode;

  if (summary.durationMs === null) {
    summary.durationMs = attempt.durationMs;
  } else if (attempt.durationMs !== null) {
    summary.durationMs += attempt.durationMs;
  }

  if (attempt.status === "timeout" || summary.status === "timeout") {
    summary.status = "timeout";
  } else if (attempt.status === "error" || summary.status === "error") {
    summary.status = "error";
  } else if (attempt.status === "success") {
    summary.status = "success";
  }
}

export function createRecognitionMetrics({ startedAt = nowMs(), pipelineStatus = "success" } = {}) {
  const metrics = {
    schemaVersion: SCHEMA_VERSION,
    pipelineStatus: normalizePipelineStatus(pipelineStatus),
    totalDurationMs: 0,
    vision: createSummaryStage(),
    ocr: createSummaryStage(),
    specialParsers: {
      cadastralGrid: createParserStage(),
      utm: createParserStage(),
      dms: createParserStage()
    },
    fallback: {
      used: false,
      type: null,
      reason: "NONE"
    },
    attempts: []
  };

  Object.defineProperty(metrics, "_startedAt", {
    value: toNonNegativeNumber(startedAt, nowMs()),
    enumerable: false,
    writable: true
  });

  return metrics;
}

export function startAttempt(metrics, {
  stage,
  provider = "vision",
  model = null,
  timeoutMs = null,
  startedAt = nowMs()
} = {}) {
  const target = ensureMetrics(metrics);
  const attempt = {
    stage: sanitizeStage(stage),
    provider: normalizeProvider(provider),
    started: true,
    durationMs: null,
    status: "skipped",
    model: sanitizeModelName(model),
    timeoutMs: toNonNegativeNumber(timeoutMs),
    errorCode: null,
    resultCount: null
  };

  Object.defineProperty(attempt, "_startedAt", {
    value: toNonNegativeNumber(startedAt, nowMs()),
    enumerable: false,
    writable: true
  });

  target.attempts.push(attempt);
  target[attempt.provider].started = true;
  target[attempt.provider].model = attempt.model || target[attempt.provider].model;
  return attempt;
}

export function finishAttempt(metrics, attempt, {
  status = "success",
  errorCode = null,
  resultCount = null,
  endedAt = nowMs(),
  durationMs = null
} = {}) {
  const target = ensureMetrics(metrics);
  const targetAttempt = target.attempts.includes(attempt) ? attempt : null;

  if (!targetAttempt) {
    throw new Error("attempt must be returned by startAttempt()");
  }

  const computedDuration = durationMs !== null && durationMs !== undefined
    ? durationMs
    : toNonNegativeNumber(endedAt, nowMs()) - toNonNegativeNumber(targetAttempt._startedAt, nowMs());

  targetAttempt.status = normalizeAttemptStatus(status);
  targetAttempt.durationMs = toNonNegativeNumber(computedDuration, 0);
  targetAttempt.errorCode = sanitizeErrorCode(errorCode);
  targetAttempt.resultCount = toNonNegativeNumber(resultCount);

  updateProviderSummary(target, targetAttempt);
  target.totalDurationMs = Math.max(0, toNonNegativeNumber(endedAt, nowMs()) - toNonNegativeNumber(target._startedAt, nowMs()));
  recomputePipelineStatus(target);
  return targetAttempt;
}

export function markFallback(metrics, {
  used = true,
  type = null,
  reason = "MANUAL_DEGRADED_PATH"
} = {}) {
  const target = ensureMetrics(metrics);
  target.fallback = {
    used: Boolean(used),
    type: Boolean(used) ? normalizeFallbackType(type) : null,
    reason: Boolean(used) ? normalizeFallbackReason(reason) : "NONE"
  };
  recomputePipelineStatus(target);
  return target.fallback;
}

export function markSpecialParser(metrics, parserName, {
  started = true,
  status = "success",
  reason = null,
  rowCount = null,
  zone = null
} = {}) {
  const target = ensureMetrics(metrics);
  const parserKey = SPECIAL_PARSERS.includes(parserName) ? parserName : null;
  if (!parserKey) {
    throw new Error(`Unsupported special parser: ${String(parserName || "")}`);
  }

  const parser = {
    started: Boolean(started),
    status: normalizeParserStatus(status),
    reason: sanitizeReason(reason)
  };

  const normalizedRowCount = toNonNegativeNumber(rowCount);
  if (normalizedRowCount !== null) {
    parser.rowCount = normalizedRowCount;
  }

  const normalizedZone = toSafeString(zone);
  if (normalizedZone) {
    parser.zone = normalizedZone.slice(0, 20);
  }

  target.specialParsers[parserKey] = parser;
  recomputePipelineStatus(target);
  return parser;
}

export function sanitizeRecognitionMetricsForResponse(metrics, { includeAttempts = false, endedAt = nowMs() } = {}) {
  const source = ensureMetrics(metrics);
  const safe = {
    schemaVersion: SCHEMA_VERSION,
    pipelineStatus: normalizePipelineStatus(source.pipelineStatus),
    totalDurationMs: toNonNegativeNumber(source.totalDurationMs, Math.max(0, toNonNegativeNumber(endedAt, nowMs()) - toNonNegativeNumber(source._startedAt, nowMs()))),
    vision: { ...source.vision },
    ocr: { ...source.ocr },
    specialParsers: {
      cadastralGrid: { ...source.specialParsers.cadastralGrid },
      utm: { ...source.specialParsers.utm },
      dms: { ...source.specialParsers.dms }
    },
    fallback: { ...source.fallback }
  };

  if (includeAttempts) {
    safe.attempts = source.attempts.map(attempt => ({
      stage: sanitizeStage(attempt.stage),
      provider: normalizeProvider(attempt.provider),
      started: Boolean(attempt.started),
      durationMs: toNonNegativeNumber(attempt.durationMs),
      status: normalizeAttemptStatus(attempt.status),
      model: sanitizeModelName(attempt.model),
      timeoutMs: toNonNegativeNumber(attempt.timeoutMs),
      errorCode: sanitizeErrorCode(attempt.errorCode),
      resultCount: toNonNegativeNumber(attempt.resultCount)
    }));
  }

  return safe;
}

export const recognitionMetricsConstants = Object.freeze({
  schemaVersion: SCHEMA_VERSION,
  pipelineStatuses: Object.freeze([...PIPELINE_STATUSES]),
  attemptStatuses: Object.freeze([...ATTEMPT_STATUSES]),
  parserStatuses: Object.freeze([...PARSER_STATUSES]),
  fallbackReasons: Object.freeze([...FALLBACK_REASONS])
});
