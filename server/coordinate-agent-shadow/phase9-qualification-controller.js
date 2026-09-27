const PUBLIC_STATES = new Set(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);

function finiteOrNull(value) {
  return Number.isFinite(value) ? Number(value) : null;
}

function safeTokenOrNull(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,80}$/.test(value) ? value : null;
}

function safeProviderRequests(value) {
  if (!Array.isArray(value)) return Object.freeze([]);
  return Object.freeze(value.map(item => Object.freeze({
    ok: item?.ok === true,
    httpStatus: finiteOrNull(item?.httpStatus),
    errorCode: safeTokenOrNull(item?.errorCode),
    usageObserved: item?.usageObserved === true,
    durationMs: finiteOrNull(item?.durationMs),
    timeoutMs: finiteOrNull(item?.timeoutMs),
    requestBuildDurationMs: finiteOrNull(item?.requestBuildDurationMs),
    requestBodyBytes: finiteOrNull(item?.requestBodyBytes),
    imageBytes: finiteOrNull(item?.imageBytes),
    schemaBytes: finiteOrNull(item?.schemaBytes),
  })));
}

function safeImageMetrics(value) {
  if (!Array.isArray(value)) return Object.freeze([]);
  return Object.freeze(value.map(item => Object.freeze({
    originalBytes: finiteOrNull(item?.originalBytes),
    optimizedBytes: finiteOrNull(item?.optimizedBytes),
    width: finiteOrNull(item?.width),
    height: finiteOrNull(item?.height),
    quality: finiteOrNull(item?.quality),
    format: safeTokenOrNull(item?.format),
    optimizationDurationMs: finiteOrNull(item?.optimizationDurationMs),
  })));
}

function publicFailure(error) {
  const realProviderCallCount = Number.isInteger(error?.realProviderCallCount)
    && error.realProviderCallCount >= 0
    ? error.realProviderCallCount
    : 0;
  return Object.freeze({
    schemaVersion: 'coordinate-agent-phase9-qualification/v1',
    status: 'FAILED',
    terminalState: 'FAILED_CLOSED',
    candidatePointCount: 0,
    verifiedPointCount: 0,
    candidateRepresentation: null,
    projectionVerification: null,
    realProviderCallCount,
    automaticRetryCount: 0,
    requestTimeoutMs: finiteOrNull(error?.requestTimeoutMs),
    imageMetrics: safeImageMetrics(error?.imageMetrics),
    usage: Object.freeze({ inputTokens: 0, outputTokens: 0, totalTokens: 0 }),
    billingStatus: 'UNKNOWN',
    mapAllowed: false,
    kmlAllowed: false,
    stateHistory: Object.freeze([]),
    toolCalls: Object.freeze([]),
    diagnostics: Object.freeze([]),
    providerRequests: safeProviderRequests(error?.providerRequests),
    validationFailureCodes: Object.freeze(['PHASE9_QUALIFICATION_FAILED']),
    sourceUnchanged: true,
  });
}

function assertPublicReport(report) {
  if (!report || typeof report !== 'object' || Array.isArray(report)) {
    throw new Error('Phase 9 qualification must return a public report');
  }
  if (!PUBLIC_STATES.has(String(report.status || ''))) {
    throw new Error('Phase 9 qualification report status is invalid');
  }
  return Object.freeze(structuredClone(report));
}

export function createPhase9QualificationController({ runQualification } = {}) {
  if (typeof runQualification !== 'function') throw new Error('runQualification is required');
  let status = Object.freeze({
    schemaVersion: 'coordinate-agent-phase9-qualification/v1',
    status: 'PENDING',
    terminalState: null,
    candidatePointCount: 0,
    verifiedPointCount: 0,
    candidateRepresentation: null,
    projectionVerification: null,
    realProviderCallCount: 0,
    automaticRetryCount: 0,
    requestTimeoutMs: null,
    imageMetrics: Object.freeze([]),
    usage: Object.freeze({ inputTokens: 0, outputTokens: 0, totalTokens: 0 }),
    billingStatus: 'UNKNOWN',
    mapAllowed: false,
    kmlAllowed: false,
    stateHistory: Object.freeze([]),
    toolCalls: Object.freeze([]),
    diagnostics: Object.freeze([]),
    providerRequests: Object.freeze([]),
    validationFailureCodes: Object.freeze([]),
    sourceUnchanged: true,
  });
  let promise = null;

  return Object.freeze({
    status: () => status,
    runOnce() {
      if (promise) return promise;
      status = Object.freeze({ ...status, status: 'RUNNING' });
      promise = Promise.resolve()
        .then(runQualification)
        .then(report => {
          status = assertPublicReport(report);
          return status;
        })
        .catch(error => {
          status = publicFailure(error);
          return status;
        });
      return promise;
    },
  });
}
