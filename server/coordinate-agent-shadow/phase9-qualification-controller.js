const PUBLIC_STATES = new Set(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);

function publicFailure(error) {
  return Object.freeze({
    schemaVersion: 'coordinate-agent-phase9-qualification/v1',
    status: 'FAILED',
    terminalState: 'FAILED_CLOSED',
    candidatePointCount: 0,
    realProviderCallCount: Number(error?.realProviderCallCount || 0),
    automaticRetryCount: 0,
    usage: Object.freeze({ inputTokens: 0, outputTokens: 0, totalTokens: 0 }),
    billingStatus: 'UNKNOWN',
    mapAllowed: false,
    kmlAllowed: false,
    stateHistory: Object.freeze([]),
    toolCalls: Object.freeze([]),
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
    realProviderCallCount: 0,
    automaticRetryCount: 0,
    usage: Object.freeze({ inputTokens: 0, outputTokens: 0, totalTokens: 0 }),
    billingStatus: 'UNKNOWN',
    mapAllowed: false,
    kmlAllowed: false,
    stateHistory: Object.freeze([]),
    toolCalls: Object.freeze([]),
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
