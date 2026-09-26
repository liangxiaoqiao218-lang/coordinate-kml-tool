export const PROVIDER_TIMEOUT_AFTER_LOCAL_OCR_CODE = "PROVIDER_TIMEOUT_AFTER_LOCAL_OCR";
export const PROVIDER_FAILURE_AFTER_LOCAL_OCR_CODE = "PROVIDER_FAILURE_AFTER_LOCAL_OCR";

export function planProviderTimeoutLocalOcrRecovery({
  localOcrAttempted = false,
  sourceText = "",
  layoutLines = [],
  axisOrderEvidence = null,
  projectedTableOcrAcquisition = false
} = {}) {
  const attempted = localOcrAttempted === true;
  const reusableSourceText = attempted ? String(sourceText || "") : "";
  const hasReusableEvidence = attempted && reusableSourceText.trim().length > 0;

  return Object.freeze({
    localOcrAttempted: attempted,
    allowNewLocalOcr: !attempted,
    hasReusableEvidence,
    shouldFailClosedWithoutNewOcr: attempted && !hasReusableEvidence,
    evidence: Object.freeze({
      sourceText: reusableSourceText,
      layoutLines: Object.freeze(attempted && Array.isArray(layoutLines) ? [...layoutLines] : []),
      axisOrderEvidence: attempted ? axisOrderEvidence || null : null,
      projectedTableOcrAcquisition: attempted && projectedTableOcrAcquisition === true
    })
  });
}
