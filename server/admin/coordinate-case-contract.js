const ISSUE_TYPES = new Set(["recognition_failed", "coordinate_correction", "kml_failed"]);
const CASE_STATUSES = new Set(["NEW", "TRIAGED", "IN_PROGRESS", "BLOCKED", "VERIFIED", "RESOLVED", "CLOSED"]);
const STAGE_STATUSES = new Set(["UNKNOWN", "PASS", "FAIL", "BLOCKED", "NOT_APPLICABLE"]);
const STAGE_KEYS = ["delivery", "problemResolution", "peerValidation", "production"];
const CRS_STATUSES = new Set(["UNKNOWN", "WORKING_ASSUMPTION", "CONFIRMED"]);
const GEOMETRY_REPRESENTATIONS = new Set(["UNKNOWN", "POLYGON", "MULTIPOINT", "LINESTRING"]);
const ARTIFACT_STATUSES = new Set(["NOT_SAVED", "AUTHORIZED_REFERENCE", "UNKNOWN"]);
const IDENTITY_STATUSES = new Set(["REQUEST_ONLY", "CURRENT", "UNKNOWN"]);
const FORBIDDEN_KEYS = /(?:image|provider|cookie|authorization|auth[_-]?header|secret|password|api[_-]?key|coordinates?|raw[_-]?response|environment|env[_-]?config)/iu;
const ALLOWED_COARSE_COORDINATE_KEYS = new Set(["coordinatetype", "coordinate_type"]);
const RESULT_HASH_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const COMMIT_PATTERN = /^[0-9a-f]{40}$/u;
const ERROR_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,159}$/u;
const ISSUE_SOURCES = new Set(["UNKNOWN", "COORDINATE_IMAGE_UPLOAD", "MANUAL_ASSISTANCE", "ADMIN"]);
const FAILURE_STAGES = new Set(["UNKNOWN", "ACQUISITION", "VALIDATION", "FINALIZATION", "MANUAL_ASSISTANCE"]);
const COORDINATE_TYPES = new Set(["UNKNOWN", "DECIMAL_DEGREES", "DMS", "UTM", "PROJECTED", "MIXED"]);
const STAGE_EVIDENCE_PREFIX = Object.freeze({
  delivery: "DELIVERY",
  problemResolution: "PROBLEM_RESOLUTION",
  peerValidation: "PEER_VALIDATION",
  production: "PRODUCTION"
});

function text(value, max = 500) {
  return String(value ?? "").trim().slice(0, max);
}

function enumValue(value, allowed, fallback) {
  const normalized = text(value, 80).toUpperCase();
  return allowed.has(normalized) ? normalized : fallback;
}

function assertNoForbiddenMaterial(value, path = "payload") {
  if (!value || typeof value !== "object") return;
  for (const [key, nested] of Object.entries(value)) {
    if (FORBIDDEN_KEYS.test(key) && !ALLOWED_COARSE_COORDINATE_KEYS.has(String(key).toLowerCase())) {
      throw Object.assign(new Error(`COORDINATE_CASE_FORBIDDEN_FIELD:${path}.${key}`), { code: "COORDINATE_CASE_FORBIDDEN_FIELD" });
    }
    assertNoForbiddenMaterial(nested, `${path}.${key}`);
  }
}

function assertSafeTextValue(value, path) {
  const source = String(value ?? "");
  if (!source) return;
  const coordinateRows = source.split(/\r?\n/u).filter(line =>
    /^\s*(?:[A-Za-z0-9_-]+\s*[|,;\t]\s*)?[-+]?\d{1,10}(?:\.\d+)?\s*[|,;\t]\s*[-+]?\d{1,10}(?:\.\d+)?(?:\s*[|,;\t].*)?\s*$/u.test(line)
    || (/\d+\s*[°º].*[NSEWO].*\d+\s*[°º].*[NSEWO]/iu.test(line))
  );
  const environmentRows = source.split(/\r?\n/u).filter(line => /^\s*[A-Z][A-Z0-9_]{2,}\s*=\s*\S+/u.test(line));
  let providerObject = false;
  if (/^\s*[\[{]/u.test(source)) {
    try {
      const parsed = JSON.parse(source);
      const serialized = JSON.stringify(parsed);
      providerObject = /"(?:choices|usage|provider|message|content|raw_response)"\s*:/iu.test(serialized);
    } catch {
      providerObject = false;
    }
  }
  const forbidden = coordinateRows.length >= 2
    || environmentRows.length >= 2
    || providerObject
    || /data:[^;,\s]+(?:;[^,\s]+)*;base64,/iu.test(source)
    || /\bBearer\s+[A-Za-z0-9._~+\/-]{8,}/iu.test(source)
    || /\b(?:Cookie|Set-Cookie)\s*[:=]/iu.test(source)
    || /\b(?:api[_ -]?key|secret|password|authorization|auth[_ -]?header|access[_ -]?token|refresh[_ -]?token)\s*[:=]\s*\S+/iu.test(source)
    || /\bsk-[A-Za-z0-9_-]{12,}\b/u.test(source)
    || /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/u.test(source)
    || /[?&](?:api[_-]?key|secret|token|authorization)=[^&\s]+/iu.test(source);
  if (forbidden) {
    throw Object.assign(new Error(`COORDINATE_CASE_FORBIDDEN_CONTENT:${path}`), { code: "COORDINATE_CASE_FORBIDDEN_CONTENT" });
  }
}

function safeText(value, max, path) {
  assertSafeTextValue(value, path);
  return text(value, max);
}

function machineErrorCode(value) {
  const normalized = text(value, 160).toUpperCase();
  if (!normalized) return null;
  if (!ERROR_CODE_PATTERN.test(normalized)) {
    throw Object.assign(new Error("COORDINATE_CASE_ERROR_CODE_INVALID"), { code: "COORDINATE_CASE_ERROR_CODE_INVALID" });
  }
  return normalized;
}

function normalizeResultIdentity(raw = {}) {
  const resultId = text(raw.resultId ?? raw.result_id, 200);
  const resultRevision = Number(raw.resultRevision ?? raw.result_revision ?? 0);
  const geometryHash = text(raw.geometryHash ?? raw.geometry_hash, 80).toLowerCase();
  const populated = [Boolean(resultId), Number.isInteger(resultRevision) && resultRevision > 0, Boolean(geometryHash)];
  if (populated.some(Boolean) && !populated.every(Boolean)) {
    throw Object.assign(new Error("COORDINATE_CASE_RESULT_IDENTITY_INCOMPLETE"), { code: "COORDINATE_CASE_RESULT_IDENTITY_INCOMPLETE" });
  }
  if (geometryHash && !RESULT_HASH_PATTERN.test(geometryHash)) {
    throw Object.assign(new Error("COORDINATE_CASE_GEOMETRY_HASH_INVALID"), { code: "COORDINATE_CASE_GEOMETRY_HASH_INVALID" });
  }
  return populated.every(Boolean)
    ? { result_id: resultId, result_revision: resultRevision, geometry_hash: geometryHash, result_identity_status: "UNKNOWN" }
    : { result_id: null, result_revision: null, geometry_hash: null, result_identity_status: "REQUEST_ONLY" };
}

export function normalizeCoordinateCaseInput(raw = {}) {
  assertNoForbiddenMaterial(raw);
  const recognitionRequestId = text(raw.recognitionRequestId ?? raw.recognition_request_id, 80).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(recognitionRequestId)) {
    throw Object.assign(new Error("COORDINATE_CASE_REQUEST_ID_INVALID"), { code: "COORDINATE_CASE_REQUEST_ID_INVALID" });
  }
  const issueType = text(raw.issueType ?? raw.issue_type, 80).toLowerCase();
  if (!ISSUE_TYPES.has(issueType)) {
    throw Object.assign(new Error("COORDINATE_CASE_ISSUE_TYPE_INVALID"), { code: "COORDINATE_CASE_ISSUE_TYPE_INVALID" });
  }
  const identity = normalizeResultIdentity(raw);
  const issueSummary = safeText(raw.issueSummary ?? raw.issue_summary, 1200, "issueSummary");
  if (!issueSummary) {
    throw Object.assign(new Error("COORDINATE_CASE_ISSUE_SUMMARY_REQUIRED"), { code: "COORDINATE_CASE_ISSUE_SUMMARY_REQUIRED" });
  }
  const sampleCount = Number(raw.sampleCount ?? raw.sample_count ?? 0);
  const commitSha = text(raw.commitSha ?? raw.commit_sha, 40).toLowerCase();
  if (commitSha && !COMMIT_PATTERN.test(commitSha)) {
    throw Object.assign(new Error("COORDINATE_CASE_COMMIT_INVALID"), { code: "COORDINATE_CASE_COMMIT_INVALID" });
  }
  const progressStatus = enumValue(raw.progressStatus ?? raw.progress_status, CASE_STATUSES, "NEW");
  const normalized = {
    recognition_request_id: recognitionRequestId,
    job_id: safeText(raw.jobId ?? raw.job_id, 160, "jobId") || null,
    ...identity,
    issue_type: issueType,
    issue_summary: issueSummary,
    resolution_summary: safeText(raw.resolutionSummary ?? raw.resolution_summary, 1200, "resolutionSummary") || null,
    owner: safeText(raw.owner, 120, "owner") || null,
    progress_status: progressStatus,
    blocker: safeText(raw.blocker, 800, "blocker") || null,
    delivery_status: enumValue(raw.deliveryStatus ?? raw.delivery_status, STAGE_STATUSES, "UNKNOWN"),
    problem_resolution_status: enumValue(raw.problemResolutionStatus ?? raw.problem_resolution_status, STAGE_STATUSES, "UNKNOWN"),
    peer_validation_status: enumValue(raw.peerValidationStatus ?? raw.peer_validation_status, STAGE_STATUSES, "UNKNOWN"),
    production_status: enumValue(raw.productionStatus ?? raw.production_status, STAGE_STATUSES, "UNKNOWN"),
    evidence_scope: safeText(raw.evidenceScope ?? raw.evidence_scope, 800, "evidenceScope") || null,
    sample_count: Number.isInteger(sampleCount) && sampleCount >= 0 ? sampleCount : 0,
    crs_evidence_status: enumValue(raw.crsEvidenceStatus ?? raw.crs_evidence_status, CRS_STATUSES, "UNKNOWN"),
    geometry_representation: enumValue(raw.geometryRepresentation ?? raw.geometry_representation, GEOMETRY_REPRESENTATIONS, "UNKNOWN"),
    original_artifact_status: enumValue(raw.originalArtifactStatus ?? raw.original_artifact_status, ARTIFACT_STATUSES, "NOT_SAVED"),
    golden_ref: safeText(raw.goldenRef ?? raw.golden_ref, 240, "goldenRef") || null,
    commit_sha: commitSha || null,
    receipt_ref: safeText(raw.receiptRef ?? raw.receipt_ref, 500, "receiptRef") || null,
    source: enumValue(raw.source, ISSUE_SOURCES, "ADMIN"),
    failure_stage: enumValue(raw.failureStage ?? raw.failure_stage, FAILURE_STAGES, "UNKNOWN"),
    error_code: machineErrorCode(raw.errorCode ?? raw.error_code),
    coordinate_type: enumValue(raw.coordinateType ?? raw.coordinate_type, COORDINATE_TYPES, "UNKNOWN"),
    runtime_commit: (() => {
      const value = text(raw.runtimeCommit ?? raw.runtime_commit, 40).toLowerCase();
      if (value && !COMMIT_PATTERN.test(value)) {
        throw Object.assign(new Error("COORDINATE_CASE_RUNTIME_COMMIT_INVALID"), { code: "COORDINATE_CASE_RUNTIME_COMMIT_INVALID" });
      }
      return value || null;
    })(),
    occurred_at: text(raw.occurredAt ?? raw.occurred_at, 80) || null
  };
  if (progressStatus === "RESOLVED") {
    const evidenceComplete = Boolean(normalized.commit_sha)
      && Boolean(normalized.receipt_ref)
      && Boolean(normalized.golden_ref)
      && normalized.sample_count > 0
      && Boolean(normalized.evidence_scope)
      && normalized.problem_resolution_status === "PASS"
      && normalized.peer_validation_status === "PASS";
    if (!evidenceComplete) {
      throw Object.assign(new Error("COORDINATE_CASE_RESOLUTION_EVIDENCE_REQUIRED"), { code: "COORDINATE_CASE_RESOLUTION_EVIDENCE_REQUIRED" });
    }
  }
  return normalized;
}

export function normalizeCoordinateCaseEvidenceInput(raw = {}) {
  assertNoForbiddenMaterial(raw);
  const referenceType = enumValue(raw.referenceType ?? raw.reference_type, new Set(["GOLDEN", "COMMIT", "RECEIPT", "REQUEST", "RESULT"]), "");
  const referenceValue = safeText(raw.referenceValue ?? raw.reference_value, 500, "referenceValue");
  const evidenceType = safeText(raw.evidenceType ?? raw.evidence_type, 120, "evidenceType").toUpperCase();
  const sampleCount = Number(raw.sampleCount ?? raw.sample_count ?? 0);
  if (!referenceType || !referenceValue || !evidenceType) {
    throw Object.assign(new Error("COORDINATE_CASE_EVIDENCE_INVALID"), { code: "COORDINATE_CASE_EVIDENCE_INVALID" });
  }
  return {
    evidence_type: evidenceType,
    status: enumValue(raw.status, STAGE_STATUSES, "UNKNOWN"),
    evidence_scope: safeText(raw.evidenceScope ?? raw.evidence_scope, 800, "evidenceScope") || null,
    sample_count: Number.isInteger(sampleCount) && sampleCount >= 0 ? sampleCount : 0,
    reference_type: referenceType,
    reference_value: referenceValue
  };
}

export function publicCoordinateCase(row = {}, evidence = []) {
  const publicCase = {
    caseId: row.case_id,
    caseNumber: row.case_number,
    recognitionRequestId: row.recognition_request_id,
    jobId: row.job_id,
    resultId: row.result_id,
    resultRevision: row.result_revision,
    geometryHash: row.geometry_hash,
    resultIdentityStatus: row.result_identity_status,
    issueType: row.issue_type,
    issueSummary: row.issue_summary,
    resolutionSummary: row.resolution_summary,
    owner: row.owner,
    progressStatus: row.progress_status,
    blocker: row.blocker,
    stages: {
      delivery: row.delivery_status,
      problemResolution: row.problem_resolution_status,
      peerValidation: row.peer_validation_status,
      production: row.production_status
    },
    evidenceScope: row.evidence_scope,
    sampleCount: Number(row.sample_count || 0),
    crsEvidenceStatus: row.crs_evidence_status,
    geometryRepresentation: row.geometry_representation,
    originalArtifactStatus: row.original_artifact_status,
    goldenRef: row.golden_ref,
    commitSha: row.commit_sha,
    receiptRef: row.receipt_ref,
    source: row.source,
    failureStage: row.failure_stage,
    errorCode: row.error_code,
    coordinateType: row.coordinate_type,
    runtimeCommit: row.runtime_commit,
    occurredAt: row.occurred_at,
    evidence: (evidence || []).map(item => ({
      evidenceId: item.evidence_id,
      evidenceType: item.evidence_type,
      status: item.status,
      scope: item.evidence_scope,
      sampleCount: Number(item.sample_count || 0),
      referenceType: item.reference_type,
      referenceValue: item.reference_value,
      createdAt: item.created_at
    })),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
  return { ...publicCase, stages: buildEffectiveCoordinateCaseStages(publicCase) };
}

export function buildEffectiveCoordinateCaseStages(item = {}) {
  return Object.fromEntries(STAGE_KEYS.map(key => {
    const status = STAGE_STATUSES.has(item?.stages?.[key]) ? item.stages[key] : "UNKNOWN";
    if (status !== "PASS") return [key, status];
    const prefix = STAGE_EVIDENCE_PREFIX[key];
    const stageEvidence = (item.evidence || []).some(evidence =>
      String(evidence?.evidenceType || "").toUpperCase().startsWith(`${prefix}_`)
      && evidence?.status === "PASS"
      && Number(evidence?.sampleCount || 0) > 0
      && Boolean(text(evidence?.scope, 800))
      && Boolean(text(evidence?.referenceType, 80))
      && Boolean(text(evidence?.referenceValue, 500))
    );
    const directReference = key === "delivery"
      ? Boolean(text(item.receiptRef, 500))
      : key === "problemResolution"
        ? Boolean(text(item.commitSha, 40))
        : key === "peerValidation"
          ? Boolean(text(item.goldenRef, 240))
          : false;
    const evidenceBackedPass = Number(item.sampleCount || 0) > 0
      && Boolean(text(item.evidenceScope, 800))
      && (stageEvidence || directReference);
    return [key, evidenceBackedPass ? "PASS" : "UNKNOWN"];
  }));
}

export function buildCoverageSummary(cases = []) {
  const summary = Object.fromEntries(STAGE_KEYS.map(key => [key, { PASS: 0, FAIL: 0, BLOCKED: 0, UNKNOWN: 0, NOT_APPLICABLE: 0, sampleCount: 0, evidenceScopes: [] }]));
  for (const item of cases) {
    const effectiveStages = buildEffectiveCoordinateCaseStages(item);
    for (const key of STAGE_KEYS) {
      const effectiveStatus = effectiveStages[key];
      summary[key][effectiveStatus] += 1;
      if (effectiveStatus === "PASS") {
        summary[key].sampleCount += Number(item.sampleCount || 0);
        if (!summary[key].evidenceScopes.includes(item.evidenceScope)) summary[key].evidenceScopes.push(item.evidenceScope);
      }
    }
  }
  return { totalCases: cases.length, stages: summary };
}

export function buildAdminModelStatus({ configuredVisionModel, configuredOcrModel, runtimeObservation } = {}) {
  const observedModel = text(runtimeObservation?.model, 160);
  const observedAt = text(runtimeObservation?.observedAt, 80);
  const evidenceType = text(runtimeObservation?.evidenceType, 80);
  return {
    configured: {
      visionModel: text(configuredVisionModel, 160) || "UNKNOWN",
      ocrModel: text(configuredOcrModel, 160) || "UNKNOWN"
    },
    observedRuntime: observedModel && observedAt && evidenceType
      ? { model: observedModel, observedAt, evidenceType }
      : { model: "UNKNOWN", observedAt: null, evidenceType: "UNKNOWN" }
  };
}

export const coordinateCaseContract = Object.freeze({
  issueTypes: [...ISSUE_TYPES],
  caseStatuses: [...CASE_STATUSES],
  stageStatuses: [...STAGE_STATUSES],
  crsStatuses: [...CRS_STATUSES],
  geometryRepresentations: [...GEOMETRY_REPRESENTATIONS]
});
