const ISSUE_TYPES = new Set(["recognition_failed", "coordinate_correction", "kml_failed"]);
const CASE_STATUSES = new Set(["NEW", "TRIAGED", "IN_PROGRESS", "BLOCKED", "VERIFIED", "CLOSED"]);
const STAGE_STATUSES = new Set(["UNKNOWN", "PASS", "FAIL", "BLOCKED", "NOT_APPLICABLE"]);
const CRS_STATUSES = new Set(["UNKNOWN", "WORKING_ASSUMPTION", "CONFIRMED"]);
const GEOMETRY_REPRESENTATIONS = new Set(["UNKNOWN", "POLYGON", "MULTIPOINT", "LINESTRING"]);
const ARTIFACT_STATUSES = new Set(["NOT_SAVED", "AUTHORIZED_REFERENCE", "UNKNOWN"]);
const IDENTITY_STATUSES = new Set(["REQUEST_ONLY", "CURRENT", "UNKNOWN"]);
const FORBIDDEN_KEYS = /(?:image|provider|cookie|authorization|auth_header|secret|password|api_key|coordinates?|raw_response|environment|env_config)/iu;
const RESULT_HASH_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const COMMIT_PATTERN = /^[0-9a-f]{40}$/u;

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
    if (FORBIDDEN_KEYS.test(key)) {
      throw Object.assign(new Error(`COORDINATE_CASE_FORBIDDEN_FIELD:${path}.${key}`), { code: "COORDINATE_CASE_FORBIDDEN_FIELD" });
    }
    assertNoForbiddenMaterial(nested, `${path}.${key}`);
  }
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
  const issueSummary = text(raw.issueSummary ?? raw.issue_summary, 1200);
  if (!issueSummary) {
    throw Object.assign(new Error("COORDINATE_CASE_ISSUE_SUMMARY_REQUIRED"), { code: "COORDINATE_CASE_ISSUE_SUMMARY_REQUIRED" });
  }
  const sampleCount = Number(raw.sampleCount ?? raw.sample_count ?? 0);
  const commitSha = text(raw.commitSha ?? raw.commit_sha, 40).toLowerCase();
  if (commitSha && !COMMIT_PATTERN.test(commitSha)) {
    throw Object.assign(new Error("COORDINATE_CASE_COMMIT_INVALID"), { code: "COORDINATE_CASE_COMMIT_INVALID" });
  }
  return {
    recognition_request_id: recognitionRequestId,
    job_id: text(raw.jobId ?? raw.job_id, 160) || null,
    ...identity,
    issue_type: issueType,
    issue_summary: issueSummary,
    resolution_summary: text(raw.resolutionSummary ?? raw.resolution_summary, 1200) || null,
    owner: text(raw.owner, 120) || null,
    progress_status: enumValue(raw.progressStatus ?? raw.progress_status, CASE_STATUSES, "NEW"),
    blocker: text(raw.blocker, 800) || null,
    delivery_status: enumValue(raw.deliveryStatus ?? raw.delivery_status, STAGE_STATUSES, "UNKNOWN"),
    problem_resolution_status: enumValue(raw.problemResolutionStatus ?? raw.problem_resolution_status, STAGE_STATUSES, "UNKNOWN"),
    peer_validation_status: enumValue(raw.peerValidationStatus ?? raw.peer_validation_status, STAGE_STATUSES, "UNKNOWN"),
    production_status: enumValue(raw.productionStatus ?? raw.production_status, STAGE_STATUSES, "UNKNOWN"),
    evidence_scope: text(raw.evidenceScope ?? raw.evidence_scope, 800) || null,
    sample_count: Number.isInteger(sampleCount) && sampleCount >= 0 ? sampleCount : 0,
    crs_evidence_status: enumValue(raw.crsEvidenceStatus ?? raw.crs_evidence_status, CRS_STATUSES, "UNKNOWN"),
    geometry_representation: enumValue(raw.geometryRepresentation ?? raw.geometry_representation, GEOMETRY_REPRESENTATIONS, "UNKNOWN"),
    original_artifact_status: enumValue(raw.originalArtifactStatus ?? raw.original_artifact_status, ARTIFACT_STATUSES, "NOT_SAVED"),
    golden_ref: text(raw.goldenRef ?? raw.golden_ref, 240) || null,
    commit_sha: commitSha || null,
    receipt_ref: text(raw.receiptRef ?? raw.receipt_ref, 500) || null
  };
}

export function normalizeCoordinateCaseEvidenceInput(raw = {}) {
  assertNoForbiddenMaterial(raw);
  const referenceType = enumValue(raw.referenceType ?? raw.reference_type, new Set(["GOLDEN", "COMMIT", "RECEIPT", "REQUEST", "RESULT"]), "");
  const referenceValue = text(raw.referenceValue ?? raw.reference_value, 500);
  const evidenceType = text(raw.evidenceType ?? raw.evidence_type, 120).toUpperCase();
  const sampleCount = Number(raw.sampleCount ?? raw.sample_count ?? 0);
  if (!referenceType || !referenceValue || !evidenceType) {
    throw Object.assign(new Error("COORDINATE_CASE_EVIDENCE_INVALID"), { code: "COORDINATE_CASE_EVIDENCE_INVALID" });
  }
  return {
    evidence_type: evidenceType,
    status: enumValue(raw.status, STAGE_STATUSES, "UNKNOWN"),
    evidence_scope: text(raw.evidenceScope ?? raw.evidence_scope, 800) || null,
    sample_count: Number.isInteger(sampleCount) && sampleCount >= 0 ? sampleCount : 0,
    reference_type: referenceType,
    reference_value: referenceValue
  };
}

export function publicCoordinateCase(row = {}, evidence = []) {
  return {
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
}

export function buildCoverageSummary(cases = []) {
  const stageKeys = ["delivery", "problemResolution", "peerValidation", "production"];
  const summary = Object.fromEntries(stageKeys.map(key => [key, { PASS: 0, FAIL: 0, BLOCKED: 0, UNKNOWN: 0, NOT_APPLICABLE: 0, sampleCount: 0, evidenceScopes: [] }]));
  for (const item of cases) {
    for (const key of stageKeys) {
      const status = STAGE_STATUSES.has(item?.stages?.[key]) ? item.stages[key] : "UNKNOWN";
      const evidenceBackedPass = status !== "PASS" || (Number(item.sampleCount || 0) > 0 && Boolean(text(item.evidenceScope, 800)));
      const effectiveStatus = evidenceBackedPass ? status : "UNKNOWN";
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
