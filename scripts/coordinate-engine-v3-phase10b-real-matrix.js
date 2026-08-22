import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

import {
  ACQUISITION_ADAPTER_STATUS,
  TABLE_CONTEXT_COMPOSITE_STATUS,
  acquirePrimaryImage,
  createTableContextComposite,
  getPrimaryProviderReadiness,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";

const repoRoot = process.cwd();
const outputPath = path.join(repoRoot, "artifacts", "phase-10b-table-context-composite-matrix.txt");
const fixture = "artifacts/fixtures/indonesia-utm50s-real-003.jpg";

function lineWriter() {
  const lines = [];
  return {
    write(value) {
      const text = typeof value === "string" ? value : JSON.stringify(value);
      lines.push(text);
      console.log(text);
    },
    save() {
      mkdirSync(path.dirname(outputPath), { recursive: true });
      writeFileSync(outputPath, `${lines.join("\n")}\n`);
    },
  };
}

function cleanPreview(value = "", limit = 180) {
  return String(value ?? "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/data:image\/[^;]+;base64,[A-Za-z0-9+/=]+/g, "[redacted-image]")
    .slice(0, limit);
}

function summarizeCandidate(candidate = {}) {
  return {
    candidateId: candidate.id || null,
    sourceType: candidate.sourceType || null,
    provenance: candidate.provenance || null,
    headers: Array.isArray(candidate.headers) ? candidate.headers : [],
    structuredRowsCount: Array.isArray(candidate.structuredRows) ? candidate.structuredRows.length : 0,
    documentCues: Array.isArray(candidate.documentCues) ? candidate.documentCues : [],
    textPreview: cleanPreview(candidate.text),
  };
}

function summarizeCandidateResult(result = {}) {
  return {
    candidateId: result.candidateId || null,
    runnerStatus: result.runnerStatus || null,
    recognizerId: result.recognizerId || null,
    matchedRecognizerIds: Array.isArray(result.candidates)
      ? result.candidates.map((item) => item.recognizerId).filter(Boolean)
      : [],
  };
}

function rowCount(adapterResult = {}) {
  return adapterResult.normalized?.coordinates?.length ?? 0;
}

function performanceClass(totalMs) {
  const value = Number(totalMs);
  if (!Number.isFinite(value)) return "UNVERIFIED";
  if (value <= 30000) return "TARGET_PASS";
  if (value <= 40000) return "ACCEPTABLE_SINGLE_CALL";
  if (value <= 60000) return "HARD_PASS_ONLY";
  return "HARD_FAIL";
}

function exactFailure({ preprocessing = {}, acquisition = {}, adapter = {}, ownerPass = false, rowPass = false } = {}) {
  if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
    return preprocessing.status || "PREPROCESSING_NOT_CREATED";
  }
  const diagnostics = acquisition.diagnostics || {};
  if (diagnostics.providerErrorCode) return diagnostics.providerErrorCode;
  if (diagnostics.jsonParseStatus === "JSON_PARSE_FAILED") return diagnostics.jsonParseReason || "JSON_PARSE_FAILURE";
  if (diagnostics.schemaValidationStatus === "SCHEMA_INVALID") return diagnostics.schemaValidationReason || "SCHEMA_VALIDATION_FAILURE";
  if (diagnostics.candidateConstructionStatus === "CANDIDATE_CONSTRUCTION_EMPTY") return diagnostics.candidateConstructionReason || "CANDIDATE_CONSTRUCTION_EMPTY";
  if (adapter.status && adapter.status !== ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT) return adapter.status;
  if (!ownerPass) return "WRONG_OWNER";
  if (!rowPass) return "ROW_COUNT_MISMATCH";
  return "NONE";
}

function decisionFor({ preprocessing = {}, acquisition = {}, ownerPass = false, rowPass = false, exact = "" } = {}) {
  if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) return "TABLE_DETECTION_FAILED";
  if (exact === "PROVIDER_TIMEOUT") return "COMPOSITE_PROVIDER_TIMEOUT";
  if (!ownerPass) return "OWNERSHIP_REGRESSION";
  if (!rowPass) return "COMPOSITE_EXTRACTION_INCOMPLETE";
  if (acquisition.status !== "SUCCESS") return "COMPOSITE_EXTRACTION_INCOMPLETE";
  return "TABLE_CONTEXT_COMPOSITE_VALIDATED";
}

const out = lineWriter();
const readiness = getPrimaryProviderReadiness();

out.write({
  phase: "10B",
  experiment: "table_context_composite_experiment",
  providerCredential: readiness.available ? "AVAILABLE" : "UNAVAILABLE",
  providerModel: readiness.model,
  fixture,
  baselineFrozen: {
    coteDIvoire: "4/4 PASS",
    indonesia001: "4/4 PASS",
    indonesia002: "6/6 PASS",
  },
});

const absolute = path.join(repoRoot, fixture);
if (!existsSync(absolute)) {
  out.write({
    type: "Indonesia #003",
    fixture,
    preprocessingStatus: "NOT_STARTED",
    providerStatus: "NOT_STARTED",
    providerCalls: 0,
    groundTruth: "FIXTURE_NOT_AVAILABLE",
    exactFailure: "FIXTURE_NOT_AVAILABLE",
    decision: "TABLE_DETECTION_FAILED",
  });
  out.save();
  process.exit(0);
}

const imageBase64 = readFileSync(absolute).toString("base64");
const preprocessingStart = Date.now();
const preprocessing = await createTableContextComposite({ imageBase64, mimeType: "image/jpeg" });
const preprocessingMs = Date.now() - preprocessingStart;

if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
  out.write({
    type: "Indonesia #003",
    fixture,
    preprocessingMode: preprocessing.preprocessingMode,
    preprocessingStatus: preprocessing.status,
    preprocessingReason: preprocessing.reason,
    preprocessingMs,
    providerCalls: 0,
    providerStatus: "NOT_STARTED",
    providerErrorCode: null,
    candidateCount: 0,
    owner: "UNVERIFIED",
    rowsOrPoints: 0,
    groundTruth: "FAIL",
    exactFailure: preprocessing.status,
    decision: "TABLE_DETECTION_FAILED",
  });
  out.save();
  process.exit(0);
}

if (!readiness.available) {
  out.write({
    type: "Indonesia #003",
    fixture,
    preprocessingMode: preprocessing.preprocessingMode,
    preprocessingStatus: preprocessing.status,
    preprocessingReason: preprocessing.reason,
    preprocessingMs,
    originalDimensions: preprocessing.originalDimensions,
    detectedTableRegion: preprocessing.detectedTableRegion,
    tableRegionPercentage: preprocessing.tableRegionPercentage,
    contextRegions: preprocessing.contextRegions?.map(({ role, x, y, width, height }) => ({ role, x, y, width, height })) || [],
    compositeDimensions: preprocessing.compositeDimensions,
    providerCalls: 0,
    providerStatus: "NOT_STARTED",
    providerErrorCode: "PROVIDER_AUTH_ERROR",
    candidateCount: 0,
    owner: "UNVERIFIED",
    rowsOrPoints: 0,
    groundTruth: "BLOCKED_BY_CREDENTIAL",
    exactFailure: "PROVIDER_AUTH_ERROR",
    decision: "CREDENTIAL_NOT_VISIBLE_TO_CODEX_PROCESS",
  });
  out.save();
  process.exit(0);
}

const acquisition = await acquirePrimaryImage({
  imageBase64: preprocessing.imageBase64,
  mimeType: preprocessing.mimeType,
});
const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
const diagnostics = acquisition.diagnostics || {};
const rows = rowCount(adapter);
const ownerPass = adapter.recognizerId === "indonesia_utm";
const rowPass = rows === 16;
const exact = exactFailure({ preprocessing, acquisition, adapter, ownerPass, rowPass });
const totalMs = preprocessingMs + Number(acquisition.timing?.totalDurationMs || 0);

out.write({
  type: "Indonesia #003",
  fixture,
  preprocessingMode: preprocessing.preprocessingMode,
  preprocessingStatus: preprocessing.status,
  preprocessingReason: preprocessing.reason,
  preprocessingMs,
  originalDimensions: preprocessing.originalDimensions,
  detectedTableRegion: preprocessing.detectedTableRegion,
  tableRegionPercentage: preprocessing.tableRegionPercentage,
  contextRegions: preprocessing.contextRegions?.map(({ role, x, y, width, height }) => ({ role, x, y, width, height })) || [],
  compositeDimensions: preprocessing.compositeDimensions,
  providerMs: acquisition.timing?.primaryDurationMs,
  totalMs,
  calls: acquisition.providerCalls,
  providerStatus: diagnostics.providerStatus || "UNAVAILABLE",
  providerErrorCode: diagnostics.providerErrorCode || null,
  responseReceived: diagnostics.providerResponseReceived === true,
  contentPresent: diagnostics.providerContentPresent === true,
  jsonParse: diagnostics.jsonParseStatus || null,
  schemaValidation: diagnostics.schemaValidationStatus || null,
  candidateConstruction: diagnostics.candidateConstructionStatus || null,
  candidateCount: acquisition.candidates.length,
  candidateSummaries: acquisition.candidates.map(summarizeCandidate),
  candidateResults: Array.isArray(adapter.candidateResults) ? adapter.candidateResults.map(summarizeCandidateResult) : [],
  owner: adapter.recognizerId || adapter.status,
  rowsOrPoints: rows,
  groundTruth: ownerPass && rowPass ? "PASS" : "FAIL",
  performance: performanceClass(totalMs),
  exactFailure: exact,
  decision: decisionFor({ preprocessing, acquisition, ownerPass, rowPass, exact }),
});

out.write("Coordinate Engine V3 Phase 10B Table Context Composite Matrix: COMPLETE");
out.save();
