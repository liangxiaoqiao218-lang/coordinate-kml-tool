import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

import {
  ACQUISITION_ADAPTER_STATUS,
  TABLE_CONTEXT_COMPOSITE_STATUS,
  acquirePrimaryImage,
  callPrimaryVisionProvider,
  createTableContextComposite,
  getPrimaryProviderReadiness,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";

const repoRoot = process.cwd();
const outputPath = path.join(repoRoot, "artifacts", "phase-10c-model-ab-matrix.txt");
const fixture = "artifacts/fixtures/indonesia-utm50s-real-003.jpg";
const challengerModel = "qwen-vl-ocr-latest";

const historicalBaseline = Object.freeze({
  model: "qwen-vl-plus",
  input: "table_context_composite",
  providerMs: 40030,
  totalMs: 40131,
  providerStatus: "TIMEOUT",
  responseReceived: false,
  candidateCount: 0,
  exactFailure: "PROVIDER_TIMEOUT",
  decision: "COMPOSITE_PROVIDER_TIMEOUT",
  rerun: false,
});

const groundTruth = Object.freeze([
  { point: "1", x: 778807.293, y: 9721476.737 },
  { point: "2", x: 778981.768, y: 9721477.288 },
  { point: "3", x: 778982.700, y: 9721182.351 },
  { point: "4", x: 778855.308, y: 9721181.948 },
  { point: "5", x: 778855.543, y: 9721107.284 },
  { point: "6", x: 778980.724, y: 9721107.010 },
  { point: "7", x: 778980.920, y: 9720910.990 },
  { point: "8", x: 779100.477, y: 9720911.109 },
  { point: "9", x: 779100.599, y: 9720788.271 },
  { point: "10", x: 778950.926, y: 9720787.948 },
  { point: "11", x: 778950.926, y: 9720833.787 },
  { point: "12", x: 778927.907, y: 9720833.787 },
  { point: "13", x: 778927.907, y: 9720922.219 },
  { point: "14", x: 778906.895, y: 9720922.219 },
  { point: "15", x: 778906.895, y: 9721078.633 },
  { point: "16", x: 778807.082, y: 9721078.633 },
]);

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
    technicalKmlReady: result.technicalKmlReady === true,
    matchedRecognizerIds: Array.isArray(result.candidates)
      ? result.candidates.map((item) => item.recognizerId).filter(Boolean)
      : [],
  };
}

function performanceClass(totalMs, hardFailure = false) {
  const value = Number(totalMs);
  if (hardFailure || !Number.isFinite(value)) return "FAIL";
  if (value <= 30000) return "TARGET_PASS";
  if (value <= 40000) return "ACCEPTABLE";
  if (value <= 60000) return "HARD_PASS_ONLY";
  return "FAIL";
}

function getRows(adapter = {}) {
  return Array.isArray(adapter.normalized?.coordinates)
    ? adapter.normalized.coordinates
    : [];
}

function validateGroundTruth(adapter = {}) {
  const rows = getRows(adapter);
  if (rows.length !== groundTruth.length) {
    return Object.freeze({
      passed: false,
      matchedRows: 0,
      expectedRows: groundTruth.length,
      actualRows: rows.length,
      mismatchedPoints: groundTruth.map((row) => row.point),
    });
  }
  const mismatches = [];
  const tolerance = 0.001;
  rows.forEach((row, index) => {
    const expected = groundTruth[index];
    const projected = row?.sourceProjected || {};
    const point = String(row?.label ?? expected.point);
    const x = Number(projected.x);
    const y = Number(projected.y);
    if (
      point !== expected.point
      || !Number.isFinite(x)
      || !Number.isFinite(y)
      || Math.abs(x - expected.x) > tolerance
      || Math.abs(y - expected.y) > tolerance
    ) {
      mismatches.push(expected.point);
    }
  });
  return Object.freeze({
    passed: mismatches.length === 0,
    matchedRows: groundTruth.length - mismatches.length,
    expectedRows: groundTruth.length,
    actualRows: rows.length,
    tolerance,
    mismatchedPoints: mismatches,
  });
}

function exactFailure({
  preprocessing = {},
  acquisition = {},
  adapter = {},
  ownerPass = false,
  rowPass = false,
  groundTruthPass = false,
} = {}) {
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
  if (!groundTruthPass) return "GROUND_TRUTH_MISMATCH";
  return "NONE";
}

function decisionFor({
  preprocessing = {},
  acquisition = {},
  ownerPass = false,
  rowPass = false,
  groundTruthPass = false,
  exact = "",
} = {}) {
  if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) return "TABLE_DETECTION_FAILED";
  if (exact === "PROVIDER_TIMEOUT") return "CHALLENGER_TIMEOUT";
  const diagnostics = acquisition.diagnostics || {};
  if (diagnostics.providerStatus === "SUCCESS" && (
    diagnostics.jsonParseStatus === "JSON_PARSE_FAILED"
    || diagnostics.schemaValidationStatus === "SCHEMA_INVALID"
    || diagnostics.candidateConstructionStatus === "CANDIDATE_CONSTRUCTION_EMPTY"
  )) {
    return "CHALLENGER_OUTPUT_CONTRACT_FAIL";
  }
  if (!ownerPass) return "CHALLENGER_OWNERSHIP_FAIL";
  if (!rowPass || !groundTruthPass) return "CHALLENGER_EXTRACTION_INCOMPLETE";
  if (Number(acquisition.timing?.totalDurationMs || 0) > 60000) return "CHALLENGER_TIMEOUT";
  return "CHALLENGER_VALIDATED";
}

function challengerEnv(modelId) {
  return {
    ...process.env,
    ALIYUN_VISION_MODEL: modelId,
    DASHSCOPE_VISION_MODEL: modelId,
  };
}

const out = lineWriter();
const readiness = getPrimaryProviderReadiness();

out.write({
  phase: "10C",
  experiment: "model_id_only_ab",
  providerCredential: readiness.available ? "AVAILABLE" : "UNAVAILABLE",
  baseline: historicalBaseline,
  challenger: challengerModel,
  frozenInput: "table_context_composite",
  prompt: "UNCHANGED",
  timeoutMs: 40000,
  maxNewProviderCalls: 1,
  retry: 0,
  targeted: 0,
  ocrFallback: 0,
  frozenRealPaths: {
    coteDIvoire: "FROZEN PASS",
    indonesia001: "FROZEN PASS",
    indonesia002: "FROZEN PASS",
  },
});

const absolute = path.join(repoRoot, fixture);
if (!existsSync(absolute)) {
  out.write({
    model: challengerModel,
    fixture,
    preprocessingStatus: "NOT_STARTED",
    providerStatus: "NOT_STARTED",
    providerCalls: 0,
    groundTruth: "FIXTURE_NOT_AVAILABLE",
    exactFailure: "FIXTURE_NOT_AVAILABLE",
    decision: "CHALLENGER_EXTRACTION_INCOMPLETE",
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
    model: challengerModel,
    fixture,
    preprocessingMode: preprocessing.preprocessingMode,
    preprocessingStatus: preprocessing.status,
    preprocessingReason: preprocessing.reason,
    preprocessingMs,
    providerCalls: 0,
    providerStatus: "NOT_STARTED",
    owner: "UNVERIFIED",
    rowsOrPoints: 0,
    groundTruth: "FAIL",
    exactFailure: preprocessing.status,
    performance: "FAIL",
    decision: "TABLE_DETECTION_FAILED",
  });
  out.save();
  process.exit(0);
}

if (!readiness.available) {
  out.write({
    model: challengerModel,
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
    responseReceived: false,
    contentPresent: false,
    candidateCount: 0,
    owner: "UNVERIFIED",
    rowsOrPoints: 0,
    groundTruth: "BLOCKED_BY_CREDENTIAL",
    technicalKmlReady: false,
    exactFailure: "PROVIDER_AUTH_ERROR",
    performance: "FAIL",
    decision: "READY_FOR_CREDENTIAL_AWARE_RUN",
  });
  out.save();
  process.exit(0);
}

const acquisition = await acquirePrimaryImage({
  imageBase64: preprocessing.imageBase64,
  mimeType: preprocessing.mimeType,
  provider: (args) => callPrimaryVisionProvider({
    ...args,
    env: challengerEnv(challengerModel),
  }),
});
const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
const diagnostics = acquisition.diagnostics || {};
const rows = getRows(adapter);
const ground = validateGroundTruth(adapter);
const ownerPass = adapter.recognizerId === "indonesia_utm";
const rowPass = rows.length === 16;
const technicalKmlReady = adapter.normalized?.technicalKmlReady === true;
const exact = exactFailure({
  preprocessing,
  acquisition,
  adapter,
  ownerPass,
  rowPass,
  groundTruthPass: ground.passed,
});
const totalMs = preprocessingMs + Number(acquisition.timing?.totalDurationMs || 0);
const decision = decisionFor({
  preprocessing,
  acquisition,
  ownerPass,
  rowPass,
  groundTruthPass: ground.passed,
  exact,
});
const hardFailure = decision !== "CHALLENGER_VALIDATED";

out.write({
  model: challengerModel,
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
  providerCalls: acquisition.providerCalls,
  providerStatus: diagnostics.providerStatus || "UNAVAILABLE",
  providerErrorCode: diagnostics.providerErrorCode || null,
  responseReceived: diagnostics.providerResponseReceived === true,
  contentPresent: diagnostics.providerContentPresent === true,
  jsonParse: diagnostics.jsonParseStatus || null,
  jsonParseReason: diagnostics.jsonParseReason || null,
  schemaValidation: diagnostics.schemaValidationStatus || null,
  schemaValidationReason: diagnostics.schemaValidationReason || null,
  candidateConstruction: diagnostics.candidateConstructionStatus || null,
  candidateCount: acquisition.candidates.length,
  candidateSummaries: acquisition.candidates.map(summarizeCandidate),
  candidateResults: Array.isArray(adapter.candidateResults) ? adapter.candidateResults.map(summarizeCandidateResult) : [],
  owner: adapter.recognizerId || adapter.status,
  rowsOrPoints: rows.length,
  groundTruth: ground.passed ? "PASS" : "FAIL",
  groundTruthSummary: ground,
  technicalKmlReady,
  exactFailure: exact,
  performance: performanceClass(totalMs, hardFailure),
  decision,
});

out.write("Coordinate Engine V3 Phase 10C Model A/B Matrix: COMPLETE");
out.save();
