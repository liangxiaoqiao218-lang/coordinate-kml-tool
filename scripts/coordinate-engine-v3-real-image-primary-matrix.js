import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

import {
  ACQUISITION_ADAPTER_STATUS,
  acquirePrimaryImage,
  getPrimaryProviderReadiness,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";

const repoRoot = process.cwd();

const matrix = [
  { type: "WGS84 Decimal", fixture: null, expectedOwner: "wgs84_decimal", expectedRows: null },
  { type: "WGS84 Table", fixture: null, expectedOwner: "wgs84_table", expectedRows: null },
  { type: "MGRS", fixture: null, expectedOwner: "mgrs", expectedRows: null },
  { type: "Generic DMS", fixture: null, expectedOwner: "generic_dms", expectedRows: null },
  { type: "Kyrgyz GK", fixture: null, expectedOwner: "kyrgyzstan_gauss_kruger", expectedRows: null },
  { type: "Madagascar", fixture: null, expectedOwner: "madagascar_cadastral", expectedRows: 32 },
  { type: "Côte d’Ivoire", fixture: "artifacts/fixtures/cote-divoire-dms-real-001.jpeg", expectedOwner: "cote_divoire_dms", expectedRows: 4 },
  { type: "Indonesia #001", fixture: "artifacts/fixtures/indonesia-utm50s-real-001.jpg", expectedOwner: "indonesia_utm", expectedRows: 4 },
  { type: "Indonesia #002", fixture: "artifacts/fixtures/indonesia-utm50s-real-002.jpg", expectedOwner: "indonesia_utm", expectedRows: 6 },
  { type: "Indonesia #003", fixture: "artifacts/fixtures/indonesia-utm50s-real-003.jpg", expectedOwner: "indonesia_utm", expectedRows: 16 },
];

function toData(file) {
  return readFileSync(file).toString("base64");
}

function safeRowCount(adapterResult) {
  return adapterResult.normalized?.coordinates?.length ?? 0;
}

function cleanPreview(value = "", limit = 300) {
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

function exactFailure({ acquisition = {}, adapter = {}, ownerPass = false, rowPass = false } = {}) {
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

const readiness = getPrimaryProviderReadiness();
console.log(`Provider Credential: ${readiness.available ? "AVAILABLE" : "UNAVAILABLE"}`);
console.log(`Provider Model: ${readiness.model}`);

if (!readiness.available) {
  for (const item of matrix) {
    const available = item.fixture ? existsSync(path.join(repoRoot, item.fixture)) : false;
    console.log(JSON.stringify({
      type: item.type,
      fixture: item.fixture || "FIXTURE_NOT_AVAILABLE",
      fixtureAvailable: available,
      providerMs: "BLOCKED_BY_CREDENTIAL",
      totalMs: "BLOCKED_BY_CREDENTIAL",
      calls: 0,
      providerStatus: "ERROR",
      providerErrorCode: "PROVIDER_AUTH_ERROR",
      providerHttpStatus: null,
      responseReceived: false,
      contentPresent: false,
      jsonParse: null,
      jsonParseReason: null,
      schemaValidation: null,
      schemaValidationReason: null,
      candidateConstruction: "CANDIDATE_CONSTRUCTION_EMPTY",
      candidateConstructionReason: "PROVIDER_AUTH_ERROR",
      candidates: 0,
      candidateSourceTypes: [],
      candidateSummaries: [],
      candidateResults: [],
      owner: "UNVERIFIED",
      rowsOrPoints: "UNVERIFIED",
      groundTruth: "BLOCKED_BY_CREDENTIAL",
      time: "BLOCKED_BY_CREDENTIAL",
      failureLayer: "PROVIDER_CREDENTIAL",
      exactFailure: "PROVIDER_AUTH_ERROR",
    }));
  }
  console.log("Coordinate Engine V3 Real Image Primary Matrix: BLOCKED_BY_CREDENTIAL");
  process.exit(0);
}

for (const item of matrix) {
  if (!item.fixture) {
    console.log(JSON.stringify({
      type: item.type,
      fixture: "FIXTURE_NOT_AVAILABLE",
      providerMs: "N/A",
      totalMs: "N/A",
      calls: 0,
      providerStatus: "NOT_STARTED",
      providerErrorCode: null,
      providerHttpStatus: null,
      responseReceived: false,
      contentPresent: false,
      jsonParse: null,
      jsonParseReason: null,
      schemaValidation: null,
      schemaValidationReason: null,
      candidateConstruction: "CANDIDATE_CONSTRUCTION_EMPTY",
      candidateConstructionReason: "FIXTURE_NOT_AVAILABLE",
      candidates: 0,
      candidateSourceTypes: [],
      candidateSummaries: [],
      candidateResults: [],
      owner: "UNVERIFIED",
      rowsOrPoints: "UNVERIFIED",
      groundTruth: "FIXTURE_NOT_AVAILABLE",
      time: "N/A",
      failureLayer: "FIXTURE",
      exactFailure: "FIXTURE_NOT_AVAILABLE",
    }));
    continue;
  }
  const absolute = path.join(repoRoot, item.fixture);
  if (!existsSync(absolute)) {
    console.log(JSON.stringify({
      type: item.type,
      fixture: item.fixture,
      providerMs: "N/A",
      totalMs: "N/A",
      calls: 0,
      providerStatus: "NOT_STARTED",
      providerErrorCode: null,
      providerHttpStatus: null,
      responseReceived: false,
      contentPresent: false,
      jsonParse: null,
      jsonParseReason: null,
      schemaValidation: null,
      schemaValidationReason: null,
      candidateConstruction: "CANDIDATE_CONSTRUCTION_EMPTY",
      candidateConstructionReason: "FIXTURE_NOT_AVAILABLE",
      candidates: 0,
      candidateSourceTypes: [],
      candidateSummaries: [],
      candidateResults: [],
      owner: "UNVERIFIED",
      rowsOrPoints: "UNVERIFIED",
      groundTruth: "FIXTURE_NOT_AVAILABLE",
      time: "N/A",
      failureLayer: "FIXTURE",
      exactFailure: "FIXTURE_NOT_AVAILABLE",
    }));
    continue;
  }

  const acquisition = await acquirePrimaryImage({
    imageBase64: toData(absolute),
    mimeType: item.fixture.endsWith(".png") ? "image/png" : "image/jpeg",
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  const diagnostics = acquisition.diagnostics || {};
  const rows = safeRowCount(adapter);
  const ownerPass = adapter.recognizerId === item.expectedOwner;
  const rowPass = item.expectedRows == null || rows === item.expectedRows;
  const hardPass = Number(acquisition.timing.totalDurationMs) <= 60000;
  console.log(JSON.stringify({
    type: item.type,
    fixture: item.fixture,
    providerMs: acquisition.timing.primaryDurationMs,
    totalMs: acquisition.timing.totalDurationMs,
    calls: acquisition.providerCalls,
    providerStatus: diagnostics.providerStatus || "UNAVAILABLE",
    providerErrorCode: diagnostics.providerErrorCode || null,
    providerHttpStatus: diagnostics.providerHttpStatus ?? null,
    responseReceived: diagnostics.providerResponseReceived === true,
    contentPresent: diagnostics.providerContentPresent === true,
    jsonParse: diagnostics.jsonParseStatus || null,
    jsonParseReason: diagnostics.jsonParseReason || null,
    schemaValidation: diagnostics.schemaValidationStatus || null,
    schemaValidationReason: diagnostics.schemaValidationReason || null,
    candidateConstruction: diagnostics.candidateConstructionStatus || null,
    candidateConstructionReason: diagnostics.candidateConstructionReason || null,
    candidates: acquisition.candidates.length,
    candidateSourceTypes: acquisition.candidates.map((candidate) => candidate.sourceType),
    candidateSummaries: acquisition.candidates.map(summarizeCandidate),
    candidateResults: Array.isArray(adapter.candidateResults) ? adapter.candidateResults.map(summarizeCandidateResult) : [],
    owner: adapter.recognizerId || adapter.status,
    rowsOrPoints: rows,
    groundTruth: ownerPass && rowPass ? "PASS" : "FAIL",
    time: hardPass ? (Number(acquisition.timing.totalDurationMs) <= 30000 ? "TARGET_PASS" : "HARD_PASS_ONLY") : "HARD_FAIL",
    failureLayer: adapter.status === ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT && ownerPass && rowPass
      ? "NONE"
      : (acquisition.status === "SUCCESS" ? "ADAPTER_OR_DETERMINISTIC" : "ACQUISITION"),
    exactFailure: exactFailure({ acquisition, adapter, ownerPass, rowPass }),
  }));
}

console.log("Coordinate Engine V3 Real Image Primary Matrix: COMPLETE");
