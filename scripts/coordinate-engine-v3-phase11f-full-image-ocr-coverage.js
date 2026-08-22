import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

import {
  ACQUISITION_ADAPTER_STATUS,
  acquirePrimaryImage,
  callPrimaryVisionProvider,
  getPrimaryProviderReadiness,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";
import {
  classifyAcquisitionEvidence,
  classifyV3Coverage,
} from "./coordinate-engine-v3-phase11d-independent-path-matrix.js";

const repoRoot = process.cwd();
const outputPath = path.join(repoRoot, "artifacts", "phase-11f-full-image-ocr-coverage.txt");
const catalogPath = path.join(repoRoot, "regression-samples", "coordinate-engine-v3-structural-fixtures.json");
const model = "qwen-vl-ocr-latest";
const inputMode = "default_primary";
const timeoutMs = 40000;

const fixtureIds = Object.freeze([
  "STRUCT_REAL_009",
  "STRUCT_REAL_017",
  "STRUCT_REAL_013",
]);

const expectedOwners = Object.freeze({
  STRUCT_REAL_009: "kyrgyzstan_gauss_kruger",
  STRUCT_REAL_017: "mgrs",
  STRUCT_REAL_013: "handwritten_dms_historical_not_ported",
});

const v3RecognizerAvailability = Object.freeze({
  STRUCT_REAL_009: true,
  STRUCT_REAL_017: true,
  STRUCT_REAL_013: false,
});

function writer(filePath = outputPath) {
  const lines = [];
  return {
    write(value) {
      const text = typeof value === "string" ? value : JSON.stringify(value);
      lines.push(text);
      console.log(text);
    },
    save() {
      mkdirSync(path.dirname(filePath), { recursive: true });
      writeFileSync(filePath, `${lines.join("\n")}\n`);
    },
  };
}

function providerEnv(modelId) {
  return {
    ...process.env,
    ALIYUN_VISION_MODEL: modelId,
    DASHSCOPE_VISION_MODEL: modelId,
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
  return Object.freeze({
    candidateId: candidate.id || null,
    sourceType: candidate.sourceType || null,
    provenance: candidate.provenance || null,
    headers: Array.isArray(candidate.headers) ? candidate.headers : [],
    structuredRowsCount: Array.isArray(candidate.structuredRows) ? candidate.structuredRows.length : 0,
    documentCues: Array.isArray(candidate.documentCues) ? candidate.documentCues : [],
    textPreview: cleanPreview(candidate.text),
  });
}

function summarizeCandidateResult(result = {}) {
  return Object.freeze({
    candidateId: result.candidateId || null,
    runnerStatus: result.runnerStatus || null,
    recognizerId: result.recognizerId || null,
    technicalKmlReady: result.technicalKmlReady === true,
    matchedRecognizerIds: Array.isArray(result.candidates)
      ? result.candidates.map((candidate) => candidate.recognizerId).filter(Boolean)
      : [],
  });
}

function mimeTypeFor(filePath = "") {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".png") return "image/png";
  if (ext === ".webp") return "image/webp";
  return "image/jpeg";
}

function loadFixtures() {
  const catalog = JSON.parse(readFileSync(catalogPath, "utf8"));
  const fixtures = Array.isArray(catalog) ? catalog : catalog.fixtures;
  return fixtureIds.map((fixtureId) => {
    const fixture = fixtures.find((item) => item.fixtureId === fixtureId);
    if (!fixture) {
      return Object.freeze({
        fixtureId,
        missingCatalogEntry: true,
        expectedOwner: expectedOwners[fixtureId],
        expectedRows: 0,
        v3RecognizerAvailable: v3RecognizerAvailability[fixtureId] === true,
      });
    }
    return Object.freeze({
      fixtureId,
      originalFilename: fixture.originalFilename,
      sourceFile: fixture.sourceFile,
      expectedOwner: expectedOwners[fixtureId] || fixture.expectedOwner,
      expectedRows: Number(fixture.expectedRows || 0),
      v3RecognizerAvailable: v3RecognizerAvailability[fixtureId] === true,
      groundTruthLevel: fixture.groundTruthLevel,
      structuralCategory: fixture.structuralCategory,
    });
  });
}

function normalizedRows(adapter = {}) {
  return Array.isArray(adapter.normalized?.coordinates) ? adapter.normalized.coordinates.length : 0;
}

function exactFailureFromEvidence(acquisitionEvidence = "") {
  if (acquisitionEvidence === "ACQUISITION_COMPLETE"
    || acquisitionEvidence === "GROUPED_ACQUISITION_COMPLETE") {
    return "NONE";
  }
  return acquisitionEvidence || "UNKNOWN_FAILURE";
}

function performance(totalMs, acquisitionEvidence = "") {
  const value = Number(totalMs);
  const complete = acquisitionEvidence === "ACQUISITION_COMPLETE"
    || acquisitionEvidence === "GROUPED_ACQUISITION_COMPLETE";
  if (!complete || !Number.isFinite(value)) return "FAIL";
  if (value <= 30000) return "TARGET_PASS";
  if (value <= 40000) return "ACCEPTABLE";
  if (value <= 60000) return "HARD_PASS_ONLY";
  return "FAIL";
}

function decisionFor({ acquisitionEvidence = "", v3EndToEnd = "", expectedRows = 0, safeRecoveredRows = 0 } = {}) {
  if (acquisitionEvidence === "PROVIDER_TIMEOUT") return "FULL_IMAGE_OCR_PROVIDER_TIMEOUT";
  if (acquisitionEvidence === "OUTPUT_CONTRACT_FAILURE") return "FULL_IMAGE_OCR_OUTPUT_CONTRACT_FAIL";
  if (acquisitionEvidence === "PROVIDER_FAILURE") return "FULL_IMAGE_OCR_PROVIDER_FAILURE";
  if (safeRecoveredRows > 0 && Number(safeRecoveredRows) < Number(expectedRows)) return "FULL_IMAGE_OCR_EXTRACTION_INCOMPLETE";
  if (acquisitionEvidence === "ACQUISITION_INCOMPLETE_EMPTY") return "FULL_IMAGE_OCR_EXTRACTION_EMPTY";
  if (v3EndToEnd === "END_TO_END_PASS") return "FULL_IMAGE_OCR_END_TO_END_PASS";
  if (v3EndToEnd === "ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED") {
    return "FULL_IMAGE_OCR_ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED";
  }
  if (v3EndToEnd === "END_TO_END_FAIL") return "FULL_IMAGE_OCR_OWNERSHIP_OR_RESULT_FAIL";
  return "FULL_IMAGE_OCR_UNRESOLVED";
}

async function runFixture(fixture) {
  const absolute = path.join(repoRoot, fixture.sourceFile || "");
  if (fixture.missingCatalogEntry || !existsSync(absolute)) {
    return Object.freeze({
      fixtureId: fixture.fixtureId,
      originalFilename: fixture.originalFilename || null,
      model,
      inputMode,
      providerCalls: 0,
      providerStatus: "NOT_STARTED",
      responseReceived: false,
      candidateCount: 0,
      safeRecoveredRows: 0,
      acquisitionEvidence: "FIXTURE_NOT_AVAILABLE",
      runnerStatus: "NOT_STARTED",
      owner: null,
      technicalKmlReady: false,
      coverageGap: false,
      exactFailure: "FIXTURE_NOT_AVAILABLE",
      performance: "FAIL",
      decision: "FIXTURE_NOT_AVAILABLE",
    });
  }

  const imageBase64 = readFileSync(absolute).toString("base64");
  const started = Date.now();
  const acquisition = await acquirePrimaryImage({
    imageBase64,
    mimeType: mimeTypeFor(absolute),
    provider: (args) => callPrimaryVisionProvider({
      ...args,
      env: providerEnv(model),
    }),
  });
  const totalMs = Date.now() - started;
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  const diagnostics = acquisition.diagnostics || {};
  const evidence = classifyAcquisitionEvidence({
    acquisition,
    expectedRows: fixture.expectedRows,
  });
  const rowsOrPoints = Math.max(evidence.safeRecoveredRows, normalizedRows(adapter));
  const technicalKmlReady = adapter.normalized?.technicalKmlReady === true;
  const v3EndToEnd = classifyV3Coverage({
    acquisitionEvidence: evidence.acquisitionEvidence,
    v3RecognizerAvailable: fixture.v3RecognizerAvailable,
    owner: adapter.recognizerId,
    expectedOwner: fixture.expectedOwner,
    rowsOrPoints,
    expectedRows: fixture.expectedRows,
    technicalKmlReady,
  });
  const exactFailure = exactFailureFromEvidence(evidence.acquisitionEvidence);

  return Object.freeze({
    fixtureId: fixture.fixtureId,
    originalFilename: fixture.originalFilename,
    expectedOwner: fixture.expectedOwner,
    expectedRows: fixture.expectedRows,
    v3RecognizerAvailable: fixture.v3RecognizerAvailable,
    model,
    inputMode,
    providerMs: acquisition.timing?.primaryDurationMs,
    totalMs,
    providerCalls: acquisition.providerCalls,
    providerStatus: diagnostics.providerStatus || "UNAVAILABLE",
    providerErrorCode: diagnostics.providerErrorCode || null,
    responseReceived: diagnostics.providerResponseReceived === true,
    contentPresent: diagnostics.providerContentPresent === true,
    jsonParse: diagnostics.jsonParseStatus || null,
    schemaValidation: diagnostics.schemaValidationStatus || null,
    candidateConstruction: diagnostics.candidateConstructionStatus || null,
    candidateCount: acquisition.candidates.length,
    candidateSummaries: acquisition.candidates.map(summarizeCandidate),
    safeRecoveredRows: evidence.safeRecoveredRows,
    acquisitionEvidence: evidence.acquisitionEvidence,
    rowEvidence: evidence.rowEvidence,
    grouped: evidence.grouped === true,
    runnerStatus: adapter.status || null,
    owner: adapter.recognizerId || adapter.status || null,
    candidateResults: Array.isArray(adapter.candidateResults)
      ? adapter.candidateResults.map(summarizeCandidateResult)
      : [],
    rowsOrPoints,
    technicalKmlReady,
    coverageGap: v3EndToEnd === "ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED",
    v3EndToEnd,
    exactFailure,
    performance: performance(totalMs, evidence.acquisitionEvidence),
    decision: decisionFor({
      acquisitionEvidence: evidence.acquisitionEvidence,
      v3EndToEnd,
      expectedRows: fixture.expectedRows,
      safeRecoveredRows: evidence.safeRecoveredRows,
    }),
  });
}

async function main() {
  const out = writer();
  const fixtures = loadFixtures();
  const readiness = getPrimaryProviderReadiness(providerEnv(model));
  out.write({
    phase: "11F",
    experiment: "full_image_ocr_model_input_coverage",
    baseline: "d1e57c47f7170282b1163b41afeb0781e9f3b6b5",
    model,
    inputMode,
    imageRepresentation: "ORIGINAL_FULL_IMAGE",
    prompt: "UNCHANGED",
    timeoutMs,
    maxNewProviderCalls: 3,
    retry: 0,
    fallback: 0,
    targeted: 0,
    providerCredential: readiness.available ? "AVAILABLE" : "UNAVAILABLE",
    fixturePlan: fixtures.map((fixture) => Object.freeze({
      fixtureId: fixture.fixtureId,
      originalFilename: fixture.originalFilename || null,
      expectedOwner: fixture.expectedOwner,
      expectedRows: fixture.expectedRows,
      v3RecognizerAvailable: fixture.v3RecognizerAvailable,
    })),
  });

  if (!readiness.available) {
    for (const fixture of fixtures) {
      out.write({
        fixtureId: fixture.fixtureId,
        originalFilename: fixture.originalFilename || null,
        model,
        inputMode,
        providerCalls: 0,
        providerStatus: "NOT_STARTED",
        responseReceived: false,
        candidateCount: 0,
        safeRecoveredRows: 0,
        acquisitionEvidence: "READY_FOR_CREDENTIAL_AWARE_RUN",
        runnerStatus: "NOT_STARTED",
        owner: null,
        technicalKmlReady: false,
        coverageGap: false,
        exactFailure: "PROVIDER_CREDENTIAL_UNAVAILABLE",
        performance: "FAIL",
        decision: "READY_FOR_CREDENTIAL_AWARE_RUN",
      });
    }
    out.write({
      decision: "READY_FOR_CREDENTIAL_AWARE_RUN",
      providerCalls: 0,
      artifact: "artifacts/phase-11f-full-image-ocr-coverage.txt",
    });
    out.write("Coordinate Engine V3 Phase 11F Full-Image OCR Coverage: READY_FOR_CREDENTIAL_AWARE_RUN");
    out.save();
    return;
  }

  let providerCalls = 0;
  const results = [];
  for (const fixture of fixtures) {
    const result = await runFixture(fixture);
    providerCalls += Number(result.providerCalls || 0);
    results.push(result);
    out.write(result);
  }
  out.write({
    decision: "FULL_IMAGE_OCR_COVERAGE_EXPERIMENT_COMPLETE",
    providerCalls,
    passCount: results.filter((result) => result.exactFailure === "NONE").length,
    timeoutCount: results.filter((result) => result.exactFailure === "PROVIDER_TIMEOUT").length,
    unresolvedCount: results.filter((result) => result.exactFailure !== "NONE").length,
  });
  out.write("Coordinate Engine V3 Phase 11F Full-Image OCR Coverage: COMPLETE");
  out.save();
}

await main();
