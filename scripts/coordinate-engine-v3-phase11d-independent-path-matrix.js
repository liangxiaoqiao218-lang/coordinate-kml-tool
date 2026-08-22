import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

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
const outputPath = path.join(repoRoot, "artifacts", "phase-11d-independent-provider-path-matrix.txt");
const reclassifiedOutputPath = path.join(repoRoot, "artifacts", "phase-11d2-reclassified-evidence.txt");
const catalogPath = path.join(repoRoot, "regression-samples", "coordinate-engine-v3-structural-fixtures.json");

export const PHASE11D_EVIDENCE_STATE = Object.freeze({
  ACQUISITION_COMPLETE: "ACQUISITION_COMPLETE",
  GROUPED_ACQUISITION_COMPLETE: "GROUPED_ACQUISITION_COMPLETE",
  ACQUISITION_INCOMPLETE: "ACQUISITION_INCOMPLETE",
  ACQUISITION_INCOMPLETE_EMPTY: "ACQUISITION_INCOMPLETE_EMPTY",
  PROVIDER_TIMEOUT: "PROVIDER_TIMEOUT",
  PATH_NOT_APPLICABLE: "PATH_NOT_APPLICABLE",
  PROVIDER_FAILURE: "PROVIDER_FAILURE",
  OUTPUT_CONTRACT_FAILURE: "OUTPUT_CONTRACT_FAILURE",
});

export const PHASE11D_V3_RESULT = Object.freeze({
  END_TO_END_PASS: "END_TO_END_PASS",
  END_TO_END_FAIL: "END_TO_END_FAIL",
  ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED: "ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const PHASE11D_PATH_COMPARISON = Object.freeze({
  PATH_A_BETTER: "PATH_A_BETTER",
  PATH_B_BETTER: "PATH_B_BETTER",
  BOTH_VALID: "BOTH_VALID",
  NEITHER_VALID: "NEITHER_VALID",
  PATH_A_ONLY_APPLICABLE: "PATH_A_ONLY_APPLICABLE",
  PATH_B_ONLY_APPLICABLE: "PATH_B_ONLY_APPLICABLE",
  BOTH_NOT_APPLICABLE: "BOTH_NOT_APPLICABLE",
  UNRESOLVED: "UNRESOLVED",
});

export const PHASE11D_PATHS = Object.freeze({
  A: Object.freeze({
    path: "PATH_A",
    model: "qwen-vl-plus",
    inputMode: "default_primary",
  }),
  B: Object.freeze({
    path: "PATH_B",
    model: "qwen-vl-ocr-latest",
    inputMode: "table_context_composite",
  }),
});

const FIXTURE_IDS = Object.freeze([
  "STRUCT_REAL_007",
  "STRUCT_REAL_009",
  "STRUCT_REAL_011",
  "STRUCT_REAL_013",
  "STRUCT_REAL_017",
]);

const HISTORICAL_COVERAGE = Object.freeze({
  STRUCT_REAL_007: Object.freeze({ v3RecognizerAvailable: false }),
  STRUCT_REAL_009: Object.freeze({ v3RecognizerAvailable: true }),
  STRUCT_REAL_011: Object.freeze({ v3RecognizerAvailable: false }),
  STRUCT_REAL_013: Object.freeze({ v3RecognizerAvailable: false }),
  STRUCT_REAL_017: Object.freeze({ v3RecognizerAvailable: true }),
});

function writer(filePath = outputPath) {
  const lines = [];
  return Object.freeze({
    write(value) {
      const text = typeof value === "string" ? value : JSON.stringify(value);
      lines.push(text);
      console.log(text);
    },
    save() {
      mkdirSync(path.dirname(filePath), { recursive: true });
      writeFileSync(filePath, `${lines.join("\n")}\n`);
    },
  });
}

function providerEnv(model) {
  return {
    ...process.env,
    ALIYUN_VISION_MODEL: model,
    DASHSCOPE_VISION_MODEL: model,
  };
}

function cleanPreview(value = "", limit = 160) {
  return String(value ?? "")
    .replace(/data:image\/[^;]+;base64,[A-Za-z0-9+/=]+/g, "[redacted-image]")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .slice(0, limit);
}

function sanitizeCandidate(candidate = {}) {
  return Object.freeze({
    candidateId: candidate.id || candidate.candidateId || null,
    sourceType: candidate.sourceType || null,
    provenance: candidate.provenance || null,
    headers: Array.isArray(candidate.headers) ? candidate.headers : [],
    structuredRowsCount: Array.isArray(candidate.structuredRows)
      ? candidate.structuredRows.length
      : Number(candidate.structuredRowsCount || 0),
    documentCues: Array.isArray(candidate.documentCues) ? candidate.documentCues : [],
    textPreview: cleanPreview(candidate.text || candidate.textPreview),
  });
}

function candidateRowCount(candidate = {}) {
  if (Array.isArray(candidate.structuredRows)) return candidate.structuredRows.length;
  return Math.max(0, Number(candidate.structuredRowsCount || 0));
}

function candidateFacts(candidates = []) {
  return (Array.isArray(candidates) ? candidates : [])
    .map((candidate, index) => Object.freeze({
      candidateId: String(candidate.id || candidate.candidateId || `candidate_${index + 1}`),
      sourceType: String(candidate.sourceType || ""),
      provenance: String(candidate.provenance || ""),
      rows: candidateRowCount(candidate),
    }))
    .filter((candidate) => candidate.rows > 0);
}

export function calculateSafeRecoveredRows(candidates = [], expectedRows = 0) {
  const expected = Number(expectedRows || 0);
  const facts = candidateFacts(candidates);
  if (!facts.length) {
    return Object.freeze({
      safeRecoveredRows: 0,
      rowEvidence: "NO_ROW_CANDIDATES",
      grouped: false,
      candidateRows: [],
    });
  }

  const unique = [];
  const seenIds = new Set();
  for (const fact of facts) {
    if (seenIds.has(fact.candidateId)) continue;
    seenIds.add(fact.candidateId);
    unique.push(fact);
  }

  const wholeRows = Math.max(0, ...unique
    .filter((candidate) => candidate.sourceType === "whole_image")
    .map((candidate) => candidate.rows));
  const nonWhole = unique.filter((candidate) => candidate.sourceType !== "whole_image");
  const nonWholeSum = nonWhole.reduce((sum, candidate) => sum + candidate.rows, 0);
  const maxSingle = Math.max(0, ...unique.map((candidate) => candidate.rows));

  if (expected > 0 && wholeRows === expected) {
    return Object.freeze({
      safeRecoveredRows: expected,
      rowEvidence: "WHOLE_IMAGE_COMPLETE",
      grouped: false,
      candidateRows: unique,
    });
  }

  if (expected > 0 && maxSingle === expected) {
    return Object.freeze({
      safeRecoveredRows: expected,
      rowEvidence: "SINGLE_CANDIDATE_COMPLETE",
      grouped: false,
      candidateRows: unique,
    });
  }

  if (expected > 0 && nonWhole.length >= 2 && nonWholeSum === expected && maxSingle < expected) {
    return Object.freeze({
      safeRecoveredRows: expected,
      rowEvidence: "DISTINCT_BLOCK_GROUPED_COMPLETE",
      grouped: true,
      candidateRows: unique,
    });
  }

  const safeRecoveredRows = expected > 0 && nonWholeSum > 0 && nonWholeSum <= expected
    ? Math.max(maxSingle, nonWholeSum)
    : maxSingle;

  return Object.freeze({
    safeRecoveredRows,
    rowEvidence: nonWhole.length >= 2 ? "DISTINCT_BLOCK_PARTIAL" : "SINGLE_CANDIDATE_PARTIAL",
    grouped: false,
    candidateRows: unique,
  });
}

function getCandidateList(pathResult = {}, acquisition = {}) {
  if (Array.isArray(acquisition.candidates) && acquisition.candidates.length) return acquisition.candidates;
  if (Array.isArray(pathResult.candidateSummaries) && pathResult.candidateSummaries.length) return pathResult.candidateSummaries;
  return [];
}

export function classifyAcquisitionEvidence({
  pathResult = {},
  acquisition = {},
  preprocessing = null,
  expectedRows = 0,
} = {}) {
  const preprocessingStatus = pathResult.preprocessingStatus || preprocessing?.status || null;
  const providerCalls = Number(pathResult.providerCalls ?? acquisition.providerCalls ?? 0);
  const providerStatus = pathResult.providerStatus || acquisition.diagnostics?.providerStatus || null;
  const providerErrorCode = pathResult.providerErrorCode || acquisition.diagnostics?.providerErrorCode || null;
  const jsonParse = pathResult.jsonParse || acquisition.diagnostics?.jsonParseStatus || null;
  const schemaValidation = pathResult.schemaValidation || acquisition.diagnostics?.schemaValidationStatus || null;
  const candidateConstruction = pathResult.candidateConstruction || acquisition.diagnostics?.candidateConstructionStatus || null;
  const responseReceived = pathResult.responseReceived ?? acquisition.diagnostics?.providerResponseReceived;
  const contentPresent = pathResult.contentPresent ?? acquisition.diagnostics?.providerContentPresent;

  if (
    preprocessingStatus === TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION
    && providerCalls === 0
    && providerStatus === "NOT_STARTED"
  ) {
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE,
      safeRecoveredRows: 0,
      rowEvidence: "PATH_PREPROCESSING_NOT_APPLICABLE",
      grouped: false,
    });
  }

  if (providerErrorCode === "PROVIDER_TIMEOUT" || providerStatus === "TIMEOUT") {
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PROVIDER_TIMEOUT,
      safeRecoveredRows: 0,
      rowEvidence: "PROVIDER_TIMEOUT",
      grouped: false,
    });
  }

  if (providerStatus && !["SUCCESS", "NOT_STARTED"].includes(providerStatus)) {
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PROVIDER_FAILURE,
      safeRecoveredRows: 0,
      rowEvidence: providerStatus,
      grouped: false,
    });
  }

  if (
    responseReceived === false
    || contentPresent === false
    || jsonParse === "JSON_PARSE_FAILED"
    || schemaValidation === "SCHEMA_INVALID"
    || (providerStatus === "SUCCESS" && candidateConstruction === "CANDIDATE_CONSTRUCTION_EMPTY")
  ) {
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.OUTPUT_CONTRACT_FAILURE,
      safeRecoveredRows: 0,
      rowEvidence: "OUTPUT_CONTRACT_FAILURE",
      grouped: false,
    });
  }

  const recovered = calculateSafeRecoveredRows(getCandidateList(pathResult, acquisition), expectedRows);
  const fallbackRows = Math.max(0, Number(pathResult.rowsOrPoints || 0));
  if (recovered.safeRecoveredRows === 0 && fallbackRows > 0) {
    const fallback = Object.freeze({
      safeRecoveredRows: fallbackRows,
      rowEvidence: "SANITIZED_ARTIFACT_ROWS_OR_POINTS",
      grouped: false,
      candidateRows: recovered.candidateRows,
    });
    if (Number(expectedRows) > 0 && fallback.safeRecoveredRows === Number(expectedRows)) {
      return Object.freeze({
        acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE,
        ...fallback,
      });
    }
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE,
      ...fallback,
    });
  }
  if (Number(expectedRows) > 0 && recovered.safeRecoveredRows === Number(expectedRows)) {
    return Object.freeze({
      acquisitionEvidence: recovered.grouped
        ? PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE
        : PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE,
      ...recovered,
    });
  }

  if (recovered.safeRecoveredRows > 0) {
    return Object.freeze({
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE,
      ...recovered,
    });
  }

  return Object.freeze({
    acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE_EMPTY,
    ...recovered,
  });
}

function isAcquisitionComplete(evidenceState = "") {
  return evidenceState === PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE
    || evidenceState === PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE;
}

export function classifyV3Coverage({
  acquisitionEvidence = "",
  v3RecognizerAvailable = false,
  owner = null,
  expectedOwner = null,
  rowsOrPoints = 0,
  expectedRows = 0,
  technicalKmlReady = false,
} = {}) {
  if (!isAcquisitionComplete(acquisitionEvidence)) return PHASE11D_V3_RESULT.NOT_APPLICABLE;
  if (!v3RecognizerAvailable) return PHASE11D_V3_RESULT.ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED;
  if (
    owner === expectedOwner
    && Number(rowsOrPoints) === Number(expectedRows)
    && technicalKmlReady === true
  ) {
    return PHASE11D_V3_RESULT.END_TO_END_PASS;
  }
  return PHASE11D_V3_RESULT.END_TO_END_FAIL;
}

export function comparePathEvidence(pathA = {}, pathB = {}) {
  const aState = pathA.acquisitionEvidence || pathA.acquisitionStructure;
  const bState = pathB.acquisitionEvidence || pathB.acquisitionStructure;
  const aComplete = isAcquisitionComplete(aState);
  const bComplete = isAcquisitionComplete(bState);
  const aApplicable = aState !== PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE;
  const bApplicable = bState !== PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE;

  if (aComplete && bComplete) return PHASE11D_PATH_COMPARISON.BOTH_VALID;
  if (aComplete && !bApplicable) return PHASE11D_PATH_COMPARISON.PATH_A_ONLY_APPLICABLE;
  if (bComplete && !aApplicable) return PHASE11D_PATH_COMPARISON.PATH_B_ONLY_APPLICABLE;
  if (aComplete) return PHASE11D_PATH_COMPARISON.PATH_A_BETTER;
  if (bComplete) return PHASE11D_PATH_COMPARISON.PATH_B_BETTER;
  if (!aApplicable && !bApplicable) return PHASE11D_PATH_COMPARISON.BOTH_NOT_APPLICABLE;
  if (!aApplicable || !bApplicable) return PHASE11D_PATH_COMPARISON.UNRESOLVED;
  return PHASE11D_PATH_COMPARISON.NEITHER_VALID;
}

export function expectedPathDecision(comparison) {
  if (comparison === PHASE11D_PATH_COMPARISON.BOTH_VALID) return "EITHER_VALID";
  if (comparison === PHASE11D_PATH_COMPARISON.PATH_A_BETTER) return "PATH_A";
  if (comparison === PHASE11D_PATH_COMPARISON.PATH_B_BETTER) return "PATH_B";
  return "UNRESOLVED";
}

function normalizedRows(adapter = {}) {
  return Array.isArray(adapter.normalized?.coordinates) ? adapter.normalized.coordinates.length : 0;
}

function summarizeRunner(adapter = {}) {
  return Object.freeze({
    status: adapter.status || null,
    owner: adapter.recognizerId || null,
    technicalKmlReady: adapter.normalized?.technicalKmlReady === true,
    candidateResults: Array.isArray(adapter.candidateResults)
      ? adapter.candidateResults.map((result) => Object.freeze({
        candidateId: result.candidateId || null,
        runnerStatus: result.runnerStatus || null,
        recognizerId: result.recognizerId || null,
        technicalKmlReady: result.technicalKmlReady === true,
        matchedRecognizerIds: Array.isArray(result.candidates)
          ? result.candidates.map((candidate) => candidate.recognizerId).filter(Boolean)
          : [],
      }))
      : [],
  });
}

function exactFailureFromEvidence(acquisitionEvidence = "") {
  if (acquisitionEvidence === PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE
    || acquisitionEvidence === PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE) {
    return "NONE";
  }
  return acquisitionEvidence;
}

function performance(totalMs, acquisitionEvidence = "") {
  const value = Number(totalMs);
  if (!isAcquisitionComplete(acquisitionEvidence) || !Number.isFinite(value)) return "FAIL";
  if (value <= 30000) return "TARGET_PASS";
  if (value <= 40000) return "ACCEPTABLE";
  return "TIMEOUT";
}

async function acquirePath(fixture, strategy) {
  const fixturePath = path.join(repoRoot, fixture.sourceFile);
  if (!existsSync(fixturePath)) {
    return Object.freeze({
      path: strategy.path,
      model: strategy.model,
      inputMode: strategy.inputMode,
      providerCalls: 0,
      providerStatus: "NOT_STARTED",
      acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE_EMPTY,
      safeRecoveredRows: 0,
      v3EndToEnd: PHASE11D_V3_RESULT.NOT_APPLICABLE,
      exactFailure: "FIXTURE_NOT_AVAILABLE",
    });
  }

  const imageBase64 = readFileSync(fixturePath).toString("base64");
  let preprocessing = null;
  let providerImageBase64 = imageBase64;
  let mimeType = "image/jpeg";
  let providerCalls = 0;

  if (strategy.inputMode === "table_context_composite") {
    preprocessing = await createTableContextComposite({ imageBase64, mimeType });
    if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
      const evidence = classifyAcquisitionEvidence({
        pathResult: {
          preprocessingStatus: preprocessing.status,
          providerCalls: 0,
          providerStatus: "NOT_STARTED",
        },
        expectedRows: fixture.expectedRows,
      });
      return Object.freeze({
        path: strategy.path,
        model: strategy.model,
        inputMode: strategy.inputMode,
        preprocessingStatus: preprocessing.status,
        providerCalls: 0,
        providerStatus: "NOT_STARTED",
        responseReceived: false,
        contentPresent: false,
        candidateCount: 0,
        rowsOrPoints: evidence.safeRecoveredRows,
        acquisitionEvidence: evidence.acquisitionEvidence,
        safeRecoveredRows: evidence.safeRecoveredRows,
        rowEvidence: evidence.rowEvidence,
        v3EndToEnd: PHASE11D_V3_RESULT.NOT_APPLICABLE,
        coverageGap: false,
        exactFailure: evidence.acquisitionEvidence,
        performance: "FAIL",
      });
    }
    providerImageBase64 = preprocessing.imageBase64;
    mimeType = preprocessing.mimeType;
  }

  const acquisition = await acquirePrimaryImage({
    imageBase64: providerImageBase64,
    mimeType,
    provider: async (args) => {
      providerCalls += 1;
      return callPrimaryVisionProvider({
        ...args,
        env: providerEnv(strategy.model),
      });
    },
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  const diagnostics = acquisition.diagnostics || {};
  const evidence = classifyAcquisitionEvidence({
    acquisition,
    preprocessing,
    expectedRows: fixture.expectedRows,
  });
  const runnerRows = normalizedRows(adapter);
  const rowsOrPoints = Math.max(evidence.safeRecoveredRows, runnerRows);
  const v3EndToEnd = classifyV3Coverage({
    acquisitionEvidence: evidence.acquisitionEvidence,
    v3RecognizerAvailable: HISTORICAL_COVERAGE[fixture.fixtureId]?.v3RecognizerAvailable === true,
    owner: adapter.recognizerId || adapter.status || null,
    expectedOwner: fixture.expectedOwner,
    rowsOrPoints: runnerRows || rowsOrPoints,
    expectedRows: fixture.expectedRows,
    technicalKmlReady: adapter.normalized?.technicalKmlReady === true,
  });
  const totalMs = acquisition.timing?.totalDurationMs;

  return Object.freeze({
    path: strategy.path,
    model: strategy.model,
    inputMode: strategy.inputMode,
    preprocessingStatus: preprocessing?.status || null,
    providerMs: acquisition.timing?.primaryDurationMs,
    totalMs,
    providerCalls: acquisition.providerCalls ?? providerCalls,
    providerStatus: diagnostics.providerStatus || "UNAVAILABLE",
    providerErrorCode: diagnostics.providerErrorCode || null,
    responseReceived: diagnostics.providerResponseReceived === true,
    contentPresent: diagnostics.providerContentPresent === true,
    jsonParse: diagnostics.jsonParseStatus || null,
    schemaValidation: diagnostics.schemaValidationStatus || null,
    candidateConstruction: diagnostics.candidateConstructionStatus || null,
    candidateCount: Array.isArray(acquisition.candidates) ? acquisition.candidates.length : 0,
    candidateSummaries: Array.isArray(acquisition.candidates) ? acquisition.candidates.map(sanitizeCandidate) : [],
    rowsOrPoints,
    acquisitionEvidence: evidence.acquisitionEvidence,
    safeRecoveredRows: evidence.safeRecoveredRows,
    rowEvidence: evidence.rowEvidence,
    runnerResult: summarizeRunner(adapter),
    owner: adapter.recognizerId || adapter.status || "UNVERIFIED",
    technicalKmlReady: adapter.normalized?.technicalKmlReady === true,
    v3EndToEnd,
    coverageGap: v3EndToEnd === PHASE11D_V3_RESULT.ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED,
    exactFailure: exactFailureFromEvidence(evidence.acquisitionEvidence),
    performance: performance(totalMs, evidence.acquisitionEvidence),
  });
}

function loadFixtures() {
  const catalog = JSON.parse(readFileSync(catalogPath, "utf8"));
  return FIXTURE_IDS.map((fixtureId) => {
    const fixture = catalog.fixtures.find((item) => item.fixtureId === fixtureId);
    if (!fixture) throw new Error(`Missing fixture ${fixtureId}`);
    return fixture;
  });
}

function parseJsonArtifact(filePath) {
  return readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
}

export function reclassifyPathResult(pathResult = {}, fixture = {}) {
  const evidence = classifyAcquisitionEvidence({
    pathResult,
    expectedRows: fixture.expectedRows,
  });
  const runnerRows = Number(pathResult.rowsOrPoints || 0);
  const v3EndToEnd = classifyV3Coverage({
    acquisitionEvidence: evidence.acquisitionEvidence,
    v3RecognizerAvailable: HISTORICAL_COVERAGE[fixture.fixtureId]?.v3RecognizerAvailable === true,
    owner: pathResult.owner || null,
    expectedOwner: fixture.expectedOwner,
    rowsOrPoints: runnerRows || evidence.safeRecoveredRows,
    expectedRows: fixture.expectedRows,
    technicalKmlReady: pathResult.technicalKmlReady === true,
  });
  return Object.freeze({
    ...pathResult,
    acquisitionEvidence: evidence.acquisitionEvidence,
    safeRecoveredRows: evidence.safeRecoveredRows,
    rowEvidence: evidence.rowEvidence,
    grouped: evidence.grouped,
    v3EndToEnd,
    coverageGap: v3EndToEnd === PHASE11D_V3_RESULT.ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED,
    exactFailure: exactFailureFromEvidence(evidence.acquisitionEvidence),
    performance: performance(pathResult.totalMs, evidence.acquisitionEvidence),
  });
}

function summarizeResults(results = [], actualProviderCalls = 0) {
  return Object.freeze({
    independentFixturesTested: results.length,
    actualProviderCalls,
    acquisitionPassPathA: results.filter((result) => isAcquisitionComplete(result.pathA.acquisitionEvidence)).length,
    acquisitionPassPathB: results.filter((result) => isAcquisitionComplete(result.pathB.acquisitionEvidence)).length,
    bothValid: results.filter((result) => result.comparison === PHASE11D_PATH_COMPARISON.BOTH_VALID).length,
    pathABetter: results.filter((result) => result.comparison === PHASE11D_PATH_COMPARISON.PATH_A_BETTER).length,
    pathBBetter: results.filter((result) => result.comparison === PHASE11D_PATH_COMPARISON.PATH_B_BETTER).length,
    neitherValid: results.filter((result) => result.comparison === PHASE11D_PATH_COMPARISON.NEITHER_VALID).length,
    pathBNotApplicable: results.filter((result) => result.pathB.acquisitionEvidence === PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE).length,
    v3RecognizerCoverageGaps: results.filter((result) => !result.v3RecognizerAvailable).length,
    newExpectedPathsResolved: results.filter((result) => result.expectedPathDecision !== "UNRESOLVED").length,
    stillUnresolved: results.filter((result) => result.expectedPathDecision === "UNRESOLVED").length,
  });
}

export function reclassifyExistingArtifact({
  inputPath = outputPath,
  fixtures = loadFixtures(),
} = {}) {
  const fixtureMap = new Map(fixtures.map((fixture) => [fixture.fixtureId, fixture]));
  const records = parseJsonArtifact(inputPath).filter((record) => record.fixtureId);
  const results = records.map((record) => {
    const fixture = fixtureMap.get(record.fixtureId);
    if (!fixture) throw new Error(`No fixture metadata for ${record.fixtureId}`);
    const pathA = reclassifyPathResult(record.pathA || {}, fixture);
    const pathB = reclassifyPathResult(record.pathB || {}, fixture);
    const comparison = comparePathEvidence(pathA, pathB);
    return Object.freeze({
      fixtureId: record.fixtureId,
      originalFilename: record.originalFilename,
      expectedHistoricalOwner: record.expectedHistoricalOwner,
      expectedRows: record.expectedRows,
      v3RecognizerAvailable: record.v3RecognizerAvailable === true,
      pathA,
      pathB,
      comparison,
      expectedPathDecision: expectedPathDecision(comparison),
    });
  });
  const actualProviderCalls = results.reduce((sum, result) => (
    sum + Number(result.pathA.providerCalls || 0) + Number(result.pathB.providerCalls || 0)
  ), 0);
  return Object.freeze({
    results,
    summary: summarizeResults(results, actualProviderCalls),
  });
}

async function runProviderMatrix() {
  const out = writer();
  const readiness = getPrimaryProviderReadiness();
  const fixtures = loadFixtures();

  out.write({
    phase: "11D",
    experiment: "independent_provider_path_evidence_matrix",
    baseline: "51176f8af14d875b8941e1729519dcf3628aa88f",
    providerCredential: readiness.available ? "AVAILABLE" : "UNAVAILABLE",
    paths: [PHASE11D_PATHS.A, PHASE11D_PATHS.B],
    timeoutMs: 40000,
    maxProviderCallsPerPath: 1,
    retry: 0,
    fallback: 0,
    fixturePlan: fixtures.map((fixture) => Object.freeze({
      fixtureId: fixture.fixtureId,
      originalFilename: fixture.originalFilename,
      expectedHistoricalOwner: fixture.expectedOwner,
      expectedRows: fixture.expectedRows,
      v3RecognizerAvailable: HISTORICAL_COVERAGE[fixture.fixtureId]?.v3RecognizerAvailable === true,
    })),
  });

  if (!readiness.available) {
    for (const fixture of fixtures) {
      out.write({
        fixtureId: fixture.fixtureId,
        originalFilename: fixture.originalFilename,
        expectedHistoricalOwner: fixture.expectedOwner,
        expectedRows: fixture.expectedRows,
        pathA: { providerCalls: 0, providerStatus: "NOT_STARTED", decision: "READY_FOR_CREDENTIAL_AWARE_RUN" },
        pathB: { providerCalls: 0, providerStatus: "NOT_STARTED", decision: "READY_FOR_CREDENTIAL_AWARE_RUN" },
        comparison: "UNRESOLVED",
        expectedPathDecision: "UNRESOLVED",
      });
    }
    out.write({
      decision: "READY_FOR_CREDENTIAL_AWARE_RUN",
      realMatrix: "NOT_RUN",
      actualProviderCalls: 0,
    });
    out.save();
    return;
  }

  const results = [];
  let actualProviderCalls = 0;
  for (const fixture of fixtures) {
    const pathA = await acquirePath(fixture, PHASE11D_PATHS.A);
    const pathB = await acquirePath(fixture, PHASE11D_PATHS.B);
    actualProviderCalls += Number(pathA.providerCalls || 0) + Number(pathB.providerCalls || 0);
    const comparison = comparePathEvidence(pathA, pathB);
    const result = Object.freeze({
      fixtureId: fixture.fixtureId,
      originalFilename: fixture.originalFilename,
      expectedHistoricalOwner: fixture.expectedOwner,
      expectedRows: fixture.expectedRows,
      v3RecognizerAvailable: HISTORICAL_COVERAGE[fixture.fixtureId]?.v3RecognizerAvailable === true,
      pathA,
      pathB,
      comparison,
      expectedPathDecision: expectedPathDecision(comparison),
    });
    results.push(result);
    out.write(result);
  }

  out.write({
    decision: "INDEPENDENT_PATH_EVIDENCE_READY",
    ...summarizeResults(results, actualProviderCalls),
  });
  out.write("Coordinate Engine V3 Phase 11D Independent Provider Path Matrix: COMPLETE");
  out.save();
}

function runReclassification() {
  const { results, summary } = reclassifyExistingArtifact();
  const out = writer(reclassifiedOutputPath);
  out.write({
    phase: "11D.2",
    experiment: "provider_evidence_classifier_reclassification",
    sourceArtifact: "artifacts/phase-11d-independent-provider-path-matrix.txt",
    providerRerun: false,
    providerCalls: 0,
  });
  for (const result of results) out.write(result);
  out.write({
    decision: "CLASSIFIER_REPAIR_RECLASSIFICATION_COMPLETE",
    ...summary,
    totalValidatedExpectedPathsAfterPhase11D2: 4 + summary.newExpectedPathsResolved,
    independentValidationFixtures: summary.newExpectedPathsResolved,
    generalizationGate: "FAIL",
  });
  out.write("Coordinate Engine V3 Phase 11D.2 Evidence Reclassification: COMPLETE");
  out.save();
}

async function main() {
  if (process.argv.includes("--reclassify-existing")) {
    runReclassification();
    return;
  }
  await runProviderMatrix();
}

const thisFile = fileURLToPath(import.meta.url);
if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(thisFile)) {
  await main();
}
