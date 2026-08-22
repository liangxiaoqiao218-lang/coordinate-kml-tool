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
const model = "qwen-vl-ocr-latest";
const outputPath = path.join(repoRoot, "artifacts", "phase-10e-ocr-compatibility-matrix.txt");
const phase10cArtifactPath = path.join(repoRoot, "artifacts", "phase-10c-model-ab-matrix.txt");

const fixtures = Object.freeze([
  {
    id: "cote_divoire",
    label: "Côte d’Ivoire",
    fixture: "artifacts/fixtures/cote-divoire-dms-real-001.jpeg",
    inputMode: "default_primary",
    expectedOwner: "cote_divoire_dms",
    expectedRows: 4,
    groundTruth: [
      { label: "1", latitude: 11.869980556, longitude: -8.892405556 },
      { label: "2", latitude: 11.871447222, longitude: -8.89255 },
      { label: "3", latitude: 11.870158333, longitude: -8.898341667 },
      { label: "4", latitude: 11.868791667, longitude: -8.898211111 },
    ],
  },
  {
    id: "indonesia_001",
    label: "Indonesia #001",
    fixture: "artifacts/fixtures/indonesia-utm50s-real-001.jpg",
    inputMode: "default_primary",
    expectedOwner: "indonesia_utm",
    expectedRows: 4,
    groundTruth: [
      { label: "1", x: 779271.176, y: 9720912.526 },
      { label: "2", x: 779554.165, y: 9720912.526 },
      { label: "3", x: 779554.165, y: 9720734.464 },
      { label: "4", x: 779271.176, y: 9720734.464 },
    ],
  },
  {
    id: "indonesia_002",
    label: "Indonesia #002",
    fixture: "artifacts/fixtures/indonesia-utm50s-real-002.jpg",
    inputMode: "default_primary",
    expectedOwner: "indonesia_utm",
    expectedRows: 6,
    groundTruth: [
      { label: "1", x: 778984.492, y: 9721476.737 },
      { label: "2", x: 779099.680, y: 9721476.848 },
      { label: "3", x: 779099.680, y: 9721110.798 },
      { label: "4", x: 778875.519, y: 9721110.798 },
      { label: "5", x: 778875.519, y: 9721180.576 },
      { label: "6", x: 778984.492, y: 9721180.576 },
    ],
  },
  {
    id: "indonesia_003",
    label: "Indonesia #003",
    fixture: "artifacts/fixtures/indonesia-utm50s-real-003.jpg",
    inputMode: "table_context_composite",
    expectedOwner: "indonesia_utm",
    expectedRows: 16,
    groundTruth: [
      { label: "1", x: 778807.293, y: 9721476.737 },
      { label: "2", x: 778981.768, y: 9721477.288 },
      { label: "3", x: 778982.700, y: 9721182.351 },
      { label: "4", x: 778855.308, y: 9721181.948 },
      { label: "5", x: 778855.543, y: 9721107.284 },
      { label: "6", x: 778980.724, y: 9721107.010 },
      { label: "7", x: 778980.920, y: 9720910.990 },
      { label: "8", x: 779100.477, y: 9720911.109 },
      { label: "9", x: 779100.599, y: 9720788.271 },
      { label: "10", x: 778950.926, y: 9720787.948 },
      { label: "11", x: 778950.926, y: 9720833.787 },
      { label: "12", x: 778927.907, y: 9720833.787 },
      { label: "13", x: 778927.907, y: 9720922.219 },
      { label: "14", x: 778906.895, y: 9720922.219 },
      { label: "15", x: 778906.895, y: 9721078.633 },
      { label: "16", x: 778807.082, y: 9721078.633 },
    ],
  },
]);

const extendedFixtureAudit = Object.freeze({
  wgs84DecimalImage: "ABSENT",
  wgs84TableImage: "ABSENT",
  mgrsImage: "ABSENT",
  genericDmsImage: "ABSENT",
  kyrgyzGkImage: "ABSENT",
  madagascarCadastralImage: "ABSENT",
  handwrittenDmsImage: "ABSENT",
  mixedLayoutImages: "ABSENT",
});

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

function challengerEnv() {
  return {
    ...process.env,
    ALIYUN_VISION_MODEL: model,
    DASHSCOPE_VISION_MODEL: model,
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

function performanceClass({ totalMs, passed }) {
  const value = Number(totalMs);
  if (!passed || !Number.isFinite(value)) return "FAIL";
  if (value <= 30000) return "TARGET_PASS";
  if (value <= 40000) return "ACCEPTABLE";
  if (value <= 60000) return "HARD_PASS_ONLY";
  return "FAIL";
}

function rows(adapter = {}) {
  return Array.isArray(adapter.normalized?.coordinates)
    ? adapter.normalized.coordinates
    : [];
}

function isIndonesiaFixture(item = {}) {
  return String(item.expectedOwner) === "indonesia_utm";
}

function validateGroundTruth(item = {}, adapter = {}) {
  const actual = rows(adapter);
  const expected = item.groundTruth || [];
  const tolerance = isIndonesiaFixture(item) ? 0.001 : 0.000001;
  const mismatchedPoints = [];
  if (actual.length !== expected.length) {
    return {
      passed: false,
      expectedRows: expected.length,
      actualRows: actual.length,
      matchedRows: 0,
      tolerance,
      mismatchedPoints: expected.map((row) => row.label),
    };
  }
  actual.forEach((point, index) => {
    const target = expected[index];
    const label = String(point.label || target.label);
    let pass = label === target.label;
    if (isIndonesiaFixture(item)) {
      const projected = point.sourceProjected || {};
      pass = pass
        && Math.abs(Number(projected.x) - target.x) <= tolerance
        && Math.abs(Number(projected.y) - target.y) <= tolerance;
    } else {
      pass = pass
        && Math.abs(Number(point.latitude) - target.latitude) <= tolerance
        && Math.abs(Number(point.longitude) - target.longitude) <= tolerance;
    }
    if (!pass) mismatchedPoints.push(target.label);
  });
  return {
    passed: mismatchedPoints.length === 0,
    expectedRows: expected.length,
    actualRows: actual.length,
    matchedRows: expected.length - mismatchedPoints.length,
    tolerance,
    mismatchedPoints,
  };
}

function exactFailure({ acquisition = {}, adapter = {}, ownerPass = false, rowPass = false, groundTruthPass = false } = {}) {
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

function fixtureDecision({ exact = "", ownerPass = false, rowPass = false, groundTruthPass = false } = {}) {
  if (exact === "PROVIDER_TIMEOUT") return "FAIL";
  return ownerPass && rowPass && groundTruthPass ? "PASS" : "FAIL";
}

function genericDmsMatched(adapter = {}) {
  return Array.isArray(adapter.candidateResults)
    && adapter.candidateResults.some((result) => (
      result.recognizerId === "generic_dms"
      || (Array.isArray(result.candidates) && result.candidates.some((candidate) => candidate.recognizerId === "generic_dms"))
    ));
}

function standardAmbiguity(adapter = {}) {
  return adapter.status === ACQUISITION_ADAPTER_STATUS.AMBIGUOUS_RECOGNIZER_MATCH ? 1 : 0;
}

function readJsonLines(file) {
  if (!existsSync(file)) return [];
  return readFileSync(file, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function reusablePhase10c003() {
  const lines = readJsonLines(phase10cArtifactPath);
  return lines.find((item) => (
    item.model === model
    && item.fixture === fixtures.find((fixtureItem) => fixtureItem.id === "indonesia_003")?.fixture
    && item.decision === "CHALLENGER_VALIDATED"
    && item.groundTruth === "PASS"
    && item.owner === "indonesia_utm"
    && Number(item.rowsOrPoints) === 16
    && item.technicalKmlReady === true
    && Number(item.providerCalls) === 1
  )) || null;
}

async function acquireFixture(item) {
  const absolute = path.join(repoRoot, item.fixture);
  const imageBase64 = readFileSync(absolute).toString("base64");
  if (item.inputMode === "table_context_composite") {
    const preprocessing = await createTableContextComposite({ imageBase64, mimeType: "image/jpeg" });
    if (preprocessing.status !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
      return {
        preprocessing,
        acquisition: {
          status: "FAILED",
          timing: { primaryDurationMs: 0, totalDurationMs: 0 },
          providerCalls: 0,
          candidates: [],
          diagnostics: {
            providerStatus: "NOT_STARTED",
            candidateConstructionStatus: "CANDIDATE_CONSTRUCTION_EMPTY",
            candidateConstructionReason: preprocessing.status,
          },
        },
        adapter: {},
      };
    }
    const acquisition = await acquirePrimaryImage({
      imageBase64: preprocessing.imageBase64,
      mimeType: preprocessing.mimeType,
      provider: (args) => callPrimaryVisionProvider({
        ...args,
        env: challengerEnv(),
      }),
    });
    const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
    return { preprocessing, acquisition, adapter };
  }
  const acquisition = await acquirePrimaryImage({
    imageBase64,
    mimeType: "image/jpeg",
    provider: (args) => callPrimaryVisionProvider({
      ...args,
      env: challengerEnv(),
    }),
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  return { preprocessing: null, acquisition, adapter };
}

function formatResult(item, acquired, reused = false) {
  if (reused) {
    return {
      fixture: item.label,
      fixturePath: item.fixture,
      model,
      inputMode: item.inputMode,
      reusedFrom: "phase-10c-model-ab-matrix",
      providerMs: acquired.providerMs,
      totalMs: acquired.totalMs,
      providerCalls: acquired.providerCalls,
      providerStatus: acquired.providerStatus,
      responseReceived: acquired.responseReceived,
      contentPresent: acquired.contentPresent,
      jsonParse: acquired.jsonParse,
      schemaValidation: acquired.schemaValidation,
      candidateCount: acquired.candidateCount,
      owner: acquired.owner,
      rowsOrPoints: acquired.rowsOrPoints,
      groundTruth: acquired.groundTruth,
      groundTruthSummary: acquired.groundTruthSummary,
      technicalKmlReady: acquired.technicalKmlReady,
      exactFailure: acquired.exactFailure,
      performance: acquired.performance,
      genericDmsMatched: false,
      standardAmbiguity: 0,
      decision: "PASS",
    };
  }
  const { acquisition, adapter } = acquired;
  const diagnostics = acquisition.diagnostics || {};
  const actualRows = rows(adapter);
  const ground = validateGroundTruth(item, adapter);
  const ownerPass = adapter.recognizerId === item.expectedOwner;
  const rowPass = actualRows.length === item.expectedRows;
  const groundTruthPass = ground.passed;
  const failure = exactFailure({ acquisition, adapter, ownerPass, rowPass, groundTruthPass });
  const pass = ownerPass && rowPass && groundTruthPass && adapter.normalized?.technicalKmlReady === true && acquisition.providerCalls === 1;
  return {
    fixture: item.label,
    fixturePath: item.fixture,
    model,
    inputMode: item.inputMode,
    providerMs: acquisition.timing?.primaryDurationMs,
    totalMs: acquisition.timing?.totalDurationMs,
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
    owner: adapter.recognizerId || adapter.status || "UNVERIFIED",
    rowsOrPoints: actualRows.length,
    groundTruth: groundTruthPass ? "PASS" : "FAIL",
    groundTruthSummary: ground,
    technicalKmlReady: adapter.normalized?.technicalKmlReady === true,
    exactFailure: failure,
    performance: performanceClass({ totalMs: acquisition.timing?.totalDurationMs, passed: pass }),
    genericDmsMatched: genericDmsMatched(adapter),
    standardAmbiguity: standardAmbiguity(adapter),
    decision: fixtureDecision({ exact: failure, ownerPass, rowPass, groundTruthPass }),
  };
}

const out = lineWriter();
const readiness = getPrimaryProviderReadiness();
const reusable003 = reusablePhase10c003();

out.write({
  phase: "10E",
  experiment: "qwen_vl_ocr_latest_real_image_compatibility",
  model,
  providerCredential: readiness.available ? "AVAILABLE" : "UNAVAILABLE",
  prompt: "UNCHANGED",
  timeoutMs: 40000,
  retry: 0,
  targeted: 0,
  ocrFallback: 0,
  defaultModelChanged: false,
  routerAdded: false,
  fixturePlan: fixtures.map((item) => ({
    fixture: item.label,
    inputMode: item.inputMode,
    expectedOwner: item.expectedOwner,
    expectedRows: item.expectedRows,
    callPlanned: item.id === "indonesia_003" && reusable003 ? false : true,
  })),
  extendedFixtureAudit,
});

if (!readiness.available) {
  for (const item of fixtures) {
    const reused = item.id === "indonesia_003" && reusable003;
    out.write(reused
      ? formatResult(item, reusable003, true)
      : {
        fixture: item.label,
        fixturePath: item.fixture,
        model,
        inputMode: item.inputMode,
        providerCalls: 0,
        providerStatus: "NOT_STARTED",
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
  }
  out.write({
    decision: "READY_FOR_CREDENTIAL_AWARE_RUN",
    realMatrix: "NOT_RUN",
  });
  out.save();
  process.exit(0);
}

const results = [];
for (const item of fixtures) {
  if (item.id === "indonesia_003" && reusable003) {
    const result = formatResult(item, reusable003, true);
    results.push(result);
    out.write(result);
    continue;
  }
  if (!existsSync(path.join(repoRoot, item.fixture))) {
    const result = {
      fixture: item.label,
      fixturePath: item.fixture,
      model,
      inputMode: item.inputMode,
      providerCalls: 0,
      providerStatus: "NOT_STARTED",
      responseReceived: false,
      contentPresent: false,
      candidateCount: 0,
      owner: "UNVERIFIED",
      rowsOrPoints: 0,
      groundTruth: "FIXTURE_NOT_AVAILABLE",
      technicalKmlReady: false,
      exactFailure: "FIXTURE_NOT_AVAILABLE",
      performance: "FAIL",
      decision: "FAIL",
    };
    results.push(result);
    out.write(result);
    continue;
  }
  const acquired = await acquireFixture(item);
  const result = formatResult(item, acquired);
  results.push(result);
  out.write(result);
}

const passCount = results.filter((result) => result.decision === "PASS").length;
out.write({
  decision: passCount === fixtures.length
    ? "OCR_COMPATIBILITY_CORE_MATRIX_PASS"
    : "OCR_COMPATIBILITY_CORE_MATRIX_FAIL",
  passCount,
  total: fixtures.length,
  modelStatus: passCount === fixtures.length
    ? "OCR_UNIVERSAL_CANDIDATE_STRENGTHENED"
    : "OCR_UNIVERSAL_CANDIDATE_NOT_PROVEN",
});
out.write("Coordinate Engine V3 Phase 10E OCR Compatibility Matrix: COMPLETE");
out.save();
