import { createLatencyBudget } from "../latency-budget.js";
import { runCoordinateEngineV3, V3_RUNNER_STATUS } from "../runner.js";
import {
  acquisitionCandidateToRunnerInput,
  calculateCandidateCompleteness,
  createAcquisitionResult,
  dedupeAcquisitionCandidates,
} from "./contracts.js";
import { createAcquisitionBudget } from "./budget.js";
import { createAcquisitionAdapterMetrics } from "./metrics.js";

export const ACQUISITION_ADAPTER_STATUS = Object.freeze({
  MATCHED_RESULT: "MATCHED_RESULT",
  NO_RECOGNIZER_MATCH: "NO_RECOGNIZER_MATCH",
  AMBIGUOUS_RECOGNIZER_MATCH: "AMBIGUOUS_RECOGNIZER_MATCH",
  MULTIPLE_CANDIDATE_CONFLICT: "MULTIPLE_CANDIDATE_CONFLICT",
});

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function summarizeRunnerResult(candidate, result) {
  return Object.freeze({
    candidateId: candidate.id,
    runnerStatus: result.status,
    recognizerId: result.recognizerId || null,
    coordinateType: result.coordinateType || null,
    technicalKmlReady: result.technicalKmlReady === true,
    warningCount: Array.isArray(result.warnings) ? result.warnings.length : 0,
    suspectedPointCount: Array.isArray(result.suspectedPoints) ? result.suspectedPoints.length : 0,
    candidates: Object.freeze(Array.isArray(result.candidates) ? result.candidates.map((item) => Object.freeze({
      recognizerId: item.recognizerId || null,
      coordinateType: item.coordinateType || null,
      portStatus: item.portStatus || null,
    })) : []),
  });
}

function summarizeConflictResult(item) {
  return Object.freeze({
    candidateIds: Object.freeze(item.candidateIds),
    recognizerId: item.recognizerId,
    coordinateType: item.coordinateType,
    coordinateCount: item.normalized?.coordinates?.length ?? 0,
    geometryType: item.normalized?.geometryType || null,
    crs: item.normalized?.crs || null,
    precisionMode: item.normalized?.precisionMode || null,
    technicalKmlReady: item.normalized?.technicalKmlReady === true,
  });
}

function groupMatchedResults(matched = []) {
  const grouped = new Map();
  for (const item of matched) {
    const key = stableJson({
      recognizerId: item.result.recognizerId,
      coordinateType: item.result.coordinateType,
      normalized: item.result.normalized,
    });
    if (!grouped.has(key)) {
      grouped.set(key, {
        candidateIds: [],
        recognizerId: item.result.recognizerId,
        coordinateType: item.result.coordinateType,
        normalized: item.result.normalized,
        verification: item.result.verification,
        warnings: item.result.warnings || [],
        suspectedPoints: item.result.suspectedPoints || [],
      });
    }
    grouped.get(key).candidateIds.push(item.candidate.id);
  }
  return Array.from(grouped.values()).map((item) => Object.freeze({
    ...item,
    candidateIds: Object.freeze(item.candidateIds),
  }));
}

function makeAdapterResult(status, extras = {}) {
  return Object.freeze({
    schemaVersion: "coordinate_engine_v3_acquisition_adapter_v1",
    status,
    normalized: null,
    verification: null,
    candidateResults: Object.freeze([]),
    conflicts: Object.freeze([]),
    metrics: createAcquisitionAdapterMetrics(),
    ...extras,
  });
}

export async function runAcquisitionCandidatesThroughRunner(acquisitionInput = {}, {
  runner = runCoordinateEngineV3,
  registry,
  latencyBudget = createLatencyBudget(),
} = {}) {
  const acquisitionResult = acquisitionInput?.schemaVersion === "coordinate_engine_v3_acquisition_v1"
    ? acquisitionInput
    : createAcquisitionResult(acquisitionInput);
  const dedupe = dedupeAcquisitionCandidates(acquisitionResult.candidates);
  const candidateResults = [];
  const matched = [];

  for (const candidate of dedupe.candidates) {
    const runnerInput = acquisitionCandidateToRunnerInput(candidate);
    const result = await runner(runnerInput, { registry, latencyBudget });
    candidateResults.push(summarizeRunnerResult(candidate, result));
    if (result.status === V3_RUNNER_STATUS.MATCHED) {
      matched.push({ candidate, result });
    }
  }

  const ambiguous = candidateResults.filter((result) => result.runnerStatus === V3_RUNNER_STATUS.AMBIGUOUS);
  if (ambiguous.length > 0 && matched.length === 0) {
    return makeAdapterResult(ACQUISITION_ADAPTER_STATUS.AMBIGUOUS_RECOGNIZER_MATCH, {
      reason: "candidate_runner_returned_ambiguous",
      candidateResults: Object.freeze(candidateResults),
      metrics: createAcquisitionAdapterMetrics({
        acquisitionResult,
        candidateCount: acquisitionResult.candidates.length,
        dedupedCandidateCount: dedupe.candidates.length,
        runnerResults: candidateResults,
      }),
    });
  }

  if (matched.length === 0) {
    return makeAdapterResult(ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH, {
      reason: "no_candidate_matched_runner",
      candidateResults: Object.freeze(candidateResults),
      metrics: createAcquisitionAdapterMetrics({
        acquisitionResult,
        candidateCount: acquisitionResult.candidates.length,
        dedupedCandidateCount: dedupe.candidates.length,
        runnerResults: candidateResults,
      }),
    });
  }

  const grouped = groupMatchedResults(matched);
  if (grouped.length > 1) {
    return makeAdapterResult(ACQUISITION_ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT, {
      reason: "multiple_distinct_candidate_results",
      candidateResults: Object.freeze(candidateResults),
      conflicts: Object.freeze(grouped.map(summarizeConflictResult)),
      metrics: createAcquisitionAdapterMetrics({
        acquisitionResult,
        candidateCount: acquisitionResult.candidates.length,
        dedupedCandidateCount: dedupe.candidates.length,
        runnerResults: candidateResults,
        conflictCount: grouped.length,
      }),
    });
  }

  const [winner] = grouped;
  return makeAdapterResult(ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT, {
    reason: "single_logical_candidate_result",
    normalized: winner.normalized,
    verification: winner.verification,
    recognizerId: winner.recognizerId,
    coordinateType: winner.coordinateType,
    mergedCandidateIds: winner.candidateIds,
    candidateResults: Object.freeze(candidateResults),
    metrics: createAcquisitionAdapterMetrics({
      acquisitionResult,
      candidateCount: acquisitionResult.candidates.length,
      dedupedCandidateCount: dedupe.candidates.length,
      runnerResults: candidateResults,
    }),
  });
}

export function shouldRequestTargetedAcquisition({
  acquisitionResult = {},
  adapterResult = {},
  candidateCompleteness,
  budget = createAcquisitionBudget(),
  minimumMs = 1000,
} = {}) {
  if (adapterResult.status !== ACQUISITION_ADAPTER_STATUS.NO_RECOGNIZER_MATCH) {
    return Object.freeze({
      targeted: false,
      reason: "usable_or_conflicted_runner_result_present",
    });
  }

  const completenessItems = Array.isArray(candidateCompleteness)
    ? candidateCompleteness
    : (Array.isArray(acquisitionResult.candidates)
      ? acquisitionResult.candidates.map(calculateCandidateCompleteness)
      : []);
  const incomplete = completenessItems.find((item) => item.incompleteStructuredCandidate === true);
  if (!incomplete) {
    return Object.freeze({
      targeted: false,
      reason: "no_specific_incomplete_structured_candidate",
    });
  }

  const providerCalls = Number(acquisitionResult.providerCalls ?? budget.providerCalls) || 0;
  if (providerCalls >= 2 || budget.providerCalls >= budget.maxProviderCalls) {
    return Object.freeze({
      targeted: false,
      reason: "PROVIDER_CALL_LIMIT_EXCEEDED",
    });
  }

  const gate = budget.canStartProviderCall({ minimumMs });
  if (!gate.allowed) {
    return Object.freeze({
      targeted: false,
      reason: gate.reason,
    });
  }

  return Object.freeze({
    targeted: true,
    reason: "incomplete_structured_candidate_with_budget",
    candidateId: incomplete.candidateId,
  });
}
