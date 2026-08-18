import { V3_RUNNER_STATUS } from "../runner.js";

export function createAcquisitionAdapterMetrics({
  acquisitionResult = {},
  candidateCount = 0,
  dedupedCandidateCount = 0,
  runnerResults = [],
  conflictCount = 0,
} = {}) {
  const results = Array.isArray(runnerResults) ? runnerResults : [];
  return Object.freeze({
    totalDurationMs: Number(acquisitionResult.timing?.totalDurationMs) || 0,
    providerCalls: Number(acquisitionResult.providerCalls) || 0,
    candidateCount: Number(candidateCount) || 0,
    dedupedCandidateCount: Number(dedupedCandidateCount) || 0,
    runnerMatchedCount: results.filter((result) => result.runnerStatus === V3_RUNNER_STATUS.MATCHED).length,
    runnerNoMatchCount: results.filter((result) => result.runnerStatus === V3_RUNNER_STATUS.NO_MATCH).length,
    runnerAmbiguousCount: results.filter((result) => result.runnerStatus === V3_RUNNER_STATUS.AMBIGUOUS).length,
    conflictCount: Number(conflictCount) || 0,
  });
}
