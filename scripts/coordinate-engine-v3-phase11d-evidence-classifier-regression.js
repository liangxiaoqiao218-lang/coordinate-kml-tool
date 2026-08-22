import assert from "node:assert/strict";

import {
  PHASE11D_EVIDENCE_STATE,
  PHASE11D_PATH_COMPARISON,
  PHASE11D_V3_RESULT,
  calculateSafeRecoveredRows,
  classifyAcquisitionEvidence,
  classifyV3Coverage,
  comparePathEvidence,
  expectedPathDecision,
} from "./coordinate-engine-v3-phase11d-independent-path-matrix.js";

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

function candidate(id, sourceType, rows) {
  return { candidateId: id, sourceType, structuredRowsCount: rows };
}

function successPath(candidates, expectedRows) {
  return classifyAcquisitionEvidence({
    pathResult: {
      providerStatus: "SUCCESS",
      providerCalls: 1,
      responseReceived: true,
      contentPresent: true,
      jsonParse: "JSON_PARSE_SUCCESS",
      schemaValidation: "SCHEMA_VALID",
      candidateConstruction: "CANDIDATE_CONSTRUCTION_SUCCESS",
      candidateSummaries: candidates,
    },
    expectedRows,
  });
}

test("single exact -> ACQUISITION_COMPLETE", () => {
  const result = successPath([candidate("table", "table", 8)], 8);
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE);
  assert.equal(result.safeRecoveredRows, 8);
});

test("single partial -> ACQUISITION_INCOMPLETE", () => {
  const result = successPath([candidate("table", "table", 7)], 8);
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE);
  assert.equal(result.safeRecoveredRows, 7);
});

test("zero rows -> ACQUISITION_INCOMPLETE_EMPTY", () => {
  const result = successPath([], 8);
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE_EMPTY);
  assert.equal(result.safeRecoveredRows, 0);
});

test("provider timeout -> PROVIDER_TIMEOUT", () => {
  const result = classifyAcquisitionEvidence({
    pathResult: {
      providerStatus: "TIMEOUT",
      providerErrorCode: "PROVIDER_TIMEOUT",
      providerCalls: 1,
    },
    expectedRows: 8,
  });
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.PROVIDER_TIMEOUT);
});

test("preprocessing not applicable -> PATH_NOT_APPLICABLE", () => {
  const result = classifyAcquisitionEvidence({
    pathResult: {
      preprocessingStatus: "PREPROCESSING_NO_STRONG_TABLE_REGION",
      providerStatus: "NOT_STARTED",
      providerCalls: 0,
    },
    expectedRows: 8,
  });
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE);
});

test("two distinct 4 + 4 expected 8 -> GROUPED_ACQUISITION_COMPLETE", () => {
  const result = successPath([
    candidate("block_1", "coordinate_block", 4),
    candidate("block_2", "coordinate_block", 4),
  ], 8);
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE);
  assert.equal(result.safeRecoveredRows, 8);
});

test("whole 8 + duplicate table 8 -> 8 not 16", () => {
  const result = calculateSafeRecoveredRows([
    candidate("whole", "whole_image", 8),
    candidate("table", "table", 8),
  ], 8);
  assert.equal(result.safeRecoveredRows, 8);
  assert.equal(result.rowEvidence, "WHOLE_IMAGE_COMPLETE");
});

test("whole 8 + block 4 + block 4 -> 8 not 16", () => {
  const result = calculateSafeRecoveredRows([
    candidate("whole", "whole_image", 8),
    candidate("block_1", "coordinate_block", 4),
    candidate("block_2", "coordinate_block", 4),
  ], 8);
  assert.equal(result.safeRecoveredRows, 8);
  assert.equal(result.rowEvidence, "WHOLE_IMAGE_COMPLETE");
});

test("block 4 + block 3 expected 8 -> INCOMPLETE 7", () => {
  const result = successPath([
    candidate("block_1", "coordinate_block", 4),
    candidate("block_2", "coordinate_block", 3),
  ], 8);
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE);
  assert.equal(result.safeRecoveredRows, 7);
});

test("sanitized artifact rowsOrPoints fallback -> INCOMPLETE 15", () => {
  const result = classifyAcquisitionEvidence({
    pathResult: {
      providerStatus: "SUCCESS",
      providerCalls: 1,
      responseReceived: true,
      contentPresent: true,
      jsonParse: "JSON_PARSE_SUCCESS",
      schemaValidation: "SCHEMA_VALID",
      candidateConstruction: "CANDIDATE_CONSTRUCTION_SUCCESS",
      candidateSummaries: [],
      rowsOrPoints: 15,
    },
    expectedRows: 16,
  });
  assert.equal(result.acquisitionEvidence, PHASE11D_EVIDENCE_STATE.ACQUISITION_INCOMPLETE);
  assert.equal(result.safeRecoveredRows, 15);
});

test("complete + recognizer unavailable -> ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED", () => {
  const result = classifyV3Coverage({
    acquisitionEvidence: PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE,
    v3RecognizerAvailable: false,
    expectedRows: 8,
    rowsOrPoints: 8,
  });
  assert.equal(result, PHASE11D_V3_RESULT.ACQUISITION_PASS_V3_RECOGNIZER_NOT_PORTED);
});

test("complete + recognizer available + wrong owner -> END_TO_END_FAIL", () => {
  const result = classifyV3Coverage({
    acquisitionEvidence: PHASE11D_EVIDENCE_STATE.ACQUISITION_COMPLETE,
    v3RecognizerAvailable: true,
    owner: "generic_dms",
    expectedOwner: "mgrs",
    expectedRows: 7,
    rowsOrPoints: 7,
    technicalKmlReady: true,
  });
  assert.equal(result, PHASE11D_V3_RESULT.END_TO_END_FAIL);
});

test("PATH A complete + PATH B timeout -> PATH_A_BETTER", () => {
  const comparison = comparePathEvidence(
    { acquisitionEvidence: PHASE11D_EVIDENCE_STATE.GROUPED_ACQUISITION_COMPLETE },
    { acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PROVIDER_TIMEOUT },
  );
  assert.equal(comparison, PHASE11D_PATH_COMPARISON.PATH_A_BETTER);
  assert.equal(expectedPathDecision(comparison), "PATH_A");
});

test("PATH A timeout + PATH B not applicable -> UNRESOLVED", () => {
  const comparison = comparePathEvidence(
    { acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PROVIDER_TIMEOUT },
    { acquisitionEvidence: PHASE11D_EVIDENCE_STATE.PATH_NOT_APPLICABLE },
  );
  assert.equal(comparison, PHASE11D_PATH_COMPARISON.UNRESOLVED);
  assert.equal(expectedPathDecision(comparison), "UNRESOLVED");
});

let passed = 0;
for (const item of tests) {
  await item.fn();
  passed += 1;
  console.log(`PASS ${item.name}`);
}

console.log(`Coordinate Engine V3 Phase 11D Evidence Classifier Regression: ${passed}/${tests.length} PASS`);
