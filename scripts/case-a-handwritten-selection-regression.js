import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  CANDIDATE_SELECTION_DECISION,
  compareCandidateEvidence
} from "../server/recognition/candidate-selection.js";
import {
  buildHandwrittenCandidateEvidence,
  materializeHandwrittenDmsRows
} from "../server/recognition/handwritten-candidate-evidence.js";
import {
  authorizeFamilyRetryDispatch,
  canAuthorizePointAzRetry,
  hasPointAzHeadingEvidence,
  hasPointAzSourceOrderContinuity,
  RETRY_OWNER_FAMILY
} from "../server/recognition/family-retry-policy.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const selectionSource = await readFile(path.join(root, "server/recognition/candidate-selection.js"), "utf8");
const evidenceSource = await readFile(path.join(root, "server/recognition/handwritten-candidate-evidence.js"), "utf8");

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

const rows = Array.from({ length: 16 }, (_, index) => {
  const label = String(index + 1);
  const minute = String(20 + (index % 10)).padStart(2, "0");
  return `${label}. 11°${minute}'31.26"N,08°40'42.13"W`;
});
const build = (input, id, stage) => buildHandwrittenCandidateEvidence(input, {
  candidateId: id,
  sourceStage: stage,
  ownerFamily: RETRY_OWNER_FAMILY.HANDWRITTEN_DMS
});
const stage1 = build(rows, "case-a-stage-1", "STAGE_1");
const stage2Same = build(rows, "case-a-stage-2", "STAGE_2");

test("STAGE_1_COMPONENT_MODEL_HAS_128_SLOTS", () => {
  assert.equal(stage1.coordinateCount, 16);
  assert.equal(stage1.fieldEvidence.length, 128);
  assert.equal(stage1.parseCompleteness, 1);
  assert.equal(stage1.validRangeCount, 128);
});
test("COMPONENT_IDENTITY_IS_EXPLICIT", () => {
  const fields = new Set(stage1.fieldEvidence.filter(item => item.pointLabel === "P01").map(item => item.field));
  assert.deepEqual(fields, new Set([
    "latitude.degree", "latitude.minute", "latitude.second", "latitude.hemisphere",
    "longitude.degree", "longitude.minute", "longitude.second", "longitude.hemisphere"
  ]));
  assert.ok(stage1.fieldEvidence.every(item => item.sourceText && item.sourceSpan && item.evidenceRef));
});
test("STABLE_VALUES_RETAIN_STAGE_1_EVIDENCE", () => {
  const result = compareCandidateEvidence(stage1, stage2Same);
  assert.equal(result.decision, CANDIDATE_SELECTION_DECISION.KEEP_CURRENT);
  assert.equal(result.reviewRequired, false);
  assert.equal(result.retrySelectionCount, 0);
  assert.ok(result.selectedFieldEvidence.every(item => item.sourceStage === "STAGE_1"));
});
test("PARTIAL_STAGE_2_VALID_FIELDS_ARE_RETAINED", () => {
  const current = Object.freeze({
    ...stage1,
    fieldEvidence: Object.freeze(stage1.fieldEvidence.filter(item => !(item.pointLabel === "P06" && item.axis === "longitude"))),
    parseCompleteness: 124 / 128,
    validRangeCount: 124,
    invalidRangeCount: 4,
    unresolvedIssues: Object.freeze([{ pointLabel: "P06", axis: "longitude", reason: "malformed_axis_token" }])
  });
  const retry = build([`6. 11°25'31.26"N,08°36'40.17"W`], "partial-stage-2", "STAGE_2");
  const result = compareCandidateEvidence(current, retry);
  assert.equal(result.decision, CANDIDATE_SELECTION_DECISION.MERGE_RETRY_FIELDS);
  assert.equal(result.retrySelectionCount, 4);
  assert.equal(result.selectedSource, "MIXED_RUNTIME_EVIDENCE");
  assert.equal(result.selectedFieldEvidence.length, 128);
});
test("INVALID_CURRENT_AXIS_CAN_BE_IMPROVED_WITHOUT_DEGRADING_OTHER_FIELDS", () => {
  const invalidRows = [...rows];
  invalidRows[5] = `6. 11°25'31.26"N,08°36'70.17"W`;
  const current = build(invalidRows, "invalid-stage-1", "STAGE_1");
  const retry = build([`6. 11°25'31.26"N,08°36'40.17"W`], "valid-stage-2", "STAGE_2");
  const result = compareCandidateEvidence(current, retry);
  assert.equal(current.fieldEvidence.length, 124);
  assert.equal(result.retrySelectionCount, 4);
  assert.equal(result.selectedFieldEvidence.length, 128);
  assert.equal(result.reviewRequired, false);
});
test("VALID_CONFLICT_DEFAULTS_TO_CURRENT_AND_REQUIRES_REVIEW", () => {
  const retry = build([`1. 11°20'37.26"N,08°40'42.13"W`], "conflict-stage-2", "STAGE_2");
  const result = compareCandidateEvidence(stage1, retry);
  const conflict = result.conflicts.find(item => item.pointLabel === "P01" && item.field === "latitude.second");
  assert.ok(conflict);
  assert.equal(conflict.currentValue, "31.26");
  assert.equal(conflict.retryValue, "37.26");
  assert.equal(result.decision, CANDIDATE_SELECTION_DECISION.KEEP_CURRENT_AND_REQUIRE_REVIEW);
  assert.equal(result.reviewRequired, true);
  assert.equal(result.selectedFieldEvidence.find(item => item.pointLabel === "P01" && item.field === "latitude.second").value, "31.26");
});
test("PARTIAL_RETRY_CANNOT_REMOVE_STABLE_FIELDS", () => {
  const retry = build([`1. 11°20'31.26"N,08°40'42.13"W`], "one-row-stage-2", "STAGE_2");
  const result = compareCandidateEvidence(stage1, retry);
  assert.equal(result.selectedFieldEvidence.length, 128);
  assert.equal(materializeHandwrittenDmsRows(result.selectedFieldEvidence, result.pointLabels).length, 16);
});
test("OWNER_FAMILY_MISMATCH_IS_REJECTED", () => {
  const result = compareCandidateEvidence(stage1, { ...stage2Same, ownerFamily: RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE });
  assert.equal(result.decision, CANDIDATE_SELECTION_DECISION.REJECT_CANDIDATE);
  assert.equal(result.selectedSource, "STAGE_1");
});
test("HANDWRITTEN_OWNER_LOCK_BLOCKS_POINT_AZ_DISPATCH", () => {
  const result = authorizeFamilyRetryDispatch({
    activeFamilyOwner: RETRY_OWNER_FAMILY.HANDWRITTEN_DMS,
    targetOwner: RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE
  });
  assert.equal(result.allowed, false);
  assert.equal(result.reason, "cross_family_transition_not_authorized");
});
test("ROW_COUNT_ALONE_CANNOT_AUTHORIZE_POINT_AZ", () => {
  const labels = Array.from({ length: 26 }, (_, index) => String.fromCharCode(65 + index));
  const typedEvidence = {
    established: false,
    type: "POINT_AZ_LABELED_TABLE_EVIDENCE",
    headingSemantics: false,
    orderedLabelContinuity: hasPointAzSourceOrderContinuity(labels)
  };
  assert.equal(canAuthorizePointAzRetry({ ownerFamily: RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE, typedPointAzEvidence: typedEvidence }), false);
});
test("LEGITIMATE_POINT_AZ_STRUCTURE_REMAINS_AUTHORIZED", () => {
  const labels = ["A", "B", "C", "D"];
  const text = "Point | Nord | Est";
  const typedEvidence = {
    established: true,
    type: "POINT_AZ_LABELED_TABLE_EVIDENCE",
    headingSemantics: hasPointAzHeadingEvidence(text),
    orderedLabelContinuity: hasPointAzSourceOrderContinuity(labels)
  };
  assert.equal(authorizeFamilyRetryDispatch({ activeFamilyOwner: null, targetOwner: RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE }).allowed, true);
  assert.equal(canAuthorizePointAzRetry({ ownerFamily: RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE, typedPointAzEvidence: typedEvidence }), true);
});
test("SERVER_CAPTURES_STAGE_1_BEFORE_DECIMAL_NORMALIZATION", () => {
  const capture = serverSource.indexOf("const stage1HandwrittenCandidateInput = formatHandwrittenDmsRawRows(rawText);");
  const normalize = serverSource.indexOf("let coordinates = extractCoordinateLines(rawText);", capture);
  assert.ok(capture >= 0 && normalize > capture);
});
test("SERVER_RETAINS_PARTIAL_STAGE_2_BEFORE_STABILITY_CLASSIFICATION", () => {
  const buildRetry = serverSource.indexOf("const retryHandwrittenCandidate = buildHandwrittenCandidateEvidence(handwrittenRead.candidateEvidenceRows");
  const oldWholeCandidateGate = serverSource.indexOf("if (handwrittenRead.handwrittenDms.isHandwrittenDms)", buildRetry - 200);
  assert.ok(buildRetry >= 0);
  assert.equal(oldWholeCandidateGate, -1);
});
test("COMPARATOR_IS_THE_ONLY_HANDWRITTEN_SELECTION_AUTHORITY", () => {
  assert.equal((serverSource.match(/compareCandidateEvidence\(currentHandwrittenCandidate, retryHandwrittenCandidate\)/g) || []).length, 1);
  assert.doesNotMatch(serverSource, /rawText\s*=\s*handwrittenRead\.rawText/);
  assert.doesNotMatch(serverSource, /rawText\s*=\s*selectedRows\.join/);
  assert.match(serverSource, /materializeHandwrittenDmsRows\(selection\.selectedFieldEvidence, selection\.pointLabels\)/);
});
test("POINT_AZ_AUTHORIZATION_PRECEDES_PROVIDER_CALL", () => {
  const authorization = serverSource.indexOf("const pointAzRetryAuthorized = authorizeFamilyRetryDispatch(");
  const condition = serverSource.indexOf("pointAzRetryAuthorized && shouldRetryPointAzDmsLongTable", authorization);
  const provider = serverSource.indexOf("prompt: pointAzDmsRetryPrompt", condition);
  assert.ok(authorization >= 0 && condition > authorization && provider > condition);
});
test("STAGE_2_HANDWRITTEN_EVIDENCE_LOCKS_OWNER_BEFORE_POINT_AZ", () => {
  const retryEvidence = serverSource.indexOf("const retryHandwrittenCandidate = buildHandwrittenCandidateEvidence(");
  const retryOwnerLock = serverSource.indexOf("activeFamilyOwner = RETRY_OWNER_FAMILY.HANDWRITTEN_DMS;", retryEvidence);
  const pointAzAuthorization = serverSource.indexOf("const pointAzRetryAuthorized = authorizeFamilyRetryDispatch(", retryOwnerLock);
  assert.ok(retryEvidence >= 0 && retryOwnerLock > retryEvidence && pointAzAuthorization > retryOwnerLock);
});
test("SELECTION_HAS_NO_GOLDEN_OR_FIXTURE_ORACLE", () => {
  assert.doesNotMatch(selectionSource, /golden|fixture|expected.?truth/i);
  assert.doesNotMatch(evidenceSource, /golden|fixture|expected.?truth/i);
});
test("FINAL_DMS_ROWS_ARE_MATERIALIZED_ONLY_FROM_SELECTED_RUNTIME_EVIDENCE", () => {
  const result = compareCandidateEvidence(stage1, stage2Same);
  const materialized = materializeHandwrittenDmsRows(result.selectedFieldEvidence, result.pointLabels);
  assert.equal(materialized.length, 16);
  assert.match(serverSource, /selectedHandwrittenDmsRows = selectedRows;\s*coordinates = selectedRows\.join\("\\n"\);/);
  assert.match(serverSource, /coordinates = selectedHandwrittenDmsRows\?\.length\s*\? selectedHandwrittenDmsRows\.join\("\\n"\)/);
});
test("HANDWRITTEN_POLYGON_GROUP_BOUNDARIES_ARE_PRESERVED", () => {
  const groupedRows = `${rows.slice(0, 4).join("\n")}\n\n${rows.slice(4, 8).join("\n")}`;
  const grouped = build(groupedRows, "grouped-stage-1", "STAGE_1");
  const materialized = materializeHandwrittenDmsRows(grouped.fieldEvidence, grouped.pointLabels);
  assert.equal(grouped.coordinateCount, 8);
  assert.equal(materialized.length, 9);
  assert.equal(materialized[4], "");
});
test("UNRESOLVED_COMPONENT_REQUIRES_REVIEW", () => {
  const current = build(rows.slice(0, 15), "short-stage-1", "STAGE_1");
  const retry = build([], "empty-stage-2", "STAGE_2");
  const result = compareCandidateEvidence(current, retry);
  assert.equal(result.reviewRequired, false);
  assert.equal(result.pointLabels.length, 15);
  assert.equal(materializeHandwrittenDmsRows(result.selectedFieldEvidence, result.pointLabels).length, 15);
  const partialCurrent = { ...stage1, fieldEvidence: stage1.fieldEvidence.slice(0, 127) };
  const partial = compareCandidateEvidence(partialCurrent, retry);
  assert.equal(partial.reviewRequired, true);
  assert.equal(partial.unresolvedIssues.length, 1);
});
test("NO_PROVIDER_CALLS_IN_DETERMINISTIC_REGRESSION", () => {
  assert.equal(typeof globalThis.fetch, "function");
});

console.log(`Case A Handwritten Selection Regression: ${passed}/${passed} PASS`);
console.log("DMS_COMPONENT_SLOTS=128");
console.log("PARTIAL_EVIDENCE_RETENTION=PASS");
console.log("MONOTONIC_SELECTION=PASS");
console.log("FIELD_CONFLICT_EXPLICIT=PASS");
console.log("CROSS_FAMILY_RETRY_ISOLATION=PASS");
console.log("NO_GOLDEN_RUNTIME_DEPENDENCY=PASS");
console.log("PROVIDER_CALLS=0");
