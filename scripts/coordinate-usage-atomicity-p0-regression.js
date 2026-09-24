import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_USAGE_COMMIT_RESULT,
  COORDINATE_USAGE_ERROR_CODE,
  CoordinateUsageAtomicityService,
  buildProjectedCoordinateReviewUsageAuthority,
  buildUnchargedCoordinateFailureResponse,
  createCoordinateUsageCommitController,
  createCoordinateUsageSessionToken,
  evaluateCoordinateUsageAuthority,
  hashCoordinateUsageSession,
  isRecognitionRequestId,
  parseCoordinateUsageSealKey,
  sealCoordinateResult,
  unsealCoordinateResult
} from "../server/coordinate-usage-atomicity.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION
} from "../server/coordinate-finalizer/reason-codes.js";
import { FAMILY_AVAILABILITY_STATUS } from "../server/coordinate-finalizer/family-availability-policy.js";
import { createGeometryHash } from "../server/coordinate-finalizer/geometry-hash.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const migrationPath = path.join(root, "supabase/migrations/20260916013501_coordinate_usage_atomicity_p0.sql");
const sealKey = Buffer.alloc(32, 7);
const sealKeyText = sealKey.toString("base64");
const requestId = "11111111-1111-4111-8111-111111111111";
const userId = "synthetic-atomicity-user";
const sessionToken = createCoordinateUsageSessionToken();
const sessionBindingSha256 = hashCoordinateUsageSession(sessionToken);
const geometry = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([20, 10]),
    Object.freeze([21, 10]),
    Object.freeze([21, 11]),
    Object.freeze([20, 10])
  ])])
});

function authorityPayload(overrides = {}) {
  const finalizedCoordinateResult = {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId: "synthetic-authority-result",
    resultRevision: 1,
    geometry,
    geometryHash: createGeometryHash(geometry),
    sourceAuthority: "coordinate_engine_v2",
    explicitAuthorityRejected: false,
    crs: FINALIZED_COORDINATE_CRS,
    availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
    availabilityReasonCode: null,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    decisionState: COORDINATE_DECISION_STATE.AUTO_EXPORT,
    gate: {
      decisionState: COORDINATE_DECISION_STATE.AUTO_EXPORT,
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
      availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      availabilityReasonCode: null
    },
    blockingReasons: [],
    requiresReview: false,
    kmlReady: true,
    ...overrides
  };
  return {
    success: true,
    rawText: "synthetic coordinate evidence 20 10",
    coordinates: "20,10",
    finalizedCoordinateResult
  };
}

function projectedReviewPayload() {
  const coordinates = [
    "A | 727250 | 1219700",
    "B | 728400 | 1219700",
    "C | 728400 | 1219500"
  ].join("\n");
  const rows = coordinates.split("\n").map(line => {
    const [label, x, y] = line.split("|").map(value => value.trim());
    return { label, x: Number(x), y: Number(y) };
  });
  const body = {
    success: true,
    requestId,
    coordinates,
    precisionMode: "projected-x-y-review",
    requiresReview: true,
    providerProjectedReviewEvidence: {
      status: "COMPLETE",
      coordinateRowCount: rows.length,
      crsEvidence: { status: "UNCONFIRMED" }
    },
    sourceCoordinateRepresentation: { displayText: coordinates },
    coordinateEngineV2: {
      coordinate_type: "projected_xy",
      requires_review: true,
      groups: [{
        requires_review: true,
        kml_ready: false,
        points: rows.map(row => ({
          ...row,
          lat: null,
          lon: null,
          source_crs: null,
          requires_review: true
        }))
      }]
    },
    finalizedCoordinateResult: {
      schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
      resultId: "synthetic-projected-review",
      resultRevision: 1,
      decisionState: COORDINATE_DECISION_STATE.BLOCKED,
      geometry: null,
      geometryHash: null,
      kmlReady: false,
      requiresReview: true
    }
  };
  body.projectedCoordinateReviewAuthority = buildProjectedCoordinateReviewUsageAuthority({
    recognitionRequestId: requestId,
    body
  });
  return body;
}

function createMemoryRpc({ commitUnknown = false } = {}) {
  const state = {
    rows: new Map(),
    freeBalance: 2,
    paidBalance: 1,
    usageLogs: [],
    prepareCalls: 0,
    commitCalls: 0,
    stateCalls: 0
  };
  return {
    state,
    async rpc(name, args) {
      if (name === "prepare_coordinate_recognition_result") {
        state.prepareCalls += 1;
        const existing = state.rows.get(args.p_recognition_request_id);
        if (existing) {
          const same = existing.userId === args.p_user_id
            && existing.sessionBindingSha256 === args.p_session_binding_sha256
            && existing.authorityResultId === args.p_authority_result_id
            && existing.envelopeSha256 === args.p_sealed_result_sha256;
          return same
            ? { data: [{ result: existing.state === "COMMITTED" ? "ALREADY_COMMITTED" : "ALREADY_PREPARED", state: existing.state }], error: null }
            : { data: null, error: { code: "P0001" } };
        }
        state.rows.set(args.p_recognition_request_id, {
          userId: args.p_user_id,
          sessionBindingSha256: args.p_session_binding_sha256,
          authorityResultId: args.p_authority_result_id,
          state: "PREPARED",
          consumeType: null,
          envelope: args.p_sealed_result,
          envelopeSha256: args.p_sealed_result_sha256
        });
        return { data: [{ result: "PREPARED", state: "PREPARED" }], error: null };
      }
      if (name === "commit_coordinate_recognition_usage") {
        state.commitCalls += 1;
        if (commitUnknown) return { data: null, error: { code: "NETWORK_OUTCOME_UNKNOWN" } };
        const row = state.rows.get(args.p_recognition_request_id);
        if (!row || row.userId !== args.p_user_id || row.sessionBindingSha256 !== args.p_session_binding_sha256) {
          return { data: [{ result: "NOT_FOUND" }], error: null };
        }
        if (row.state === "COMMITTED") {
          return { data: [{ result: "ALREADY_COMMITTED", state: row.state, consume_type: row.consumeType,
            quota: { freeConvertCount: state.freeBalance, paidConvertCount: state.paidBalance } }], error: null };
        }
        if (state.freeBalance > 0) {
          state.freeBalance -= 1;
          row.consumeType = "free";
        } else if (state.paidBalance > 0) {
          state.paidBalance -= 1;
          row.consumeType = "paid";
        } else {
          row.state = "FAILED";
          return { data: [{ result: "QUOTA_EXHAUSTED", state: "FAILED", consume_type: "none",
            quota: { freeConvertCount: 0, paidConvertCount: 0 } }], error: null };
        }
        row.state = "COMMITTED";
        state.usageLogs.push({ recognitionRequestId: args.p_recognition_request_id, consumeType: row.consumeType });
        return { data: [{ result: "COMMITTED", state: row.state, consume_type: row.consumeType,
          quota: { freeConvertCount: state.freeBalance, paidConvertCount: state.paidBalance } }], error: null };
      }
      if (name === "get_coordinate_recognition_commit_state") {
        state.stateCalls += 1;
        const row = state.rows.get(args.p_recognition_request_id);
        if (!row || row.userId !== args.p_user_id || row.sessionBindingSha256 !== args.p_session_binding_sha256) {
          return { data: [{ result: "NOT_FOUND" }], error: null };
        }
        return { data: [{
          result: row.state === "COMMITTED" ? "ALREADY_COMMITTED" : row.state,
          state: row.state,
          consume_type: row.consumeType,
          quota: { freeConvertCount: state.freeBalance, paidConvertCount: state.paidBalance },
          sealed_result: row.state === "COMMITTED" ? row.envelope : null,
          sealed_result_sha256: row.state === "COMMITTED" ? row.envelopeSha256 : null
        }], error: null };
      }
      throw new Error("UNEXPECTED_RPC");
    }
  };
}

const tests = [];
const test = (name, fn) => tests.push({ name, fn });

test("recognition request IDs are UUIDs and session bindings are one-way hashes", () => {
  assert.equal(isRecognitionRequestId(randomUUID()), true);
  assert.equal(isRecognitionRequestId("recognition_legacy"), false);
  assert.equal(sessionToken.length, 43);
  assert.match(sessionBindingSha256, /^[a-f0-9]{64}$/);
  assert.equal(sessionBindingSha256.includes(sessionToken), false);
});

test("seal key parsing accepts exactly 32 bytes and rejects weak configuration", () => {
  assert.deepEqual(parseCoordinateUsageSealKey(sealKeyText), sealKey);
  assert.deepEqual(parseCoordinateUsageSealKey(sealKey.toString("hex")), sealKey);
  assert.equal(parseCoordinateUsageSealKey("short-secret"), null);
});

test("AES-GCM envelope round-trips only with the bound request ID", () => {
  const payload = authorityPayload();
  const sealed = sealCoordinateResult(payload, { key: sealKey, recognitionRequestId: requestId, random: () => Buffer.alloc(12, 3) });
  assert.deepEqual(unsealCoordinateResult(sealed.envelope, { key: sealKey, recognitionRequestId: requestId }), payload);
  assert.throws(
    () => unsealCoordinateResult(sealed.envelope, { key: sealKey, recognitionRequestId: "22222222-2222-4222-8222-222222222222" }),
    { code: COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID }
  );
  const serialized = JSON.stringify(sealed.envelope);
  assert.equal(serialized.includes("coordinates"), false);
  assert.equal(serialized.includes("20,10"), false);
});

test("positive predicate accepts a valid server authority result", () => {
  const evaluated = evaluateCoordinateUsageAuthority({ httpStatus: 200, body: authorityPayload() });
  assert.equal(evaluated.eligible, true);
  assert.equal(evaluated.identity.resultId, "synthetic-authority-result");
});

test("review-required authority may be charged while KML remains safely blocked", () => {
  const body = authorityPayload({
    decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
    gate: {
      decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
      availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      availabilityReasonCode: null
    },
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    blockingReasons: [
      { code: COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED },
      { code: COORDINATE_GATE_REASON.REVIEW_REQUIRED },
      { code: COORDINATE_GATE_REASON.KML_NOT_READY }
    ],
    requiresReview: true,
    kmlReady: false
  });
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, true);
});

test("missing availability identity cannot enter PREPARED", () => {
  const body = authorityPayload();
  delete body.finalizedCoordinateResult.availabilityStatus;
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, false);
});

test("review-required authority accepts coherent technical KML readiness behind pending confirmation", () => {
  const body = authorityPayload({
    decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
    gate: {
      decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
      availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      availabilityReasonCode: null
    },
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    blockingReasons: [{ code: COORDINATE_GATE_REASON.REVIEW_REQUIRED }],
    requiresReview: true,
    technicalKmlReady: true,
    kmlReady: true
  });
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, true);
});

test("complete projected review evidence is chargeable while map and KML remain blocked", () => {
  const body = projectedReviewPayload();
  const evaluated = evaluateCoordinateUsageAuthority({ httpStatus: 200, body });
  assert.equal(evaluated.eligible, true);
  assert.equal(evaluated.reason, "PROJECTED_REVIEW_SERVER_AUTHORITY_ESTABLISHED");
  assert.match(evaluated.identity.geometryHash, /^sha256:[a-f0-9]{64}$/);
  assert.equal(body.finalizedCoordinateResult.geometry, null);
  assert.equal(body.finalizedCoordinateResult.kmlReady, false);
  assert.equal(body.finalizedCoordinateResult.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
});

test("projected review authority rejects coordinate, CRS, and export-permission tampering", () => {
  for (const mutate of [
    body => { body.coordinates = body.coordinates.replace("727250", "727251"); },
    body => { body.providerProjectedReviewEvidence.crsEvidence.status = "INFERRED"; },
    body => { body.finalizedCoordinateResult.kmlReady = true; },
    body => { body.finalizedCoordinateResult.geometry = geometry; }
  ]) {
    const body = structuredClone(projectedReviewPayload());
    mutate(body);
    assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, false);
  }
});

test("projected review result is sealed and consumes exactly one usage", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  const controller = createCoordinateUsageCommitController({
    atomicityService: service,
    recognitionRequestId: requestId,
    userId,
    sessionBindingSha256
  });
  controller.schedule({ note: "synthetic projected review" });
  const settled = await controller.settle({ httpStatus: 200, body: projectedReviewPayload() });
  assert.equal(settled.kind, "USAGE_COMMITTED");
  assert.equal(mock.state.freeBalance, 1);
  assert.equal(mock.state.usageLogs.length, 1);
  assert.equal(JSON.stringify(mock.state.rows.get(requestId).envelope).includes("727250"), false);
});

test("review-required KML readiness is rejected without matching technical authority", () => {
  const body = authorityPayload({
    decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
    gate: {
      decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
      availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      availabilityReasonCode: null
    },
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    blockingReasons: [{ code: COORDINATE_GATE_REASON.REVIEW_REQUIRED }],
    requiresReview: true,
    technicalKmlReady: false,
    kmlReady: true
  });
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, false);
});

test("review-required KML readiness is rejected when a KML blocker remains", () => {
  const body = authorityPayload({
    decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
    gate: {
      decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
      qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
      confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
      availabilityStatus: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      availabilityReasonCode: null
    },
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    blockingReasons: [
      { code: COORDINATE_GATE_REASON.REVIEW_REQUIRED },
      { code: COORDINATE_GATE_REASON.KML_NOT_READY }
    ],
    requiresReview: true,
    technicalKmlReady: true,
    kmlReady: true
  });
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, false);
});

test("gate mirrors and explicit rejection identity cannot be forged or omitted", () => {
  const contradictoryGate = authorityPayload();
  contradictoryGate.finalizedCoordinateResult.gate = {
    decisionState: COORDINATE_DECISION_STATE.AUTO_EXPORT,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.FAILED,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    availabilityStatus: "BLOCKED_BY_PROVIDER",
    availabilityReasonCode: "SYNTHETIC_CONTRADICTION"
  };
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body: contradictoryGate }).eligible, false);

  const missingExplicitRejection = authorityPayload();
  delete missingExplicitRejection.finalizedCoordinateResult.explicitAuthorityRejected;
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body: missingExplicitRejection }).eligible, false);

  const missingGateMirror = authorityPayload();
  delete missingGateMirror.finalizedCoordinateResult.gate.confirmationStatus;
  assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body: missingGateMirror }).eligible, false);
});

test("uncharged responses are fixed-field failures and cannot disclose Provider-derived text", () => {
  const request = buildUnchargedCoordinateFailureResponse({
    recognitionRequestId: requestId,
    body: {
      model: "provider-model",
      rawText: "sensitive provider-derived text",
      coordinates: "20,10",
      warning: "unbounded warning",
      quota: { free_convert_count: 2, secret: "do-not-copy" }
    }
  });
  assert.deepEqual(Object.keys(request).sort(), [
    "code", "coordinates", "error", "providerCallCount", "providerCompletionState", "quota", "rawText", "reason", "recoveryRequired", "recoveryTerminal", "requestId", "retryAllowed", "success", "usageConsumed", "userUsageConsumed"
  ].sort());
  assert.equal(request.success, false);
  assert.equal(request.reason, "coordinate_authority_not_established");
  assert.equal(request.rawText, "");
  assert.equal(request.coordinates, "");
  assert.equal(request.recoveryRequired, false);
  assert.equal(request.recoveryTerminal, true);
  assert.equal(request.providerCompletionState, "NOT_STARTED");
  assert.equal(request.providerCallCount, 0);
  assert.equal(request.userUsageConsumed, false);
  assert.deepEqual(request.quota, { free_convert_count: 2 });
  assert.equal(JSON.stringify(request).includes("provider-derived"), false);
  assert.equal(JSON.stringify(request).includes("do-not-copy"), false);
});

for (const [name, body] of [
  ["BLOCKED", authorityPayload({ decisionState: "BLOCKED", gate: { decisionState: "BLOCKED" }, geometry: null, geometryHash: null })],
  ["missing geometry", authorityPayload({ geometry: null, geometryHash: null })],
  ["geometry hash mismatch", authorityPayload({ geometryHash: `sha256:${"0".repeat(64)}` })],
  ["malformed Finalizer identity", authorityPayload({ resultRevision: 0 })],
  ["source blocker", authorityPayload({ blockingReasons: [{ code: COORDINATE_GATE_REASON.CRS_NOT_FINALIZED }] })],
  ["failed HTTP", authorityPayload()]
]) {
  test(`${name} cannot enter PREPARED`, () => {
    const evaluated = evaluateCoordinateUsageAuthority({ httpStatus: name === "failed HTTP" ? 500 : 200, body });
    assert.equal(evaluated.eligible, false);
  });
}

test("prepare stores ciphertext only and atomic commit consumes free quota exactly once", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  const payload = authorityPayload();
  const prepared = await service.prepare({ recognitionRequestId: requestId, userId, sessionBindingSha256, responsePayload: payload,
    providerCostState: "USAGE_REPORTED" });
  assert.equal(prepared.result, "PREPARED");
  const row = mock.state.rows.get(requestId);
  const serializedEnvelope = JSON.stringify(row.envelope);
  assert.equal(serializedEnvelope.includes(payload.rawText), false);
  assert.equal(serializedEnvelope.includes(payload.coordinates), false);
  const first = await service.commit({ recognitionRequestId: requestId, userId, sessionBindingSha256 });
  const second = await service.commit({ recognitionRequestId: requestId, userId, sessionBindingSha256 });
  assert.equal(first.result, "COMMITTED");
  assert.equal(second.result, "ALREADY_COMMITTED");
  assert.equal(mock.state.freeBalance, 1);
  assert.equal(mock.state.paidBalance, 1);
  assert.equal(mock.state.usageLogs.length, 1);
});

test("recovery returns the committed sealed result without Provider or second charge", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  const payload = authorityPayload();
  await service.prepare({ recognitionRequestId: requestId, userId, sessionBindingSha256, responsePayload: payload });
  const recovered = await service.recover({ recognitionRequestId: requestId, userId, sessionBindingSha256 });
  assert.equal(recovered.result, "ALREADY_COMMITTED");
  assert.deepEqual(recovered.responsePayload, payload);
  assert.equal(mock.state.commitCalls, 1);
  assert.equal(mock.state.usageLogs.length, 1);
});

test("committed envelope corruption never becomes an uncharged recovery", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  const payload = authorityPayload();
  await service.prepare({ recognitionRequestId: requestId, userId, sessionBindingSha256, responsePayload: payload });
  await service.commit({ recognitionRequestId: requestId, userId, sessionBindingSha256 });
  mock.state.rows.get(requestId).envelopeSha256 = "0".repeat(64);
  const recovered = await service.recover({ recognitionRequestId: requestId, userId, sessionBindingSha256 });
  assert.equal(recovered.result, COORDINATE_USAGE_COMMIT_RESULT.COMMITTED_RESULT_UNAVAILABLE);
  assert.equal(recovered.state, "COMMITTED");
  assert.equal("responsePayload" in recovered, false);
});

test("session mismatch cannot recover or commit another request", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  await service.prepare({ recognitionRequestId: requestId, userId, sessionBindingSha256, responsePayload: authorityPayload() });
  const result = await service.recover({ recognitionRequestId: requestId, userId, sessionBindingSha256: "0".repeat(64) });
  assert.equal(result.result, "NOT_FOUND");
  assert.equal(mock.state.freeBalance, 2);
});

test("unknown database commit outcome is never reported as uncharged", async () => {
  const mock = createMemoryRpc({ commitUnknown: true });
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  const controller = createCoordinateUsageCommitController({
    atomicityService: service,
    recognitionRequestId: requestId,
    userId,
    sessionBindingSha256
  });
  controller.schedule({ note: "synthetic" });
  const settled = await controller.settle({ httpStatus: 200, body: authorityPayload() });
  assert.equal(settled.kind, "USAGE_COMMIT_OUTCOME_UNKNOWN");
  assert.equal(settled.committed, false);
});

test("prepare idempotency conflict is recoverable uncertainty, not proof of no charge", async () => {
  const conflictRpc = {
    async rpc() {
      return { data: null, error: { code: "P0001" } };
    }
  };
  const service = new CoordinateUsageAtomicityService({ supabase: conflictRpc, sealKey });
  await assert.rejects(
    service.prepare({
      recognitionRequestId: requestId,
      userId,
      sessionBindingSha256,
      responsePayload: authorityPayload()
    }),
    error => error?.code === "USAGE_COMMIT_OUTCOME_UNKNOWN"
  );
});

test("failure payload and non-authoritative success remain unprepared and uncharged", async () => {
  const mock = createMemoryRpc();
  const service = new CoordinateUsageAtomicityService({ supabase: mock, sealKey });
  for (const response of [
    { httpStatus: 422, body: { success: false, code: "COORDINATE_RECOGNITION_FAILED_CLOSED" } },
    { httpStatus: 200, body: { success: true, finalizedCoordinateResult: {} } }
  ]) {
    const controller = createCoordinateUsageCommitController({ atomicityService: service, recognitionRequestId: randomUUID(), userId,
      sessionBindingSha256 });
    controller.schedule({ note: "synthetic" });
    const settled = await controller.settle(response);
    assert.equal(settled.kind, "UNCHARGED_RESPONSE");
  }
  assert.equal(mock.state.prepareCalls, 0);
  assert.equal(mock.state.commitCalls, 0);
});

test("migration freezes private outbox atomic locks unique idempotency and RPC privileges", async () => {
  const sql = await readFile(migrationPath, "utf8");
  assert.match(sql, /create table private\.coordinate_recognition_commits/i);
  assert.match(sql, /sealed_result jsonb not null/i);
  assert.match(sql, /sealed_result \?& array\['version', 'iv', 'ciphertext', 'authTag'\]/i);
  assert.match(sql, /sealed_result - array\['version', 'iv', 'ciphertext', 'authTag'\] = '\{\}'::jsonb/i);
  assert.doesNotMatch(sql, /jsonb_object_length/i);
  assert.match(sql, /encryption_version = 'AES_256_GCM_V1'/i);
  assert.match(sql, /create unique index usage_logs_recognition_request_id_uidx/i);
  assert.match(sql, /where recognition_request_id is not null/i);
  assert.equal((sql.match(/security definer/gi) || []).length, 3);
  assert.equal((sql.match(/set search_path = ''/gi) || []).length, 3);
  assert.equal((sql.match(/grant execute on function [\s\S]*? to service_role;/gi) || []).length, 3);
  assert.equal((sql.match(/revoke all on function [\s\S]*? from public, anon, authenticated;/gi) || []).length, 3);
  const commit = sql.slice(sql.indexOf("create or replace function public.commit_coordinate_recognition_usage"),
    sql.indexOf("create or replace function public.get_coordinate_recognition_commit_state"));
  assert.ok(commit.indexOf("from private.coordinate_recognition_commits") < commit.indexOf("from public.users"));
  assert.ok(commit.indexOf("free_convert_count") < commit.indexOf("paid_convert_count"));
  assert.match(commit, /for update/g);
  assert.match(commit, /insert into public\.usage_logs/i);
  assert.match(commit, /set state = 'COMMITTED'/i);
  assert.equal(/coordinate[s]?\s+(text|jsonb)/i.test(sql), false);
});

test("frontend and server bind one request ID to recovery without Provider replay", async () => {
  const [server, index, deadline] = await Promise.all([
    readFile(path.join(root, "server.js"), "utf8"),
    readFile(path.join(root, "index.html"), "utf8"),
    readFile(path.join(root, "server/coordinate-finalizer/recognition-deadline.js"), "utf8")
  ]);
  assert.match(server, /\/api\/recognize-coordinates\/recover/);
  assert.match(server, /\/api\/recognize-coordinates\/session/);
  assert.match(server, /coordinateUsageAtomicity\.recover/);
  assert.match(server, /USAGE_COMMIT_OUTCOME_UNKNOWN/);
  assert.match(server, /const terminalNoCharge = recovered\.result === COORDINATE_USAGE_COMMIT_RESULT\.NOT_FOUND\s*\|\| \(recovered\.result === COORDINATE_USAGE_COMMIT_RESULT\.EXPIRED/);
  assert.match(server, /recoveryTerminal: terminalNoCharge/);
  assert.match(server, /usageConsumed: terminalNoCharge \? false : null/);
  assert.match(server, /const recognitionPayload = \{\s*success: true,/);
  assert.match(server, /settlement\.kind === "UNCHARGED_RESPONSE" && settlement\.authorityReason !== "REGRESSION_TEST"/);
  assert.match(server, /buildUnchargedCoordinateFailureResponse\(\{/);
  assert.doesNotMatch(server, /settlement\.kind === "UNCHARGED_RESPONSE"\s*&& body\?\.success === true/);
  assert.match(index, /crypto\?\.randomUUID/);
  assert.match(index, /prepareCoordinateUsageSession\(currentVisitorId\)/);
  assert.match(index, /prepareCoordinateUsageSession\(currentVisitorId\);\s*const useAsyncJob = await shouldUseAsyncCoordinateRecognition\(file\);\s*if \(!useAsyncJob\) rememberPendingCoordinateCommitRequestId\(recognitionRequestId\);/);
  assert.match(index, /response = await fetch\(useAsyncJob \? "\/api\/recognize-coordinates\/jobs" : "\/api\/recognize-coordinates"/);
  assert.match(index, /"x-recognition-request-id": recognitionRequestId/);
  assert.match(index, /recoverCommittedCoordinateResult\(recognitionRequestId, currentVisitorId\)/);
  assert.match(index, /sessionStorage\.setItem\(PENDING_COORDINATE_COMMIT_REQUEST_KEY, String\(value\)\.toLowerCase\(\)\)/);
  assert.match(index, /if \(!recoveryOnly\) \{\s*const recovered = await recoverCommittedCoordinateResult/);
  assert.match(index, /let responseWasRecovery = usageRecoveryOnly/);
  assert.match(index, /await agenticCoordinateInitializationPromise/);
  assert.match(index, /agenticCoordinateController\?\.enabled[\s\S]*agenticCoordinateController\.recoverPending\(\)[\s\S]*resumePendingCoordinateWorkOnPageShow\(\)/);
  assert.match(index, /async function resumePendingCoordinateWorkOnPageShow\(\)/);
  assert.match(index, /if \(!file && !recoveryOnly\) \{\s*return;\s*\}/);
  assert.match(index, /pendingCoordinateRecoveryPromise = recognizeImage\(\)/);
  assert.match(index, /!responseWasRecovery && data\?\.usageConsumed === false/);
  assert.match(index, /responseWasRecovery && data\?\.recoveryTerminal === true && data\?\.usageConsumed === false/);
  assert.doesNotMatch(index, /data\?\.usageConsumed === false \|\| recoveryOnly/);
  assert.match(deadline, /request_hard_deadline_during_usage_commit/);
  assert.match(deadline, /usageConsumed: null/);
});

let passed = 0;
for (const entry of tests) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.name}`);
    throw error;
  }
}
console.log(`Coordinate Usage Atomicity P0 regression: ${passed}/${tests.length} PASS`);
