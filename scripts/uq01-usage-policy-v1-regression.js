import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import {
  FREE_DAY_TIMEZONE,
  FREE_TRIAL_DAILY_MAX,
  FREE_TRIAL_LIFETIME_MAX,
  USAGE_EVENT_TYPE,
  UsageTokenError,
  createServiceOperationId,
  createUsageTokenAuthority,
  getFreeDayKey,
  isCoordinateResultChargeable,
  isJudgeResultChargeable,
  isServiceOperationId,
  toSharedQuotaPayload
} from "../server/usage/usage-policy.js";
import { SupabaseUsageLedger } from "../server/usage/supabase-usage-ledger.js";

const cases = [];
function check(id, fn) { fn(); cases.push({ id, status: "PASS" }); }

function createRuntimeSupabaseHarness() {
  const state = {
    users: new Map(),
    usage: new Map(),
    events: new Map(),
    chargeCounts: { coordinate: 0, judge: 0 },
    judgeCaseWrites: 0,
    failJudgePersistence: false
  };

  function quota(identity) {
    const current = state.usage.get(identity) || { daily: 0, lifetime: 0 };
    return {
      success: true,
      reason: "ok",
      free_daily_used: current.daily,
      free_lifetime_used: current.lifetime,
      free_trial_daily_max: FREE_TRIAL_DAILY_MAX,
      free_trial_lifetime_max: FREE_TRIAL_LIFETIME_MAX,
      free_daily_remaining: Math.max(0, FREE_TRIAL_DAILY_MAX - current.daily),
      free_lifetime_remaining: Math.max(0, FREE_TRIAL_LIFETIME_MAX - current.lifetime),
      paid_convert_count: 20,
      paid_judge_count: 20
    };
  }

  class Query {
    constructor(table) {
      this.table = table;
      this.operation = "select";
      this.payload = null;
      this.filters = {};
    }
    select() { return this; }
    insert(payload) { this.operation = "insert"; this.payload = payload; return this; }
    update(payload) { this.operation = "update"; this.payload = payload; return this; }
    upsert(payload) { this.operation = "upsert"; this.payload = payload; return this; }
    eq(name, value) { this.filters[name] = value; return this; }
    gte() { return this; }
    lt() { return this; }
    order() { return this; }
    limit() { return this; }
    range() { return this; }
    maybeSingle() { return this.execute(true); }
    single() { return this.execute(true); }
    then(resolve, reject) { return this.execute(false).then(resolve, reject); }
    async execute(single) {
      if (this.table === "system_config") {
        return { data: single ? {
          monthly_price: 99,
          monthly_judge_count: 50,
          monthly_convert_count: 50,
          add_price: 19,
          add_count: 10,
          free_trial_daily_max: FREE_TRIAL_DAILY_MAX,
          free_trial_lifetime_max: FREE_TRIAL_LIFETIME_MAX,
          updated_at: "2026-09-05T00:00:00.000Z"
        } : [], error: null };
      }
      if (this.table === "users") {
        const identity = String(this.filters.user_id || this.payload?.user_id || "");
        const existing = state.users.get(identity) || {
          user_id: identity,
          is_vip: false,
          free_convert_count: 3,
          free_judge_count: 3,
          paid_convert_count: 20,
          paid_judge_count: 20,
          updated_at: "2026-09-05T00:00:00.000Z"
        };
        const row = this.operation === "update" || this.operation === "upsert" || this.operation === "insert"
          ? { ...existing, ...(this.payload || {}) }
          : existing;
        state.users.set(identity, row);
        return { data: single ? row : [row], error: null };
      }
      if (this.table === "judge_cases" && this.operation === "insert") {
        if (state.failJudgePersistence) {
          return { data: null, error: { code: "UQ01_TEST_PERSISTENCE_FAILURE", message: "deterministic persistence failure" } };
        }
        state.judgeCaseWrites += 1;
        const row = { case_id: `uq01-case-${state.judgeCaseWrites}` };
        return { data: single ? row : [row], error: null };
      }
      return { data: single ? null : [], error: null };
    }
  }

  const supabase = {
    from(table) { return new Query(table); },
    async rpc(name, args) {
      if (name === "uq01_get_usage_quota") return { data: quota(args.p_usage_identity), error: null };
      if (name !== "uq01_consume_usage_event") {
        return { data: null, error: { code: "UQ01_TEST_RPC_UNKNOWN", message: name } };
      }
      const key = `${args.p_usage_identity}:${args.p_service_operation_id}:${args.p_usage_event_type}`;
      const existing = state.events.get(key);
      if (existing) return { data: { ...existing, idempotent: true }, error: null };
      const current = state.usage.get(args.p_usage_identity) || { daily: 0, lifetime: 0 };
      const next = { daily: current.daily + 1, lifetime: current.lifetime + 1 };
      state.usage.set(args.p_usage_identity, next);
      const product = args.p_usage_event_type === USAGE_EVENT_TYPE.JUDGE ? "judge" : "coordinate";
      state.chargeCounts[product] += 1;
      const result = {
        ...quota(args.p_usage_identity),
        success: true,
        reason: "ok",
        idempotent: false,
        event_id: `uq01-${product}-event-${state.chargeCounts[product]}`,
        charge_source: "free"
      };
      state.events.set(key, result);
      return { data: result, error: null };
    }
  };
  return { supabase, state };
}

async function runActualRouteRuntimeChild() {
  process.env.NODE_ENV = "test";
  process.env.UQ01_RUNTIME_REGRESSION_MODE = "true";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "uq01-runtime-regression-server-only-signing-secret";
  process.env.ALIYUN_API_KEY = "uq01-local-provider-stub";
  process.env.ALIYUN_BASE_URL = "http://127.0.0.1:1/compatible-mode/v1";
  process.env.PORT = "0";
  delete process.env.REGRESSION_TEST_MODE;
  delete process.env.ENABLE_REGRESSION_TEST_MODE;

  const harness = createRuntimeSupabaseHarness();
  const losses = { coordinate: 1, judge: 1 };
  globalThis.__GEOKITLAB_UQ01_RUNTIME_REGRESSION_DEPENDENCIES__ = {
    supabase: harness.supabase,
    shouldDropCommittedResponse({ route }) {
      if (!losses[route]) return false;
      losses[route] -= 1;
      return true;
    }
  };

  const nativeFetch = globalThis.fetch;
  let providerMode = "coordinate";
  let localProviderCalls = 0;
  globalThis.fetch = async (input, init) => {
    const url = String(input);
    if (!url.startsWith("http://127.0.0.1:1/compatible-mode/v1/")) {
      return nativeFetch(input, init);
    }
    localProviderCalls += 1;
    const content = providerMode === "judge"
      ? "【结论】测试图片可进入人工复核。\n【等级】B\n【下一步】补充现场照片。"
      : [
          `08°00'00.00\"W,11°00'00.00\"N`,
          `08°00'30.00\"W,11°00'00.00\"N`,
          `08°00'30.00\"W,11°00'30.00\"N`,
          `08°00'00.00\"W,11°00'30.00\"N`
        ].join("\n");
    return new Response(JSON.stringify({
      model: "uq01-local-provider-stub",
      choices: [{ message: { content } }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 }
    }), { status: 200, headers: { "content-type": "application/json" } });
  };

  let runtimeServer;
  let listeningResolve;
  const listening = new Promise(resolve => { listeningResolve = resolve; });
  const originalListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function(_port, callback) {
    runtimeServer = this;
    this.once("listening", listeningResolve);
    return originalListen.call(this, 0, "127.0.0.1", callback);
  };
  await import(`../server.js?uq01-runtime=${Date.now()}`);
  await listening;
  http.Server.prototype.listen = originalListen;
  const baseUrl = `http://127.0.0.1:${runtimeServer.address().port}`;

  const sessionResponse = await nativeFetch(`${baseUrl}/api/usage/session`, { method: "POST" });
  assert.equal(sessionResponse.status, 200);
  const cookie = String(sessionResponse.headers.get("set-cookie") || "").split(";")[0];
  assert.match(cookie, /^geokit_usage_identity_v1=uq01\./);
  const fileBytes = Buffer.from("uq01-runtime-route-fixture-v1");
  const requestFingerprint = createHash("sha256").update(fileBytes).digest("hex");

  async function operation(tool) {
    const response = await nativeFetch(`${baseUrl}/api/usage/operation`, {
      method: "POST",
      headers: { cookie, "content-type": "application/json" },
      body: JSON.stringify({ tool, requestFingerprint })
    });
    assert.equal(response.status, 200);
    return (await response.json()).operationToken;
  }
  async function upload(pathname, field, token) {
    const body = new FormData();
    body.append(field, new Blob([fileBytes], { type: "image/jpeg" }), "uq01-runtime.jpg");
    if (pathname.includes("analyze")) body.append("judgeType", "mine-land");
    return nativeFetch(`${baseUrl}${pathname}`, {
      method: "POST",
      headers: { cookie, "x-usage-operation-token": token },
      body
    });
  }

  const coordinateToken = await operation("coordinate");
  await upload("/api/recognize-coordinates", "image", coordinateToken).then(
    () => { throw new Error("coordinate response loss was not observed"); },
    () => null
  );
  const coordinateRetry = await upload("/api/recognize-coordinates", "image", coordinateToken);
  assert.equal(coordinateRetry.status, 200);
  assert.equal((await coordinateRetry.json()).usageCharged, true);
  assert.equal(harness.state.chargeCounts.coordinate, 1);

  const coordinateNewToken = await operation("coordinate");
  const coordinateNew = await upload("/api/recognize-coordinates", "image", coordinateNewToken);
  assert.equal(coordinateNew.status, 200);
  assert.equal(harness.state.chargeCounts.coordinate, 2);

  providerMode = "judge";
  const judgeToken = await operation("judge");
  await upload("/api/analyze-mining-image", "images", judgeToken).then(
    () => { throw new Error("judge response loss was not observed"); },
    () => null
  );
  const judgeRetry = await upload("/api/analyze-mining-image", "images", judgeToken);
  assert.equal(judgeRetry.status, 200);
  assert.equal(harness.state.chargeCounts.judge, 1);

  harness.state.failJudgePersistence = true;
  const failedJudgeToken = await operation("judge");
  const failedJudge = await upload("/api/analyze-mining-image", "images", failedJudgeToken);
  assert.equal(failedJudge.status, 500);
  assert.equal((await failedJudge.json()).reason, "judge_persistence_failed");
  assert.equal(harness.state.chargeCounts.judge, 1);

  await new Promise(resolve => runtimeServer.close(resolve));
  globalThis.fetch = nativeFetch;
  delete globalThis.__GEOKITLAB_UQ01_RUNTIME_REGRESSION_DEPENDENCIES__;
  return {
    coordinateChargeAfterRetry: 1,
    coordinateChargeAfterNewOperation: harness.state.chargeCounts.coordinate,
    judgeChargeAfterRetry: harness.state.chargeCounts.judge,
    judgePersistenceFailureChargeDelta: 0,
    judgeCaseWrites: harness.state.judgeCaseWrites,
    localProviderCalls,
    listenerClosed: runtimeServer.listening === false
  };
}

async function runActualRouteRuntimeRegression() {
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "--uq01-runtime-child"], {
    cwd: process.cwd(),
    env: { ...process.env },
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  const [code] = await once(child, "exit");
  assert.equal(code, 0, `runtime child failed\n${stdout}\n${stderr}`);
  const marker = stdout.match(/UQ01_RUNTIME_RESULT=([^\r\n]+)/);
  assert.ok(marker, `runtime result marker missing\n${stdout}`);
  return JSON.parse(Buffer.from(marker[1], "base64url").toString("utf8"));
}

if (process.argv[2] === "--uq01-runtime-child") {
  const result = await runActualRouteRuntimeChild();
  process.stdout.write(`UQ01_RUNTIME_RESULT=${Buffer.from(JSON.stringify(result)).toString("base64url")}\n`);
  process.exit(0);
}

check("policy_constants", () => {
  assert.equal(FREE_TRIAL_DAILY_MAX, 3);
  assert.equal(FREE_TRIAL_LIFETIME_MAX, 12);
  assert.equal(FREE_DAY_TIMEZONE, "Asia/Shanghai");
});
check("china_midnight_before", () => assert.equal(getFreeDayKey("2026-09-04T15:59:59.999Z"), "2026-09-04"));
check("china_midnight_after", () => assert.equal(getFreeDayKey("2026-09-04T16:00:00.000Z"), "2026-09-05"));
check("server_operation_id", () => assert.equal(isServiceOperationId(createServiceOperationId()), true));
check("invalid_operation_id_rejected", () => assert.equal(isServiceOperationId("client-choice"), false));

const tokenAuthority = createUsageTokenAuthority({ secret: "uq01-regression-signing-secret-value-2026" });
const identityA = tokenAuthority.issueIdentity();
const identityB = tokenAuthority.issueIdentity();
const requestFingerprint = "a".repeat(64);
const coordinateOperationA = tokenAuthority.issueOperation({
  identityToken: identityA.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
});
function mutateTokenPayload(token, mutate) {
  const [prefix, body, signature] = String(token).split(".");
  const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
  const mutatedBody = Buffer.from(JSON.stringify(mutate(payload))).toString("base64url");
  return `${prefix}.${mutatedBody}.${signature}`;
}
function mutateTokenSignature(token) {
  const parts = String(token).split(".");
  const index = parts[2].length - 1;
  parts[2] = `${parts[2].slice(0, index)}${parts[2][index] === "A" ? "B" : "A"}`;
  return parts.join(".");
}
check("server_attested_usage_identity", () => assert.match(identityA.usageIdentity, /^visitor:[0-9a-f-]{36}$/i));
check("signed_operation_resolves", () => assert.equal(tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: coordinateOperationA.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}).serviceOperationId, coordinateOperationA.serviceOperationId));
check("plain_uuid_operation_forgery_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: createServiceOperationId(),
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}), UsageTokenError));
check("forged_verified_marker_rejected", () => assert.throws(() => tokenAuthority.resolveIdentity("verified:visitor:known"), UsageTokenError));
check("usage_cookie_payload_tamper_rejected", () => assert.throws(() => tokenAuthority.resolveIdentity(
  mutateTokenPayload(identityA.token, payload => ({ ...payload, usageIdentity: "visitor:00000000-0000-4000-8000-000000000000" }))
), UsageTokenError));
check("usage_cookie_signature_tamper_rejected", () => assert.throws(() => tokenAuthority.resolveIdentity(
  mutateTokenSignature(identityA.token)
), UsageTokenError));
check("unsigned_usage_cookie_rejected", () => assert.throws(() => tokenAuthority.resolveIdentity(
  Buffer.from(JSON.stringify({ usageIdentity: identityA.usageIdentity })).toString("base64url")
), UsageTokenError));
check("malformed_usage_cookie_rejected", () => assert.throws(() => tokenAuthority.resolveIdentity("uq01.invalid"), UsageTokenError));
check("cross_identity_operation_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityB.token,
  operationToken: coordinateOperationA.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}), UsageTokenError));
check("cross_tool_operation_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: coordinateOperationA.token,
  usageEventType: USAGE_EVENT_TYPE.JUDGE,
  requestFingerprint
}), UsageTokenError));
check("cross_payload_operation_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: coordinateOperationA.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint: "b".repeat(64)
}), UsageTokenError));
check("operation_payload_tamper_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: mutateTokenPayload(coordinateOperationA.token, payload => ({ ...payload, serviceOperationId: createServiceOperationId() })),
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}), UsageTokenError));
check("operation_signature_tamper_rejected", () => assert.throws(() => tokenAuthority.resolveOperation({
  identityToken: identityA.token,
  operationToken: mutateTokenSignature(coordinateOperationA.token),
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}), UsageTokenError));

let expiryClock = Date.parse("2026-09-05T00:00:00.000Z");
const expiryAuthority = createUsageTokenAuthority({
  secret: "uq01-expiry-regression-signing-secret-value",
  now: () => expiryClock,
  identityTtlMs: 2_000,
  operationTtlMs: 500
});
const expiringIdentity = expiryAuthority.issueIdentity();
const expiringOperation = expiryAuthority.issueOperation({
  identityToken: expiringIdentity.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
});
expiryClock += 501;
check("expired_operation_token_rejected", () => assert.throws(() => expiryAuthority.resolveOperation({
  identityToken: expiringIdentity.token,
  operationToken: expiringOperation.token,
  usageEventType: USAGE_EVENT_TYPE.COORDINATE,
  requestFingerprint
}), UsageTokenError));
expiryClock += 1_500;
check("expired_usage_identity_rejected", () => assert.throws(() => expiryAuthority.resolveIdentity(expiringIdentity.token), UsageTokenError));

const goodResult = {
  finalizedCoordinateResult: {
    resultId: "result-1",
    resultRevision: 1,
    geometryHash: "a".repeat(64),
    geometry: { type: "Polygon", coordinates: [[[8, 11], [9, 11], [9, 12], [8, 11]]] },
    decisionState: "REVIEW_PENDING",
    qualityStatus: "REVIEW",
    kmlReady: true
  }
};
check("review_required_usable_charges", () => assert.equal(isCoordinateResultChargeable(goodResult), true));
check("finalizer_missing_zero_charge", () => assert.equal(isCoordinateResultChargeable({}), false));
check("hard_blocked_zero_charge", () => assert.equal(isCoordinateResultChargeable({ finalizedCoordinateResult: { ...goodResult.finalizedCoordinateResult, decisionState: "BLOCKED" } }), false));
check("invalid_geometry_zero_charge", () => assert.equal(isCoordinateResultChargeable({ finalizedCoordinateResult: { ...goodResult.finalizedCoordinateResult, geometry: null } }), false));
check("judge_persisted_charges", () => assert.equal(isJudgeResultChargeable({ normalizedResult: "valid", persistedCaseId: "case-1" }), true));
check("judge_persistence_failure_zero_charge", () => assert.equal(isJudgeResultChargeable({ normalizedResult: "valid", persistedCaseId: null }), false));

const state = new Map();
const events = new Map();
const paid = { coordinate: 2, judge: 1 };
const fakeSupabase = {
  async rpc(name, args) {
    const identity = args.p_usage_identity;
    const day = args.p_free_day;
    const current = state.get(identity) || { day, daily: 0, lifetime: 0 };
    if (current.day !== day) { current.day = day; current.daily = 0; }
    if (name === "uq01_get_usage_quota") return { data: payload(current), error: null };
    const key = `${identity}:${args.p_service_operation_id}:${args.p_usage_event_type}`;
    if (events.has(key)) return { data: { ...events.get(key), idempotent: true }, error: null };
    let source = "";
    const product = args.p_usage_event_type === USAGE_EVENT_TYPE.JUDGE ? "judge" : "coordinate";
    if (current.daily < 3 && current.lifetime < 12) { current.daily += 1; current.lifetime += 1; source = "free"; }
    else if (paid[product] > 0) { paid[product] -= 1; source = `paid_${product === "coordinate" ? "convert" : "judge"}`; }
    else return { data: { success: false, reason: "limit_exceeded", ...payload(current) }, error: null };
    state.set(identity, current);
    const result = { success: true, reason: "ok", charge_source: source, event_id: key, ...payload(current) };
    events.set(key, result);
    return { data: result, error: null };
  }
};
function payload(s) {
  return {
    free_daily_used: s.daily,
    free_lifetime_used: s.lifetime,
    free_daily_remaining: Math.max(0, 3 - s.daily),
    free_lifetime_remaining: Math.max(0, 12 - s.lifetime),
    paid_convert_count: paid.coordinate,
    paid_judge_count: paid.judge
  };
}
const ledger = new SupabaseUsageLedger({ supabase: fakeSupabase });
const identity = "visitor-uq01";
const op1 = createServiceOperationId();
const first = await ledger.consume({ usageIdentity: identity, serviceOperationId: op1, usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:00:00Z" });
const duplicate = await Promise.all(Array.from({ length: 8 }, () => ledger.consume({ usageIdentity: identity, serviceOperationId: op1, usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:00:00Z" })));
check("first_successful_result_charges_once", () => assert.equal(first.quota.free_lifetime_used, 1));
check("duplicate_request_exactly_once", () => assert.equal(duplicate.every(item => item.quota.free_lifetime_used === 1), true));
check("concurrent_duplicate_exactly_once", () => assert.equal(events.size, 1));
check("response_loss_retry_exactly_once", () => assert.equal(duplicate[0].idempotent, true));
check("server_retry_exactly_once", () => assert.equal(duplicate.at(-1).idempotent, true));

let usageTotal = first.quota.free_lifetime_used;
for (const action of ["first_kml", "second_kml", "manual_edit_kml", "second_edit_kml", "revert_kml", "map_open"]) {
  check(`${action}_zero_additional_charge`, () => assert.equal(usageTotal, 1));
}
const op2 = createServiceOperationId();
const sameImageNewOperation = await ledger.consume({ usageIdentity: identity, serviceOperationId: op2, usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:01:00Z" });
check("same_image_new_operation_charges", () => assert.equal(sameImageNewOperation.quota.free_lifetime_used, 2));
const judge = await ledger.consume({ usageIdentity: identity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.JUDGE, now: "2026-09-04T12:02:00Z" });
check("shared_free_wallet_coordinate_and_judge", () => assert.equal(judge.quota.free_daily_remaining, 0));
const paidCoordinate = await ledger.consume({ usageIdentity: identity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:03:00Z" });
check("paid_coordinate_preserved_after_free", () => assert.equal(paidCoordinate.source, "paid_convert"));
const nextDay = await ledger.read(identity, "coordinate", "2026-09-04T16:00:00Z");
check("daily_reset_preserves_lifetime", () => assert.equal(nextDay.quota.free_lifetime_used, 3));

const lifetimeIdentity = "visitor-lifetime-boundary";
state.set(lifetimeIdentity, { day: "2026-09-04", daily: 2, lifetime: 11 });
const lifetimeTwelfth = await ledger.consume({ usageIdentity: lifetimeIdentity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:00:00Z" });
const lifetimeNextDay = await ledger.read(lifetimeIdentity, "coordinate", "2026-09-04T16:00:00Z");
check("lifetime_twelfth_consumed", () => assert.equal(lifetimeTwelfth.quota.free_lifetime_used, 12));
check("lifetime_12_next_day_still_exhausted", () => {
  assert.equal(lifetimeNextDay.quota.free_daily_remaining, 3);
  assert.equal(lifetimeNextDay.quota.free_lifetime_remaining, 0);
  assert.equal(lifetimeNextDay.quota.free_shared_remaining, 0);
  assert.equal(lifetimeNextDay.quota.freeTrial.exhausted, true);
});

const reverseIdentity = "visitor-judge-judge-coordinate";
await ledger.consume({ usageIdentity: reverseIdentity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.JUDGE, now: "2026-09-04T12:00:00Z" });
await ledger.consume({ usageIdentity: reverseIdentity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.JUDGE, now: "2026-09-04T12:01:00Z" });
const reverseThird = await ledger.consume({ usageIdentity: reverseIdentity, serviceOperationId: createServiceOperationId(), usageEventType: USAGE_EVENT_TYPE.COORDINATE, now: "2026-09-04T12:02:00Z" });
check("judge_2_coordinate_1_shared_daily", () => assert.equal(reverseThird.quota.free_daily_remaining, 0));
const eventsBeforeReopen = events.size;
check("reopen_existing_result_zero_charge", () => assert.equal(events.size, eventsBeforeReopen));

const runtimeState = { charged: new Set(), total: 0 };
let destroyFirstResponse = true;
const runtimeServer = http.createServer(async (req, res) => {
  let body = "";
  for await (const chunk of req) body += chunk;
  const parsed = JSON.parse(body || "{}");
  const resolved = tokenAuthority.resolveOperation({
    identityToken: String(req.headers.cookie || "").replace(/^geokit_usage_identity_v1=/, ""),
    operationToken: req.headers["x-usage-operation-token"],
    usageEventType: USAGE_EVENT_TYPE.COORDINATE,
    requestFingerprint: parsed.requestFingerprint
  });
  if (!runtimeState.charged.has(resolved.serviceOperationId)) {
    runtimeState.charged.add(resolved.serviceOperationId);
    runtimeState.total += 1;
  }
  if (destroyFirstResponse) {
    destroyFirstResponse = false;
    req.socket.destroy();
    return;
  }
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ success: true, serviceOperationId: resolved.serviceOperationId, totalCharge: runtimeState.total }));
});
await new Promise(resolve => runtimeServer.listen(0, "127.0.0.1", resolve));
const runtimeUrl = `http://127.0.0.1:${runtimeServer.address().port}/recognize`;
const runtimeHeaders = {
  "Content-Type": "application/json",
  Cookie: `geokit_usage_identity_v1=${identityA.token}`,
  "x-usage-operation-token": coordinateOperationA.token
};
await fetch(runtimeUrl, { method: "POST", headers: runtimeHeaders, body: JSON.stringify({ requestFingerprint }) }).catch(() => null);
const runtimeRetry = await fetch(runtimeUrl, { method: "POST", headers: runtimeHeaders, body: JSON.stringify({ requestFingerprint }) }).then(response => response.json());
await new Promise(resolve => runtimeServer.close(resolve));
check("http_response_loss_retry_gate", () => assert.equal(runtimeRetry.serviceOperationId, coordinateOperationA.serviceOperationId));
check("http_response_loss_retry_total_charge", () => assert.equal(runtimeRetry.totalCharge, 1));
const newOperationSameImage = tokenAuthority.issueOperation({ identityToken: identityA.token, usageEventType: USAGE_EVENT_TYPE.COORDINATE, requestFingerprint });
check("same_image_new_operation_identity", () => assert.notEqual(newOperationSameImage.serviceOperationId, coordinateOperationA.serviceOperationId));

const actualRouteRuntime = await runActualRouteRuntimeRegression();
check("coordinate_http_response_loss_actual_route", () => assert.equal(actualRouteRuntime.coordinateChargeAfterRetry, 1));
check("coordinate_same_file_new_actual_operation_charges", () => assert.equal(actualRouteRuntime.coordinateChargeAfterNewOperation, 2));
check("judge_http_response_loss_actual_route", () => assert.equal(actualRouteRuntime.judgeChargeAfterRetry, 1));
check("judge_persistence_failure_actual_route_zero_charge", () => assert.equal(actualRouteRuntime.judgePersistenceFailureChargeDelta, 0));
check("actual_route_runtime_service_stopped", () => assert.equal(actualRouteRuntime.listenerClosed, true));

const shared = toSharedQuotaPayload({ free_daily_remaining: 2, free_lifetime_remaining: 8, paid_convert_count: 4, paid_judge_count: 5 });
check("shared_read_model", () => {
  assert.deepEqual(shared.freeTrial, { todayRemaining: 2, lifetimeRemaining: 8, exhausted: false });
  assert.deepEqual(shared.paid, { coordinateRemaining: 4, judgeRemaining: 5 });
  assert.equal(shared.convert_remaining, 6);
  assert.equal(shared.judge_remaining, 7);
});

const serverSource = fs.readFileSync("server.js", "utf8");
const html = fs.readFileSync("index.html", "utf8");
const admin = fs.readFileSync("admin.html", "utf8");
const pricing = fs.readFileSync("pricing-config.js", "utf8");
const migrationPath = fs.readdirSync("supabase/migrations").find(name => name.endsWith("_uq01_usage_policy_v1.sql"));
assert.ok(migrationPath);
const migration = fs.readFileSync(`supabase/migrations/${migrationPath}`, "utf8");
check("coordinate_charge_after_finalizer", () => {
  assert.match(serverSource, /!isCoordinateResultChargeable\(body\)/);
  assert.match(serverSource, /sendUsageAwareCoordinateResponse/);
  assert.match(serverSource, /reason: "deferred_until_finalized_response"/);
});
check("judge_persistence_before_charge", () => assert.ok(serverSource.indexOf("isJudgeResultChargeable") < serverSource.indexOf("usageEventType: USAGE_EVENT_TYPE.JUDGE")));
check("client_consume_authority_removed", () => assert.match(serverSource, /client_usage_consume_forbidden/));
check("server_usage_session_endpoint", () => assert.match(serverSource, /app\.post\("\/api\/usage\/session"/));
check("server_operation_token_endpoint", () => assert.match(serverSource, /app\.post\("\/api\/usage\/operation"/));
check("quota_uses_attested_identity", () => assert.match(serverSource, /app\.get\("\/api\/usage\/quota"[\s\S]*?usageIdentity = resolveUsageIdentity\(req\)/));
check("forged_visitor_fields_cannot_select_ledger", () => {
  assert.match(serverSource, /usageIdentity = operation\.usageIdentity;[\s\S]*?visitorId = usageIdentity;/);
  const quotaRoute = serverSource.match(/app\.get\("\/api\/usage\/quota"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.doesNotMatch(quotaRoute, /req\.(query|body)|x-visitor-id/);
});
check("coordinate_uses_attested_operation", () => assert.match(serverSource, /resolveUsageOperation\(req, USAGE_EVENT_TYPE\.COORDINATE, req\.file\)/));
check("judge_uses_attested_operation", () => assert.match(serverSource, /resolveUsageOperation\(req, USAGE_EVENT_TYPE\.JUDGE, firstFile\)/));
check("kml_has_zero_usage_call", () => assert.doesNotMatch(html.match(/async function downloadKmlInternal[\s\S]*?\n    }\n\n    /)?.[0] || "", /consumeUsage\(/));
check("admin_shared_free_model", () => { assert.match(admin, /pricingFreeDailyMax/); assert.match(admin, /pricingFreeLifetimeMax/); assert.doesNotMatch(admin, /id="quotaFree(Convert|Judge)"/); });
check("admin_legacy_free_save_authority_removed", () => {
  const quotaSave = admin.match(/\/quota`, \{[\s\S]*?\n\s*\}\);/)?.[0] || "";
  assert.doesNotMatch(quotaSave, /free_(convert|judge)_count/);
});
check("client_shared_free_model", () => { assert.match(html, /freeTrial\?\.todayRemaining/); assert.match(html, /freeTrial\?\.lifetimeRemaining/); assert.match(html, /freeTrial\?\.exhausted/); });
check("client_operation_retry_token", () => { assert.match(html, /pendingUsageOperations/); assert.match(html, /x-usage-operation-token/); });
check("lifetime_exhausted_copy", () => {
  assert.equal(html.includes("\\u514d\\u8d39\\u4f53\\u9a8c\\u5df2\\u7528\\u5b8c"), true);
  assert.equal(html.includes("\\u5f00\\u901a\\u670d\\u52a1\\u540e\\u53ef\\u7ee7\\u7eed\\u4f7f\\u7528"), true);
});
check("pricing_shared_free_model", () => { assert.match(pricing, /dailyMax: 3/); assert.match(pricing, /lifetimeMax: 12/); });
check("paid_pricing_unchanged", () => {
  assert.match(pricing, /price: 99,[\s\S]*judgeCount: 50,[\s\S]*convertCount: 50/);
  assert.equal((pricing.match(/price: 19/g) || []).length, 2);
});
check("atomic_rpc", () => { assert.match(migration, /pg_advisory_xact_lock/); assert.match(migration, /unique \(usage_identity, service_operation_id, usage_event_type\)/i); });
check("rpc_execute_privileges", () => { assert.match(migration, /revoke execute[\s\S]*from public, anon, authenticated/i); assert.match(migration, /grant execute[\s\S]*to service_role/i); });
check("rls_enabled", () => { assert.match(migration, /usage_charge_events enable row level security/i); assert.match(migration, /usage_free_trial_state enable row level security/i); });
check("security_definer_safe", () => { assert.match(migration, /security definer[\s\S]*set search_path = ''/i); assert.match(migration, /public\.usage_charge_events/); });
check("legacy_logs_not_reinterpreted", () => assert.doesNotMatch(migration, /from public\.usage_logs/i));

console.log(JSON.stringify({ suite: "uq01-usage-policy-v1-regression", passed: cases.length, cases }, null, 2));
