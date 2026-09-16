import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS,
  createCoordinateUsageRuntimeDiagnostic,
  parseCoordinateUsageSealKey
} from "../server/coordinate-usage-atomicity.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const expectedProjectRef = "xyiffmpzdtmurmnsibdt";
const supabaseUrl = `https://${expectedProjectRef}.supabase.co`;
const sealKey = parseCoordinateUsageSealKey(Buffer.alloc(32, 11).toString("base64"));
const requestId = "11111111-1111-4111-8111-111111111111";
const sessionBindingSha256 = "2".repeat(64);
const expectedStatuses = [
  "READY",
  "SUPABASE_URL_MISSING",
  "SUPABASE_PROJECT_REF_MISMATCH",
  "SERVICE_ROLE_KEY_MISSING",
  "SEAL_KEY_INVALID",
  "RPC_SCHEMA_CACHE_ERROR",
  "RPC_PERMISSION_ERROR",
  "RPC_AUTH_ERROR",
  "RPC_NETWORK_ERROR",
  "RPC_UNKNOWN_ERROR"
];
const allowedStatuses = new Set(expectedStatuses);
const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

function createDiagnostic(overrides = {}) {
  return createCoordinateUsageRuntimeDiagnostic({
    supabase: {
      async rpc(name, params) {
        assert.equal(name, "get_coordinate_recognition_commit_state");
        assert.equal(params.p_recognition_request_id, requestId);
        assert.equal(params.p_user_id, "synthetic-runtime-diagnostic");
        assert.equal(params.p_session_binding_sha256, sessionBindingSha256);
        return { data: [{ result: "NOT_FOUND" }], error: null };
      }
    },
    supabaseUrl,
    serviceRoleKeyPresent: true,
    sealKey,
    expectedProjectRef,
    timeoutMs: 100,
    randomRequestId: () => requestId,
    randomSessionBinding: () => sessionBindingSha256,
    ...overrides
  });
}

test("diagnostic status vocabulary is exact and closed", () => {
  assert.deepEqual(
    Object.values(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS).sort(),
    [...expectedStatuses].sort()
  );
});

async function assertStatus(expected, overrides = {}) {
  const diagnostic = createDiagnostic(overrides);
  const result = await diagnostic.runOnce();
  assert.equal(result.status, expected);
  assert.equal(result.probeComplete, true);
  assert.ok(allowedStatuses.has(result.status));
  return { diagnostic, result };
}

test("successful synthetic read-only RPC reports READY", async () => {
  const { result } = await assertStatus(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.READY);
  assert.equal(result.productionProjectRefMatch, true);
});

test("configuration failures fail closed before RPC", async () => {
  const cases = [
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SUPABASE_URL_MISSING, { supabaseUrl: "" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SUPABASE_PROJECT_REF_MISMATCH, { supabaseUrl: "https://aaaaaaaaaaaaaaaaaaaa.supabase.co" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SERVICE_ROLE_KEY_MISSING, { serviceRoleKeyPresent: false }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SEAL_KEY_INVALID, { sealKey: null }]
  ];
  for (const [expected, override] of cases) {
    let rpcCount = 0;
    await assertStatus(expected, {
      ...override,
      supabase: { async rpc() { rpcCount += 1; throw new Error("must_not_run"); } }
    });
    assert.equal(rpcCount, 0);
  }
});

test("RPC errors map only to fixed redacted status enums", async () => {
  const cases = [
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_SCHEMA_CACHE_ERROR, { code: "PGRST002", message: "sensitive schema text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_SCHEMA_CACHE_ERROR, { code: "PGRST106", details: "sensitive schema text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_SCHEMA_CACHE_ERROR, { code: "PGRST202", hint: "sensitive function text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_PERMISSION_ERROR, { code: "42501", message: "sensitive grant text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_AUTH_ERROR, { code: "PGRST301", message: "sensitive auth text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_AUTH_ERROR, { status: 403, message: "sensitive auth text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR, { code: "ETIMEDOUT", message: "sensitive network text" }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR, { code: "UNLISTED", message: "sensitive unknown text" }]
  ];
  for (const [expected, error] of cases) {
    const { result } = await assertStatus(expected, {
      supabase: { async rpc() { return { data: null, error }; } }
    });
    const serialized = JSON.stringify(result);
    assert.equal(serialized.includes("sensitive"), false);
    assert.deepEqual(Object.keys(result).sort(), ["probeComplete", "productionProjectRefMatch", "status"]);
  }

  const realisticResponses = [
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_AUTH_ERROR, { status: 403, error: { code: "", message: "sensitive outer auth text" } }],
    [COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR, { status: 0, error: { code: "", message: "sensitive outer network text" } }]
  ];
  for (const [expected, response] of realisticResponses) {
    const { result } = await assertStatus(expected, {
      supabase: { async rpc() { return { data: null, ...response }; } }
    });
    assert.equal(JSON.stringify(result).includes("sensitive"), false);
  }
});

test("thrown network failures and timeouts stay redacted", async () => {
  const thrown = new TypeError("secret-bearing network failure");
  const first = await assertStatus(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR, {
    supabase: { async rpc() { throw thrown; } }
  });
  assert.equal(JSON.stringify(first.result).includes(thrown.message), false);

  await assertStatus(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR, {
    timeoutMs: 5,
    supabase: { rpc() { return new Promise(() => {}); } }
  });
});

test("synthetic identity generation failures remain a fixed completed status", async () => {
  await assertStatus(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR, {
    randomRequestId() { throw new Error("sensitive entropy failure"); }
  });
});

test("runOnce performs at most one RPC and returns one cached snapshot", async () => {
  let rpcCount = 0;
  const diagnostic = createDiagnostic({
    supabase: {
      async rpc() {
        rpcCount += 1;
        return { data: [{ result: "NOT_FOUND" }], error: null };
      }
    }
  });
  const [first, second, third] = await Promise.all([
    diagnostic.runOnce(),
    diagnostic.runOnce(),
    diagnostic.runOnce()
  ]);
  assert.equal(rpcCount, 1);
  assert.strictEqual(first, second);
  assert.strictEqual(second, third);
  assert.strictEqual(diagnostic.snapshot(), first);
});

test("diagnostic uses only the read-only recovery-state RPC", async () => {
  const rpcNames = [];
  const diagnostic = createDiagnostic({
    supabase: {
      async rpc(name, params) {
        rpcNames.push(name);
        assert.match(params.p_recognition_request_id, /^[0-9a-f-]{36}$/);
        assert.equal(params.p_user_id, "synthetic-runtime-diagnostic");
        assert.match(params.p_session_binding_sha256, /^[a-f0-9]{64}$/);
        return { data: [{ result: "NOT_FOUND" }], error: null };
      }
    }
  });
  await diagnostic.runOnce();
  assert.deepEqual(rpcNames, ["get_coordinate_recognition_commit_state"]);
  assert.equal(rpcNames.some(name => /prepare|commit(?!_state)/i.test(name)), false);
});

test("version integration exposes only fixed diagnostic fields and awaits one startup probe", async () => {
  const server = await readFile(path.join(root, "server.js"), "utf8");
  assert.match(server, /await coordinateUsageRuntimeDiagnostic\.runOnce\(\);\s*\r?\n\s*app\.listen/);
  assert.match(server, /coordinateUsageAtomicityRuntimeDiagnostic:\s*\{/);
  assert.match(server, /probeComplete: coordinateUsageDiagnostic\.probeComplete === true/);
  assert.match(server, /productionProjectRefMatch: coordinateUsageDiagnostic\.productionProjectRefMatch === true/);
  assert.doesNotMatch(server, /coordinateUsageAtomicityRuntimeDiagnostic:\s*\{[^}]*?(?:supabaseUrl|serviceRoleKey|sealKey|error|message|details|hint)/s);
});

let passed = 0;
for (const { name, fn } of tests) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}
console.log(`Coordinate usage runtime diagnostic regression: ${passed}/${tests.length} PASS`);
