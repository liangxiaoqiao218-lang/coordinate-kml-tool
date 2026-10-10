import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { COORDINATE_THREE_CASE_RC } from "../server/recognition/coordinate-three-case-rc-admission.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = relativePath => fs.readFileSync(path.join(root, relativePath), "utf8");
const manifestPath = path.join(root, "docs/coordinate-three-case-rc-manifest.json");
const manifestBytes = fs.readFileSync(manifestPath);
const manifest = JSON.parse(manifestBytes.toString("utf8"));
const client = read("scripts/coordinate-three-case-rc-client.ps1");
const admission = read("server/recognition/coordinate-three-case-rc-admission.js");
const migration = read("supabase/migrations/20261011051500_coordinate_three_case_rc_r3.sql");
const artifactHelper = read("scripts/coordinate-three-case-rc-result-artifact.ps1");

const expected = Object.freeze({
  runId: "coordinate-three-case-rc-20261011-r3",
  batchId: "coordinate-three-case-rc-20261011-v3",
  model: "qwen3.8-flash",
  manifestSha256: "f511490c8e0b28d18dfd1867a2d50c28890e6011065c9b478bd01fd3ba04b7fe"
});

const results = [];
function test(name, action) {
  try {
    action();
    results.push({ name, status: "PASS" });
  } catch (error) {
    results.push({ name, status: "FAIL", code: error?.message || String(error) });
  }
}

test("manifest bytes match the frozen R3 digest", () => {
  const actual = createHash("sha256").update(manifestBytes).digest("hex");
  assert.equal(actual, expected.manifestSha256);
});

test("manifest admission client and migration share exact R3 identity", () => {
  assert.equal(manifest.runId, expected.runId);
  assert.equal(manifest.batchId, expected.batchId);
  assert.equal(COORDINATE_THREE_CASE_RC.runId, expected.runId);
  assert.equal(COORDINATE_THREE_CASE_RC.batchId, expected.batchId);
  assert.equal(COORDINATE_THREE_CASE_RC.manifestSha256, expected.manifestSha256);
  for (const source of [client, admission, migration]) {
    assert.match(source, new RegExp(expected.runId, "u"));
    assert.match(source, new RegExp(expected.batchId, "u"));
  }
});

test("R3 migration inserts exactly the three frozen cases", () => {
  const rows = migration.match(/\('coordinate-three-case-rc-20261011-r3',\s*'(indonesia|mgrs|bftm)'/gu) || [];
  assert.equal(rows.length, 3);
  assert.equal(new Set(rows).size, 3);
  assert.match(migration, /41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2/u);
  assert.match(migration, /af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004/u);
  assert.match(migration, /4567ce9889c47d65414b19e543c06b5322b86c53b76bb53e87da761bc0988e1d/u);
});

test("R3 window remains 30 minutes with one active and three total dispatches", () => {
  assert.match(migration, /3,\s*3,\s*1,\s*600,\s*1800/u);
  assert.equal(manifest.runWindowSeconds, 1800);
  assert.equal(manifest.claimTtlSeconds, 600);
  assert.equal(manifest.maxConcurrentDispatches, 1);
  assert.equal(manifest.cases.reduce((sum, entry) => sum + entry.maxVisionCalls, 0), 3);
});

test("R3 creates no replacement budget pool", () => {
  assert.doesNotMatch(migration, /create\s+table/iu);
  assert.doesNotMatch(migration, /insert\s+into\s+private\.coordinate_three_case_rc_non_indonesia_budget/iu);
  assert.doesNotMatch(migration, /insert\s+into\s+private\.indonesia_structured_b_batches/iu);
});

test("R3 dispatch and activation bind to the two historical ledgers", () => {
  assert.match(migration, /batch_id = 'indonesia-ab-20261010-v1'/u);
  assert.match(migration, /batch_id = 'coordinate-three-case-rc-20261011-v1'/u);
  assert.match(migration, /v_legacy\.dispatched_attempts >= v_legacy\.max_attempts/u);
  assert.match(migration, /v_shared\.dispatched_count \+ 2 > v_shared\.max_dispatched/u);
});

test("closed R1 and R2 runs are not reopened", () => {
  assert.doesNotMatch(migration, /update\s+private\.coordinate_three_case_rc_runs[\s\S]{0,300}coordinate-three-case-rc-20261011-r[12]/iu);
  assert.doesNotMatch(migration, /insert[\s\S]{0,200}coordinate-three-case-rc-20261011-r[12]/iu);
});

test("R3 replaces only four budget-bound RPCs and locks them to service_role", () => {
  assert.equal((migration.match(/create or replace function public\.(?:activate|dispatch|settle|read)_coordinate_three_case_rc(?:_run|_status)?\(/gu) || []).length, 4);
  assert.equal((migration.match(/security definer set search_path = ''/gu) || []).length, 4);
  assert.equal((migration.match(/revoke all on function/gu) || []).length, 4);
  assert.equal((migration.match(/grant execute on function/gu) || []).length, 4);
});

test("model and one-call settlement remain exact with no retry or direct OCR", () => {
  assert.equal(manifest.model, expected.model);
  assert.equal(COORDINATE_THREE_CASE_RC.model, expected.model);
  assert.equal(COORDINATE_THREE_CASE_RC.automaticRetries, 0);
  assert.equal(COORDINATE_THREE_CASE_RC.directOcrCalls, 0);
  assert.match(migration, /p_model is distinct from 'qwen3\.8-flash'/u);
  assert.match(migration, /p_provider_call_count not in \(0, 1\)/u);
  assert.match(migration, /v_case\.dispatch_count <> 0/u);
});

test("client preserves actual Provider count and exact model match", () => {
  assert.match(client, /modelMatch = \[string\]\$result\.model -eq \$expectedModel/u);
  assert.match(client, /providerCallCount = \$providerCalls\.value/u);
  assert.match(client, /providerCallCountKnown = \$providerCalls\.known/u);
  assert.match(client, /providerCallLimitExceeded = \$providerCalls\.limitExceeded/u);
  assert.doesNotMatch(client, /\[Math\]::Min\([^\n]*providerCallCount/iu);
});

test("client writes an independently verified retained result after each terminal case", () => {
  const recordIndex = client.indexOf("New-CoordinateThreeCaseValidatedResultRecord");
  const artifactIndex = client.indexOf("Write-CoordinateThreeCaseValidatedResultArtifact", recordIndex);
  assert.ok(recordIndex >= 0 && artifactIndex > recordIndex);
  assert.match(client, /\.validated-results\.json/u);
  assert.match(artifactHelper, /Move-Item -LiteralPath \$temporary -Destination \$resolved -Force/u);
  assert.match(artifactHelper, /Get-FileHash -LiteralPath \$resolved -Algorithm SHA256/u);
});

test("retained result helper is explicit-whitelist and omits forbidden raw material", () => {
  for (const forbidden of ["claimToken", "jobAccessToken", "rawProviderResponse", "Authorization", "sourceCandidates", "base64"]) {
    assert.doesNotMatch(artifactHelper, new RegExp(`^[^#\\r\\n]*${forbidden}`, "imu"));
  }
  assert.match(artifactHelper, /finalizedResult =/u);
  assert.match(artifactHelper, /geometryHash = \$safeGeometryHash/u);
  assert.match(artifactHelper, /consumerIdentityMatch = \$consumerIdentityMatch/u);
});

const failed = results.filter(result => result.status !== "PASS");
const summary = {
  suite: "COORDINATE_THREE_CASE_RC_R3_BINDING",
  passed: results.length - failed.length,
  failed: failed.length,
  total: results.length,
  providerCalls: 0,
  httpRequests: 0,
  cases: results
};
console.log(JSON.stringify(summary, null, 2));
if (failed.length > 0) process.exitCode = 1;
