import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { COORDINATE_THREE_CASE_RC, readCoordinateThreeCaseRcConfig } from "../server/recognition/coordinate-three-case-rc-admission.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifestPath = path.join(root, "docs", "coordinate-three-case-rc-manifest.json");
const migrationPath = path.join(root, "supabase", "migrations", "20261012090000_coordinate_three_case_rc_r7.sql");
const manifestBytes = fs.readFileSync(manifestPath);
const manifest = JSON.parse(manifestBytes.toString("utf8"));
const digest = crypto.createHash("sha256").update(manifestBytes).digest("hex");
const results = [];
const check = (name, fn) => {
  try { fn(); results.push({ name, pass: true }); }
  catch (error) { results.push({ name, pass: false, error: error?.message || String(error) }); }
};

check("R7 run and batch are frozen", () => {
  assert.equal(COORDINATE_THREE_CASE_RC.runId, "coordinate-three-case-rc-20261011-r7");
  assert.equal(COORDINATE_THREE_CASE_RC.batchId, "coordinate-three-case-rc-20261011-v7");
  assert.equal(manifest.runId, COORDINATE_THREE_CASE_RC.runId);
  assert.equal(manifest.batchId, COORDINATE_THREE_CASE_RC.batchId);
});
check("manifest hash is exact", () => assert.equal(digest, COORDINATE_THREE_CASE_RC.manifestSha256));
check("three image identities are preserved", () => {
  assert.deepEqual(manifest.cases.map(item => item.imageSha256), [
    "41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2",
    "af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004",
    "4567ce9889c47d65414b19e543c06b5322b86c53b76bb53e87da761bc0988e1d"
  ]);
});
check("Indonesia historical budget does not reset", () => {
  assert.equal(manifest.indonesiaBudget.historicalBatchId, "indonesia-ab-20261010-v1");
  assert.equal(manifest.indonesiaBudget.maxAttempts, 6);
  assert.equal(manifest.indonesiaBudget.totalCostLimitCny, 5);
});
check("shared budget extends same pool from two to four", () => {
  assert.equal(manifest.nonIndonesiaBudget.historicalBatchId, "coordinate-three-case-rc-20261011-v1");
  assert.equal(manifest.nonIndonesiaBudget.maxDispatched, 4);
  assert.equal(manifest.nonIndonesiaBudget.totalCostLimitCny, 4);
});
check("no retries or direct OCR calls", () => {
  assert.equal(manifest.automaticRetries, 0);
  assert.equal(manifest.directOcrCalls, 0);
  assert.ok(manifest.cases.every(item => item.maxVisionCalls === 1));
});
check("runtime config requires exact R7 identity", () => {
  const config = readCoordinateThreeCaseRcConfig({
    COORDINATE_THREE_CASE_RC_ENABLED: "true",
    COORDINATE_THREE_CASE_RC_RUN_ID: manifest.runId,
    COORDINATE_THREE_CASE_RC_WINDOW_SECONDS: "1800",
    COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: "600",
    SUPABASE_URL: "https://thjojitdafxfarhxhoyo.supabase.co",
    RC_SUPABASE_PROJECT_REF: "thjojitdafxfarhxhoyo",
    RENDER_SERVICE_NAME: "coordinate-kml-tool-rc",
    ALIYUN_VISION_MODEL: "qwen3.8-flash",
    ALIYUN_BASE_URL: "https://dashscope.aliyuncs.com/compatible-mode/v1"
  });
  assert.equal(config.ready, true);
});
check("migration is one-shot, extends the existing shared pool, and remains service-role-only", () => {
  const sql = fs.readFileSync(migrationPath, "utf8");
  assert.match(sql, /where batch_id = 'coordinate-three-case-rc-20261011-v1'[\s\S]*max_dispatched = 2[\s\S]*total_cost_limit_cny = 2\.000000/u);
  assert.match(sql, /'coordinate-three-case-rc-20261011-r7', 'coordinate-three-case-rc-20261011-v7'/u);
  assert.match(sql, /revoke all on function public\.activate_coordinate_three_case_rc_run[\s\S]*from public, anon, authenticated/u);
  assert.match(sql, /grant execute on function public\.read_coordinate_three_case_rc_status[\s\S]*to service_role/u);
  assert.doesNotMatch(sql, /grant execute[\s\S]*to anon|grant execute[\s\S]*to authenticated/u);
});
check("migration preserves the approved historical snapshot and bounded shared constraints", () => {
  const sql = fs.readFileSync(migrationPath, "utf8");
  assert.match(sql, /v_legacy\.dispatched_attempts is distinct from 3/u);
  assert.match(sql, /v_legacy\.reserved_cost_cny is distinct from 2\.448000/u);
  assert.match(sql, /v_shared\.dispatched_count is distinct from 2/u);
  assert.match(sql, /v_shared\.reserved_cost_cny is distinct from 2\.000000/u);
  assert.match(sql, /check \(max_dispatched in \(2, 4\)\)/u);
  assert.match(sql, /check \(total_cost_limit_cny in \(2\.000000, 4\.000000\)\)/u);
  assert.match(sql, /from pg_catalog\.pg_constraint c[\s\S]*pg_catalog\.pg_get_expr/u);
  assert.match(sql, /coalesce\(cardinality\(v_max_constraints\), 0\) <> 1/u);
  assert.match(sql, /coalesce\(cardinality\(v_cost_constraints\), 0\) <> 1/u);
  assert.match(sql, /RC_R7_SHARED_BUDGET_CONSTRAINT_IDENTITY_MISMATCH/u);
  assert.doesNotMatch(sql, /drop constraint coordinate_three_case_rc_non_indonesia_budget_/u);
  assert.doesNotMatch(sql, /drop table|truncate/u);
});

const failed = results.filter(item => !item.pass);
console.log(JSON.stringify({ suite: "Coordinate Three-Case RC R7 Binding", passed: results.length - failed.length, total: results.length, cases: results }, null, 2));
if (failed.length) process.exitCode = 1;
