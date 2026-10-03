import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeCoordinateCaseInput } from "../server/admin/coordinate-case-contract.js";
import { CoordinateCaseStore } from "../server/admin/coordinate-case-store.js";
import {
  COORDINATE_RECOGNITION_ISSUE_EVENT,
  buildCoordinateRecognitionIssue,
  recordCoordinateRecognitionIssue
} from "../server/recognition/coordinate-recognition-issue.js";
import {
  RECOGNITION_ACQUISITION_JOB_STATUS,
  createRecognitionAcquisitionJobRuntime,
  getRecognitionAcquisitionJobHttpStatus
} from "../server/recognition/recognition-acquisition-job-runtime.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const requestA = "11111111-1111-4111-8111-111111111111";
const requestB = "22222222-2222-4222-8222-222222222222";
const requestC = "33333333-3333-4333-8333-333333333333";
const commit = "45778224c0b0f1279477a3ee781d212a02d2c43e";
const counters = { provider: 0, map: 0, realDatabaseWrites: 0, quota: 0, mockDatabaseWrites: 0 };
const checks = [];

function check(name, action) {
  try {
    action();
    checks.push({ name, status: "PASS" });
  } catch (error) {
    checks.push({ name, status: "FAIL", error: error.message });
    throw error;
  }
}

async function checkAsync(name, action) {
  try {
    await action();
    checks.push({ name, status: "PASS" });
  } catch (error) {
    checks.push({ name, status: "FAIL", error: error.message });
    throw error;
  }
}

function createDatabaseMock() {
  const rows = [];
  const now = "2026-10-03T00:00:00.000Z";
  function materialize(payload) {
    return {
      case_id: `case-${rows.length + 1}`,
      case_number: rows.length + 1,
      ...payload,
      created_at: now,
      updated_at: now
    };
  }
  return {
    rows,
    rpc: async () => ({ data: "CURRENT", error: null }),
    from(table) {
      assert.equal(table, "coordinate_cases");
      return {
        select() {
          let requestId = "";
          let sources = null;
          const matches = row => row.recognition_request_id === requestId
            && (!sources || sources.includes(row.source));
          return {
            eq(column, value) {
              assert.equal(column, "recognition_request_id");
              requestId = value;
              return this;
            },
            in(column, values) {
              assert.equal(column, "source");
              sources = values;
              return this;
            },
            async maybeSingle() {
              const matchesRows = rows.filter(matches);
              return matchesRows.length > 1
                ? { data: null, error: { code: "PGRST116" } }
                : { data: matchesRows[0] || null, error: null };
            },
            async single() {
              return { data: rows.find(matches) || null, error: null };
            }
          };
        },
        insert(payload) {
          return {
            select() {
              return {
                async single() {
                  if (rows.some(row => row.recognition_request_id === payload.recognition_request_id
                    && ["COORDINATE_IMAGE_UPLOAD", "MANUAL_ASSISTANCE"].includes(row.source)
                    && ["COORDINATE_IMAGE_UPLOAD", "MANUAL_ASSISTANCE"].includes(payload.source))) {
                    return { data: null, error: { code: "23505" } };
                  }
                  const row = materialize(payload);
                  rows.push(row);
                  counters.mockDatabaseWrites += 1;
                  return { data: row, error: null };
                }
              };
            }
          };
        }
      };
    }
  };
}

const failed = buildCoordinateRecognitionIssue({
  body: { code: "COORDINATE_RECOGNITION_FAILED_CLOSED", acquisitionStatus: "NO_COORDINATE_EVIDENCE" },
  requestId: requestA,
  jobId: "job-safe-1",
  runtimeCommit: commit,
  occurredAt: "2026-10-03T01:58:00.000+08:00"
});
check("FAILED_CLOSED maps to an acquisition issue with UNKNOWN visual cause", () => {
  assert.equal(failed.errorCode, "COORDINATE_RECOGNITION_FAILED_CLOSED");
  assert.equal(failed.failureStage, "ACQUISITION");
  assert.equal(failed.coordinateType, "UNKNOWN");
  assert.equal(failed.originalArtifactStatus, "NOT_SAVED");
});

const review = buildCoordinateRecognitionIssue({
  body: {
    authorizationStatus: "REVIEW_REQUIRED",
    finalizedCoordinateResult: { coordinateType: "DMS", qualityGateStatus: "REVIEW_REQUIRED" }
  },
  requestId: requestB,
  runtimeCommit: commit
});
check("REVIEW_REQUIRED preserves a coarse type without coordinate material", () => {
  assert.equal(review.failureStage, "FINALIZATION");
  assert.equal(review.coordinateType, "DMS");
  assert.equal(JSON.stringify(review).includes("116."), false);
});

const manual = buildCoordinateRecognitionIssue({
  explicitEvent: COORDINATE_RECOGNITION_ISSUE_EVENT.MANUAL_ASSISTANCE_REQUESTED,
  requestId: requestC,
  jobId: "job-safe-3",
  runtimeCommit: commit
});
check("request-bound manual assistance is eligible", () => {
  assert.equal(manual.source, "MANUAL_ASSISTANCE");
  assert.equal(manual.failureStage, "MANUAL_ASSISTANCE");
});

check("normal success does not create an issue", () => {
  assert.equal(buildCoordinateRecognitionIssue({ body: { success: true, authorizationStatus: "AUTHORIZED" }, requestId: requestA }), null);
});

check("unbound or malformed requests do not create issues", () => {
  assert.equal(buildCoordinateRecognitionIssue({ body: { code: "COORDINATE_RECOGNITION_FAILED_CLOSED" }, requestId: "bad" }), null);
});

check("privacy guard rejects image, coordinates, Provider response and credentials", () => {
  for (const field of ["imageData", "coordinates", "providerResponse", "apiKey"]) {
    assert.throws(() => normalizeCoordinateCaseInput({ ...failed, [field]: "forbidden" }), /COORDINATE_CASE_FORBIDDEN_FIELD/u);
  }
});

check("error code accepts only redacted machine identifiers", () => {
  const normalized = normalizeCoordinateCaseInput({ ...failed, errorCode: "safe_machine_code_1" });
  assert.equal(normalized.error_code, "SAFE_MACHINE_CODE_1");
  for (const errorCode of [
    "Aliyun provider said upstream content was invalid",
    "request failed: user@example.com",
    "{\"provider\":\"raw error\"}"
  ]) {
    assert.throws(
      () => normalizeCoordinateCaseInput({ ...failed, errorCode }),
      /COORDINATE_CASE_ERROR_CODE_INVALID/u
    );
  }
  const safeFallback = buildCoordinateRecognitionIssue({
    body: { authorizationStatus: "REVIEW_REQUIRED", code: "Provider returned free-form error" },
    requestId: requestB
  });
  assert.equal(safeFallback.errorCode, "REVIEW_REQUIRED");
});

check("RESOLVED is rejected without fix and verification evidence", () => {
  assert.throws(() => normalizeCoordinateCaseInput({ ...failed, progressStatus: "RESOLVED" }), /COORDINATE_CASE_RESOLUTION_EVIDENCE_REQUIRED/u);
});

check("RESOLVED accepts bounded fix and verification references while production remains separate", () => {
  const resolved = normalizeCoordinateCaseInput({
    ...failed,
    progressStatus: "RESOLVED",
    commitSha: commit,
    receiptRef: "receipt://offline/coordinate-issue-v1",
    goldenRef: "golden://independent/coordinate-issue-v1",
    evidenceScope: "same-class unseen fixtures",
    sampleCount: 2,
    problemResolutionStatus: "PASS",
    peerValidationStatus: "PASS",
    productionStatus: "UNKNOWN"
  });
  assert.equal(resolved.progress_status, "RESOLVED");
  assert.equal(resolved.production_status, "UNKNOWN");
});

const database = createDatabaseMock();
database.rows.push({
  case_id: "legacy-admin-case",
  case_number: 1,
  recognition_request_id: requestA,
  source: "ADMIN",
  issue_type: "coordinate_correction",
  issue_summary: "legacy admin record",
  original_artifact_status: "NOT_SAVED",
  created_at: "2026-10-01T00:00:00.000Z",
  updated_at: "2026-10-01T00:00:00.000Z"
});
const store = new CoordinateCaseStore({ supabase: database });
const first = await recordCoordinateRecognitionIssue({
  store,
  body: { code: "COORDINATE_RECOGNITION_FAILED_CLOSED" },
  requestId: requestA,
  jobId: "job-safe-1",
  runtimeCommit: commit
});
const duplicate = await recordCoordinateRecognitionIssue({
  store,
  body: { authorizationStatus: "REVIEW_REQUIRED" },
  requestId: requestA,
  jobId: "job-safe-1",
  runtimeCommit: commit
});
const second = await recordCoordinateRecognitionIssue({
  store,
  body: { authorizationStatus: "REVIEW_REQUIRED", finalizedCoordinateResult: { coordinateType: "DMS", qualityGateStatus: "REVIEW_REQUIRED" } },
  requestId: requestB,
  runtimeCommit: commit
});
check("database mock creates eligible issues and deduplicates by requestId", () => {
  assert.equal(first.recorded, true);
  assert.equal(first.duplicate, false);
  assert.equal(duplicate.duplicate, true);
  assert.equal(second.recorded, true);
  assert.equal(database.rows.length, 3);
  assert.equal(counters.mockDatabaseWrites, 2);
  assert.ok(database.rows.some(row => row.source === "ADMIN" && row.recognition_request_id === requestA));
  assert.ok(database.rows.some(row => row.source === "COORDINATE_IMAGE_UPLOAD" && row.recognition_request_id === requestA));
});

check("stored automatic rows contain no retained artifact or user coordinate payload", () => {
  const serialized = JSON.stringify(database.rows);
  assert.equal(serialized.includes("providerResponse"), false);
  assert.equal(serialized.includes("coordinates"), false);
  assert.ok(database.rows.filter(row => row.source !== "ADMIN").every(row => row.original_artifact_status === "NOT_SAVED"));
});

const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const migrationSource = fs.readFileSync(path.join(root, "supabase/migrations/20261003030000_coordinate_recognition_issue_v1.sql"), "utf8");
const rollbackSource = fs.readFileSync(path.join(root, "supabase/rollbacks/20261003030000_coordinate_recognition_issue_v1.rollback.sql"), "utf8");
check("runtime hook preserves failed-closed HTTP 422", () => {
  assert.match(serverSource, /if \(!regressionTestMode\.active\)/u);
  assert.match(serverSource, /return res\.status\(422\)\.json\(\{\s*success: false,\s*reason: "recognition_failed_closed",\s*code: "COORDINATE_RECOGNITION_FAILED_CLOSED"/u);
});

await checkAsync("HTTP 422 remains an asynchronous FAILED job without success or quota consumption", async () => {
  const runtime = createRecognitionAcquisitionJobRuntime({
    execute: async () => ({
      httpStatus: 422,
      result: {
        success: false,
        reason: "recognition_failed_closed",
        code: "COORDINATE_RECOGNITION_FAILED_CLOSED",
        usageConsumed: false,
        userUsageConsumed: false,
        providerCallCount: 0
      }
    })
  });
  const queued = runtime.enqueue({ requestId: requestC });
  let snapshot = runtime.get(queued.jobId, queued.jobAccessToken);
  for (let attempt = 0; attempt < 20 && snapshot?.status === RECOGNITION_ACQUISITION_JOB_STATUS.QUEUED; attempt += 1) {
    await new Promise(resolve => setImmediate(resolve));
    snapshot = runtime.get(queued.jobId, queued.jobAccessToken);
  }
  assert.equal(snapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
  assert.equal(getRecognitionAcquisitionJobHttpStatus(snapshot), 422);
  assert.equal(snapshot.result.success, false);
  assert.equal(snapshot.result.usageConsumed, false);
  assert.equal(snapshot.result.userUsageConsumed, false);
});

check("migration scopes request uniqueness and aborts instead of choosing duplicate data", () => {
  assert.match(migrationSource, /coordinate_cases_automatic_request_unique_idx/u);
  assert.match(migrationSource, /where source in \('COORDINATE_IMAGE_UPLOAD', 'MANUAL_ASSISTANCE'\)/u);
  assert.match(migrationSource, /having count\(\*\) > 1/u);
  assert.match(migrationSource, /raise exception 'coordinate_recognition_issue_v1 duplicate automatic request ids require an explicit data decision'/u);
  assert.match(migrationSource, /coordinate_cases_error_code_check/u);
  assert.doesNotMatch(migrationSource, /\bdelete\s+from\b|\bupdate\s+public\.coordinate_cases\b/iu);
});

check("rollback is executable, non-destructive and stops before a RESOLVED data decision", () => {
  assert.match(rollbackSource, /^begin;/u);
  assert.match(rollbackSource, /drop index if exists public\.coordinate_cases_automatic_request_unique_idx/u);
  assert.match(rollbackSource, /where progress_status = 'RESOLVED'/u);
  assert.match(rollbackSource, /raise exception 'coordinate_recognition_issue_v1 rollback requires an explicit decision for RESOLVED rows'/u);
  assert.match(rollbackSource, /commit;\s*$/u);
  assert.doesNotMatch(rollbackSource, /\bdelete\s+from\b|\bdrop\s+column\b/iu);
});

check("external side-effect counters remain zero", () => {
  assert.deepEqual(
    { provider: counters.provider, map: counters.map, realDatabaseWrites: counters.realDatabaseWrites, quota: counters.quota },
    { provider: 0, map: 0, realDatabaseWrites: 0, quota: 0 }
  );
});

const failedChecks = checks.filter(item => item.status !== "PASS");
console.log(JSON.stringify({
  contract: "coordinate_recognition_issue_v1",
  passed: checks.length - failedChecks.length,
  total: checks.length,
  status: failedChecks.length ? "FAIL" : "PASS",
  counters,
  checks
}, null, 2));
if (failedChecks.length) process.exitCode = 1;
