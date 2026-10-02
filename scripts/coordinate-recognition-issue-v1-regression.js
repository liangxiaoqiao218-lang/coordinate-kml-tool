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
          return {
            eq(column, value) {
              assert.equal(column, "recognition_request_id");
              requestId = value;
              return this;
            },
            async maybeSingle() {
              return { data: rows.find(row => row.recognition_request_id === requestId) || null, error: null };
            },
            async single() {
              return { data: rows.find(row => row.recognition_request_id === requestId) || null, error: null };
            }
          };
        },
        insert(payload) {
          return {
            select() {
              return {
                async single() {
                  if (rows.some(row => row.recognition_request_id === payload.recognition_request_id)) {
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
  assert.equal(database.rows.length, 2);
  assert.equal(counters.mockDatabaseWrites, 2);
});

check("stored automatic rows contain no retained artifact or user coordinate payload", () => {
  const serialized = JSON.stringify(database.rows);
  assert.equal(serialized.includes("providerResponse"), false);
  assert.equal(serialized.includes("coordinates"), false);
  assert.ok(database.rows.every(row => row.original_artifact_status === "NOT_SAVED"));
});

const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const migrationSource = fs.readFileSync(path.join(root, "supabase/migrations/20261003030000_coordinate_recognition_issue_v1.sql"), "utf8");
check("runtime hook is disabled in regression mode and migration enforces request uniqueness", () => {
  assert.match(serverSource, /if \(!regressionTestMode\.active\)/u);
  assert.match(migrationSource, /unique index if not exists coordinate_cases_request_unique_idx/u);
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
