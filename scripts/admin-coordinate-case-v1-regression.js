import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import express from "express";
import {
  buildAdminModelStatus,
  buildCoverageSummary,
  coordinateCaseContract,
  normalizeCoordinateCaseEvidenceInput,
  normalizeCoordinateCaseInput,
  publicCoordinateCase
} from "../server/admin/coordinate-case-contract.js";
import { registerAdminCoordinateCaseRoutes } from "../server/admin/coordinate-case-routes.js";

const receiptDir = path.resolve(process.env.RECEIPT_DIR || "");
if (!process.env.RECEIPT_DIR || fs.existsSync(receiptDir)) throw new Error("RECEIPT_DIR_MUST_BE_NEW");
fs.mkdirSync(receiptDir, { recursive: true });

const requestOnlyId = "11111111-1111-4111-8111-111111111111";
const currentId = "22222222-2222-4222-8222-222222222222";
const currentResultId = "result-current";
const currentGeometryHash = `sha256:${"a".repeat(64)}`;
const cases = [];
const evidence = [];
let providerCalls = 0;
let sequence = 0;

function rowFor(input) {
  sequence += 1;
  return {
    case_id: `case-${sequence}`,
    case_number: sequence,
    ...input,
    created_at: "2026-10-01T06:30:00.000Z",
    updated_at: "2026-10-01T06:30:00.000Z"
  };
}

const store = {
  async list() {
    const output = cases.map(row => publicCoordinateCase(row, evidence.filter(item => item.case_id === row.case_id)));
    return { setupRequired: false, cases: output, coverage: buildCoverageSummary(output) };
  },
  async create(raw) {
    const input = normalizeCoordinateCaseInput(raw);
    if (input.result_id) {
      if (input.recognition_request_id !== currentId || input.result_id !== currentResultId || input.result_revision !== 2 || input.geometry_hash !== currentGeometryHash) {
        throw Object.assign(new Error("COORDINATE_CASE_RESULT_IDENTITY_CONFLICT"), { code: "COORDINATE_CASE_RESULT_IDENTITY_CONFLICT" });
      }
      input.result_identity_status = "CURRENT";
    }
    const row = rowFor(input);
    cases.push(row);
    return publicCoordinateCase(row, []);
  },
  async update(caseId, raw) {
    const index = cases.findIndex(item => item.case_id === caseId);
    if (index < 0) throw new Error("NOT_FOUND");
    const input = normalizeCoordinateCaseInput({ ...cases[index], ...raw });
    if (input.result_id && (input.result_id !== currentResultId || input.result_revision !== 2 || input.geometry_hash !== currentGeometryHash)) {
      throw Object.assign(new Error("COORDINATE_CASE_RESULT_IDENTITY_CONFLICT"), { code: "COORDINATE_CASE_RESULT_IDENTITY_CONFLICT" });
    }
    input.result_identity_status = input.result_id ? "CURRENT" : "REQUEST_ONLY";
    cases[index] = { ...cases[index], ...input };
    return publicCoordinateCase(cases[index], []);
  },
  async addEvidence(caseId, raw) {
    const item = { evidence_id: `e-${evidence.length + 1}`, case_id: caseId, ...normalizeCoordinateCaseEvidenceInput(raw), created_at: "2026-10-01T06:31:00.000Z" };
    evidence.push(item);
    return publicCoordinateCase({}, [item]).evidence[0];
  }
};

const app = express();
app.use(express.json());
const requireAdmin = (req, res, next) => req.get("x-admin-password") === "test-admin" ? next() : res.status(401).json({ error: "unauthorized" });
registerAdminCoordinateCaseRoutes(app, {
  requireAdmin,
  requireStore: () => true,
  store,
  contract: coordinateCaseContract,
  getModelStatus: () => buildAdminModelStatus({
    configuredVisionModel: "configured-vision",
    configuredOcrModel: "configured-ocr",
    runtimeObservation: { model: "observed-runtime", observedAt: "2026-10-01T06:32:00.000Z", evidenceType: "PROVIDER_RESPONSE_MODEL" }
  })
});

const server = await new Promise(resolve => {
  const instance = app.listen(0, "127.0.0.1", () => resolve(instance));
});
const base = `http://127.0.0.1:${server.address().port}`;
const results = [];

async function check(name, fn) {
  try {
    await fn();
    results.push({ name, status: "PASS" });
  } catch (error) {
    results.push({ name, status: "FAIL", error: error.stack || error.message });
    throw error;
  }
}

async function request(url, options = {}) {
  return fetch(`${base}${url}`, { ...options, headers: { "content-type": "application/json", "x-admin-password": "test-admin", ...(options.headers || {}) } });
}

try {
  await check("non-admin is denied", async () => assert.equal((await fetch(`${base}/api/admin/coordinate-cases`)).status, 401));
  await check("request-only failure case is accepted", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: requestOnlyId, issueType: "recognition_failed", issueSummary: "没有形成结果",
      crsEvidenceStatus: "UNKNOWN", originalArtifactStatus: "NOT_SAVED"
    }) });
    assert.equal(response.status, 201);
    assert.equal((await response.json()).case.resultIdentityStatus, "REQUEST_ONLY");
  });
  await check("current result identity is accepted", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: currentId, resultId: currentResultId, resultRevision: 2, geometryHash: currentGeometryHash,
      issueType: "coordinate_correction", issueSummary: "坐标解释需纠正", crsEvidenceStatus: "WORKING_ASSUMPTION",
      geometryRepresentation: "MULTIPOINT", deliveryStatus: "PASS", productionStatus: "PASS", evidenceScope: "自交点位合同", sampleCount: 2
    }) });
    assert.equal(response.status, 201);
    assert.equal((await response.json()).case.resultIdentityStatus, "CURRENT");
  });
  await check("stale result identity is blocked", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: currentId, resultId: currentResultId, resultRevision: 1, geometryHash: currentGeometryHash,
      issueType: "coordinate_correction", issueSummary: "旧版本"
    }) });
    assert.equal(response.status, 400);
  });
  await check("incomplete geometry identity is blocked", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: currentId, resultId: currentResultId, resultRevision: 2,
      issueType: "coordinate_correction", issueSummary: "缺少哈希"
    }) });
    assert.equal(response.status, 400);
  });
  await check("original image material cannot enter contract", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: requestOnlyId, issueType: "recognition_failed", issueSummary: "失败", originalImage: "base64"
    }) });
    assert.equal(response.status, 400);
  });
  await check("evidence reference is stored without source material", async () => {
    const response = await request("/api/admin/coordinate-cases/case-2/evidence", { method: "POST", body: JSON.stringify({
      evidenceType: "DELIVERY_SELF_INTERSECTION_POINTS", status: "PASS", evidenceScope: "不同同类与正常对照", sampleCount: 3,
      referenceType: "RECEIPT", referenceValue: "Temp/admin-coordinate-case-v1-targeted/results.json"
    }) });
    assert.equal(response.status, 201);
  });
  await check("partial progress update preserves result identity", async () => {
    const response = await request("/api/admin/coordinate-cases/case-2", { method: "PATCH", body: JSON.stringify({
      progressStatus: "IN_PROGRESS", owner: "L"
    }) });
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.case.resultId, currentResultId);
    assert.equal(payload.case.progressStatus, "IN_PROGRESS");
  });
  await check("coverage counts only evidence-backed PASS", async () => {
    cases.push(rowFor(normalizeCoordinateCaseInput({ recognitionRequestId: "33333333-3333-4333-8333-333333333333", issueType: "kml_failed", issueSummary: "KML失败", productionStatus: "PASS", sampleCount: 0 })));
    const payload = await (await request("/api/admin/coordinate-cases")).json();
    assert.equal(payload.coverage.stages.production.PASS, 0);
    assert.ok(payload.coverage.stages.production.UNKNOWN >= 1);
    assert.equal(payload.coverage.stages.delivery.PASS, 1);
    assert.equal(payload.coverage.stages.production.PASS, 0);
    assert.ok(payload.coverage.stages.production.UNKNOWN >= 2);
  });
  await check("one stage evidence cannot prove another stage", async () => {
    const payload = await (await request("/api/admin/coordinate-cases")).json();
    assert.equal(payload.coverage.stages.delivery.PASS, 1);
    assert.equal(payload.coverage.stages.production.PASS, 0);
  });
  await check("explicit direct references support only their assigned stages", async () => {
    const direct = publicCoordinateCase(rowFor(normalizeCoordinateCaseInput({
      recognitionRequestId: "55555555-5555-4555-8555-555555555555", issueType: "coordinate_correction", issueSummary: "证据引用",
      deliveryStatus: "PASS", problemResolutionStatus: "PASS", peerValidationStatus: "PASS", productionStatus: "PASS",
      evidenceScope: "明确引用", sampleCount: 2, receiptRef: "Temp/delivery/results.json",
      commitSha: "b".repeat(40), goldenRef: "golden/self-intersection"
    })), []);
    const coverage = buildCoverageSummary([direct]);
    assert.equal(coverage.stages.delivery.PASS, 1);
    assert.equal(coverage.stages.problemResolution.PASS, 1);
    assert.equal(coverage.stages.peerValidation.PASS, 1);
    assert.equal(coverage.stages.production.PASS, 0);
  });
  await check("forbidden content is rejected without echo", async () => {
    const forbiddenValues = [
      "1 | 778984.492 | 9721476.737\n2 | 779099.680 | 9721476.848",
      JSON.stringify({ choices: [{ message: { content: "raw" } }], usage: { total_tokens: 1 } }),
      "Authorization: Bearer abcdefghijklmnop",
      "Cookie: session=abcdef",
      "API_KEY=abcdefghijklmnop",
      "password: do-not-store-this",
      "data:image/png;base64,AAAA",
      "SUPABASE_URL=https://example.invalid\nSUPABASE_SERVICE_ROLE_KEY=do-not-store"
    ];
    for (const issueSummary of forbiddenValues) {
      const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
        recognitionRequestId: requestOnlyId, issueType: "recognition_failed", issueSummary
      }) });
      assert.equal(response.status, 400);
      const payload = JSON.stringify(await response.json());
      assert.equal(payload.includes(issueSummary), false);
    }
  });
  await check("redacted text and short evidence references remain allowed", async () => {
    const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
      recognitionRequestId: "66666666-6666-4666-8666-666666666666", issueType: "recognition_failed",
      issueSummary: "坐标区域未形成可检查结果，已移除原始材料。", blocker: "等待同类脱敏证据",
      evidenceScope: "两份脱敏对照", sampleCount: 2, receiptRef: "Temp/redacted-case/results.json"
    }) });
    assert.equal(response.status, 201);
  });
  await check("four stages remain separate", async () => {
    const payload = await (await request("/api/admin/coordinate-cases")).json();
    assert.deepEqual(Object.keys(payload.coverage.stages), ["delivery", "problemResolution", "peerValidation", "production"]);
  });
  await check("configured and observed models remain distinct", async () => {
    const payload = await (await request("/api/admin/model-status")).json();
    assert.equal(payload.configured.visionModel, "configured-vision");
    assert.equal(payload.observedRuntime.model, "observed-runtime");
    assert.equal(JSON.stringify(payload).includes("API_KEY"), false);
  });
  await check("missing runtime evidence is UNKNOWN", async () => assert.equal(buildAdminModelStatus({ configuredVisionModel: "x" }).observedRuntime.model, "UNKNOWN"));
  await check("CRS assumption is not promoted", async () => {
    const payload = await (await request("/api/admin/coordinate-cases")).json();
    assert.equal(payload.cases.find(item => item.resultId === currentResultId).crsEvidenceStatus, "WORKING_ASSUMPTION");
  });
  await check("Polygon MultiPoint and explicit LineString references stay separate", async () => {
    for (const geometryRepresentation of ["POLYGON", "MULTIPOINT", "LINESTRING"]) {
      const response = await request("/api/admin/coordinate-cases", { method: "POST", body: JSON.stringify({
        recognitionRequestId: "44444444-4444-4444-8444-444444444444", issueType: "coordinate_correction",
        issueSummary: `${geometryRepresentation} contract`, geometryRepresentation, crsEvidenceStatus: "WORKING_ASSUMPTION"
      }) });
      assert.equal(response.status, 201);
    }
    const payload = await (await request("/api/admin/coordinate-cases")).json();
    assert.deepEqual(new Set(payload.cases.slice(-3).map(item => item.geometryRepresentation)), new Set(["POLYGON", "MULTIPOINT", "LINESTRING"]));
  });
  await check("self-intersection representations remain independent", async () => {
    const geometrySource = fs.readFileSync(path.resolve("server/coordinate-finalizer/geometry-finalizer.js"), "utf8");
    const representationSource = fs.readFileSync(path.resolve("server/recognition/coordinate-representation-contract.js"), "utf8");
    assert.match(representationSource, /MultiPoint/u);
    assert.match(representationSource, /LineString/u);
    assert.match(geometrySource, /GEOMETRY_SELF_INTERSECTION/u);
  });
  await check("judge case history remains available", async () => {
    const serverSource = fs.readFileSync(path.resolve("server.js"), "utf8");
    const adminSource = fs.readFileSync(path.resolve("admin.html"), "utf8");
    assert.match(serverSource, /\/api\/admin\/judge-cases/u);
    assert.match(adminSource, /快判历史/u);
  });
  await check("migration denies browser roles and preserves separate tables", async () => {
    const sql = fs.readFileSync(path.resolve("supabase/migrations/20261001063000_admin_coordinate_case_v1.sql"), "utf8");
    assert.match(sql, /enable row level security/giu);
    assert.match(sql, /revoke all on table public\.coordinate_cases from anon, authenticated/iu);
    assert.doesNotMatch(sql, /insert into public\.judge_cases/iu);
  });
  await check("admin UI defaults to coordinate section", async () => {
    const html = fs.readFileSync(path.resolve("admin.html"), "utf8");
    assert.match(html, /id="coordinateCaseSection"/u);
    assert.match(html, /id="judgeCaseSection" hidden/u);
    assert.match(html, /本次交付/u);
    assert.match(html, /生产生效/u);
    assert.match(html, /FAIL \$\{Number\(stage\.FAIL/u);
    assert.match(html, /BLOCKED \$\{Number\(stage\.BLOCKED/u);
    assert.match(html, /UNKNOWN \$\{Number\(stage\.UNKNOWN/u);
    assert.match(html, /N\/A \$\{Number\(stage\.NOT_APPLICABLE/u);
  });
  await check("response exposes no sealed result or coordinates", async () => {
    const payload = JSON.stringify(await (await request("/api/admin/coordinate-cases")).json());
    assert.equal(/sealed_result|coordinates|providerRaw|cookie|authorization/iu.test(payload), false);
  });
} finally {
  await new Promise(resolve => server.close(resolve));
}

const manifest = {
  schemaVersion: "admin-coordinate-case-v1-regression@1",
  commit: process.env.TEST_GIT_COMMIT || "WORKTREE",
  generatedAt: new Date().toISOString(),
  result: `${results.filter(item => item.status === "PASS").length}/${results.length}`,
  providerCalls,
  tests: results
};
fs.writeFileSync(path.join(receiptDir, "results.json"), `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`admin-coordinate-case-v1-regression: ${manifest.result} PASS`);
console.log(`real provider calls: ${providerCalls}`);
