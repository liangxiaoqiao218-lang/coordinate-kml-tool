import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_REPRESENTATION_ROLE,
  buildCoordinateRepresentationContract,
  createCoordinateRepresentation
} from "../server/recognition/coordinate-representation-contract.js";

const root = fileURLToPath(new URL("../", import.meta.url));
const receiptDirectory = String(process.env.SELF_INTERSECTION_OUTPUT_RECEIPT_DIR || "").trim();
assert.ok(receiptDirectory, "SELF_INTERSECTION_OUTPUT_RECEIPT_DIR is required");
const checks = [];
const check = (name, action) => { action(); checks.push(name); console.log(`PASS ${name}`); };

const crossed = [[-3, 10], [-2.99, 10.01], [-3, 10.01], [-2.99, 10]];
const normal = [[-3, 10], [-2.99, 10], [-2.99, 10.01], [-3, 10.01]];

check("self-intersecting Polygon is independently blocked while source-order points remain inspectable", () => {
  const polygon = createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.BOUNDARY,
    positions: crossed, boundaryBlocked: true });
  const points = createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.REVIEW_POINTS,
    positions: crossed });
  const contract = buildCoordinateRepresentationContract([polygon, points]);
  assert.equal(contract.representations[0].result.geometry.type, "Polygon");
  assert.deepEqual(contract.representations[0].result.geometry.coordinates[0].slice(0, -1), crossed);
  assert.equal(contract.representations[0].outputCapabilities.mapReady, false);
  assert.equal(contract.representations[0].outputCapabilities.kmlReady, false);
  assert.ok(contract.representations[0].outputCapabilities.blockReasons.includes("GEOMETRY_SELF_INTERSECTION"));
  assert.equal(contract.representations[1].result.geometry.type, "MultiPoint");
  assert.deepEqual(contract.representations[1].result.geometry.coordinates, crossed);
  assert.equal(contract.representations[1].outputCapabilities.mapReady, true);
  assert.equal(contract.representations[1].outputCapabilities.kmlReady, true);
  assert.notEqual(contract.representations[0].result.resultId, contract.representations[1].result.resultId);
  assert.notEqual(contract.representations[0].result.geometryHash, contract.representations[1].result.geometryHash);
});

check("normal Polygon remains independently inspectable", () => {
  const polygon = createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.BOUNDARY,
    positions: normal });
  assert.equal(polygon.outputCapabilities.mapReady, true);
  assert.equal(polygon.outputCapabilities.kmlReady, true);
});

check("independent samples produce MultiPoint without a boundary claim", () => {
  const points = createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.REVIEW_POINTS,
    positions: normal });
  assert.equal(points.result.geometry.type, "MultiPoint");
  assert.equal(points.boundarySemantics, false);
});

check("LineString requires explicit source line intent", () => {
  assert.throws(() => createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE,
    positions: normal }), /EXPLICIT_LINE_INTENT_REQUIRED/u);
  const line = createCoordinateRepresentation({ role: COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE,
    positions: normal, explicitLineIntent: true });
  assert.equal(line.result.geometry.type, "LineString");
  assert.equal(line.outputCapabilities.kmlReady, true);
});

const golden = JSON.parse(readFileSync(path.join(root, "release-governance", "sr08d5-golden-policy.json"), "utf8"));
check("utm30_burkina_003 policy remains scoped to the blocked original Polygon", () => {
  const policy = golden.cases?.utm30_burkina_003?.releasePolicy;
  assert.equal(policy.expectedKmlReady, false);
  assert.equal(policy.geometryBlockerMustRemainActive, true);
  assert.equal(golden.cases?.utm30_burkina_003?.evidenceMetadata?.reason,
    "SELF_INTERSECTION_REQUIRES_FAIL_CLOSED");
});

const port = 18175;
const stdout = [];
const stderr = [];
const child = spawn(process.execPath, ["server.js"], {
  cwd: root,
  windowsHide: true,
  detached: process.platform !== "win32",
  stdio: ["ignore", "pipe", "pipe"],
  env: {
    ...process.env,
    PORT: String(port),
    NODE_ENV: "test",
    ENABLE_REGRESSION_TEST_MODE: "true",
    SPATIAL_RESULT_ENABLED: "true",
    ALIYUN_API_KEY: "",
    DASHSCOPE_API_KEY: "",
    OPENAI_API_KEY: "",
    SUPABASE_URL: "",
    SUPABASE_SERVICE_ROLE_KEY: "",
    SUPABASE_ANON_KEY: "",
    USAGE_SERVICE_ROLE_KEY: "",
    DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
  }
});
child.stdout.on("data", chunk => stdout.push(String(chunk)));
child.stderr.on("data", chunk => stderr.push(String(chunk)));

async function waitReady() {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (child.exitCode != null) throw new Error(`server exited ${child.exitCode}: ${stderr.join("")}`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/version`, { signal: AbortSignal.timeout(500) });
      if (response.ok) return;
    } catch { /* bounded readiness polling */ }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`server readiness timeout: ${stderr.join("")}`);
}

async function finalizeProjected(coordinateText, geometryIntent = "polygon") {
  const pendingResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-manual-finalize`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ coordinateText, requireConfirmation: false })
  });
  const pending = await pendingResponse.json();
  assert.equal(pendingResponse.status, 200, JSON.stringify(pending));
  const result = pending.finalizedCoordinateResult;
  const confirmedResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-projection-confirmation`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ resultId: result.resultId, resultRevision: result.resultRevision,
      sourceCrs: "utm30n", coordinateText, geometryIntent })
  });
  const confirmed = await confirmedResponse.json();
  assert.equal(confirmedResponse.status, 200, JSON.stringify(confirmed));
  return { pending, confirmed };
}

try {
  await waitReady();
  const crossedText = ["1 | 500000 | 1000000", "2 | 501000 | 1001000",
    "3 | 500000 | 1001000", "4 | 501000 | 1000000"].join("\n");
  const { pending, confirmed } = await finalizeProjected(crossedText);
  check("local HTTP keeps blocked Polygon and inspectable MultiPoint as separate results", () => {
    assert.equal(confirmed.geometryMode, "points_only");
    assert.equal(confirmed.boundaryBlocked, true);
    assert.equal(confirmed.finalizedCoordinateResult.geometry.type, "MultiPoint");
    assert.equal(confirmed.kmlReady, true);
    assert.equal(confirmed.coordinateRepresentationContract.representations.length, 2);
    const [polygon, points] = confirmed.coordinateRepresentationContract.representations;
    assert.equal(polygon.result.geometry.type, "Polygon");
    assert.equal(polygon.outputCapabilities.kmlReady, false);
    assert.equal(points.result.geometry.type, "MultiPoint");
    assert.equal(points.outputCapabilities.kmlReady, true);
    assert.notEqual(polygon.result.resultId, points.result.resultId);
    assert.notEqual(pending.finalizedCoordinateResult.resultId, points.result.resultId);
  });
  const selected = confirmed.finalizedCoordinateResult;
  const preview = async identity => {
    const response = await fetch(`http://127.0.0.1:${port}/api/map-preview`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(identity)
    });
    return { status: response.status, body: await response.json() };
  };
  const validPreview = await preview(selected);
  check("local HTTP accepts matching point identity and geometry hash", () => {
    assert.equal(validPreview.status, 200);
    assert.equal(validPreview.body.mapPreviewObject.geometry.type, "MultiPoint");
    assert.equal(validPreview.body.kmlEligibility.allowed, true);
  });
  const stale = await preview({ ...selected, resultRevision: selected.resultRevision + 1 });
  const wrongHash = await preview({ ...selected, geometryHash: "sha256:invalid" });
  const wrongId = await preview({ ...selected, resultId: "missing-result" });
  check("local HTTP rejects stale revision, identity and geometry hash conflicts", () => {
    assert.equal(stale.status, 409);
    assert.equal(stale.body.code, "STALE_CONFIRMATION_REVISION");
    assert.equal(wrongHash.status, 409);
    assert.equal(wrongHash.body.code, "GEOMETRY_HASH_MISMATCH");
    assert.equal(wrongId.status, 404);
  });
  const line = await finalizeProjected(crossedText, "line");
  check("local HTTP creates a LineString only for explicit line intent", () => {
    assert.equal(line.confirmed.geometryMode, "review_line");
    assert.equal(line.confirmed.boundaryBlocked, false);
    assert.equal(line.confirmed.finalizedCoordinateResult.geometry.type, "LineString");
    assert.equal(line.confirmed.finalizedCoordinateResult.kmlReady, true);
  });
  const missingCrsPendingResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-manual-finalize`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ coordinateText: crossedText })
  });
  const missingCrs = await missingCrsPendingResponse.json();
  check("missing CRS retains projected candidates while map and KML remain closed", () => {
    assert.equal(missingCrs.finalizedCoordinateResult.geometry, null);
    assert.equal(missingCrs.finalizedCoordinateResult.kmlReady, false);
    assert.equal(missingCrs.coordinateEngineV2.groups[0].points.length, 4);
  });
} finally {
  child.kill();
  await new Promise(resolve => child.once("close", resolve));
}

mkdirSync(receiptDirectory, { recursive: true });
writeFileSync(path.join(receiptDirectory, "results.json"), JSON.stringify({
  schemaVersion: "self_intersection_output_contract_regression_v1",
  origin: "LOCAL_SYNTHETIC_PRODUCT_AND_HTTP",
  checks,
  realProviderCalls: 0,
  serverStdout: stdout.join("").trim(),
  serverStderr: stderr.join("").trim()
}, null, 2), { flag: "wx" });
console.log(`Self-intersection Output Contract: ${checks.length}/${checks.length} PASS; REAL_PROVIDER_CALLS=0`);
