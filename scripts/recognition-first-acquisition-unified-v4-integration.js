import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import net from "node:net";
import { fileURLToPath } from "node:url";
import path from "node:path";
import sharp from "sharp";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rows = Array.from({ length: 20 }, (_, index) => (
  `${index + 1} ${655000 - (index * 500)} ${1_333_600 + (index * 1200)}`
));
const providerText = [
  "CONTEXT | ITRF 2008 / Projection BFTM",
  "HEADING | Boundary",
  "Point X Y",
  ...rows.slice(0, 10),
  "Point | X | Y",
  ...rows.slice(10)
].join("\n");

if (process.argv.includes("--server")) {
  const { default: http } = await import("node:http");
  const nativeFetch = globalThis.fetch;
  let providerCalls = 0;
  let submittedImageCount = 0;
  globalThis.fetch = async (url, init) => {
    if (String(url) !== "http://127.0.0.1:1/v1/chat/completions") {
      return nativeFetch(url, init);
    }
    providerCalls += 1;
    assert.equal(providerCalls, 1);
    const request = JSON.parse(init.body);
    const content = request.messages.flatMap(message => (
      Array.isArray(message.content) ? message.content : []
    ));
    submittedImageCount = content.filter(item => item?.type === "image_url").length;
    assert.ok(submittedImageCount > 1);
    return new Response(JSON.stringify({
      id: "offline-provider-v4",
      choices: [{ message: { content: providerText } }],
      usage: { total_tokens: 1 }
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function listen(port, callback) {
    this.once("listening", () => process.send({ port: this.address().port }));
    return nativeListen.call(this, port, "127.0.0.1", callback);
  };
  process.on("message", message => {
    if (message === "stats") process.send({ providerCalls, submittedImageCount });
  });
  await import("../server.js");
  await new Promise(() => {});
}

const portProbe = net.createServer();
portProbe.listen(0, "127.0.0.1");
await once(portProbe, "listening");
const reservedPort = portProbe.address().port;
await new Promise((resolve, reject) => portProbe.close(error => (error ? reject(error) : resolve())));

const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "--server"], {
  cwd: root,
  windowsHide: true,
  stdio: ["ignore", "pipe", "pipe", "ipc"],
  env: {
    SystemRoot: process.env.SystemRoot,
    PATH: process.env.PATH,
    NODE_ENV: "test",
    PORT: String(reservedPort),
    ENABLE_REGRESSION_TEST_MODE: "true",
    ALIYUN_API_KEY: "local-mock-only",
    ALIYUN_BASE_URL: "http://127.0.0.1:1/v1",
    DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
  }
});
let stdout = "";
let stderr = "";
child.stdout.on("data", chunk => { stdout += chunk.toString(); });
child.stderr.on("data", chunk => { stderr += chunk.toString(); });
const signal = AbortSignal.timeout(45_000);

try {
  const [{ port }] = await once(child, "message", { signal });
  const syntheticLongImage = await sharp({
    create: {
      width: 800,
      height: 3200,
      channels: 3,
      background: "white"
    }
  }).png().toBuffer();
  const form = new FormData();
  form.set("visitorId", "unified-acquisition-v4-regression");
  form.set("image", new Blob([syntheticLongImage], { type: "image/png" }), "long-table.png");
  const terminalResponse = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
    method: "POST",
    headers: {
      "x-regression-test": "1",
      "x-visitor-id": "unified-acquisition-v4-regression"
    },
    body: form,
    signal
  });
  const result = await terminalResponse.json();
  const terminalDiagnostic = JSON.stringify({ result, stdout, stderr });
  assert.equal(terminalResponse.status, 200, terminalDiagnostic);
  assert.equal(result.success, true);
  assert.equal(result.acquisitionStatus, "COMPLETED");
  assert.equal(result.authorizationStatus, "REVIEW_REQUIRED");
  assert.equal(result.resultStatus, "needs_review");
  assert.equal(result.mapReady, false);
  assert.equal(result.kmlReady, false);
  assert.equal(result.mapStatus, "CLOSED");
  assert.equal(result.kmlStatus, "CLOSED");
  assert.equal(result.rawText, providerText);
  assert.equal(result.candidateCoordinates.length, 20);
  assert.equal(result.candidateCoordinateGroups.length, 1);
  assert.deepEqual(result.candidateCoordinates.map(candidate => candidate.sourceLabel), (
    Array.from({ length: 20 }, (_, index) => String(index + 1))
  ));
  assert.ok(result.visibleCrsEvidence.some(item => /BFTM/iu.test(item.text)));
  assert.ok(result.imageAcquisitionEvidence.imageCount > 1);
  assert.ok(result.imageAcquisitionEvidence.detailTileCount > 0);
  assert.equal(result.recognitionAcquisition.providerCompletionState, "SUCCEEDED");
  assert.equal(result.recognitionAcquisition.diagnostics.boundRowCount, 20);
  assert.equal(result.recognitionAcquisition.diagnostics.unboundRowCount, 0);
  assert.ok(result.reviewReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"));

  const statsPromise = once(child, "message", { signal });
  child.send("stats");
  const [stats] = await statsPromise;
  assert.equal(stats.providerCalls, 1);
  assert.equal(stats.submittedImageCount, result.imageAcquisitionEvidence.imageCount);
  assert.match(stdout, /Recognition acquisition evidence:/u);
  assert.match(stdout, /Recognition acquisition final state:/u);
  assert.equal((stdout.match(/Recognition acquisition evidence:/gu) || []).length, 1);
  assert.equal((stdout.match(/Recognition acquisition final state:/gu) || []).length, 1);
  assert.doesNotMatch(stdout, /655000/u);
  assert.doesNotMatch(stderr, /(?:Error|ERR_|Unhandled|AssertionError)/u);
  console.log("recognition-first acquisition unified v4 integration: PASS (HTTP 200, 20 candidates, one mock Provider call, one multi-image request, review-only Map/KML fail-close)");
} finally {
  const ended = once(child, "exit");
  child.kill();
  await ended;
}
