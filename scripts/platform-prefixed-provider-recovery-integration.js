import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { evaluateCoordinateUsageAuthority } from "../server/coordinate-usage-atomicity.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rows = [
  "g-8.11623600,10.83650900",
  "g-8.11597800,10.88405100",
  "g-8.06284900,10.79351300",
  "g-8.09804000,10.79267000",
  "g-8.11623600,10.83650900"
];

if (process.argv.includes("--server")) {
  const { default: http } = await import("node:http");
  let providerCalls = 0;
  globalThis.fetch = async (url, init) => {
    if (String(url) !== "http://127.0.0.1:1/v1/chat/completions") {
      throw new Error("TEST_EXTERNAL_NETWORK_FORBIDDEN");
    }
    providerCalls += 1;
    assert.equal(providerCalls, 1);
    const request = JSON.parse(init.body);
    const prompt = request.messages.map(message => JSON.stringify(message.content)).join(" ");
    assert.match(prompt, /UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE/u);
    return new Response(JSON.stringify({
      choices: [{
        message: {
          content: ["UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE", ...rows].join("\n")
        }
      }]
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function listen(port, callback) {
    this.once("listening", () => process.send({ port: this.address().port }));
    return nativeListen.call(this, port, "127.0.0.1", callback);
  };
  process.on("message", message => {
    if (message === "stats") process.send({ providerCalls });
  });
  await import("../server.js");
  await new Promise(() => {});
}

const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "--server"], {
  cwd: root,
  windowsHide: true,
  stdio: ["ignore", "pipe", "pipe", "ipc"],
  env: {
    SystemRoot: process.env.SystemRoot,
    PATH: process.env.PATH,
    NODE_ENV: "test",
    PORT: "0",
    ENABLE_REGRESSION_TEST_MODE: "true",
    ALIYUN_API_KEY: "local-mock-only",
    ALIYUN_BASE_URL: "http://127.0.0.1:1/v1",
    DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
  }
});
child.stdout.resume();
child.stderr.resume();
const signal = AbortSignal.timeout(20_000);
try {
  const [{ port }] = await once(child, "message", { signal });
  const fixturePath = String(process.env.PLATFORM_PREFIXED_FIXTURE || "").trim();
  const syntheticPng = fixturePath
    ? await readFile(fixturePath)
    : Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
      "base64"
    );
  const form = new FormData();
  form.set("visitorId", "platform-provider-recovery-regression");
  form.set("image", new Blob([syntheticPng], { type: "image/png" }), "platform-coordinate.png");
  const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
    method: "POST",
    headers: {
      "x-regression-test": "1",
      "x-regression-case-id": "platform-prefixed-provider-recovery"
    },
    body: form,
    signal
  });
  const payload = await response.json();
  assert.equal(response.status, 200, JSON.stringify(payload));
  assert.equal(payload.precisionMode, "wgs84-platform-lonlat-coordinates");
  if (fixturePath) {
    assert.equal(payload.rawText.split(/\r?\n/u).filter(Boolean).length, 5);
  } else {
    assert.equal(payload.rawText, rows.map(row => row.slice(1)).join("\n"));
  }
  assert.equal(payload.axisOrderEvidence?.axisOrder, "longitude_latitude");
  assert.equal(payload.coordinateEngineV2?.requires_review, false);
  assert.equal(payload.finalizedCoordinateResult?.geometry?.type, "Polygon");
  assert.equal(payload.finalizedCoordinateResult?.geometry?.coordinates?.[0]?.length, 5);
  assert.equal(payload.finalizedCoordinateResult?.kmlReady, true);
  const validation = payload.coordinateEngineV2?.groups?.[0]?.validation;
  const selectedCandidate = validation?.coordinate_order_candidates?.find(candidate => (
    candidate.interpretation === validation.selected_interpretation
  ));
  assert.equal(selectedCandidate?.geometry_policy, "wgs84_terminal_closure_validation");
  assert.equal(selectedCandidate?.original_point_count, 5);
  assert.equal(selectedCandidate?.validation_point_count, 4);
  assert.equal(selectedCandidate?.duplicate_coordinate_count, 1);
  const authority = evaluateCoordinateUsageAuthority({ httpStatus: response.status, body: payload });
  assert.equal(authority.eligible, true, JSON.stringify({ authority, finalizedCoordinateResult: payload.finalizedCoordinateResult }));
  const statsPromise = once(child, "message", { signal });
  child.send("stats");
  const [stats] = await statsPromise;
  assert.equal(stats.providerCalls, 1);
  console.log("Platform-prefixed Provider recovery integration: 14/14 PASS");
  console.log("PROVIDER_CALLS=1 (mock only)");
} finally {
  const ended = once(child, "exit");
  child.kill();
  await ended;
}
