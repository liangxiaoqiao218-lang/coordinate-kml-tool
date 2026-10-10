import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const mode = "indonesia_utm50s_structured_b";

if (process.argv.includes("--http-child")) {
  const { default: http } = await import("node:http");
  const nativeListen = http.Server.prototype.listen;
  let providerCalls = 0;
  http.Server.prototype.listen = function patchedListen(_port, callback) {
    return nativeListen.call(this, 0, "127.0.0.1", () => { process.send?.({ port: this.address().port }); callback?.(); });
  };
  globalThis.fetch = async () => {
    providerCalls += 1;
    if (providerCalls > 1) throw new Error("UNEXPECTED_SECOND_PROVIDER_CALL");
    return new Response(JSON.stringify({ id: "mock-indonesia-structured-terminal-barrier", model: "qwen3.8-flash", choices: [{ finish_reason: "stop", message: { content: "{}" } }] }), { status: 200, headers: { "content-type": "application/json" } });
  };
  process.on("message", message => { if (message === "stats") process.send?.({ providerCalls }); });
  await import("../server.js");
  await new Promise(() => {});
}

if (!process.argv.includes("--http-child")) {
  const injectedClassification = `    const route = classifyOneShotStructuredFamily();
    const oneShotFamilyClassification = {
      attempted: true, route, sourceText: "", layoutLines: Object.freeze([]), axisEvidenceText: "X | Y", axisOrderEvidence: null,
      projectedTableOcrAcquisition: true,
      sourceContextText: "No. X Y LATITUDE LONGITUDE\\nSISTEM KOORDINAT UTM WGS 1984 ZONA 50S",
      sourceContextProvenance: Object.freeze({ schema_version: "projected_source_context_v1", image_sha256: coordinateImageIdentity.image_sha256, same_request: true, mode: "overview_footer_composite" }),
      contract: createOneShotAcquisitionContract({ route })
    };
`;
  const preload = `import { registerHooks } from 'node:module';
registerHooks({load(url, context, nextLoad) {
  const result = nextLoad(url, context); if (!url.endsWith('/server.js')) return result; let source = String(result.source);
  const callStart = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
  const callEnd = source.indexOf('    });', callStart) + 7;
  if (callStart < 0 || callEnd < 7) throw new Error('INDONESIA_STRUCTURED_TERMINAL_INJECTION_NOT_INSTALLED');
  return {...result, source: source.slice(0, callStart) + ${JSON.stringify(injectedClassification)} + source.slice(callEnd)};
}});`;
  const child = spawn(process.execPath, ["--import", `data:text/javascript,${encodeURIComponent(preload)}`, fileURLToPath(import.meta.url), "--http-child"], {
    cwd: root, windowsHide: true, stdio: ["ignore", "pipe", "pipe", "ipc"],
    env: { SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: "test", PORT: "0", ENABLE_REGRESSION_TEST_MODE: "true", INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "true", ALIYUN_API_KEY: "local-mock-only", ALIYUN_BASE_URL: "http://127.0.0.1:1/v1", SUPABASE_URL: "", SUPABASE_SERVICE_ROLE_KEY: "", SPATIAL_RESULT_ENABLED: "true", DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__") }
  });
  let stderr = ""; child.stdout.resume(); child.stderr.on("data", chunk => { stderr += String(chunk); });
  const signal = AbortSignal.timeout(60_000);
  try {
    const [{ port }] = await Promise.race([once(child, "message", { signal }), once(child, "exit").then(([code]) => { throw new Error(`HTTP_CHILD_EXITED_BEFORE_READY:${code}`); })]);
    const fixture = await readFile(path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg"));
    const form = new FormData(); form.set("visitorId", "indonesia-structured-terminal-barrier"); form.set("coordinateProductMode", mode); form.set("image", new Blob([fixture], { type: "image/jpeg" }), "indonesia-structured.jpg");
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, { method: "POST", headers: { "x-regression-test": "1", "x-regression-case-id": "indonesia-structured-terminal-barrier", "x-coordinate-regression-failure": "INDONESIA_STRUCTURED_POST_PROVIDER_INTERNAL_FAILURE" }, body: form, signal });
    const payload = await response.json(); const statsPromise = once(child, "message", { signal }); child.send("stats"); const [stats] = await statsPromise;
    assert.equal(response.status, 422, JSON.stringify(payload)); assert.equal(payload.success, false); assert.equal(payload.code, "COORDINATE_POST_PROVIDER_PROCESSING_FAILED"); assert.equal(stats.providerCalls, 1); assert.notEqual(payload.finalizedCoordinateResult?.technicalKmlReady, true); assert.notEqual(payload.finalizedCoordinateResult?.kmlReady, true);
    console.log("Indonesia structured terminal barrier: 1/1 PASS");
    console.log(JSON.stringify({ status: "PASS", scenario: "post-provider internal failure", mockProviderCalls: stats.providerCalls, realProviderCalls: 0, fallbackCalls: 0, databaseWrites: 0, terminalCode: payload.code }, null, 2));
  } catch (error) { if (stderr) process.stderr.write(stderr.slice(-8000)); throw error; }
  finally { child.kill(); await once(child, "exit").catch(() => {}); }
}
