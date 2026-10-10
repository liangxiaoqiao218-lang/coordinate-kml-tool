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
  globalThis.fetch = async () => { providerCalls += 1; throw new Error("PROVIDER_MUST_NOT_BE_CALLED_AFTER_BUDGET_REJECTION"); };
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
  const injectedBudgetStop = `        const injectedBudgetStop = new Error("REGRESSION_PROVIDER_ADMISSION_BUDGET_REJECTION");
        injectedBudgetStop.code = RECOGNITION_BUDGET_CODE;
        injectedBudgetStop.reason = "provider_admission_budget_insufficient";
        throw injectedBudgetStop;
`;
  const preload = `import { registerHooks } from 'node:module';
registerHooks({load(url, context, nextLoad) {
  const result = nextLoad(url, context); if (!url.endsWith('/server.js')) return result; let source = String(result.source);
  const classificationStart = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
  const classificationEnd = source.indexOf('    });', classificationStart) + 7;
  if (classificationStart < 0 || classificationEnd < 7) throw new Error('INDONESIA_STRUCTURED_BUDGET_CLASSIFICATION_INJECTION_NOT_INSTALLED');
  source = source.slice(0, classificationStart) + ${JSON.stringify(injectedClassification)} + source.slice(classificationEnd);
  const providerMarker = '        structuredProviderResponse = await callAliyunVision(buildCoordinateStructuredCall({';
  const providerStart = source.indexOf(providerMarker); if (providerStart < 0) throw new Error('INDONESIA_STRUCTURED_BUDGET_STOP_INJECTION_NOT_INSTALLED');
  source = source.slice(0, providerStart) + ${JSON.stringify(injectedBudgetStop)} + source.slice(providerStart);
  return {...result, source};
}});`;
  const child = spawn(process.execPath, ["--import", `data:text/javascript,${encodeURIComponent(preload)}`, fileURLToPath(import.meta.url), "--http-child"], {
    cwd: root, windowsHide: true, stdio: ["ignore", "pipe", "pipe", "ipc"],
    env: { SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: "test", PORT: "0", ENABLE_REGRESSION_TEST_MODE: "true", INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "true", ALIYUN_API_KEY: "local-mock-only", ALIYUN_BASE_URL: "http://127.0.0.1:1/v1", SUPABASE_URL: "", SUPABASE_SERVICE_ROLE_KEY: "", SPATIAL_RESULT_ENABLED: "true", DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__") }
  });
  let stderr = ""; child.stdout.resume(); child.stderr.on("data", chunk => { stderr += String(chunk); }); const signal = AbortSignal.timeout(60_000);
  try {
    const [{ port }] = await Promise.race([once(child, "message", { signal }), once(child, "exit").then(([code]) => { throw new Error(`HTTP_CHILD_EXITED_BEFORE_READY:${code}`); })]);
    const fixture = await readFile(path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg"));
    const form = new FormData(); form.set("visitorId", "indonesia-structured-budget-stop"); form.set("coordinateProductMode", mode); form.set("image", new Blob([fixture], { type: "image/jpeg" }), "indonesia-structured.jpg");
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, { method: "POST", headers: { "x-regression-test": "1", "x-regression-case-id": "indonesia-structured-budget-stop" }, body: form, signal });
    const payload = await response.json(); const statsPromise = once(child, "message", { signal }); child.send("stats"); const [stats] = await statsPromise;
    assert.equal(response.status, 503, JSON.stringify(payload)); assert.equal(payload.success, false); assert.equal(payload.code, "RECOGNITION_BUDGET_EXHAUSTED"); assert.equal(payload.reason, "provider_admission_budget_insufficient"); assert.equal(payload.usageConsumed, false); assert.equal(payload.retryAllowed, true); assert.equal(stats.providerCalls, 0);
    console.log("Indonesia structured budget stop: 1/1 PASS"); console.log(JSON.stringify({ status: "PASS", scenario: "provider admission budget rejected before call", httpStatus: response.status, responseCode: payload.code, responseReason: payload.reason, providerCalls: stats.providerCalls, realProviderCalls: 0, databaseWrites: 0 }, null, 2));
  } catch (error) { if (stderr) process.stderr.write(stderr.slice(-8000)); throw error; }
  finally { child.kill(); await once(child, "exit").catch(() => {}); }
}
