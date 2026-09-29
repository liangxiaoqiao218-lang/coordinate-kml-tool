import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dmsOnlyProviderText = [
  `2°31'2,794" S | 119°30'31,553" E`,
  `2°31'2,783" S | 119°30'35,279" E`,
  `2°31'14,694" S | 119°30'35,302" E`,
  `2°31'14,708" S | 119°30'28,050" E`,
  `2°31'12,437" S | 119°30'28,046" E`,
  `2°31'12,430" S | 119°30'31,571" E`
].join("\n");
const localSourceContext = [
  "No. X Y LATITUDE LONGITUDE",
  `8 1 778984,492 9721476,737 2°31'2,794"S 119°30'31,553"E 8`,
  `2 779099,680 9721476,848 2°31'2,783"S 119°30'35,279"E`,
  `3 779099,680 9721110,798 2°31'14,694"S 119°30'35,302"E`,
  `2 4 778875,519 9721110,798 2°31'14,708"S 119°30'28,050"E`,
  `5 778875,519 9721180,576 2°31'12,437"S 119°30'28,046"E`,
  `g 6 778984492 9721180576 2°31'12,430"S 119°30'31,571"E g`,
  "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S"
].join("\n");

if (process.argv[2] === "--child") {
  const { default: http } = await import("node:http");
  const nativeListen = http.Server.prototype.listen;
  let providerCalls = 0;
  http.Server.prototype.listen = function patchedListen(_port, callback) {
    return nativeListen.call(this, 0, "127.0.0.1", () => {
      process.send?.({ port: this.address().port });
      callback?.();
    });
  };
  globalThis.fetch = async () => {
    providerCalls += 1;
    if (providerCalls > 1) throw new Error("UNEXPECTED_SECOND_PROVIDER_CALL");
    return new Response(JSON.stringify({
      id: "mock-multi-representation-dms",
      choices: [{ message: { content: process.env.TEST_PROVIDER_TEXT } }]
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  process.on("message", message => {
    if (message === "stats") process.send?.({ providerCalls });
  });
  await import("../server.js");
  await new Promise(() => {});
}

if (process.argv[2] !== "--child") {
  const injectedClassification = `    const testSourceContextText = String(process.env.TEST_LOCAL_SOURCE_CONTEXT || "");
    const route = classifyOneShotStructuredFamily();
    const oneShotFamilyClassification = {
      attempted: true,
      route,
      sourceText: "",
      layoutLines: Object.freeze([]),
      axisEvidenceText: "LATITUDE S | LONGITUDE E\\nX | Y",
      axisOrderEvidence: null,
      projectedTableOcrAcquisition: true,
      sourceContextText: testSourceContextText,
      sourceContextProvenance: Object.freeze({
        schema_version: "projected_source_context_v1",
        image_sha256: coordinateImageIdentity.image_sha256,
        same_request: true,
        mode: "overview_footer_composite"
      }),
      contract: createOneShotAcquisitionContract({ route })
    };
`;
  const preload = `import { registerHooks } from 'node:module';
registerHooks({load(url, context, nextLoad) {
  const result = nextLoad(url, context);
  if (!url.endsWith('/server.js')) return result;
  let source = String(result.source);
  const callStart = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
  const callEnd = source.indexOf('    });', callStart) + 7;
  if (callStart < 0 || callEnd < 7) throw new Error('MULTI_REPRESENTATION_HTTP_INJECTION_NOT_INSTALLED');
  return {...result, source: source.slice(0, callStart) + ${JSON.stringify(injectedClassification)} + source.slice(callEnd)};
}});`;
  const child = spawn(process.execPath, [
    "--import", `data:text/javascript,${encodeURIComponent(preload)}`,
    fileURLToPath(import.meta.url), "--child"
  ], {
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
      SUPABASE_URL: "",
      SUPABASE_SERVICE_ROLE_KEY: "",
      SPATIAL_RESULT_ENABLED: "true",
      TEST_PROVIDER_TEXT: dmsOnlyProviderText,
      TEST_LOCAL_SOURCE_CONTEXT: localSourceContext,
      DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
    }
  });
  let stderr = "";
  child.stdout.resume();
  child.stderr.on("data", chunk => { stderr += String(chunk); });
  const signal = AbortSignal.timeout(30_000);
  try {
    const [{ port }] = await once(child, "message", { signal });
    const fixture = await readFile(path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg"));
    const form = new FormData();
    form.set("visitorId", "multi-representation-http-regression");
    form.set("image", new Blob([fixture], { type: "image/jpeg" }), "multi-representation.jpg");
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
      method: "POST",
      headers: { "x-regression-test": "1", "x-regression-case-id": "multi-representation-dms-only" },
      body: form,
      signal
    });
    const payload = await response.json();
    const statsPromise = once(child, "message", { signal });
    child.send("stats");
    const [stats] = await statsPromise;
    assert.equal(response.status, 200, JSON.stringify(payload));
    assert.equal(payload.success, true);
    assert.equal(payload.authorizationStatus, "REVIEW_REQUIRED");
    assert.equal(payload.resultStatus, "needs_review");
    assert.equal(payload.requiresReview, true);
    assert.equal(payload.mapStatus, "ENABLED");
    assert.equal(payload.mapReady, true);
    assert.equal(payload.kmlStatus, "ENABLED");
    assert.equal(payload.kmlReady, true);
    assert.equal(payload.finalizedCoordinateResult?.decisionState, "REVIEW_REQUIRED");
    assert.equal(payload.finalizedCoordinateResult?.confirmationStatus, "pending");
    assert.equal(payload.recognitionFileName, "multi-representation.jpg");
    assert.equal(payload.multiRepresentationEvidence?.status, "COMPLETE");
    assert.equal(payload.multiRepresentationEvidence?.sourceRowCount, 6);
    assert.deepEqual(payload.multiRepresentationEvidence?.labels, ["1", "2", "3", "4", "5", "6"]);
    assert.deepEqual(payload.providerDmsReviewEvidence?.sourceLabels, ["1", "2", "3", "4", "5", "6"]);
    assert.deepEqual(payload.sourceCoordinateRepresentation?.pointLabels, ["1", "2", "3", "4", "5", "6"]);
    assert.equal(payload.sourceCoordinateRepresentation?.rows?.length, 6);
    assert.ok(!String(payload.sourceCoordinateRepresentation?.displayText || "").includes("POINT | X | Y"));
    assert.equal(stats.providerCalls, 1);
  } catch (error) {
    if (stderr) process.stderr.write(stderr.slice(-8000));
    throw error;
  } finally {
    child.kill();
    await once(child, "exit").catch(() => {});
  }
  console.log("multi-representation HTTP regression: PASS (mock Provider calls: 1; real Provider calls: 0)");
}
