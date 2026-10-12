import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import {
  MINING_JUDGEABILITY_CONTRACT_VERSION,
  MINING_JUDGEABILITY_FAILURE_PRECEDENCE,
  MINING_JUDGEABILITY_FAILURE_REASON,
  applyMiningJudgeabilityGate,
  buildMiningJudgeabilityFailurePayload,
  evaluateMiningJudgeability
} from "../server/mining-judgeability.js";

const __filename = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(__filename), "..");

function providerOutput({ status = "JUDGEABLE", reason = "JUDGEABLE", body = "" } = {}) {
  return `【可判读性】\n${status}\n\n【可判读性原因】\n${reason}\n\n${body}`;
}

function collectKeys(value, target = new Set()) {
  if (!value || typeof value !== "object") return target;
  for (const [key, nested] of Object.entries(value)) {
    target.add(key);
    collectKeys(nested, target);
  }
  return target;
}

test("exact structured JUDGEABLE decision is the only passing contract", () => {
  const gate = applyMiningJudgeabilityGate(providerOutput({
    body: "【对象类型】\n矿石\n\n【结论】\n继续观察。"
  }));
  assert.equal(gate.allowed, true);
  assert.equal(gate.evaluation.status, "JUDGEABLE");
  assert.equal(gate.evaluation.primaryReason, null);
  assert.equal(gate.evaluation.contractVersion, MINING_JUDGEABILITY_CONTRACT_VERSION);
});

test("missing judgeability section fails closed", () => {
  const gate = applyMiningJudgeabilityGate("【结论】\n可见明显人工扰动。\n\n【等级】\nB");
  assert.equal(gate.allowed, false);
  assert.equal(gate.statusCode, 422);
  assert.equal(gate.evaluation.primaryReason, MINING_JUDGEABILITY_FAILURE_REASON.TARGET_NOT_IDENTIFIABLE);
});

test("malformed status and missing reason fail closed", () => {
  for (const output of [
    providerOutput({ status: "PASS", reason: "JUDGEABLE" }),
    providerOutput({ status: "JUDGEABLE", reason: "" }),
    providerOutput({ status: "FAILED", reason: "" })
  ]) {
    assert.equal(applyMiningJudgeabilityGate(output).allowed, false);
  }
});

test("status must be the complete exact normalized token", () => {
  for (const status of ["JUDGEABLE extra", "JUDGEABLE\nextra", "JUDGEABLE FAILED"]) {
    const gate = applyMiningJudgeabilityGate(providerOutput({ status, reason: "JUDGEABLE" }));
    assert.equal(gate.allowed, false, `${JSON.stringify(status)} must fail closed`);
    assert.equal(gate.statusCode, 422);
  }
});

test("judgeability status and reason sections must each appear exactly once", () => {
  const duplicateStatus = `${providerOutput()}\n\n【可判读性】\nJUDGEABLE`;
  const duplicateReason = `${providerOutput()}\n\n【可判读性原因】\nJUDGEABLE`;
  const contradictoryStatus = `${providerOutput()}\n\n【可判读性】\nFAILED`;
  for (const output of [duplicateStatus, duplicateReason, contradictoryStatus]) {
    const gate = applyMiningJudgeabilityGate(output);
    assert.equal(gate.allowed, false);
    assert.equal(gate.evaluation.status, "FAILED");
  }
});

test("all approved failure reasons are recognized", () => {
  for (const reason of MINING_JUDGEABILITY_FAILURE_PRECEDENCE) {
    const evaluation = evaluateMiningJudgeability(providerOutput({ status: "FAILED", reason }));
    assert.equal(evaluation.status, "FAILED");
    assert.equal(evaluation.primaryReason, reason);
  }
});

test("Chinese visibility observations map to the approved reason enum", () => {
  const cases = [
    ["目标区被不透明多边形填充覆盖。", "ANNOTATION_OCCLUDED"],
    ["菜单和控制面板遮挡目标区域。", "UI_OCCLUDED"],
    ["云层遮挡使关键地表不可见。", "CLOUD_OBSCURED"],
    ["目标区域存在遮挡。", "TARGET_OCCLUDED"],
    ["图片分辨率过低，无法辨认细节。", "RESOLUTION_TOO_LOW"],
    ["无法确定需要分析的目标区域。", "TARGET_NOT_IDENTIFIABLE"],
    ["缺少上下游和周边空间语境。", "INSUFFICIENT_SPATIAL_CONTEXT"]
  ];
  for (const [reasonText, expected] of cases) {
    const evaluation = evaluateMiningJudgeability(providerOutput({ status: "FAILED", reason: reasonText }));
    assert.equal(evaluation.primaryReason, expected);
  }
});

test("failure precedence is deterministic and specific occlusion wins", () => {
  const output = providerOutput({
    status: "FAILED",
    reason: "目标区被不透明 Polygon 覆盖，同时有 UI 菜单遮挡、云层和低分辨率问题。"
  });
  assert.equal(evaluateMiningJudgeability(output).primaryReason, "ANNOTATION_OCCLUDED");
});

test("failure evidence overrides a contradictory JUDGEABLE token", () => {
  const output = providerOutput({
    status: "JUDGEABLE",
    reason: "JUDGEABLE",
    body: "目标区域被不透明标注覆盖，但仍输出肯定结论。"
  });
  assert.equal(evaluateMiningJudgeability(output).primaryReason, "ANNOTATION_OCCLUDED");
});

test("failure payload is a closed safe union without substantive fields", () => {
  const payload = buildMiningJudgeabilityFailurePayload({ primaryReason: "ANNOTATION_OCCLUDED" });
  assert.deepEqual(Object.keys(payload), ["success", "reason", "judgeability", "safe_message", "retry"]);
  assert.equal(payload.success, false);
  assert.equal(payload.reason, "JUDGEABILITY_FAILED");
  assert.equal(payload.judgeability.status, "FAILED");
  assert.equal(payload.judgeability.primary_reason, "ANNOTATION_OCCLUDED");
  assert.equal(payload.retry.user_charge, false);
  assert.equal(payload.retry.user_initiated_only, true);
  const forbiddenKeys = [
    "result", "analysis", "message", "content", "rawOutput", "score", "grade", "confidence",
    "recordId", "caseId", "case_id", "shareId", "image", "imageUrl", "imageURL"
  ];
  const keys = collectKeys(payload);
  forbiddenKeys.forEach(key => assert.equal(keys.has(key), false, `${key} must be absent`));
});

test("unknown failure input becomes a fixed safe reason", () => {
  const payload = buildMiningJudgeabilityFailurePayload({ primaryReason: "UNTRUSTED_PROVIDER_TEXT" });
  assert.equal(payload.judgeability.primary_reason, "TARGET_NOT_IDENTIFIABLE");
  assert.equal(JSON.stringify(payload).includes("UNTRUSTED_PROVIDER_TEXT"), false);
});

test("negative golden record is metadata-only and declares zero side effects", () => {
  const fixturePath = path.join(root, "fixtures", "mining-judgeability", "GJ-MINING-OCCLUDED-POLYGON-001.json");
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  assert.equal(fixture.containsCustomerData, false);
  assert.equal(fixture.containsImage, false);
  assert.equal(fixture.containsCoordinates, false);
  assert.equal(fixture.inputClass, "SYNTHETIC_METADATA_ONLY");
  assert.equal(fixture.expected.httpStatus, 422);
  assert.equal(fixture.expected.primaryReason, "ANNOTATION_OCCLUDED");
  assert.equal(fixture.expected.userCharge, false);
  assert.equal(fixture.expected.recordWriteCount, 0);
  assert.equal(fixture.expected.caseWriteCount, 0);
  assert.equal(fixture.expected.historyWriteCount, 0);
  assert.equal(fixture.expected.shareWriteCount, 0);
});

test("malicious provider claims cannot cross a failed production gate", () => {
  const sideEffects = { normalize: 0, record: 0, charge: 0, caseWrite: 0, share: 0 };
  const maliciousOutput = providerOutput({
    status: "FAILED",
    reason: "ANNOTATION_OCCLUDED",
    body: "【结论】\n可见老采坑、河道和明显人工扰动。\n\n【等级】\nB\n\n【判读可信度】\n高\n\n【潜力评分】\n70分"
  });
  const gate = applyMiningJudgeabilityGate(maliciousOutput);
  if (gate.allowed) {
    sideEffects.normalize += 1;
    sideEffects.record += 1;
    sideEffects.charge += 1;
    sideEffects.caseWrite += 1;
    sideEffects.share += 1;
  }
  assert.equal(gate.allowed, false);
  assert.deepEqual(sideEffects, { normalize: 0, record: 0, charge: 0, caseWrite: 0, share: 0 });
  const serialized = JSON.stringify(gate.payload);
  for (const forbidden of ["老采坑", "河道", "人工扰动", "70分", '"grade":"B"', '"confidence":"高"']) {
    assert.equal(serialized.includes(forbidden), false, `${forbidden} must be stripped`);
  }
});

test("server invokes the gate before normalization, records, charge, case writes and success response", () => {
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const start = server.indexOf("const originalContent = response.choices");
  const end = server.indexOf("res.json(responsePayload);", start);
  const imageBranch = server.slice(start, end);
  const gateAt = imageBranch.indexOf("applyMiningJudgeabilityGate(rawOutput)");
  assert.ok(gateAt >= 0);
  for (const marker of [
    "normalizeJudgeOutput(rawOutput)",
    "data.records.push(record)",
    "consumeUsage(visitorId, \"judge\"",
    "writeJudgeCase({",
    "const responsePayload = {"
  ]) {
    assert.ok(imageBranch.indexOf(marker) > gateAt, `${marker} must remain after the gate`);
  }
  assert.match(imageBranch, /return res\.status\(judgeabilityGate\.statusCode\)\.json\(judgeabilityGate\.payload\)/);
});

test("server prompt requires the structured visibility decision before substantive analysis", () => {
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const promptAt = server.indexOf("【可判读性合同：必须首先输出】");
  const stageAt = server.indexOf("Stage 0 必须先做【对象类型】识别", promptAt);
  assert.ok(promptAt >= 0);
  assert.ok(stageAt > promptAt);
  assert.match(server.slice(promptAt, stageAt), /不得继续输出矿业类型、采坑、河道、扰动、Score、Grade、Confidence/);
});

test("failed judgeability does not return or log provider body", () => {
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const start = server.indexOf("console.log(\"AI判读阿里云返回元数据");
  const gateEnd = server.indexOf("const normalizedOutput = normalizeJudgeOutput(rawOutput);", start);
  const protectedRegion = server.slice(start, gateEnd);
  assert.equal(protectedRegion.includes("data: response"), false);
  assert.equal(protectedRegion.includes("content: response"), false);
  assert.equal(protectedRegion.includes("content: rawOutput"), false);
  assert.equal(protectedRegion.includes("rawOutput,"), false);
});

test("pre-gate mining logs do not persist upload file names", () => {
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const start = server.indexOf("const judgeImageFiles = imageFiles.slice(0, 1)");
  const end = server.indexOf("const judgeabilityGate = applyMiningJudgeabilityGate(rawOutput)", start);
  const preGate = server.slice(start, end);
  assert.equal(preGate.includes("file.originalname"), false);
  assert.equal(preGate.includes("fileNames:"), false);

  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const uploadStart = html.indexOf("const compressedFiles = await Promise.all");
  const uploadEnd = html.indexOf("const sendJudgeRequest = async", uploadStart);
  const browserUploadLogRegion = html.slice(uploadStart, uploadEnd);
  assert.equal(browserUploadLogRegion.includes("file.name"), false);
  assert.equal(browserUploadLogRegion.includes("compressed.name"), false);
});

test("frontend handles failure even if HTTP status is accidentally successful", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const failedAt = html.indexOf('data?.reason === "JUDGEABILITY_FAILED"');
  const successAt = html.indexOf('console.log("AI判读进入成功展示分支")');
  assert.ok(failedAt >= 0);
  assert.ok(failedAt < successAt);
  assert.match(html.slice(failedAt, successAt), /handleJudgeabilityFailure\(data\)/);
});

test("frontend failure handler clears authority and performs no history or share write", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const start = html.indexOf("function handleJudgeabilityFailure");
  const end = html.indexOf("if (judgeResult)", start);
  const handler = html.slice(start, end);
  assert.match(handler, /judgeResult\.value = ""/);
  assert.match(handler, /clearJudgeDecisionCard\(\)/);
  assert.match(handler, /resetJudgeFeedback\(\)/);
  for (const forbidden of ["localStorage", "saveJudgeResultToRecent", "renderJudgeDecisionCard", "showJudgeFeedback", "trackEvent", "fetch("]) {
    assert.equal(handler.includes(forbidden), false, `${forbidden} is forbidden in failure handler`);
  }
});

test("frontend uses a fixed allowlist instead of provider safe_message", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const start = html.indexOf("function handleJudgeabilityFailure");
  const end = html.indexOf("if (judgeResult)", start);
  const handler = html.slice(start, end);
  assert.match(handler, /JUDGEABILITY_SAFE_MESSAGES\[primaryReason\]/);
  assert.equal(handler.includes("data.safe_message"), false);
});

// Incremental acceptance: execute the actual route, transport and browser handler.
// Only external I/O is replaced. No server startup, network, database or file writes.
const recoveryServer = fs.readFileSync(path.join(root, "server.js"), "utf8");
const recoveryHtml = fs.readFileSync(process.env.MINING_UI_HTML_PATH || path.join(root, "index.html"), "utf8");
function recoverySection(source, start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.ok(from >= 0 && to > from, `Missing production source boundary: ${start}`);
  return source.slice(from, to);
}

function miningRecoveryRuntime(options = {}) {
  const calls = { fetch: 0, charge: 0, caseWrite: 0, adminWrite: 0, usageLog: 0, logs: [], payloads: [] };
  const consoleStub = Object.fromEntries(["log", "warn", "error"].map(key => [key, (...args) => calls.logs.push(args)]));
  const data = { records: [], usage: {}, events: [], featureFlags: {} };
  const output = options.output ?? providerOutput({ body: "【对象类型】\n矿石\n\n【场景分类】\n原矿石\n\n【结论】\n仅作外观初筛。\n\n【等级】\nC\n\n【关键依据】\n可见岩石结构。" });
  const provider = options.provider ?? {
    model: "qwen3.8-flash", choices: [{ message: { content: output } }],
    usage: { prompt_tokens: 1000, completion_tokens: 300 }
  };
  const context = vm.createContext({
    console: consoleStub, process: { env: {} }, Buffer, AbortController, setTimeout, clearTimeout,
    aliyunApiKey: "synthetic-key", aliyunBaseURL: options.base ?? "https://dashscope.aliyuncs.com/compatible-mode/v1",
    aliyunMiningModel: options.model ?? "qwen3.8-flash", aliyunVisionModel: "coordinate-model-unchanged",
    latestProviderModelObservation: "coordinate-observation-unchanged",
    isFamilyRetryAllowed: () => true, getRecognitionBudget: () => null,
    getRecognitionDeadlineSignal: () => null,
    composeAbortSignals: signals => ({ signal: signals[0], cleanup() {} }),
    fetch: async (url, init) => {
      calls.fetch++;
      calls.payloads.push(JSON.parse(init.body));
      if (options.transportError) throw Object.assign(new Error("untrusted synthetic failure"), { name: options.transportError });
      return { status: options.status ?? 200, ok: (options.status ?? 200) < 300, json: async () => provider };
    },
    upload: { fields: () => null },
    app: { post: (url, middleware, handler) => { assert.equal(url, "/api/analyze-mining-image"); context.route = handler; } },
    readAdminData: async () => data, ensureUser: () => ({ visitorId: "offline-only" }),
    getEffectivePermissions: () => ({ aiJudgeEnabled: true }), updateUserVisitMeta: async () => {},
    checkUsage: async () => ({ allowed: true }),
    consumeUsage: async (id, type, req, settings) => {
      calls.charge++;
      assert.equal(settings.allowSourceFallback, false);
      calls.cost = settings.metadata;
      if (options.chargeThrows) throw new Error("untrusted database error");
      return options.chargeResult ?? { success: true, source: "free", quota: { free: 2 } };
    },
    appendUsageLog: async () => { calls.usageLog++; },
    writeJudgeCase: async () => { calls.caseWrite++; if (options.caseThrows) throw new Error("untrusted storage error"); return "offline-case"; },
    writeAdminData: async () => { calls.adminWrite++; },
    makeId: () => "offline-record", getNowISO: () => "2026-10-12T00:00:00Z",
    getQuotaExhaustedCode: () => "JUDGE_QUOTA_EXHAUSTED", applyMiningJudgeabilityGate
  });
  vm.runInContext([
    recoverySection(recoveryServer, "function extractProviderMessageText(", "function buildProviderMessageDiagnostic("),
    recoverySection(recoveryServer, "function getAliyunChatCompletionsUrl(", "async function callAliyunProductionLayoutQualificationProbe("),
    recoverySection(recoveryServer, "async function callAliyunVision(", "function getHandwrittenDmsTranscriptionPrompt("),
    recoverySection(recoveryServer, "// Mining evidence presentation only:", 'app.get("/api/config"'),
    recoverySection(recoveryServer, 'app.post("/api/analyze-mining-image"', 'app.post("/api/regression/parse-coordinate-text"')
  ].join("\n"), context);
  return {
    context, calls, data,
    async run() {
      const req = { headers: {}, body: { visitorId: "offline-only" }, get: () => "offline-only",
        files: { images: [{ mimetype: "image/jpeg", size: 8, originalname: "judge-upload.jpg", buffer: Buffer.from("offline") }] } };
      const result = { statusCode: 200, body: null };
      const res = { status(code) { result.statusCode = code; return this; }, json(body) { result.body = body; return this; } };
      await context.route(req, res);
      return result;
    }
  };
}

test("mining recovery: real route uses dedicated model once and settles once", async () => {
  const runtime = miningRecoveryRuntime();
  const result = await runtime.run();
  assert.equal(result.statusCode, 200);
  assert.equal(result.body.success, true);
  assert.match(result.body.result, /外观描述，待核对|结果不完整，待核对/);
  assert.equal(runtime.calls.fetch, 1);
  assert.equal(runtime.calls.charge, 1);
  assert.equal(runtime.calls.caseWrite, 1);
  assert.equal(runtime.calls.adminWrite, 1);
  assert.equal(runtime.calls.usageLog, 1);
  const payload = runtime.calls.payloads[0];
  assert.equal(payload.model, "qwen3.8-flash");
  assert.equal(payload.enable_thinking, false);
  assert.equal(payload.max_tokens, 360);
  assert.equal(payload.messages[0].content.filter(item => item.type === "image_url").length, 1);
  assert.equal(runtime.context.latestProviderModelObservation, "coordinate-observation-unchanged");
  assert.equal(runtime.calls.cost.estimated_cost_cny, 0.00161);
  assert.equal(runtime.calls.cost.provider_model_match, true);
});

test("mining recovery: missing dedicated model and wrong endpoint fail before I/O", async () => {
  for (const options of [{ model: "" }, { model: "qwen-vl-plus" }, { base: "https://example.invalid/v1" }]) {
    const runtime = miningRecoveryRuntime(options);
    const result = await runtime.run();
    assert.equal(result.body.reason, "MINING_CONFIG_UNAVAILABLE");
    assert.equal(runtime.calls.fetch, 0);
    assert.equal(runtime.calls.charge, 0);
    assert.equal(runtime.calls.adminWrite, 0);
  }
});

test("mining recovery: 404 classifications preserve no raw Provider error", async () => {
  for (const [code, reason] of [
    ["model_not_found", "MINING_MODEL_UNAVAILABLE"], ["endpoint_not_found", "MINING_ENDPOINT_NOT_FOUND"],
    ["untrusted-code", "MINING_PROVIDER_404_UNKNOWN"]
  ]) {
    const runtime = miningRecoveryRuntime({ status: 404, provider: { error: { code, message: "PRIVATE_SYNTHETIC_MARKER" } } });
    const result = await runtime.run();
    assert.equal(result.body.reason, reason);
    assert.equal(result.body.quotaStatus, "NOT_ATTEMPTED");
    assert.equal(runtime.calls.fetch, 1);
    assert.equal(runtime.calls.charge, 0);
    assert.equal(runtime.calls.caseWrite, 0);
    assert.equal(runtime.calls.adminWrite, 0);
    assert.equal(JSON.stringify([result, runtime.calls.logs]).includes("PRIVATE_SYNTHETIC_MARKER"), false);
    assert.equal(JSON.stringify([result, runtime.calls.logs]).includes("untrusted-code"), false);
  }
});

test("mining recovery: transport and other HTTP failures never retry or settle", async () => {
  for (const [options, reason] of [
    [{ transportError: "AbortError" }, "MINING_PROVIDER_TIMEOUT"],
    [{ transportError: "TypeError" }, "MINING_PROVIDER_NETWORK_ERROR"],
    [{ status: 429 }, "MINING_PROVIDER_HTTP_ERROR"], [{ status: 500 }, "MINING_PROVIDER_HTTP_ERROR"]
  ]) {
    const runtime = miningRecoveryRuntime(options);
    const result = await runtime.run();
    assert.equal(result.body.reason, reason);
    assert.equal(runtime.calls.fetch, 1);
    assert.equal(runtime.calls.charge, 0);
  }
});

test("mining recovery: actual visibility gate still prevents charge and persistence", async () => {
  const runtime = miningRecoveryRuntime({ output: providerOutput({ status: "FAILED", reason: "ANNOTATION_OCCLUDED" }) });
  const result = await runtime.run();
  assert.equal(result.statusCode, 422);
  assert.equal(result.body.reason, "JUDGEABILITY_FAILED");
  assert.equal(runtime.calls.fetch, 1);
  assert.equal(runtime.calls.charge, 0);
  assert.equal(runtime.calls.caseWrite, 0);
  assert.equal(runtime.calls.adminWrite, 0);
});

test("mining recovery: unknown settlement is explicit with no compensation or retry", async () => {
  for (const options of [{ chargeThrows: true }, { chargeResult: { success: false } }, { caseThrows: true }]) {
    const runtime = miningRecoveryRuntime(options);
    const result = await runtime.run();
    assert.equal(result.body.reason, "MINING_SETTLEMENT_UNKNOWN");
    assert.equal(result.body.quotaStatus, "UNKNOWN");
    assert.equal(runtime.calls.fetch, 1);
    assert.equal(runtime.calls.charge, 1);
    assert.equal(runtime.calls.adminWrite, 0);
  }
});

test("mining recovery: usage must be observed and pricing bound stays within reservation", async () => {
  const runtime = miningRecoveryRuntime({ provider: { choices: [{ message: { content: providerOutput() } }] } });
  const result = await runtime.run();
  assert.equal(result.body.reason, "MINING_USAGE_UNVERIFIED");
  assert.equal(runtime.calls.charge, 0);
  const maximal = runtime.context.buildMiningAiCostMetadata({ input_tokens: 991808, output_tokens: 360 }, "qwen3.8-flash");
  assert.ok(maximal.estimated_cost_cny <= 0.816);
  for (const usage of [{}, { prompt_tokens: -1, completion_tokens: 1 }, { prompt_tokens: 10, completion_tokens: 361 }]) {
    assert.throws(() => runtime.context.buildMiningAiCostMetadata(usage), /MINING_USAGE_UNVERIFIED/);
  }
});

test("mining recovery: shared coordinate transport keeps default behavior", async () => {
  const runtime = miningRecoveryRuntime({ status: 404, provider: { error: { code: "model_not_found" } } });
  await assert.rejects(runtime.context.callAliyunVision({ modelName: "coordinate-model-unchanged", prompt: "offline", imageItems: [] }), error => {
    assert.equal(error.code, "ALIYUN_HTTP_ERROR");
    assert.equal(error.reason, "PROVIDER_HTTP_ERROR");
    assert.equal(error.miningReason, undefined);
    return true;
  });
  assert.equal(runtime.calls.payloads[0].model, "coordinate-model-unchanged");
  const success = miningRecoveryRuntime();
  await success.context.callAliyunVision({ modelName: "coordinate-model-unchanged", prompt: "offline", imageItems: [] });
  assert.equal(success.context.latestProviderModelObservation.evidenceType, "PROVIDER_RESPONSE_MODEL");
  assert.match(recoveryServer, /options\.allowSourceFallback !== false && freshStatus\.allowed/);
});

async function runMiningBrowserFailure(mode) {
  const seen = { fetch: 0, messages: [], history: 0 };
  const context = vm.createContext({
    console: { log() {}, warn() {}, error() {} },
    judgeOperationVersion: 0, judgeRequestInFlight: false, judgeSubmissionBlocked: false,
    updateJudgeUploadControls() {}, getCurrentJudgeImageDataUrl: () => "",
    showJudgeProgressVisual() {},
    selectedJudgeImageFiles: [{ size: 100, type: "image/jpeg" }], judgeImageInput: { files: [] },
    loadUserConfig: async () => {}, canUse: () => true, startJudgeProgress: () => 1,
    clearInterval() {}, compressJudgeImage: async file => file, trackEvent() {},
    FormData: class { append() {} }, judgeType: { value: "mine-land" }, visitorId: "offline", userCode: "offline",
    getSourceHeaders: headers => headers, judgeResult: { value: "" }, hideJudgeProgressVisual() {}, clearJudgeDecisionCard() {},
    showJudgeMessage: message => seen.messages.push(message),
    saveJudgeResultToRecent: () => { seen.history++; },
    fetch: async () => {
      seen.fetch++;
      if (mode === "network") throw new Error("PRIVATE_SYNTHETIC_MARKER");
      return { ok: mode === "parse", status: mode === "parse" ? 200 : 502,
        clone: () => ({ json: async () => {
          if (mode === "parse") throw new Error("PRIVATE_SYNTHETIC_MARKER");
          return { success: false, reason: mode, detail: "PRIVATE_SYNTHETIC_MARKER" };
        } }) };
    }
  });
  vm.runInContext(recoverySection(recoveryHtml, "    function getMiningFailureMessage(", "    async function copyText("), context);
  await context.analyzeMiningImage();
  return seen;
}

test("mining recovery: actual browser entry never retries failure and renders fixed text", async () => {
  for (const reason of ["network", "parse", "timeout", "MINING_MODEL_UNAVAILABLE", "MINING_PROVIDER_404_UNKNOWN", "MINING_SETTLEMENT_UNKNOWN", "untrusted"]) {
    const seen = await runMiningBrowserFailure(reason);
    assert.equal(seen.fetch, 1, reason);
    assert.equal(seen.history, 0);
    assert.ok(seen.messages.some(message => /请勿重复上传/.test(message)));
    assert.equal(JSON.stringify(seen).includes("PRIVATE_SYNTHETIC_MARKER"), false);
  }
});


// Incremental quality acceptance. These are the THREE EXISTING PROCESSED records,
// not raw Provider responses, new photos, new calls, or mineral ground truth.
// No server startup, network, database, or persistent browser writes are allowed.
const miningQualityCases = [
  {
    "caseId": "78ac3123-dbde-4ccf-9973-cb263cbd5bf6",
    "uploadSha256": "0d4c95e8bfb863d4953437dad6fc64c95bce641757060da807d1b02d445c686c",
    "oldGrade": "B",
    "oldScore": 65,
    "text": "【对象类型】\n矿石。图片主体为一块手持的岩石样本，表面可见明显的矿物结晶和氧化痕迹。\n\n【场景分类】\n矿化岩石。岩石表面有灰白色脉状物质穿插在褐黄色基岩中，符合矿化蚀变特征。\n\n【结论】\n石英脉穿插明显，值得敲开看。\n\n【等级】\nB。有明确的脉石结构和蚀变现象，具备找矿线索，但未见肉眼金。\n\n【判读可信度】\n高。样本清晰，结构关系明确，背景无干扰。\n\n【潜力评分】\n65分。属于典型的硅化/铁染构造，是常见的载金围岩环境，有一定观察价值。\n\n【关键依据】\n1. 灰白色条带（疑似石英或方解石）呈网状充填裂隙，说明有过热液活动。\n2. 基岩呈现明显的褐铁矿化（黄褐色），这是硫化物氧化的典型特征。\n3. 整体结构松散，断口粗糙，非单一金属块，符合野外采掘的原生矿石特征。\n\n【主要风险】\n1. 这种“铁帽”和石英脉虽然常见于金矿，但也大量存在于无经济价值的普通硅化带。\n2. 目前仅看到构造，未见到具体的金属硫化物（如黄铁矿、毒砂等），需警惕只是普通的废石。\n\n【下一步】\n1. 敲开样本拍断面。\n2. 补拍带比例物的清晰近景和原地环境。\n\n【一句话总结】\n先按误判图处理，补到结构证据后再看。"
  },
  {
    "caseId": "aa557ac8-3b31-4392-87d7-b215769f9718",
    "uploadSha256": "abac27f12f45d9aa0b56e304086ccf84c899a450b1f9fe7d3762c5a05e5671d8",
    "oldGrade": "C",
    "oldScore": 45,
    "text": "【对象类型】\n矿石。主体是一块被手持的、带有明显氧化铁染色的白色脉石矿物标本。\n\n【场景分类】\n自然金块：外观具备自然金块形态特征，仍需检测确认。\n\n【结论】\n有石英和褐铁矿，未见金。\n\n【等级】\nC。具备典型的蚀变围岩特征（石英+褐铁矿），是常见的找矿指示物，但肉眼未见自然金颗粒。\n\n【判读可信度】\n高。图片清晰度高，对焦准确，能清楚分辨矿物结构和表面纹理。\n\n【潜力评分】\n45分。属于正常的矿化线索，值得留意但不能直接作为见金依据。\n\n【关键依据】\n1. 基质主要为乳白色石英，具有油脂光泽或玻璃光泽。\n2. 表面覆盖大量红褐色/黄褐色物质，为典型的褐铁矿化（铁帽）现象。\n3. 整体形态呈不规则棱角状，断口粗糙，无金属延展性或熔炼痕迹。\n\n【主要风险】\n1. 容易将表面的黄褐色氧化铁误认为是黄金。\n2. 仅凭外观无法排除内部包裹极细小微粒金的可能，但也可能完全无金。\n\n【下一步】\n1. 敲开石头观察新鲜断面，看是否有裂隙控制的金线或硫化物。\n2. 使用放大镜仔细检查石英与褐铁矿交界处是否有片状或粒状金属反光。\n\n【一句话总结】\n典型铁染石英，看着像那么回事，但没"
  },
  {
    "caseId": "c1bff339-6077-4db6-af31-e4dbcb18f889",
    "uploadSha256": "0d4c95e8bfb863d4953437dad6fc64c95bce641757060da807d1b02d445c686c",
    "oldGrade": "C",
    "oldScore": 45,
    "text": "【对象类型】\n矿石。图片主体为一块手持的岩石样本，表面可见明显的矿物脉体与围岩结构。\n\n【场景分类】\n矿化岩石。岩石表面有清晰的浅色细脉（疑似石英或碳酸盐脉）穿插在褐黄色风化基岩中，属于典型的野外露头/采坑取样特征。\n\n【结论】\n有脉体穿插，但未见明显金点。\n\n【等级】\nC。具备一定构造特征（细脉），可作为找矿线索观察，但缺乏直接的高价值金属赋存证据。\n\n【判读可信度】\n高。目标清晰，纹理、裂隙及颜色对比度高，足以支撑初步地质判断。\n\n【潜力评分】\n45分。有脉不代表有金，目前仅能看出是含脉体的硅化/铁染岩石，需进一步验证。\n\n【关键依据】\n1. 存在明确的浅色细脉穿插于褐黄色基岩中，说明该处经历过热液活动或构造破碎充填。\n2. 岩石整体呈现强烈的褐铁矿化（铁染）特征，这是硫化物氧化后的常见现象。\n3. 脉体较细且未见到明显的“大肚子”膨胀段，也未见肉眼可见的金属硫化物（如黄铁矿颗粒）。\n\n【主要风险】\n1. 常见的误判：将普通的石英细脉或方解石脉当成含金脉体。\n2. 强铁染可能掩盖了内部真实的矿物成分，导致漏看细微的硫化物共生关系。\n\n【下一步】\n1. 敲开样本拍断面。\n2. 补拍带比例物的清晰近景和原地环境。\n\n【一句话总结】\n先按误判图处理，补到结构证据后再看。"
  }
];

function miningQualityContext() {
  const effects = { network: 0, historyWrites: 0 };
  const stored = new Map();
  const stopNetwork = () => { effects.network++; throw new Error("OFFLINE_QUALITY_NETWORK_FORBIDDEN"); };
  const escape = value => String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  const context = vm.createContext({
    console: { log() {}, warn() {}, error() {} },
    judgeDetailAnimations: [], judgeOperationVersion: 0, judgeRequestInFlight: false,
    updateJudgeUploadControls() {},
    fetch: stopNetwork, XMLHttpRequest: stopNetwork, WebSocket: stopNetwork,
    escapeHtml: escape, judgeResult: { value: "", innerHTML: "" },
    judgeDecisionCard: { hidden: true, innerHTML: "", className: "" },
    judgeRecentPanel: { hidden: true }, judgeRecentList: { innerHTML: "" },
    judgeRecentBest: { textContent: "", classList: { remove() {} } },
    currentJudgeImageDataUrl: "", judgeImagePreview: null,
    JUDGE_HISTORY_KEY: "offline-existing-texts",
    localStorage: {
      getItem: key => stored.get(key) || null,
      setItem: (key, value) => { effects.historyWrites++; stored.set(key, value); },
      removeItem: key => { effects.historyWrites++; stored.delete(key); }
    },
    clearJudgeDecisionCard() {}, showJudgeMessage() {}
  });
  const sharedStart = "// Mining evidence presentation only:";
  vm.runInContext([
    recoverySection(recoveryHtml, sharedStart, "    function getJudgeRecentItems("),
    recoverySection(recoveryHtml, "    function getJudgeRecentItems(", "    function removeJudgeRecentByCurrentImage("),
    recoverySection(recoveryHtml, "    function escapeJudgeDetailHtml(", "    function handleJudgeabilityFailure("),
    recoverySection(recoveryHtml, "    function buildJudgeCopyText(", "    function copyJudgeResult(")
  ].join("\n"), context);
  const backend = vm.createContext({ fetch: stopNetwork });
  vm.runInContext([
    recoverySection(recoveryServer, sharedStart, "function stripMarkdownCodeBlock("),
    recoverySection(recoveryServer, "function extractJudgeCaseGrade(", "function hashJudgeCaseImage(")
  ].join("\n"), backend);
  return { context, backend, effects, stored };
}

const qualityForbidden = /【(?:等级|潜力评分|判读可信度|一句话总结|结论)】|(?:45|65)分|等级\s*[BCD]|可信度\s*(?:<[^>]+>)*高|开始有点意思|风险偏高别重投入|老采坑环境|黄铁矿感|当前最高|新高！/;

// Run only these presentation increments with --test-name-pattern='mining confirmed UI:'.
// Reuse the existing three processed texts; never dispatch a recognition request.
test("mining confirmed UI: compact summary leaves complete evidence in detail exactly once", () => {
  const { context, effects } = miningQualityContext();
  for (const fixture of miningQualityCases) {
    const view = context.buildMiningEvidenceView(fixture.text);
    context.judgeResult.value = fixture.text;
    context.renderJudgeDecisionCard(fixture.text);
    context.renderJudgeDetail(fixture.text);
    const summary = context.judgeDecisionCard.innerHTML;
    const detail = context.judgeResult.innerHTML;
    assert.doesNotMatch(summary, /<h3>|<ul>|<li>/);
    assert.ok(summary.includes(context.escapeHtml(view.status)));
    assert.ok(summary.includes(context.escapeHtml(view.conflict ? "分类待核对" : view.objectType + " · " + view.sceneType)));
    for (const value of [...view.features, ...view.limitations, ...view.actions, ...view.notes, ...view.issues]) {
      assert.ok(detail.includes(context.escapeJudgeDetailHtml(value)), value);
      if (value !== view.features[0] && value !== view.actions[0]) {
        assert.ok(!summary.includes(context.escapeHtml(value)), "only the leading evidence/action may be summarized");
      }
    }
    for (const title of ["结果状态", "对象类型", "场景分类"]) assert.ok(!detail.includes(`<h3>${title}</h3>`));
    assert.ok(detail.includes(context.escapeJudgeDetailHtml(view.imageReadability)));
    assert.doesNotMatch(summary + detail, qualityForbidden);
    assert.equal(context.buildJudgeCopyText(), context.formatMiningEvidenceView(view));
  }
  assert.deepEqual(effects, { network: 0, historyWrites: 0 });
});

test("mining confirmed UI: approved dimensions are mining-scoped and long detail is not clipped", () => {
  const css = recoverySection(recoveryHtml, "    /* MINING_CONFIRMED_LAYOUT_START", "    /* MINING_CONFIRMED_LAYOUT_END");
  const rules = css.replace(/\/\*[\s\S]*?\*\//g, "").matchAll(/([^{}]+)\{([^{}]*)\}/g);
  for (const [, selectors] of rules) {
    for (const selector of selectors.split(/,(?![^()]*\))/)) assert.ok(selector.trim().startsWith("#judgePage"), selector);
  }
  assert.match(css, /#judgePage \.page-nav \{ position: static;/);
  assert.match(css, /grid-template-columns: 92px minmax\(0, 1fr\)/);
  assert.match(css, /\.judge-detail-content \{[^}]*min-height: 0; max-height: none; overflow: visible;/);
  assert.match(css, /\.judge-result-tools \{ position: static;/);
  assert.match(css, /\.judge-detail-content h3 \{[^}]*font-size: 16px; font-weight: 700;/);
  assert.match(css, /\.judge-detail-content ul \{[^}]*font-size: 16px; font-weight: 500; line-height: 1\.5;/);
  const { context, effects } = miningQualityContext();
  context.renderJudgeDetail(miningQualityCases[0].text);
  assert.match(context.judgeResult.innerHTML, /补拍带比例物的清晰近景和原地环境/);
  assert.deepEqual(effects, { network: 0, historyWrites: 0 });
});

test("mining confirmed UI: upload and existing actions retain their identities without new submission wiring", () => {
  const page = recoverySection(recoveryHtml, '    <section id="judgePage"', '    <section id="goldPage"');
  for (const id of ["judgeImageInput", "judgeQuickUpload", "judgeUploadCard", "judgeImagePreview", "judgeResult", "judgeDecisionCard", "judgeQuotaStatus"]) {
    assert.equal(page.split(`id="${id}"`).length - 1, 1, id);
  }
  assert.match(page, /上传矿地图片，查看初步判读/);
  assert.match(page, /结果仅供参考，不能替代现场采样、检测与专业判断/);
  assert.match(page, /onclick="clearJudgeResult\(\)"/);
  assert.match(page, /onclick="copyJudgeResult\(\)"/);
  assert.match(page, /onclick="chooseJudgeReplacement\(\)">更换图片/);
  assert.doesNotMatch(page, /analyzeMiningImage\(|fetch\(|示例图片|补充图片/);
});

test("mining confirmed UI: composed inline scripts parse without execution or network", () => {
  let parsed = 0;
  for (const [, attrs, source] of recoveryHtml.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)) {
    if (/\bsrc=|application\/ld\+json|\btype=["']module/.test(attrs) || !source.trim()) continue;
    new vm.Script(source);
    parsed++;
  }
  assert.ok(parsed > 0);
});

test("mining quality: only the three historical processed texts are replayed", () => {
  assert.equal(miningQualityCases.length, 3);
  assert.equal(miningQualityCases[0].uploadSha256, miningQualityCases[2].uploadSha256);
  assert.notEqual(miningQualityCases[0].uploadSha256, miningQualityCases[1].uploadSha256);
  assert.match(miningQualityCases[0].text, /65分/);
  assert.match(miningQualityCases[1].text, /自然金块/);
  assert.match(miningQualityCases[2].text, /采坑取样/);
  const { backend, effects } = miningQualityContext();
  for (const fixture of miningQualityCases) {
    const result = backend.normalizeJudgeOutput(fixture.text);
    assert.doesNotMatch(result, qualityForbidden);
    assert.match(result, /模型原文未取得/);
    assert.match(result, /单张照片不能确认矿物成分/);
    const view = backend.buildMiningEvidenceView(result);
    assert.equal(view.objectType, "矿石");
    assert.equal(backend.extractJudgeCaseGrade(result), null);
    assert.deepEqual(Array.from(backend.extractJudgeCaseKeywords(result)), []);
    assert.equal(backend.extractJudgeCaseImageType(result), "矿石");
    assert.equal(backend.extractJudgeCaseSuggestedNextImage(result), backend.buildMiningEvidenceView(result).actions[0]);
    assert.equal(backend.normalizeJudgeOutput(result), result, "format must be idempotent");
  }
  assert.equal(effects.network, 0);
});

test("mining quality: backend and frontend use the identical evidence parser", () => {
  const from = "// Mining evidence presentation only:";
  assert.equal(
    recoverySection(recoveryServer, from, "function normalizeJudgeOutput(").trim(),
    recoverySection(recoveryHtml, from, "function cleanupJudgeHistoryStorage(").trim()
  );
  const { context, backend, effects } = miningQualityContext();
  for (const fixture of miningQualityCases) {
    assert.equal(JSON.stringify(context.buildMiningEvidenceView(fixture.text)), JSON.stringify(backend.buildMiningEvidenceView(fixture.text)));
    assert.equal(context.formatMiningEvidenceView(context.buildMiningEvidenceView(fixture.text)), backend.normalizeJudgeOutput(fixture.text));
  }
  assert.equal(effects.network, 0);
});

test("mining quality: explicit rock classification survives context and negation", () => {
  const { context, backend, effects } = miningQualityContext();
  for (const fixture of [miningQualityCases[0], miningQualityCases[2]]) {
    const view = context.buildMiningEvidenceView(fixture.text);
    assert.equal(view.objectType, "矿石");
    assert.equal(view.sceneType, "矿化岩石");
    assert.equal(view.conflict, false);
    context.renderJudgeDecisionCard(fixture.text);
    assert.match(context.judgeDecisionCard.innerHTML, /矿石 · 矿化岩石/);
    assert.doesNotMatch(context.judgeDecisionCard.innerHTML, /老采坑环境|黄铁矿感/);
    assert.equal(backend.checkJudgeObjectTypeConsistency(backend.normalizeJudgeOutput(fixture.text)).forcedDowngrade, false);
  }
  assert.match(context.buildMiningEvidenceView(miningQualityCases[2].text).features.join("\n"), /也未见肉眼可见的金属硫化物/);
  assert.equal(effects.network, 0);
});

test("mining quality: conflicting and incomplete second result is not repaired into a claim", () => {
  const { context, backend, effects } = miningQualityContext();
  const fixture = miningQualityCases[1];
  const view = backend.buildMiningEvidenceView(fixture.text);
  assert.equal(view.conflict, true);
  assert.equal(view.objectType, "矿石");
  assert.equal(view.sceneType, "自然金块", "retain the recorded conflicting field instead of guessing a replacement");
  assert.match(view.issues.join("\n"), /尾句可能不完整/);
  const result = backend.normalizeJudgeOutput(fixture.text);
  const check = backend.checkJudgeObjectTypeConsistency(result);
  assert.equal(check.consistencyCheck, "conflict");
  assert.equal(check.forcedDowngrade, false);
  context.renderJudgeDecisionCard(result);
  assert.match(context.judgeDecisionCard.innerHTML, /分类待核对/);
  assert.match(context.judgeDecisionCard.innerHTML, /无金属延展性或熔炼痕迹/);
  assert.doesNotMatch(context.judgeDecisionCard.innerHTML, qualityForbidden);
  assert.equal(effects.network, 0);
});

test("mining quality: card detail copy recent restore and save cannot reintroduce scoring", () => {
  const { context, backend, effects, stored } = miningQualityContext();
  const history = miningQualityCases.map((item, i) => ({ id: item.caseId, result: item.text, grade: item.oldGrade, score: item.oldScore, time: 1791802934000 + i * 60000 }));
  stored.set("offline-existing-texts", JSON.stringify(history));
  context.cleanupJudgeHistoryStorage();
  assert.equal(effects.historyWrites, 0, "do not erase original history during cleanup");
  for (let i = 0; i < miningQualityCases.length; i++) {
    const fixture = miningQualityCases[i];
    for (const text of [fixture.text, backend.normalizeJudgeOutput(fixture.text)]) {
      context.judgeResult.value = text;
      context.renderJudgeDecisionCard(text);
      context.renderJudgeDetail(text);
      context.renderJudgeRecent();
      const surfaces = [context.judgeDecisionCard.innerHTML, context.judgeResult.innerHTML, context.judgeRecentList.innerHTML, context.buildJudgeCopyText()];
      for (const surface of surfaces) {
        assert.doesNotMatch(surface, qualityForbidden);
        assert.doesNotMatch(surface, /值得继续投入|有点意思可以再看/);
      }
      assert.match(context.judgeResult.innerHTML, /证据限制/);
      assert.match(context.buildJudgeCopyText(), /【补证动作】/);
    }
    context.restoreJudgeRecent(i);
    assert.doesNotMatch(context.judgeDecisionCard.innerHTML, qualityForbidden);
  }
  const before = stored.get("offline-existing-texts");
  assert.equal(before, JSON.stringify(history), "presentation must not rewrite historical evidence");
  context.saveJudgeResultToRecent(backend.normalizeJudgeOutput(miningQualityCases[0].text));
  const saved = JSON.parse(stored.get("offline-existing-texts"))[0];
  for (const field of ["grade", "score", "decisionText"]) assert.equal(field in saved, false);
  assert.equal(effects.historyWrites, 1, "only simulated in-memory history persistence");
  assert.equal(effects.network, 0);
});

test("mining quality: request wiring preserves model budget and makes no stronger quality claim", () => {
  const start = recoveryServer.indexOf("    const prompt = `你是图片外观初筛助手");
  assert.ok(start > 0);
  const end = recoveryServer.indexOf("    const aliyunStartedAt", start);
  const prompt = recoveryServer.slice(start, end);
  assert.match(prompt, /不得输出等级、潜力分、可信度分、投资判断/);
  assert.match(prompt, /JUDGEABLE 仅表示可描述外观/);
  assert.match(recoveryServer, /finishReason: response\?\.choices\?\.\[0\]\?\.finish_reason/);
  assert.match(recoveryServer.slice(end, end + 1400), /maxTokens: 360/);
  assert.match(recoveryServer.slice(end, end + 1400), /temperature: 0\.3/);
  for (const removed of ["function protectNaturalGoldJudgeOutput", "function extractJudgeScore", "FAST_JUDGE_COPY", "variationSeed", "pickFastJudgeCopy"]) {
    assert.equal((recoveryServer + recoveryHtml).includes(removed), false, removed);
  }
});

// Only this new increment is executed for the 2026-10-13 result-value repair.
// Photo-derived notes below are manual fixture text, NEVER a provider result or mobile pass.
const riverbankMaterial = {
  imageSha256: "39cacb80c6453c211c1c95e84f45b1f36ae18619c0e42f8c8ebe26f4104e7b24",
  evidenceClass: "MANUAL_PHOTO_NOTES_NOT_MODEL_OUTPUT",
  text: "【对象类型】\n地表环境\n【场景分类】\n人工采坑\n【图像可判读性】\n可见坑壁和河道，缺比例尺。\n【可见事实】\n前景土坑含砾石，底部积水，后方有河道，画面边缘可见挖斗。\n【专业判断】\n可见土砾剖面可作为记录层位的观察点；不能由此认定含矿层。\n【证据限制】\n积水来源、层厚及是否采矿未确定；不确认坑壁安全。\n【补证动作】\n在安全位置补拍带比例尺的完整坑壁及其与河道的相对位置，记录各层深度；不进入未确认安全的坑内。"
};

function miningValueRuntime(options = {}) {
  const seen = { requests: 0, messages: [], saved: [], quota: 0, renderOrder: [] };
  const context = vm.createContext({
    console: { log() {}, warn() {}, error() {} },
    judgeOperationVersion: 1, judgeRequestInFlight: false, judgeSubmissionBlocked: false,
    selectedJudgeImageFiles: [{ name: "existing-fixture.jpg", size: 13209022, type: "image/jpeg" }],
    judgeImageInput: { files: [], value: "" }, judgeType: { value: "mine-land" },
    currentJudgeImageDataUrl: "data:image/jpeg;base64,INPUT_A",
    getCurrentJudgeImageDataUrl() { return context.currentJudgeImageDataUrl; },
    updateJudgeUploadControls() {}, loadUserConfig: async () => {}, canUse: () => true,
    startJudgeProgress() {}, clearInterval() {}, showJudgeProgressVisual() {}, hideJudgeProgressVisual() {},
    trackEvent() {}, resetJudgeFeedback() {}, showJudgeFeedback() {},
    judgeResult: { value: "" }, clearJudgeDecisionCard() {},
    getJudgeRecentItems: () => [],
    renderJudgeDecisionCard(text, binding) { seen.renderOrder.push(["summary", text, binding?.image || binding?.item?.image]); },
    revealJudgeDetail() { seen.renderOrder.push(["animation"]); },
    saveJudgeResultToRecent(text, image) { seen.saved.push({ text, image }); return { item: { text, image } }; },
    updateQuotaDisplay() { seen.quota++; }, loadUserQuota() { seen.quota++; },
    showJudgeMessage(text, isError = false) { seen.messages.push({ text, isError }); },
    sanitizeUserFacingTechMessage: text => text, showQuotaModal() {}, handleJudgeabilityFailure() {},
    visitorId: "offline", userCode: "offline", FormData: class { append() {} },
    getSourceHeaders: headers => headers,
    compressJudgeImage: options.compress || (async () => ({ size: 75000, type: "image/jpeg" })),
    fetch: async () => {
      seen.requests++;
      if (options.fetch) return options.fetch();
      return { ok: true, status: 200, clone: () => ({ json: async () => ({ success: true, result: miningQualityCases[0].text, quota: {} }) }) };
    }
  });
  vm.runInContext(recoverySection(recoveryHtml, "    function getMiningFailureMessage(", "    async function copyText("), context);
  return { context, seen };
}

test("mining value UI: successful large-original compression has no false size warning and one dispatch", async () => {
  const { context, seen } = miningValueRuntime();
  await context.analyzeMiningImage();
  assert.equal(seen.requests, 1, "stubbed local transport only");
  assert.equal(seen.saved.length, 1);
  assert.equal(seen.saved[0].image, "data:image/jpeg;base64,INPUT_A");
  assert.ok(!seen.messages.some(item => item.isError || /图片过大/.test(item.text)));
  assert.equal(context.judgeRequestInFlight, false);
  assert.equal(context.judgeSubmissionBlocked, false);
  assert.equal(seen.quota, 1);
});

test("mining value UI: local failures never fall back to the original or dispatch", async () => {
  for (const reason of ["image_invalid", "image_decode_failed", "image_dimensions_invalid", "image_processing_failed", "image_compressed_too_large"]) {
    const { context, seen } = miningValueRuntime({ compress: async () => { throw Object.assign(new Error("private"), { reason }); } });
    await context.analyzeMiningImage();
    assert.equal(seen.requests, 0, reason);
    assert.equal(seen.saved.length, 0);
    assert.equal(context.judgeRequestInFlight, false);
    assert.equal(context.judgeSubmissionBlocked, false);
    assert.ok(seen.messages.some(item => item.text === context.getMiningFailureMessage(reason) && item.isError));
    assert.ok(!JSON.stringify(seen).includes("private"));
  }
});

test("mining value UI: compressor keeps byte limits and reports decode dimensions encoding separately", async () => {
  for (const scenario of ["valid", "decode", "dimensions", "encoding", "oversized"]) {
    const canvas = () => ({
      getContext: () => ({ fillRect() {}, drawImage() {} }),
      toBlob(callback) { callback(scenario === "encoding" ? null : { size: scenario === "oversized" ? 1048577 : 75000 }); }
    });
    const context = vm.createContext({
      console: { log() {}, warn() {} },
      isUnsupportedJudgeImage: () => false,
      getMiningFailureMessage: reason => reason,
      loadImageForCompression: async () => {
        if (scenario === "decode") throw new Error("decode");
        return { naturalWidth: scenario === "dimensions" ? 0 : 5712, naturalHeight: 4284 };
      },
      document: { createElement: canvas },
      File: class { constructor(blobs, name, options) { this.size = blobs[0].size; this.type = options.type; this.name = name; } }
    });
    vm.runInContext(recoverySection(recoveryHtml, "    function judgeImageError(", "    function syncJudgeImageInputFiles("), context);
    const file = { size: 13209022, type: "image/jpeg" };
    if (scenario === "valid") assert.equal((await context.compressJudgeImage(file)).size, 75000);
    else {
      const reason = { decode: "image_decode_failed", dimensions: "image_dimensions_invalid", encoding: "image_processing_failed", oversized: "image_compressed_too_large" }[scenario];
      await assert.rejects(context.compressJudgeImage(file), error => error.reason === reason);
    }
  }
});

test("mining value UI: concurrent duplicate cannot dispatch and late completion retains captured image", async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const { context, seen } = miningValueRuntime({ fetch: () => pending });
  const first = context.analyzeMiningImage();
  for (let i = 0; i < 8; i++) await Promise.resolve();
  assert.equal(seen.requests, 1);
  await context.analyzeMiningImage();
  assert.equal(seen.requests, 1);
  context.currentJudgeImageDataUrl = "data:image/jpeg;base64,INPUT_B";
  context.judgeOperationVersion++;
  release({ ok: true, status: 200, clone: () => ({ json: async () => ({ success: true, result: miningQualityCases[2].text, quota: {} }) }) });
  await first;
  assert.equal(seen.saved.length, 1);
  assert.equal(seen.saved[0].image, "data:image/jpeg;base64,INPUT_A");
  assert.equal(seen.renderOrder.length, 0, "late result cannot overwrite the newer view");
  assert.equal(seen.quota, 1);
});

test("mining value UI: ambiguous dispatched failure blocks another submission without retry", async () => {
  for (const mode of ["network", "settlement", "parse"]) {
    const { context, seen } = miningValueRuntime({ fetch: async () => {
      if (mode === "network") throw new Error("offline network simulation");
      return { ok: mode === "parse", status: mode === "parse" ? 200 : 502, clone: () => ({ json: async () => {
        if (mode === "parse") throw new Error("parse");
        return { success: false, reason: "MINING_SETTLEMENT_UNKNOWN" };
      } }) };
    } });
    await context.analyzeMiningImage();
    await context.analyzeMiningImage();
    assert.equal(seen.requests, 1);
    assert.equal(context.judgeSubmissionBlocked, true);
    assert.equal(seen.saved.length, 0);
    assert.ok(seen.messages.some(item => /请勿重复上传/.test(item.text)));
  }
});

test("mining value UI: newest local selection wins and clears old text before file reading", async () => {
  const reads = [];
  const dispatches = [];
  let cancelled = 0;
  const context = vm.createContext({
    console: { log() {}, warn() {} }, selectedJudgeImageFiles: [], selectedJudgeImageUrls: [],
    currentJudgeImageDataUrl: "OLD", judgeOperationVersion: 0, judgeRequestInFlight: false, judgeSubmissionBlocked: false,
    judgeImageInput: { files: [], value: "" }, judgeImagePreview: { style: {}, innerHTML: "" },
    judgeResult: { value: "OLD RESULT" }, cancelJudgeDetailAnimation() { cancelled++; },
    clearJudgeDecisionCard() {}, resetJudgeFeedback() {}, hideJudgeProgressVisual() {}, closeImageModal() {},
    updateJudgeUploadControls() {}, syncJudgeImageInputFiles() {}, showJudgeMessage() {},
    getJudgeRecentItems: () => [], isImageUpload: () => true, isUnsupportedJudgeImage: () => false,
    URL: { createObjectURL: file => "blob:" + file.name, revokeObjectURL() {} },
    fileToDataUrl: file => new Promise(resolve => reads.push({ name: file.name, resolve })),
    analyzeMiningImage: async operation => dispatches.push({ operation, image: context.currentJudgeImageDataUrl })
  });
  vm.runInContext(recoverySection(recoveryHtml, "    async function showJudgeSelectedImage(", "    function openJudgeSelectedImage("), context);
  context.judgeImageInput.files = [{ name: "A.jpg" }];
  const a = context.handleJudgeImageChange();
  assert.equal(context.judgeResult.value, "");
  context.judgeImageInput.files = [{ name: "B.jpg" }];
  const b = context.handleJudgeImageChange();
  reads[1].resolve("IMAGE_B"); await b;
  reads[0].resolve("IMAGE_A"); await a;
  assert.equal(context.currentJudgeImageDataUrl, "IMAGE_B");
  assert.equal(dispatches.length, 1);
  assert.equal(dispatches[0].image, "IMAGE_B");
  assert.equal(context.judgeImagePreview.innerHTML, "");
  assert.equal(context.judgeImagePreview.style.display, "none");
  assert.ok(cancelled >= 2);
});

test("mining value UI: historical records keep provenance and four layers without inventing score or interpretation", () => {
  const { context, backend, effects } = miningQualityContext();
  for (const fixture of miningQualityCases) {
    const normalized = backend.normalizeJudgeOutput(fixture.text);
    assert.equal(backend.normalizeJudgeOutput(normalized), normalized);
    assert.equal(context.formatMiningEvidenceView(context.buildMiningEvidenceView(fixture.text)), normalized);
    context.judgeResult.value = normalized;
    context.renderJudgeDecisionCard(normalized, { image: "data:image/jpeg;base64,EXISTING" });
    context.renderJudgeDetail(normalized);
    const card = context.judgeDecisionCard.innerHTML;
    assert.match(card, /评分 \/ 等级：未评估/);
    assert.match(card, /结论可信度：未校准/);
    assert.match(card, /历史描述（未核）/);
    assert.match(card, /历史描述和动作尚未逐项核对/);
    assert.match(card, /下一步/);
    assert.match(card, /更换图片/);
    assert.equal((card.match(/<img /g) || []).length, 1);
    assert.doesNotMatch(card, qualityForbidden);
    assert.match(normalized, /本记录未单独提供证据支持的专业判断/);
    assert.match(normalized, /模型原文未取得/);
    for (const section of ["可见事实", "专业判断", "证据限制", "补证动作"]) assert.ok(context.judgeResult.innerHTML.includes(section));
    assert.equal(context.buildJudgeCopyText(), normalized);
  }
  assert.deepEqual(effects, { network: 0, historyWrites: 0 });
});

test("mining value UI: riverbank manual material is not a model pass and conditional interpretation survives both renderers", () => {
  assert.equal(riverbankMaterial.evidenceClass, "MANUAL_PHOTO_NOTES_NOT_MODEL_OUTPUT");
  const { context, backend, effects } = miningQualityContext();
  const view = backend.buildMiningEvidenceView(riverbankMaterial.text);
  assert.equal(view.objectType, "地表环境");
  assert.equal(view.sceneType, "人工采坑");
  assert.equal(view.conflict, false);
  assert.equal(JSON.stringify(context.buildMiningEvidenceView(riverbankMaterial.text)), JSON.stringify(view));
  context.renderJudgeDecisionCard(riverbankMaterial.text);
  context.renderJudgeDetail(riverbankMaterial.text);
  assert.match(context.judgeDecisionCard.innerHTML, /不能由此认定含矿层/);
  assert.match(context.judgeResult.innerHTML, /不进入未确认安全的坑内/);
  assert.match(context.judgeResult.innerHTML, /是否采矿未确定/);
  assert.doesNotMatch(context.judgeDecisionCard.innerHTML, /grade-[ABCD]|\d+分|可信度：高/);
  assert.deepEqual(effects, { network: 0, historyWrites: 0 });
});

test("mining value UI: restored result keeps first-upload entry hidden with no duplicate upload control", () => {
  const context = vm.createContext({
    selectedJudgeImageFiles: [], judgeAddImageButton: { disabled: false },
    judgeImageCount: {}, judgeUploadCard: { hidden: false }, judgeQuickUpload: { hidden: false },
    judgeImageInput: { disabled: false }, judgeDecisionCard: { hidden: false },
    judgeRequestInFlight: false, judgeSubmissionBlocked: false
  });
  vm.runInContext(recoverySection(recoveryHtml, "    function updateJudgeUploadControls(", "    function chooseJudgeReplacement("), context);
  context.updateJudgeUploadControls();
  assert.equal(context.judgeUploadCard.hidden, true);
  assert.equal(context.judgeQuickUpload.hidden, true);
  context.judgeDecisionCard.hidden = true;
  context.updateJudgeUploadControls();
  assert.equal(context.judgeUploadCard.hidden, false);
  context.selectedJudgeImageFiles = [{}];
  context.updateJudgeUploadControls();
  assert.equal(context.judgeQuickUpload.hidden, false);
  context.judgeRequestInFlight = true;
  context.updateJudgeUploadControls();
  assert.equal(context.judgeImageInput.disabled, true);
});

test("mining value UI: paragraph animation is short cancellable and respects reduced motion", () => {
  const { context } = miningQualityContext();
  const animations = [];
  let reduced = false;
  const paragraphs = Array.from({ length: 9 }, () => ({
    animate(frames, options) { const item = { frames, options, cancelled: false, cancel() { this.cancelled = true; } }; animations.push(item); return item; }
  }));
  const listeners = [];
  context.window = { matchMedia: () => ({ matches: reduced }) };
  context.judgeResult.querySelectorAll = () => paragraphs;
  context.judgeResult.addEventListener = (event, fn) => listeners.push({ event, fn });
  context.revealJudgeDetail();
  assert.equal(animations.length, 9);
  assert.ok(animations.every(a => a.options.duration + a.options.delay <= 370));
  listeners.find(item => item.event === "pointerdown").fn();
  assert.ok(animations.every(a => a.cancelled));
  reduced = true;
  context.revealJudgeDetail();
  assert.equal(animations.length, 9);
  context.renderJudgeDetail("");
  assert.match(context.judgeResult.innerHTML, /judge-detail-placeholder/);
});

test("mining value UI: scoped layout and scripts remain parseable without execution", () => {
  const css = recoverySection(recoveryHtml, "    /* MINING_CONFIRMED_LAYOUT_START", "    /* MINING_CONFIRMED_LAYOUT_END");
  for (const [, selectors] of css.replace(/\/\*[\s\S]*?\*\//g, "").matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    for (const selector of selectors.split(/,(?![^()]*\))/)) assert.ok(selector.trim().startsWith("#judgePage"));
  }
  assert.match(css, /#judgePage \.page-nav \{ position: static;/);
  assert.match(css, /max-height: none; overflow: visible/);
  assert.match(css, /\.message.error \{ color: #785b24; background: #fffbeb;/);
  assert.match(css, /\.judge-change-image \{[^}]*min-height: 44px/);
  const preview = recoverySection(recoveryHtml, "    async function showJudgeSelectedImage(", "    async function handleJudgeImageChange(");
  assert.doesNotMatch(preview, /<img|preview-button|image-preview-row/);
  for (const [, attrs, source] of recoveryHtml.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)) {
    if (/\bsrc=|application\/ld\+json|\btype=["']module/.test(attrs) || !source.trim()) continue;
    new vm.Script(source);
  }
});

test("mining value UI: shared parser parity prompt boundaries and frozen provider settings", () => {
  const marker = "// Mining evidence presentation only:";
  assert.equal(recoverySection(recoveryServer, marker, "function normalizeJudgeOutput(").trim(),
    recoverySection(recoveryHtml, marker, "function cleanupJudgeHistoryStorage(").trim());
  const prompt = recoverySection(recoveryServer, "    const prompt = `你是图片外观初筛助手", "    const aliyunEndedAt");
  for (const phrase of ["可见事实", "专业判断", "证据限制", "补证动作", "未见金点不等于无金", "普通地表不强行矿化解读", "不得输出等级"]) assert.ok(prompt.includes(phrase));
  assert.match(prompt, /maxTokens: 360/);
  assert.match(prompt, /temperature: 0\.3/);
  assert.match(prompt, /modelName: aliyunMiningModel/);
  assert.match(prompt, /timeoutMs: 35000/);
});
