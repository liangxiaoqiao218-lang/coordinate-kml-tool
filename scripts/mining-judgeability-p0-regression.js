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
const recoveryHtml = fs.readFileSync(path.join(root, "index.html"), "utf8");
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
    recoverySection(recoveryServer, "function normalizeJudgeOutput(", 'app.get("/api/config"'),
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
  assert.match(result.body.result, /仅作外观初筛/);
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
