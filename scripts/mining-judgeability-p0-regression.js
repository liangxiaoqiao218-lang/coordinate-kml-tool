import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
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
