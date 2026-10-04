import fs from "node:fs";
import path from "node:path";
import assert from "node:assert/strict";
import vm from "node:vm";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?:[A-Za-z]:)/, match => match.slice(1))), "..");
const source = fs.readFileSync(path.join(root, "index.html"), "utf8");
const checks = [];

function check(name, predicate) {
  if (!predicate) throw new Error(`FAIL ${name}`);
  checks.push(name);
}

check("result overview restores legacy area type region count and coordinate type fields",
  ["区域类型", "所属国家/地区", "坐标数量", "坐标类型"].every(label => source.includes(`key: "${label}"`)));
check("result overview only appends a legal valid area",
  /const area = getRecognitionSummaryArea\(meta\);[\s\S]*?if \(area\) \{[\s\S]*?chips\.push\(\{ key: "面积", value: area, tone: "valid" \}\);/.test(source));
check("result overview has valid warning and neutral tones",
  ["is-valid", "is-warning", "is-neutral"].every(name => source.includes(name)));
check("area overview restores title dot framed panel and wrapping pills",
  source.includes('label.textContent = "区域概览"')
  && /\.recognition-status\.summary\s*\{[\s\S]*?border:\s*1px solid #bbf7d0;/.test(source)
  && /\.recognition-summary-label::before\s*\{[\s\S]*?background:\s*#16a34a;/.test(source)
  && /\.recognition-summary-meta\s*\{[\s\S]*?display:\s*flex;[\s\S]*?flex-wrap:\s*wrap;/.test(source)
  && /\.recognition-summary-pill\s*\{[\s\S]*?border-radius:\s*999px;[\s\S]*?background:\s*#dcfce7;/.test(source));
check("area overview preserves warning and unknown semantics",
  /\.recognition-summary-pill\.is-warning\s*\{[\s\S]*?background:\s*#fff7ed;/.test(source)
  && /\.recognition-summary-pill\.is-neutral\s*\{[\s\S]*?background:\s*#f8fafc;/.test(source)
  && source.includes('review.requiresReview ? "warning" : "valid"')
  && source.includes('value: coordinateType || "未知"'));
check("coordinate quota remains in workspace title row",
  /class="workspace-title-row"[\s\S]*?<h1>坐标与 KML<\/h1>[\s\S]*?id="convertQuotaStatus"/.test(source));
check("judge quota is inside judge title row",
  /class="page-title-row judge-title-row"[\s\S]*?id="judgeQuotaStatus"[\s\S]*?<\/div>\s*<p class="intro">/.test(source));
check("coordinate and judge usage share the grey-blue treatment",
  /\.workspace-title-row \.quota-status[\s\S]*?background:\s*#f1f5f9;[\s\S]*?color:\s*#64748b;/.test(source)
  && /\.page-title-row\.judge-title-row \.quota-status[\s\S]*?background:\s*#f1f5f9;[\s\S]*?color:\s*#64748b;/.test(source));
check("judge upload removes the redundant layer label and outer frame",
  !source.includes('id="judgeUploadLayerTitle"')
  && /\.judge-panel \.judge-upload-layer\s*\{[\s\S]*?padding:\s*0;[\s\S]*?border:\s*0;[\s\S]*?background:\s*transparent;/.test(source));
check("ordinary review acknowledgement is not rendered as an action",
  source.includes("if (!isConfirmed && !ordinaryReviewOnly)")
  && !source.includes('confirmButton.addEventListener("click", ordinaryReviewOnly'));
check("real point and server confirmation actions remain",
  source.includes("confirmPointGeometryIntentReview") && source.includes("confirmHandwrittenDmsReview"));
check("map share uses a line icon", /id="spatialShareCardAction"[\s\S]*?<svg[\s\S]*?<circle/.test(source));
check("map sheet toggle uses a line icon", /class="spatial-sheet-chevron"[\s\S]*?<svg[\s\S]*?<path/.test(source));
check("map sheet uses emerald blue treatment", source.includes("linear-gradient(135deg, rgba(236, 253, 245"));
check("map sheet toggle controls the actual detail visibility", source.includes("spatialResultDetails.hidden = !next"));
check("map summary and detail share display-only boundary point counting",
  source.includes("getSpatialPositions(preview?.geometry).length")
  && (source.match(/const pointCount = getSpatialDisplayPointCount\(preview, facts\);/g) || []).length === 2);
check("empty center fact is hidden", source.includes('spatialCentroidFact.hidden = !centroid'));
check("recognition detail starts tall and scrolls internally",
  /\.debug-panel textarea\s*\{[\s\S]*?min-height:\s*220px;[\s\S]*?max-height:\s*240px;[\s\S]*?resize:\s*none;/.test(source)
  && source.includes("Math.max(220, Number(debugText.scrollHeight) || 220)"));
check("judge has upload summary and detail layers",
  ["judge-upload-layer", "judge-summary-layer", "judge-detail-layer"].every(name => source.includes(name)));
check("judge metrics exclude recommendation", /judge-official-main[\s\S]*?可信度[\s\S]*?<\/div>\s*<div class="judge-decision-recommendation"/.test(source));
check("judge suggestion strips replacement-square glyphs", source.includes('.replace(/[□�\\uFFFD]/g, "")'));
check("judge clear feedback is transient", source.includes("}, 1600);"));
check("manual gold quote panel is visible", source.includes('<div class="quote-placeholder">'));
check("realtime gold price line is hidden", source.includes('<p id="goldPriceInfo" hidden></p>'));
check("gold price endpoint is not called on page load", !/renderJudgeRecent\(\);\s*if \(!document\.querySelector\("\.quote-placeholder"\)/.test(source));
check("gold settlement keeps original formula", source.includes("goldWeight * shopQuote * (purity / 100)"));
check("gold copy preserves manual quote and estimated settlement", source.includes("金店报价币种") && source.includes("预计可卖金额"));
check("help is page-specific", ["page-help-home", "page-help-coordinate", "page-help-judge", "page-help-gold"].every(name => source.includes(name)));
check("footer separates about contact and manual recognition assistance",
  source.includes("<summary>关于我们</summary>")
  && source.includes("<summary>联系我们</summary>")
  && source.includes('class="footer-text-link"')
  && !source.includes("<summary>关于与联系</summary>"));

// Execute only the real display/copy functions from this candidate. No page
// bootstrap, business service, credential, real clipboard or network is loaded.
function productFunction(name) {
  const pattern = new RegExp(`^    (?:async )?function ${name}\\(`, "m");
  const start = source.search(pattern);
  assert.notEqual(start, -1, `missing product function ${name}`);
  const end = source.indexOf("\n    }", start);
  assert.notEqual(end, -1, `missing function boundary ${name}`);
  return source.slice(start, end + "\n    }".length);
}

const nodes = Object.fromEntries([
  "spatialGeometryType", "spatialResultWarning", "spatialReviewCompact", "spatialMapFailure",
  "spatialAreaFact", "spatialAreaValue", "spatialLengthFact", "spatialLengthLabel", "spatialLengthValue",
  "spatialPointCount", "spatialCentroidFact", "spatialCentroid", "spatialRegionalView",
  "spatialKmlAction", "spatialCollapsedSummary", "spatialResultDiagnostic"
].map(name => [name, { textContent: "", hidden: false, dataset: {} }]));
const isolation = { networkAttempts: 0, clipboardWrites: 0, businessWrites: 0 };
const messages = [];
const copiedTexts = [];
const blocked = () => { isolation.networkAttempts++; throw new Error("UI_REGRESSION_NETWORK_DENIED"); };
const context = vm.createContext({
  ...nodes,
  spatialRegressionTestMode: false,
  syncKmlActionVisualState() {},
  judgeResult: { value: "" },
  navigator: { clipboard: { writeText: async text => {
    isolation.clipboardWrites++;
    copiedTexts.push(String(text));
  } } },
  showJudgeMessage: (...args) => messages.push(args),
  fetch: blocked,
  XMLHttpRequest: blocked,
  WebSocket: blocked,
  EventSource: blocked
}, { codeGeneration: { strings: false, wasm: false } });
const functions = [
  "getSpatialPositions", "getSpatialDisplayPointCount", "formatSpatialMeters", "formatSpatialArea",
  "formatSpatialCollapsedArea", "spatialHasBoundaryReviewWarning", "spatialReviewRequired",
  "spatialWarningText", "spatialCollapsedSummaryText", "renderSpatialResult",
  "getJudgeSection", "getJudgeGrade", "clampNumber", "extractJudgeScore", "extractJudgeConfidence",
  "getJudgeDecisionMeta", "buildJudgeCopyText", "copyJudgeResult"
];
vm.runInContext(functions.map(productFunction).join("\n"), context, { timeout: 1000 });

const triangle = [[10, 10], [11, 10], [10, 11], [10, 10]];
const square = [[12, 12], [14, 12], [14, 14], [12, 14], [12, 12]];
const hole = [[12.5, 12.5], [13, 12.5], [12.5, 13], [12.5, 12.5]];
const boundaries = [
  { name: "closed triangle", type: "Polygon", coordinates: [triangle], expected: 3 },
  { name: "closed square", type: "Polygon", coordinates: [square], expected: 4 },
  { name: "multipolygon", type: "MultiPolygon", coordinates: [[triangle], [square]], expected: 7 },
  { name: "polygon hole retains outer-boundary display convention", type: "Polygon", coordinates: [square, hole], expected: 4 },
  { name: "open triangle display does not close or mutate geometry", type: "Polygon", coordinates: [triangle.slice(0, -1)], expected: 3 },
  { name: "repeated vertex retains existing summary display convention", type: "Polygon", coordinates: [[triangle[0], triangle[1], triangle[1], triangle[2], triangle[0]]], expected: 3 },
  { name: "point", type: "Point", coordinates: [10, 10], expected: 1 },
  { name: "multipoint", type: "MultiPoint", coordinates: [[10, 10], [11, 10]], expected: 2 },
  { name: "line", type: "LineString", coordinates: [[10, 10], [11, 10]], expected: 2 }
];
const displayCases = [];
for (const boundary of boundaries) {
  for (const available of [false, true]) {
    for (const needsReview of [false, true]) {
      const payload = {
        mapPreviewObject: {
          geometryType: boundary.type,
          geometry: { type: boundary.type, coordinates: structuredClone(boundary.coordinates) },
          previewWarnings: needsReview ? ["矿区轮廓待核对"] : []
        },
        spatialFactsStatus: available ? "available" : "unavailable",
        spatialFacts: available ? { pointCount: boundary.expected, areaMeters2: 10000, perimeterMeters: 400, lengthMeters: 100, centroid: [10, 10] } : null,
        regionalViewCount: 0,
        kmlEligibility: { allowed: true }
      };
      // Include the exact defect: even if a supplied statistic counts closure,
      // Polygon display still follows the existing summary's boundary convention.
      if (available && boundary.name === "closed triangle") payload.spatialFacts.pointCount = 4;
      const before = JSON.stringify(payload);
      context.payload = payload;
      vm.runInContext("renderSpatialResult(payload)", context, { timeout: 1000 });
      assert.equal(nodes.spatialPointCount.textContent, `${boundary.expected} 个`);
      assert.match(nodes.spatialCollapsedSummary.textContent, new RegExp(`(?:^|[ ·])${boundary.expected}点(?:$|（)`));
      assert.equal(JSON.stringify(payload), before, "display must not mutate coordinates, facts, eligibility or warnings");
      const record = { name: boundary.name, facts: available ? "available" : "unavailable", needsReview,
        detail: nodes.spatialPointCount.textContent, summary: nodes.spatialCollapsedSummary.textContent, inputUnchanged: true };
      displayCases.push(record);
      checks.push(`display ${record.name} / ${record.facts} / review=${needsReview}`);
    }
  }
}

const syntheticJudge = [
  ["结论", "合成矿地含可复核线索，尚不能确认矿化。"],
  ["等级", "B"], ["判读可信度", "中"], ["潜力评分", "68分"],
  ["关键依据", "合成可见石英脉与氧化铁线索。"],
  ["主要风险", "缺少现场尺度与独立检测。"],
  ["下一步", "补充比例尺照片并进行独立检测。"],
  ["一句话总结", "仅作合成 UI 复制验收，不是模型结论。"]
];
const judgeInput = syntheticJudge.map(([name, value]) => `【${name}】\n${value}`).join("\n\n");
const expectedJudgeCopy = [
  "【核心判断】", "是否值得继续：👍 有点意思（可以再看）", "判断等级：B", "判读可信度：中", "本次评分：68"
].join("\n") + "\n\n" + judgeInput;
context.judgeResult.value = judgeInput;
assert.equal(vm.runInContext("buildJudgeCopyText()", context, { timeout: 1000 }), expectedJudgeCopy);
vm.runInContext("copyJudgeResult()", context, { timeout: 1000 });
await Promise.resolve();
assert.equal(copiedTexts.length, 1, "nonempty copy must really call the isolated clipboard sink");
assert.equal(copiedTexts[0], expectedJudgeCopy, "actual copied text must match independent expected fields");
assert.ok(copiedTexts[0].length > 0, "empty-string equality must never pass nonempty copy acceptance");
for (const [name, value] of syntheticJudge) assert.ok(copiedTexts[0].includes(`【${name}】\n${value}`));
assert.equal(context.judgeResult.value, judgeInput, "copy must not alter source result");
assert.equal(messages.at(-1)?.[0], "判读结果已复制。");
checks.push("nonempty synthetic judge result copies exact expected fields through actual product handler");
context.judgeResult.value = "";
vm.runInContext("copyJudgeResult()", context, { timeout: 1000 });
assert.equal(copiedTexts.length, 1, "empty result must not add a clipboard write");
assert.equal(messages.at(-1)?.[0], "暂无判读结果可复制。");
checks.push("empty judge result remains guarded and is not nonempty acceptance evidence");
assert.equal(isolation.networkAttempts, 0);
assert.equal(isolation.businessWrites, 0);

console.log(JSON.stringify({ status: "PASS", passed: checks.length, failed: 0, checks,
  displayCases, judgeCopy: { synthetic: true, clipboard: "MEMORY_SINK_NOT_OS_CLIPBOARD",
    expectedText: expectedJudgeCopy, actualCopiedText: copiedTexts[0], nonempty: true, exactMatch: true },
  isolation: { ...isolation, providerCalls: 0, mapServiceCalls: 0, databaseWrites: 0, userQuotaWrites: 0 },
  limitations: ["Function-level display/copy regression, not whole-page browser interaction or production deployment acceptance."]
}, null, 2));
