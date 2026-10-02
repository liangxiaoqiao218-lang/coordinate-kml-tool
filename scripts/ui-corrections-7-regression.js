import fs from "node:fs";
import path from "node:path";

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
check("map point count falls back to actual geometry positions", source.includes("getSpatialPositions(preview?.geometry).length"));
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

console.log(JSON.stringify({ status: "PASS", passed: checks.length, failed: 0, checks }, null, 2));
