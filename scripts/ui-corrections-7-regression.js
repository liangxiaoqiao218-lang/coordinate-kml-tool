import fs from "node:fs";
import path from "node:path";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?:[A-Za-z]:)/, match => match.slice(1))), "..");
const source = fs.readFileSync(path.join(root, "index.html"), "utf8");
const checks = [];

function check(name, predicate) {
  if (!predicate) throw new Error(`FAIL ${name}`);
  checks.push(name);
}

check("result overview has independent status, point, area and region cells",
  ["状态", "点数", "面积", "国家／地区"].every(label => source.includes(`key: "${label}"`)));
check("result overview has valid warning and neutral tones",
  ["is-valid", "is-warning", "is-neutral"].every(name => source.includes(name)));
check("coordinate quota remains in workspace title row",
  /class="workspace-head"[\s\S]*?id="convertQuotaStatus"/.test(source));
check("judge quota is inside judge title row",
  /class="page-title-row judge-title-row"[\s\S]*?id="judgeQuotaStatus"[\s\S]*?<\/div>\s*<p class="intro">/.test(source));
check("ordinary review acknowledgement is not rendered as an action",
  source.includes("if (!isConfirmed && !ordinaryReviewOnly)")
  && !source.includes('confirmButton.addEventListener("click", ordinaryReviewOnly'));
check("real point and server confirmation actions remain",
  source.includes("confirmPointGeometryIntentReview") && source.includes("confirmHandwrittenDmsReview"));
check("map share uses a line icon", /id="spatialShareCardAction"[\s\S]*?<svg[\s\S]*?<circle/.test(source));
check("map sheet toggle uses a line icon", /class="spatial-sheet-chevron"[\s\S]*?<svg[\s\S]*?<path/.test(source));
check("map sheet uses emerald blue treatment", source.includes("linear-gradient(135deg, rgba(236, 253, 245"));
check("map point count falls back to actual geometry positions", source.includes("getSpatialPositions(preview?.geometry).length"));
check("empty center fact is hidden", source.includes('spatialCentroidFact.hidden = !centroid'));
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

console.log(JSON.stringify({ status: "PASS", passed: checks.length, failed: 0, checks }, null, 2));
