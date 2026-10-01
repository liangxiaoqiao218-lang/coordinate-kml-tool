import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = await readFile(path.join(repoRoot, "index.html"), "utf8");

function section(id, nextId) {
  const start = html.indexOf(`<section id="${id}"`);
  const end = html.indexOf(`<section id="${nextId}"`, start);
  assert.notEqual(start, -1, `${id} must exist`);
  assert.notEqual(end, -1, `${nextId} must follow ${id}`);
  return html.slice(start, end);
}

const coordinate = section("coordinatePage", "spatialResultPage");
const judge = section("judgePage", "goldPage");
const goldStart = html.indexOf('<section id="goldPage"');
const goldEnd = html.indexOf("</main>", goldStart);
const gold = html.slice(goldStart, goldEnd);

assert.equal((coordinate.match(/<h1>/g) || []).length, 1, "coordinate page has one primary title");
assert.ok(coordinate.indexOf("coordinateUploadPanel") < coordinate.indexOf("coordinate-paste-panel"), "image upload is the first coordinate input");
assert.match(coordinate, /<details class="coordinate-paste-panel">[\s\S]*<summary>粘贴坐标文本<\/summary>/u);
assert.match(coordinate, /<details class="coordinate-example"><summary>查看输入示例<\/summary>/u);
assert.match(coordinate, /id="coordinateSecondaryActions"[^>]*hidden/u);
assert.match(html, /\[hidden\]\s*\{\s*display:\s*none\s*!important;/u);
assert.match(html, /#coordinateInput\s*\{[\s\S]*?min-height:\s*116px;[\s\S]*?field-sizing:\s*content;/u);
assert.match(html, /\.page-nav\s*\{[\s\S]*?grid-template-columns:\s*repeat\(4,\s*minmax\(0,\s*1fr\)\)/u);
assert.match(html, /@media \(max-width:\s*350px\)/u);
assert.doesNotMatch(html, /<section class="product-faq"/u, "large per-page FAQ cards are removed");
assert.ok(judge.indexOf("judgeUploadCard") < judge.indexOf("judge-result-wrap"), "judge upload precedes results");
assert.match(html, /\.judge-panel \.judge-result-wrap:has\(\.judge-detail-placeholder\)\s*\{\s*display:\s*none;/u);
assert.match(gold, /id="goldResultCard"[^>]*hidden/u);
assert.match(gold, /id="goldActions"[^>]*hidden/u);
assert.match(html, /resultCard\.hidden = false;[\s\S]*actionsEl\.hidden = false;/u);
assert.match(html, /<details class="footer-details">[\s\S]*<summary>帮助<\/summary>/u);
assert.match(html, /<details class="footer-details">[\s\S]*<summary>关于与联系<\/summary>/u);
assert.match(html, /粤ICP备2026099318号-1/u);
assert.match(html, /粤公网安备44030002015944号/u);
assert.match(html, /id="mapPreviewAction"[\s\S]*id="coordinateKmlAction"[\s\S]*id="coordinateCopyAction"/u, "map, KML and copy actions remain available");
assert.match(html, /openManualSupport\(\)|toggleManualSupport\(\)/u, "manual correction remains reachable");

console.log("Mobile UI simplification regression: 21/21 PASS");
console.log("PROVIDER_CALLS=0");
