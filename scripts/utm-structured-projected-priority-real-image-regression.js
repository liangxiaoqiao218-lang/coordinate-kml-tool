import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";

import { buildProjectedTableVisionTiles } from "../server/utm-intent/projected-table-image-tiles.js";
import { runCrsVisionPass } from "../server/crs-evidence/crs-vision-pass.js";
import { buildShadowIntentFromCrsVision } from "../server/crs-evidence/shadow-pipeline.js";
import {
  buildStructuredUtmTableRetryPrompt,
  buildProjectedXyRetryPrompt,
  buildStructuredUtmPriority,
  mergeProjectedXyRows,
  mergeStructuredUtmTableRows,
  parseStructuredUtmTableModelText,
  runProjectedXyOnlyPass,
  runStructuredUtmTablePass
} from "../server/utm-intent/structured-projected-priority.js";

const fixtureRoot = process.env.UTM_REAL_IMAGE_ROOT
  || "D:\\萨赫勒数字科技有限公司\\关于西非的业务\\测试素材";
const allCases = [
  {
    name: "Indonesia UTM50S 01",
    file: "印尼矿地01.jpg",
    expected: [[779271.176, 9720912.526], [779554.165, 9720912.526], [779554.165, 9720734.464], [779271.176, 9720734.464]]
  },
  {
    name: "Indonesia UTM50S 02",
    file: "印尼矿地02.jpg",
    expected: [[778984.492, 9721476.737], [779099.680, 9721476.848], [779099.680, 9721110.798], [778875.519, 9721110.798], [778875.519, 9721180.576], [778984.492, 9721180.576]]
  },
  {
    name: "Indonesia UTM50S 03",
    file: "印尼矿地03.jpg",
    expected: [[778807.293, 9721476.737], [778981.768, 9721477.288], [778982.700, 9721182.351], [778855.308, 9721181.948], [778855.543, 9721107.284], [778980.724, 9721107.010], [778980.920, 9720910.990], [779100.477, 9720911.109], [779100.599, 9720788.271], [778950.926, 9720787.948], [778950.926, 9720833.787], [778927.907, 9720833.787], [778927.907, 9720922.219], [778906.895, 9720922.219], [778906.895, 9721078.633], [778807.082, 9721078.633]]
  }
];
const cases = process.env.UTM_REAL_IMAGE_CASE
  ? allCases.filter(testCase => testCase.file.includes(process.env.UTM_REAL_IMAGE_CASE))
  : allCases;

function parseEnvText(text) {
  const values = {};
  for (const sourceLine of String(text || "").split(/\r?\n/)) {
    const line = sourceLine.trim();
    if (!line || line.startsWith("#")) continue;
    const separator = line.indexOf("=");
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    values[key] = value;
  }
  return values;
}

async function loadProviderConfig() {
  const envFile = process.env.CRS_EVIDENCE_ENV_FILE;
  const fileValues = envFile ? parseEnvText(await readFile(envFile, "utf8")) : {};
  const get = key => process.env[key] || fileValues[key] || "";
  const apiKey = get("ALIYUN_API_KEY") || get("DASHSCOPE_API_KEY");
  const baseUrl = get("ALIYUN_BASE_URL") || get("DASHSCOPE_BASE_URL") || "https://dashscope.aliyuncs.com/compatible-mode/v1";
  const model = get("ALIYUN_VISION_MODEL") || get("DASHSCOPE_VISION_MODEL") || "qwen-vl-plus";
  const ocrModel = get("ALIYUN_OCR_MODEL") || get("DASHSCOPE_OCR_MODEL") || "qwen-vl-ocr-latest";
  if (!apiKey) throw new Error("Missing ALIYUN_API_KEY or DASHSCOPE_API_KEY");
  return { apiKey, baseUrl: baseUrl.replace(/\/+$/, ""), model, ocrModel };
}

async function invokeAliyunVision(config, { prompt, imageItems, maxTokens = 3200, model = config.model }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 90000);
  try {
    const endpoint = config.baseUrl.endsWith("/chat/completions") ? config.baseUrl : `${config.baseUrl}/chat/completions`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { Authorization: `Bearer ${config.apiKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: [{ type: "text", text: prompt }, ...imageItems] }],
        temperature: 0,
        max_tokens: maxTokens
      }),
      signal: controller.signal
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body?.error?.message || body?.message || `HTTP ${response.status}`);
    return body?.choices?.[0]?.message?.content || "";
  } finally {
    clearTimeout(timer);
  }
}

const config = await loadProviderConfig();
let passed = 0;
for (const testCase of cases) {
  const filePath = path.join(fixtureRoot, testCase.file);
  const buffer = await readFile(filePath);
  const imageItems = [{
    type: "image_url",
    image_url: { url: `data:image/jpeg;base64,${buffer.toString("base64")}`, detail: "high" }
  }];
  let summary;
  let crs = null;
  let table = null;
  let xyOnlyTable = null;
  let priority = null;
  try {
    const crsVision = await runCrsVisionPass({
      imageItems,
      invokeVision: args => invokeAliyunVision(config, { ...args, maxTokens: 1200 })
    });
    crs = buildShadowIntentFromCrsVision(crsVision);
    table = await runStructuredUtmTablePass({
      imageItems,
      invokeVision: args => invokeAliyunVision(config, args)
    });
    xyOnlyTable = await runProjectedXyOnlyPass({
      imageItems,
      invokeVision: args => invokeAliyunVision(config, { ...args, model: config.model, maxTokens: 2200 })
    });
    table = mergeProjectedXyRows(table, xyOnlyTable, { shadowIntent: crs.shadowIntent });
    priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
    let tableTiles = [];
    if (priority?.reason === "transformation_verification_failed") {
      tableTiles = await buildProjectedTableVisionTiles(buffer);
      const xyRetryText = await invokeAliyunVision(config, {
        prompt: buildProjectedXyRetryPrompt(priority),
        imageItems: tableTiles.length > 0 ? tableTiles : imageItems,
        model: config.model,
        maxTokens: 1200
      });
      const xyRetryRows = parseStructuredUtmTableModelText(xyRetryText);
      table = mergeProjectedXyRows(table, xyRetryRows, { shadowIntent: crs.shadowIntent });
      priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
    }
    for (const retryModel of [config.ocrModel, config.model]) {
      if (priority?.reason !== "transformation_verification_failed") break;
      const retryText = await invokeAliyunVision(config, {
        prompt: buildStructuredUtmTableRetryPrompt(priority),
        imageItems: tableTiles.length > 0 ? tableTiles : imageItems,
        model: retryModel
      });
      const retryRows = parseStructuredUtmTableModelText(retryText);
      table = mergeStructuredUtmTableRows(table, retryRows, { shadowIntent: crs.shadowIntent });
      priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
    }

    assert.equal(crs.shadowIntent.confidence, "confirmed");
    assert.equal(crs.shadowIntent.epsg, "EPSG:32750");
    assert.equal(priority?.accepted, true);
    assert.equal(priority?.typedUtmIntent?.epsg, "EPSG:32750");
    assert.equal(priority?.table?.rows?.length, testCase.expected.length);
    priority.table.rows.forEach((row, index) => {
      assert.equal(row.easting, testCase.expected[index][0], `point ${index + 1} X mismatch`);
      assert.equal(row.northing, testCase.expected[index][1], `point ${index + 1} Y mismatch`);
    });
    assert.equal(priority?.transformationVerification?.status, "match");
    assert.ok(priority.transformationVerification.comparedRows >= 3);

    passed += 1;
    summary = {
      sample: testCase.name,
      status: "PASS",
      pointCount: priority.table.rows.length,
      typedCrs: priority.typedUtmIntent,
      verification: priority.transformationVerification
    };
  } catch (error) {
    summary = {
      sample: testCase.name,
      status: "FAIL",
      error: error.message,
      shadowIntent: crs?.shadowIntent || null,
      tableStatus: table?.status || null,
      tableRows: table?.rows || [],
      xyOnlyRows: xyOnlyTable?.rows || [],
      xyOnlyRawText: xyOnlyTable?.rawModelText || "",
      priority
    };
  }
  console.log(JSON.stringify(summary));
}

console.log(`Structured Projected Priority Real Image Regression: ${passed}/${cases.length} PASS`);
if (passed !== cases.length) process.exitCode = 1;
