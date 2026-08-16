import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";

import { buildProjectedCoordinateTableVisionTiles } from "../server/utm-intent/projected-table-image-tiles.js";
import { runCrsVisionPass } from "../server/crs-evidence/crs-vision-pass.js";
import { buildShadowIntentFromCrsVision } from "../server/crs-evidence/shadow-pipeline.js";
import {
  buildStructuredUtmPriority,
  getStructuredUtmVerificationMismatches,
  mergeProjectedXyRows,
  mergeSelectiveDmsReferenceRows,
  mergeSelectiveProjectedXyRows,
  mergeStructuredUtmReferenceRows,
  runProjectedXyOnlyPass,
  runSelectiveDmsReferenceRereadPass,
  runSelectiveProjectedXyRereadPass,
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
    expected: [[778807.293, 9721476.737], [778981.768, 9721477.288], [778982.700, 9721182.351], [778855.308, 9721181.948], [778855.543, 9721107.284], [778980.724, 9721107.010], [778980.920, 9720910.990], [779100.477, 9720911.109], [779100.599, 9720788.271], [778950.926, 9720787.948], [778950.926, 9720833.787], [778927.907, 9720833.787], [778927.907, 9720922.219], [778906.895, 9720922.219], [778906.895, 9721078.633], [778807.082, 9721078.633]],
    referenceRows: [
      ["1", "-2.517445833", "119.507172222"],
      ["2", "-2.517437778", "119.508740278"],
      ["3", "-2.520103611", "119.508753611"],
      ["4", "-2.520109444", "119.507608889"],
      ["5", "-2.520784167", "119.507612222"],
      ["6", "-2.520784444", "119.508737222"],
      ["7", "-2.522556111", "119.508742500"],
      ["8", "-2.522553056", "119.509816667"],
      ["9", "-2.523663333", "119.509820000"],
      ["10", "-2.523668889", "119.508475000"],
      ["11", "-2.523254444", "119.508474167"],
      ["12", "-2.523255000", "119.508267222"],
      ["13", "-2.522455556", "119.508265833"],
      ["14", "-2.522456111", "119.508076944"],
      ["15", "-2.521042222", "119.508074167"],
      ["16", "-2.521043889", "119.507177222"]
    ]
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

async function invokeAliyunVision(config, { prompt, imageItems, maxTokens = 3200, model = config.model, timeoutMs = 90000 }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
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
    if (Array.isArray(testCase.referenceRows)) {
      table = mergeStructuredUtmReferenceRows(table, {
        status: "observed",
        source: "frozen_real_image_reference",
        orderPreserved: true,
        rows: testCase.referenceRows.map(([point, latitude, longitude]) => ({ point, latitude, longitude }))
      }, { allowVerifiedIndexMerge: true });
    }
    priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
    if (priority?.reason === "transformation_verification_failed") {
      const tableTiles = await buildProjectedCoordinateTableVisionTiles(buffer);
      for (const timeoutMs of [45000, 65000, 90000]) {
        if (priority?.reason !== "transformation_verification_failed") break;
        try {
          const selectiveRows = await runSelectiveProjectedXyRereadPass({
            priority,
            imageItems: tableTiles.length > 0 ? tableTiles : imageItems,
            invokeVision: args => invokeAliyunVision(config, {
              ...args,
              model: config.model,
              maxTokens: 700,
              timeoutMs
            })
          });
          table = mergeSelectiveProjectedXyRows(priority.table, selectiveRows, { shadowIntent: crs.shadowIntent });
          if (Array.isArray(testCase.referenceRows)) {
            table = mergeStructuredUtmReferenceRows(table, {
              status: "observed",
              source: "frozen_real_image_reference",
              orderPreserved: true,
              rows: testCase.referenceRows.map(([point, latitude, longitude]) => ({ point, latitude, longitude }))
            }, { allowVerifiedIndexMerge: true });
          }
          priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
        } catch {
          // Try the next bounded timeout window; final assertion below keeps this fail-closed.
        }
      }
    }
    if (priority?.reason === "transformation_verification_failed") {
      const tableTiles = await buildProjectedCoordinateTableVisionTiles(buffer);
      for (const timeoutMs of [45000, 65000, 90000]) {
        if (priority?.reason !== "transformation_verification_failed") break;
        try {
          const selectiveReferenceRows = await runSelectiveDmsReferenceRereadPass({
            priority,
            imageItems: tableTiles.length > 0 ? tableTiles : imageItems,
            invokeVision: args => invokeAliyunVision(config, {
              ...args,
              model: config.model,
              maxTokens: 700,
              timeoutMs
            })
          });
          table = mergeSelectiveDmsReferenceRows(priority.table, selectiveReferenceRows, { shadowIntent: crs.shadowIntent });
          priority = buildStructuredUtmPriority({ shadowIntent: crs.shadowIntent, table });
        } catch {
          // Try the next bounded timeout window; final assertion below keeps this fail-closed.
        }
      }
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
      verification: priority.transformationVerification,
      remainingMismatches: getStructuredUtmVerificationMismatches(priority).map(item => item.point)
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
