import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";

import { runCrsVisionPass } from "../server/crs-evidence/crs-vision-pass.js";
import { buildShadowIntentFromCrsVision } from "../server/crs-evidence/shadow-pipeline.js";

const defaultFixtureRoot = "D:\\萨赫勒数字科技有限公司\\关于西非的业务\\测试素材";
const defaultNegativeRoot = "C:\\Users\\Mir-1\\Documents\\Codex\\2026-08-05\\coordinate-kml-tool-v11-main-v1.1-clean\\test-fixtures\\coordinate-recognition";

const cases = [
  { name: "Indonesia UTM50S 01", path: path.join(defaultFixtureRoot, "印尼矿地01.jpg"), kind: "utm50s" },
  { name: "Indonesia UTM50S 02", path: path.join(defaultFixtureRoot, "印尼矿地02.jpg"), kind: "utm50s" },
  { name: "Indonesia UTM50S 03", path: path.join(defaultFixtureRoot, "印尼矿地03.jpg"), kind: "utm50s" },
  { name: "BFTM negative", path: path.join(defaultNegativeRoot, "bftm", "bftm_burkina_002.jpg"), kind: "negative" },
  { name: "MGRS negative", path: path.join(defaultNegativeRoot, "mgrs", "mgrs_myanmar_001.jpg"), kind: "negative" },
  { name: "Kyrgyz GK negative", path: path.join(defaultNegativeRoot, "kyrgyz", "kyrgyz_gk_001.png"), kind: "negative" }
];

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
  let fileValues = {};
  const envFile = process.env.CRS_EVIDENCE_ENV_FILE;
  if (envFile) fileValues = parseEnvText(await readFile(envFile, "utf8"));
  const get = key => process.env[key] || fileValues[key] || "";
  const apiKey = get("ALIYUN_API_KEY") || get("DASHSCOPE_API_KEY");
  const baseUrl = get("ALIYUN_BASE_URL") || get("DASHSCOPE_BASE_URL") || "https://dashscope.aliyuncs.com/compatible-mode/v1";
  const model = get("ALIYUN_VISION_MODEL") || get("DASHSCOPE_VISION_MODEL") || "qwen-vl-plus";
  if (!apiKey) throw new Error("Missing ALIYUN_API_KEY or DASHSCOPE_API_KEY");
  return { apiKey, baseUrl: baseUrl.replace(/\/+$/, ""), model };
}

function mimeFor(filePath) {
  return /\.png$/i.test(filePath) ? "image/png" : "image/jpeg";
}

async function invokeAliyunVision(config, { prompt, imageItems }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 90000);
  try {
    const endpoint = config.baseUrl.endsWith("/chat/completions") ? config.baseUrl : `${config.baseUrl}/chat/completions`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { Authorization: `Bearer ${config.apiKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: config.model,
        messages: [{ role: "user", content: [{ type: "text", text: prompt }, ...imageItems] }],
        temperature: 0,
        max_tokens: 1200
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

function validate(testCase, result) {
  if (testCase.kind === "utm50s") {
    assert.equal(result.crsEvidence.projection, "utm");
    assert.equal(result.crsEvidence.datum, "WGS84");
    assert.equal(result.crsEvidence.zone, 50);
    assert.equal(result.crsEvidence.hemisphere, "south");
    assert.equal(result.shadowIntent.confidence, "confirmed");
    assert.equal(result.shadowIntent.epsg, "EPSG:32750");
    return;
  }
  assert.notEqual(result.shadowIntent.confidence, "confirmed");
  assert.equal(result.shadowIntent.epsg, null);
}

const config = await loadProviderConfig();
const summary = [];
for (const testCase of cases) {
  const buffer = await readFile(testCase.path);
  const hash = createHash("sha256").update(buffer).digest("hex");
  const imageItems = [{ type: "image_url", image_url: { url: `data:${mimeFor(testCase.path)};base64,${buffer.toString("base64")}` } }];
  let acquisition;
  let result;
  let status = "PASS";
  let error = null;
  try {
    acquisition = await runCrsVisionPass({
      imageItems,
      invokeVision: args => invokeAliyunVision(config, args)
    });
    result = buildShadowIntentFromCrsVision(acquisition);
    validate(testCase, result);
  } catch (caught) {
    status = "FAIL";
    error = caught.message;
  }
  const row = {
    sample: testCase.name,
    file: testCase.path,
    sha256: hash,
    status,
    crsVision: acquisition || null,
    crsEvidence: result?.crsEvidence || null,
    shadowIntent: result?.shadowIntent || null,
    error
  };
  summary.push(row);
  console.log(JSON.stringify(row, null, 2));
}

const passed = summary.filter(row => row.status === "PASS").length;
console.log(`\nReal Image CRS Evidence Regression: ${passed}/${summary.length} PASS`);
if (passed !== summary.length) process.exitCode = 1;
