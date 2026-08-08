import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const fixtureRoot = process.env.COORDINATE_TYPE_FIXTURE_ROOT
  || "D:\\萨赫勒数字科技有限公司\\关于西非的业务\\测试素材";
const envFile = process.env.CRS_EVIDENCE_ENV_FILE;
const port = Number(process.env.COORDINATE_LIVE_GATE_PORT || 33117);
const apiUrl = `http://127.0.0.1:${port}/api/recognize-coordinates`;

const cases = [
  {
    name: "Indonesia UTM50S 01",
    file: "印尼矿地01.jpg",
    sampleId: "utm50s_indonesia_01",
    expectedType: "utm_projected_xy",
    expectedMode: "utm-projected-x-y",
    expectedEpsg: "EPSG:32750",
    expectedConfirmation: "awaiting_confirmation",
    expectedQualityGate: "passed",
    expectedKmlReady: false,
    expectedReview: false
  },
  {
    name: "Indonesia UTM50S 02",
    file: "印尼矿地02.jpg",
    sampleId: "utm50s_indonesia_02",
    expectedType: "utm_projected_xy",
    expectedMode: "utm-projected-x-y",
    expectedEpsg: "EPSG:32750",
    expectedConfirmation: "awaiting_confirmation",
    expectedQualityGate: "passed",
    expectedKmlReady: false,
    expectedReview: false
  },
  {
    name: "Indonesia UTM50S 03",
    file: "印尼矿地03.jpg",
    sampleId: "utm50s_indonesia_03",
    expectedType: "utm_projected_xy",
    expectedMode: "utm-projected-x-y",
    expectedEpsg: "EPSG:32750",
    expectedConfirmation: "awaiting_confirmation",
    expectedQualityGate: "passed",
    expectedKmlReady: false,
    expectedReview: false
  },
  {
    name: "Cote d'Ivoire DMS 03",
    file: "科特迪瓦03.png",
    sampleId: "cote_divoire_single_03",
    expectedType: "cote_divoire_geographic_dms_table",
    expectedMode: "cote-divoire-geographic-dms-table",
    expectedEpsg: null,
    expectedConfirmation: "not_required",
    expectedQualityGate: "passed",
    expectedKmlReady: true,
    expectedReview: false
  },
  {
    name: "Cote d'Ivoire DMS 04",
    file: "科特迪瓦04.png",
    sampleId: "cote_divoire_single_04",
    expectedType: "cote_divoire_geographic_dms_table",
    expectedMode: "cote-divoire-geographic-dms-table",
    expectedEpsg: null,
    expectedConfirmation: "not_required",
    expectedQualityGate: "passed",
    expectedKmlReady: true,
    expectedReview: false
  }
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

function getMimeType(filePath) {
  return /\.png$/i.test(filePath) ? "image/png" : "image/jpeg";
}

function getEpsg(data) {
  return data?.typedUtmIntent?.epsg
    || data?.crsEvidence?.shadowIntent?.epsg
    || data?.coordinateArbitration?.epsg
    || null;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForServer(child, timeoutMs = 30000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (child.exitCode !== null) throw new Error(`Local server exited with code ${child.exitCode}`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/version`);
      if (response.ok) return;
    } catch {
      // Server is still starting.
    }
    await delay(250);
  }
  throw new Error("Timed out waiting for the local Final Image Gate server.");
}

async function recognize(testCase) {
  const filePath = path.join(fixtureRoot, testCase.file);
  const fileBuffer = await readFile(filePath);
  const form = new FormData();
  const visitorId = `coordinate-live-gate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  form.append("image", new Blob([fileBuffer], { type: getMimeType(filePath) }), testCase.file);
  form.append("visitorId", visitorId);
  form.append("regressionSampleId", testCase.sampleId);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 240000);
  try {
    const response = await fetch(apiUrl, {
      method: "POST",
      body: form,
      headers: {
        "x-visitor-id": visitorId,
        "x-source": "coordinate-type-arbitration-live-image-regression",
        "x-regression-test": "true"
      },
      signal: controller.signal
    });
    const responseText = await response.text();
    let data;
    try {
      data = JSON.parse(responseText);
    } catch {
      throw new Error(`Coordinate API returned non-JSON HTTP ${response.status}`);
    }
    if (!response.ok) throw new Error(data?.error || data?.message || `Coordinate API HTTP ${response.status}`);
    return data;
  } finally {
    clearTimeout(timer);
  }
}

if (!envFile) throw new Error("CRS_EVIDENCE_ENV_FILE must point to .env.vision-test");
const envValues = parseEnvText(await readFile(envFile, "utf8"));
const apiKey = envValues.ALIYUN_API_KEY || envValues.DASHSCOPE_API_KEY || process.env.ALIYUN_API_KEY || process.env.DASHSCOPE_API_KEY;
if (!apiKey) throw new Error("Missing ALIYUN_API_KEY or DASHSCOPE_API_KEY");

const childEnv = {
  ...process.env,
  ...envValues,
  ENABLE_REGRESSION_TEST_MODE: "true",
  PORT: String(port)
};
const child = spawn(process.execPath, ["server.js"], {
  cwd: repoRoot,
  env: childEnv,
  stdio: ["ignore", "pipe", "pipe"],
  windowsHide: true
});
let serverOutput = "";
const retainServerOutput = chunk => {
  serverOutput = `${serverOutput}${chunk}`.slice(-12000);
};
child.stdout.on("data", retainServerOutput);
child.stderr.on("data", retainServerOutput);

let passed = 0;
try {
  await waitForServer(child);
  for (const testCase of cases) {
    let summary;
    try {
      const data = await recognize(testCase);
      const epsg = getEpsg(data);
      assert.equal(data.coordinateType, testCase.expectedType, "coordinateType");
      assert.equal(data.precisionMode, testCase.expectedMode, "precisionMode");
      assert.equal(epsg, testCase.expectedEpsg, "EPSG");
      assert.equal(data.confirmationStatus, testCase.expectedConfirmation, "confirmationStatus");
      assert.equal(data.qualityGateStatus, testCase.expectedQualityGate, "qualityGateStatus");
      assert.equal(data.kml_ready, testCase.expectedKmlReady, "kml_ready");
      assert.equal(data.requires_review, testCase.expectedReview, "requires_review");
      assert.ok(data.coordinateArbitration, "coordinateArbitration missing");
      assert.equal(data.coordinateArbitration.coordinateType, testCase.expectedType, "finalized arbitration type");
      passed += 1;
      summary = {
        sample: testCase.file,
        coordinateType: data.coordinateType,
        precisionMode: data.precisionMode,
        crs: epsg,
        confirmationStatus: data.confirmationStatus,
        qualityGateStatus: data.qualityGateStatus,
        kml_ready: data.kml_ready,
        requires_review: data.requires_review,
        result: "PASS"
      };
    } catch (error) {
      summary = {
        sample: testCase.file,
        result: "FAIL",
        error: error.message
      };
    }
    console.log(JSON.stringify(summary));
  }

  const swapped = buildFinalizedCoordinateVerificationResponse({
    precisionMode: "wgs84-chat-coordinates",
    warning: "possible swapped lat/lon",
    chatCoordinates: {
      isChatCoordinates: true,
      warnings: ["possible swapped lat/lon"]
    }
  });
  assert.equal(swapped.requires_review, true);
  assert.equal(swapped.qualityGateStatus, "blocked");
  assert.equal(swapped.kml_ready, false);
  console.log(JSON.stringify({ sample: "swapped_lat_lon_safety_gate", requires_review: true, qualityGateStatus: "blocked", kml_ready: false, result: "PASS" }));
} catch (error) {
  const safeServerTail = serverOutput
    .split(/\r?\n/)
    .filter(line => !/api[_-]?key|authorization|bearer/i.test(line))
    .slice(-20)
    .join("\n");
  if (safeServerTail) console.error(safeServerTail);
  throw error;
} finally {
  if (child.exitCode === null) {
    child.kill();
    await Promise.race([once(child, "exit"), delay(3000)]);
    if (child.exitCode === null) child.kill("SIGKILL");
  }
}

console.log(`Coordinate Type Arbitration Live Image Regression: ${passed}/${cases.length} PASS`);
if (passed !== cases.length) process.exitCode = 1;
