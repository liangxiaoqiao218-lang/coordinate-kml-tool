import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
const goldenRoot = path.join(repoRoot, "regression-samples", "OCR_GOLDEN");
const recordsPath = path.join(goldenRoot, "records.yaml");
const fixtureRoot = path.join(goldenRoot, "fixtures");
const apiUrl = process.env.OCR_GOLDEN_API_URL || "http://127.0.0.1:3000/api/recognize-coordinates";
const liveProvider = process.env.OCR_GOLDEN_PROVIDER_MODE === "live";
const coordinateTolerance = Number(process.env.OCR_GOLDEN_TOLERANCE || "0.000001");

function sha256(buffer) {
  return createHash("sha256").update(buffer).digest("hex");
}

function readImageDimensions(buffer) {
  if (buffer.length >= 24 && buffer.subarray(1, 4).toString("ascii") === "PNG") {
    return { width: buffer.readUInt32BE(16), height: buffer.readUInt32BE(20) };
  }
  if (buffer[0] === 0xff && buffer[1] === 0xd8) {
    let offset = 2;
    while (offset + 9 < buffer.length) {
      if (buffer[offset] !== 0xff) { offset += 1; continue; }
      const marker = buffer[offset + 1];
      const length = buffer.readUInt16BE(offset + 2);
      if ([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker)) {
        return { width: buffer.readUInt16BE(offset + 7), height: buffer.readUInt16BE(offset + 5) };
      }
      if (!Number.isSafeInteger(length) || length < 2) break;
      offset += length + 2;
    }
  }
  return { width: null, height: null };
}

function flattenCoordinatePairs(value, output = []) {
  if (Array.isArray(value)) {
    if (value.length >= 2 && Number.isFinite(Number(value[0])) && Number.isFinite(Number(value[1]))) {
      output.push([Number(value[0]), Number(value[1])]);
    } else {
      value.forEach(item => flattenCoordinatePairs(item, output));
    }
  } else if (value && typeof value === "object") {
    if (Number.isFinite(Number(value.longitude)) && Number.isFinite(Number(value.latitude))) {
      output.push([Number(value.longitude), Number(value.latitude)]);
    } else if (Number.isFinite(Number(value.lon)) && Number.isFinite(Number(value.lat))) {
      output.push([Number(value.lon), Number(value.lat)]);
    } else {
      Object.values(value).forEach(item => flattenCoordinatePairs(item, output));
    }
  }
  return output;
}

function coordinatesMatch(actual, expected) {
  if (actual.length < expected.length) return false;
  return expected.every((pair, index) => Math.abs(actual[index][0] - pair[0]) <= coordinateTolerance
    && Math.abs(actual[index][1] - pair[1]) <= coordinateTolerance);
}

async function callRecognition(record, fixturePath) {
  const bytes = await readFile(fixturePath);
  const form = new FormData();
  form.append("image", new Blob([bytes]), path.basename(fixturePath));
  form.append("visitorId", `ocr-golden-${record.sample_id}`);
  const startedAt = Date.now();
  const response = await fetch(apiUrl, {
    method: "POST",
    headers: {
      "x-visitor-id": `ocr-golden-${record.sample_id}`,
      "x-source": "ocr-golden-runner",
      "x-regression-test": "true"
    },
    body: form
  });
  const payload = await response.json().catch(() => ({}));
  const elapsedMs = Date.now() - startedAt;
  if (!response.ok) {
    const code = payload.code || payload.error || `HTTP_${response.status}`;
    const providerBlocked = response.status === 503 || /credential|api.?key|provider|配置/i.test(String(code));
    return { status: providerBlocked ? "BLOCKED_BY_PROVIDER" : "FAIL", httpStatus: response.status, code, elapsedMs };
  }
  const actual = flattenCoordinatePairs(payload.finalizedCoordinateResult?.geometry?.coordinates || payload.coordinateEngineV2?.groups || []);
  return {
    status: coordinatesMatch(actual, record.expected_wgs84 || []) ? "PASS" : "FAIL",
    httpStatus: response.status,
    code: "SUCCESS",
    elapsedMs,
    actualPointCount: actual.length,
    expectedPointCount: (record.expected_wgs84 || []).length
  };
}

const recordsDocument = parseYaml(await readFile(recordsPath, "utf8"));
const records = Array.isArray(recordsDocument.records) ? recordsDocument.records : [];
const required = records.filter(record => record.evidence_level === "confirmed"
  && record.validation_status === "READY_INITIAL_SCOPE_POSITIVE");
const fixtureAudit = [];
let fixtureBlocked = false;

for (const record of records) {
  const fixturePath = path.join(fixtureRoot, path.basename(String(record.original_image || "")));
  let audit;
  try {
    const bytes = await readFile(fixturePath);
    const dimensions = readImageDimensions(bytes);
    const expectedResolution = String(record.resolution || "").toLowerCase();
    const actualResolution = `${dimensions.width}x${dimensions.height}`;
    const valid = sha256(bytes) === String(record.image_hash_sha256 || "").toLowerCase()
      && bytes.length === Number(record.image_bytes)
      && actualResolution === expectedResolution;
    audit = { sampleId: record.sample_id, fixturePath, exists: true, sha256: sha256(bytes), bytes: bytes.length, resolution: actualResolution, valid };
    if (!valid) fixtureBlocked = true;
  } catch {
    audit = { sampleId: record.sample_id, fixturePath, exists: false, valid: false };
    fixtureBlocked = true;
  }
  fixtureAudit.push(audit);
}

const results = [];
let status = fixtureBlocked ? "BLOCKED_BY_FIXTURE" : "READY";
if (!fixtureBlocked && liveProvider) {
  for (const record of required) {
    const fixturePath = path.join(fixtureRoot, path.basename(record.original_image));
    const result = await callRecognition(record, fixturePath);
    results.push({ sampleId: record.sample_id, ...result });
    if (result.status === "BLOCKED_BY_PROVIDER") { status = "BLOCKED_BY_PROVIDER"; break; }
    if (result.status === "FAIL") status = "FAIL";
  }
  if (results.length === required.length && results.every(result => result.status === "PASS")) status = "PASS";
} else if (!fixtureBlocked && !liveProvider) {
  status = "READY";
}

const output = {
  suite: "ocr-golden-runner",
  status,
  providerMode: liveProvider ? "live" : "not_requested",
  recordCount: records.length,
  confirmedCount: records.filter(record => record.evidence_level === "confirmed").length,
  requiredCount: required.length,
  requiredExecuted: results.length,
  requiredSkipped: Math.max(0, required.length - results.length),
  createdValidatedBy: "unrecorded_in_source_records",
  fixtureAudit,
  results
};
console.log(JSON.stringify(output, null, 2));
if (["FAIL", "BLOCKED_BY_FIXTURE", "BLOCKED_BY_PROVIDER"].includes(status)) process.exitCode = 1;
