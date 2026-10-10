import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

import {
  createRecognitionAcquisitionJobRuntime,
  RECOGNITION_ACQUISITION_JOB_STATUS
} from "../server/recognition/recognition-acquisition-job-runtime.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const requestedMode = "indonesia_utm50s_structured_b";
const cases = [];

function test(name, action) {
  action();
  cases.push(name);
}

function extractFunction(source, name) {
  const marker = `function ${name}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${name} must exist`);
  let bodyStart = -1;
  let parameterDepth = 0;
  for (let index = start + marker.length - 1; index < source.length; index += 1) {
    if (source[index] === "(") parameterDepth += 1;
    if (source[index] === ")") parameterDepth -= 1;
    if (parameterDepth === 0 && source[index] === "{") {
      bodyStart = index;
      break;
    }
  }
  assert.notEqual(bodyStart, -1, `${name} must have a body`);
  let depth = 0;
  let quote = "";
  let escaped = false;
  for (let index = bodyStart; index < source.length; index += 1) {
    const character = source[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === quote) quote = "";
      continue;
    }
    if (character === "\"" || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    if (character === "{") depth += 1;
    if (character === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${name} body is incomplete`);
}

test("candidate UI is explicit, hidden by default, and unchecked", () => {
  assert.match(indexSource, /id="indonesiaStructuredProductModeControl"[^>]*hidden/u);
  assert.match(indexSource, /id="indonesiaStructuredProductMode" type="checkbox"/u);
  assert.doesNotMatch(indexSource, /id="indonesiaStructuredProductMode"[^>]*checked/u);
});

test("server exposes candidate availability only outside production and behind the exact flag", () => {
  assert.match(serverSource, /indonesiaStructuredProductEnabled = process\.env\.INDONESIA_STRUCTURED_B_PRODUCT_ENABLED === "true"/u);
  assert.match(serverSource, /indonesiaStructuredProductUiAvailable = process\.env\.NODE_ENV !== "production"\s*&& indonesiaStructuredProductEnabled/u);
  assert.match(serverSource, /coordinateProductCandidates:\s*\{\s*indonesiaUtm50StructuredB:\s*\{\s*available: indonesiaStructuredProductUiAvailable/u);
});

test("production UI resolver appends the exact mode only when available and selected", () => {
  const functionNames = ["isIndonesiaStructuredProductModeAvailable", "resolveIndonesiaStructuredProductMode", "appendIndonesiaStructuredProductMode"];
  const functionSource = functionNames.map(name => extractFunction(indexSource, name)).join("\n");
  const context = vm.createContext({
    FormData,
    INDONESIA_STRUCTURED_PRODUCT_MODE: requestedMode,
    appConfig: { coordinateProductCandidates: { indonesiaUtm50StructuredB: { available: false } } },
    indonesiaStructuredProductMode: { checked: false }
  });
  vm.runInContext(`${functionSource}\nthis.appendMode = appendIndonesiaStructuredProductMode;`, context);
  const selected = new FormData();
  assert.equal(context.appendMode(selected, { available: true, selected: true }), requestedMode);
  assert.equal(selected.get("coordinateProductMode"), requestedMode);
  const unavailable = new FormData();
  assert.equal(context.appendMode(unavailable, { available: false, selected: true }), "");
  assert.equal(unavailable.has("coordinateProductMode"), false);
  const unselected = new FormData();
  assert.equal(context.appendMode(unselected, { available: true, selected: false }), "");
  assert.equal(unselected.has("coordinateProductMode"), false);
});

test("the same FormData is used for sync and async endpoints", () => {
  assert.match(indexSource, /appendIndonesiaStructuredProductMode\(formData\);[\s\S]{0,1800}fetch\(useAsyncJob \? "\/api\/recognize-coordinates\/jobs" : "\/api\/recognize-coordinates"/u);
});

test("jobs route preserves multipart body and worker forwards string fields to the real handler", () => {
  assert.match(serverSource, /body:\s*\{ \.\.\.req\.body \}/u);
  assert.match(serverSource, /Object\.entries\(input\.body \|\| \{\}\)/u);
  assert.match(serverSource, /typeof value === "string"\) form\.append\(key, value\)/u);
  assert.match(serverSource, /\/api\/internal\/recognize-coordinates-long/u);
  assert.match(serverSource, /upload\.single\("image"\),\s*recognizeCoordinatesHandler/u);
});

const capturedInputs = [];
const runtime = createRecognitionAcquisitionJobRuntime({
  execute: async input => {
    capturedInputs.push(input);
    return { httpStatus: 422, result: { success: false, reason: "STRUCTURED_PRODUCT_FEATURE_DISABLED", code: "STRUCTURED_PRODUCT_FEATURE_DISABLED", providerCallCount: 0 } };
  }
});
const queued = runtime.enqueue({
  requestId: "11111111-1111-4111-8111-111111111111",
  file: { buffer: Buffer.from("offline"), mimetype: "image/png", originalname: "offline.png" },
  body: { visitorId: "offline", coordinateProductMode: requestedMode },
  forwardHeaders: { "x-regression-test": "1" }
});
let terminal = null;
for (let attempt = 0; attempt < 50; attempt += 1) {
  await new Promise(resolve => setTimeout(resolve, 0));
  terminal = runtime.get(queued.jobId, queued.jobAccessToken);
  if ([RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED].includes(terminal?.status)) break;
}
test("executable async runtime delivers the exact selected mode to its handler input", () => {
  assert.equal(capturedInputs.length, 1);
  assert.equal(capturedInputs[0].body.coordinateProductMode, requestedMode);
  assert.equal(terminal?.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
  assert.equal(terminal?.result?.code, "STRUCTURED_PRODUCT_FEATURE_DISABLED");
  assert.equal(terminal?.result?.providerCallCount, 0);
});

console.log(`Indonesia structured product UI/async routing: ${cases.length}/${cases.length} PASS`);
for (const name of cases) console.log(`PASS ${name}`);
