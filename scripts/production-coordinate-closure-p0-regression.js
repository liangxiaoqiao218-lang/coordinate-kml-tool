import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = await readFile(path.join(root, "index.html"), "utf8");

for (const match of source.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/giu)) {
  if (!/application\/ld\+json/iu.test(match[0])) assert.doesNotThrow(() => new Function(match[1]));
}

function extractFunction(name) {
  const start = source.indexOf(`function ${name}`);
  assert.notEqual(start, -1, `missing ${name}`);
  const brace = source.indexOf("{", start);
  let depth = 0;
  for (let index = brace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") depth -= 1;
    if (depth === 0) return source.slice(start, index + 1);
  }
  throw new Error(`unterminated ${name}`);
}

const sampleGroups = [[
  { longitude: -9.020463888888889, latitude: 11.72123611111111 },
  { longitude: -9.015563888888888, latitude: 11.719222222222223 },
  { longitude: -9.016297222222223, latitude: 11.717605555555556 },
  { longitude: -9.02090277777778, latitude: 11.719805555555556 }
]];
const context = vm.createContext({
  activeRecognitionAcquisitionResult: { requiresReview: true, authorizationStatus: "REVIEW_REQUIRED" },
  activeFinalizedCoordinateResult: { mapReady: false, kmlReady: false },
  getKmlCoordinateGroups: () => structuredClone(sampleGroups),
  getFinalizedCoordinateIdentity: () => null,
  countCoordinatePairsInGroups: groups => groups.reduce((total, group) => total + group.length, 0),
  Date
});
vm.runInContext([
  extractFunction("getConvertibleCoordinateGroups"),
  extractFunction("hasConvertibleCoordinateResult"),
  extractFunction("coordinateResultNeedsReview"),
  extractFunction("shouldUseProvisionalCoordinateResult"),
  extractFunction("coordinateRecognitionActionBlocked"),
  extractFunction("buildProvisionalCoordinateGeometry"),
  extractFunction("createProvisionalSpatialPayload")
].join("\n"), context);

assert.equal(context.hasConvertibleCoordinateResult(), true);
assert.equal(context.coordinateRecognitionActionBlocked("map"), false);
assert.equal(context.coordinateRecognitionActionBlocked("kml"), false);
assert.equal(context.shouldUseProvisionalCoordinateResult(), true);
const geometry = context.buildProvisionalCoordinateGeometry();
assert.equal(geometry.type, "Polygon");
assert.equal(geometry.coordinates[0].length, 5);
assert.deepEqual(geometry.coordinates[0][0], geometry.coordinates[0].at(-1));
const payload = context.createProvisionalSpatialPayload();
assert.equal(payload.mapPreviewObject.previewEligibility.allowed, true);
assert.equal(payload.kmlEligibility.allowed, true);
assert.equal(payload.kmlEligibility.confirmationStatus, "REVIEW_REQUIRED");

assert.match(source, /coordinates-unverified\.kml/u);
assert.match(source, /UNVERIFIED: This file is provided for visual checking/u);
assert.match(source, /图片内容较多，还需要一点时间。/u);
assert.match(source, /识别时间较长。您可以继续等待、停止识别，或寻求人工协助。/u);
assert.match(source, /继续等待/u);
assert.match(source, /人工协助/u);
assert.doesNotMatch(source, /appendDebug\(`异步任务状态：\$\{status\}\\n任务编号/u);
assert.doesNotMatch(source, /appendDebug\(`识别原文：/u);
assert.doesNotMatch(source, /appendDebug\(`后端坐标提取结果：/u);
assert.match(source, /currentLines\[currentLines\.length - 1\] === entry/u);

console.log("production coordinate closure p0 regression: PASS");
