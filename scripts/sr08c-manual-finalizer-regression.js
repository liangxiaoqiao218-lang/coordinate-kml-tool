import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseUrl = String(process.env.SR08C_BASE_URL || "http://127.0.0.1:3000").replace(/\/$/, "");

async function post(route, body) {
  const response = await fetch(`${baseUrl}${route}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-source": "sr08c-manual-regression" },
    body: JSON.stringify(body)
  });
  return { response, payload: await response.json().catch(() => ({})) };
}

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${functionName} body is not closed`);
}

const html = await readFile(path.join(repoRoot, "index.html"), "utf8");
const normalizeManualCoordinateTextForFinalizer = Function(`
  ${extractFunctionSource(html, "stripLeadingCoordinateLabel")}
  ${extractFunctionSource(html, "normalizeManualCoordinateTextForFinalizer")}
  return normalizeManualCoordinateTextForFinalizer;
`)();

const firstText = "12.319572, -11.178174\n12.320000, -11.179000\n12.318500, -11.179500";
const secondText = "12.319572, -11.178174\n12.321000, -11.180000\n12.318500, -11.179500";

const initial = await post("/api/coordinate-manual-finalize", { coordinateText: firstText });
assert.equal(initial.response.status, 200);
const revision1 = initial.payload.finalizedCoordinateResult;
assert.equal(revision1.sourceAuthority, "manual_input");
assert.equal(revision1.resultRevision, 1);
assert.equal(revision1.decisionState, "AUTO_EXPORT");
assert.ok(revision1.geometryHash);

const revised = await post("/api/coordinate-revision", {
  resultId: revision1.resultId,
  resultRevision: revision1.resultRevision,
  geometryHash: revision1.geometryHash,
  coordinateText: secondText
});
assert.equal(revised.response.status, 200);
const revision2 = revised.payload.finalizedCoordinateResult;
assert.equal(revision2.sourceAuthority, "manual_input");
assert.equal(revision2.resultId, revision1.resultId);
assert.equal(revision2.resultRevision, 2);
assert.notEqual(revision2.geometryHash, revision1.geometryHash);
assert.equal(revision2.decisionState, "AUTO_EXPORT");

const stale = await post("/api/coordinate-confirmation", {
  resultId: revision1.resultId,
  resultRevision: revision1.resultRevision,
  geometryHash: revision1.geometryHash,
  action: "accept"
});
assert.equal(stale.response.status, 409);
assert.equal(stale.payload.code, "STALE_CONFIRMATION_REVISION");

const invalid = await post("/api/coordinate-manual-finalize", { coordinateText: "not coordinates" });
assert.equal(invalid.response.status, 422);

const exactCoordinate = "116.391245,39.907654";
const numberedPolygon = [
  "1. 116.391245,39.907654",
  "2. 116.392245,39.907654",
  "3. 116.392245,39.908654",
  "4. 116.391245,39.908654"
].join("\n");
const normalizedRequestCases = [
  ["RAW_DECIMAL", exactCoordinate, "Point", [116.391245, 39.907654]],
  ["NUMBERED_DOT", `1. ${exactCoordinate}`, "Point", [116.391245, 39.907654]],
  ["NUMBERED_PAREN", `1) ${exactCoordinate}`, "Point", [116.391245, 39.907654]],
  ["NUMBERED_COLON", `1: ${exactCoordinate}`, "Point", [116.391245, 39.907654]],
  ["NUMBERED_POLYGON", numberedPolygon, "Polygon", null]
];

for (const [name, rawText, geometryType, pointCoordinates] of normalizedRequestCases) {
  const coordinateText = normalizeManualCoordinateTextForFinalizer(rawText);
  const result = await post("/api/coordinate-manual-finalize", { coordinateText });
  assert.equal(result.response.status, 200, `${name} must finalize`);
  const finalized = result.payload.finalizedCoordinateResult;
  assert.equal(finalized.geometry.type, geometryType, `${name} geometry`);
  assert.equal(finalized.decisionState, "AUTO_EXPORT", `${name} Gate`);
  assert.equal(finalized.kmlReady, true, `${name} KML readiness`);
  if (pointCoordinates) assert.deepEqual(finalized.geometry.coordinates, pointCoordinates, `${name} axis order`);
  if (geometryType === "Polygon") {
    assert.equal(finalized.geometry.coordinates[0].length, 5, "Polygon must close without mutating request text");
  }
}

const strictNumbered = await post("/api/coordinate-manual-finalize", { coordinateText: `1. ${exactCoordinate}` });
assert.equal(strictNumbered.response.status, 422, "server strict manual boundary must reject raw UI numbering");

const ensureIndex = html.indexOf("await ensureManualInputFinalized();");
const gateIndex = html.indexOf("if (shouldBlockFinalizedCoordinateKml())", ensureIndex);
assert.ok(ensureIndex > 0 && gateIndex > ensureIndex, "manual finalization must precede the KML permission Gate");
assert.match(html, /fetch\("\/api\/coordinate-manual-finalize"/);

console.log(JSON.stringify({
  suite: "sr08c-manual-finalizer-regression",
  passed: 11,
  cases: ["MANUAL_FINALIZE", "MANUAL_REVISION", "STALE_REJECTED", "INVALID_BLOCKED", "RAW_DECIMAL", "NUMBERED_DOT", "NUMBERED_PAREN", "NUMBERED_COLON", "NUMBERED_POLYGON", "SERVER_STRICT_BOUNDARY", "KML_AUTHORITY_ORDER"]
}, null, 2));

