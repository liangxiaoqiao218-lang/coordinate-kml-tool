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

const html = await readFile(path.join(repoRoot, "index.html"), "utf8");
const ensureIndex = html.indexOf("await ensureManualInputFinalized();");
const gateIndex = html.indexOf("if (shouldBlockFinalizedCoordinateKml())", ensureIndex);
assert.ok(ensureIndex > 0 && gateIndex > ensureIndex, "manual finalization must precede the KML permission Gate");
assert.match(html, /fetch\("\/api\/coordinate-manual-finalize"/);

console.log(JSON.stringify({
  suite: "sr08c-manual-finalizer-regression",
  passed: 5,
  cases: ["MANUAL_FINALIZE", "MANUAL_REVISION", "STALE_REJECTED", "INVALID_BLOCKED", "KML_AUTHORITY_ORDER"]
}, null, 2));

