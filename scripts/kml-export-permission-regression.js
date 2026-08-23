import assert from "node:assert/strict";
import fs from "node:fs";

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const signatureOpen = source.indexOf("(", start);
  let signatureDepth = 0;
  let signatureClose = -1;
  for (let index = signatureOpen; index < source.length; index += 1) {
    if (source[index] === "(") signatureDepth += 1;
    if (source[index] === ")") {
      signatureDepth -= 1;
      if (signatureDepth === 0) {
        signatureClose = index;
        break;
      }
    }
  }
  const openBrace = source.indexOf("{", signatureClose);
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

const indexHtml = fs.readFileSync("index.html", "utf8");
const kmlFunctionNames = [
  "escapeXml",
  "coordinatesAreSame",
  "getClosedPolygonPairs",
  "formatKmlCoordinates",
  "buildPointKml",
  "buildLineStringKml",
  "buildPolygonKml",
  "buildChatCoordinatesKml"
];
const kmlFunctions = Function(`
  ${kmlFunctionNames.map(name => extractFunctionSource(indexHtml, name)).join("\n")}
  return { buildChatCoordinatesKml };
`)();

const pointPairs = [{ longitude: -11.178174, latitude: 12.319572 }];
const linePairs = [
  ...pointPairs,
  { longitude: -11.179, latitude: 12.32 }
];
const polygonPairs = [
  ...linePairs,
  { longitude: -11.1795, latitude: 12.3185 }
];
const pointKml = kmlFunctions.buildChatCoordinatesKml(pointPairs, "Point");
const lineKml = kmlFunctions.buildChatCoordinatesKml(linePairs, "LineString");
const polygonKml = kmlFunctions.buildChatCoordinatesKml(polygonPairs, "Polygon");
assert.match(pointKml, /<Point>[\s\S]*-11\.178174,12\.319572,0[\s\S]*<\/Point>/);
assert.match(lineKml, /<LineString>[\s\S]*-11\.178174,12\.319572,0 -11\.179,12\.32,0[\s\S]*<\/LineString>/);
assert.match(
  polygonKml,
  /<Polygon>[\s\S]*-11\.178174,12\.319572,0 -11\.179,12\.32,0 -11\.1795,12\.3185,0 -11\.178174,12\.319572,0[\s\S]*<\/Polygon>/
);
assert.equal(
  new Blob([pointKml], { type: "application/vnd.google-earth.kml+xml;charset=utf-8" }).type,
  "application/vnd.google-earth.kml+xml;charset=utf-8"
);

const baseUrl = String(process.env.KML_REGRESSION_BASE_URL || "http://127.0.0.1:3000").replace(/\/$/, "");
const visitorId = `kml-permission-regression-${Date.now()}`;

async function request(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, options);
  const payload = await response.json();
  return { response, payload };
}

const config = await request(`/api/config?visitorId=${encodeURIComponent(visitorId)}`);
assert.equal(config.response.status, 200);
assert.equal(config.payload.user.plan, "free");
assert.equal(config.payload.user.isVip, false);
assert.equal(config.payload.permissions.kmlExportEnabled, true);

const initialQuota = await request(`/api/usage/quota?visitorId=${encodeURIComponent(visitorId)}`);
assert.equal(initialQuota.response.status, 200);
assert.equal(initialQuota.payload.source, "local_development");
assert.equal(initialQuota.payload.quota.free_convert_count, 3);
assert.equal(initialQuota.payload.quota.paid_convert_count, 0);

const remaining = [2, 1, 0];
for (const expectedRemaining of remaining) {
  const consumed = await request("/api/usage/consume", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-visitor-id": visitorId
    },
    body: JSON.stringify({ visitorId, type: "convert" })
  });
  assert.equal(consumed.response.status, 200);
  assert.equal(consumed.payload.success, true);
  assert.equal(consumed.payload.source, "free");
  assert.equal(consumed.payload.quota.free_convert_count, expectedRemaining);
}

const exhausted = await request("/api/usage/consume", {
  method: "POST",
  headers: {
    "content-type": "application/json",
    "x-visitor-id": visitorId
  },
  body: JSON.stringify({ visitorId, type: "convert" })
});
assert.equal(exhausted.response.status, 403);
assert.equal(exhausted.payload.success, false);
assert.equal(exhausted.payload.reason, "limit_exceeded");
assert.equal(exhausted.payload.code, "CONVERT_QUOTA_EXHAUSTED");
assert.equal(
  exhausted.payload.quota.free_convert_count + exhausted.payload.quota.paid_convert_count,
  0
);

console.log(JSON.stringify({
  suite: "kml-export-permission-regression",
  passed: 5,
  cases: [
    { id: "free_identity_and_entitlement", status: "PASS" },
    { id: "point_quota_authorized", status: "PASS" },
    { id: "line_quota_authorized", status: "PASS" },
    { id: "polygon_quota_authorized", status: "PASS" },
    { id: "quota_exhausted_denied", status: "PASS", code: exhausted.payload.code }
  ]
}, null, 2));
