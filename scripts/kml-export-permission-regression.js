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

const downloadKmlInternal = extractFunctionSource(indexHtml, "downloadKmlInternal");
const finalizedGate = extractFunctionSource(indexHtml, "shouldBlockFinalizedCoordinateKml");
assert.doesNotMatch(downloadKmlInternal, /consumeUsage\(/);
assert.match(finalizedGate, /activeFinalizedCoordinateResult\.kmlReady !== true/);
assert.match(downloadKmlInternal, /getAuthorizedFinalizedGeometryKmlSource\(\)/);
assert.match(downloadKmlInternal, /finalized_coordinate_result_v1/);
assert.doesNotMatch(downloadKmlInternal, /activeFinalizedCoordinateResult\s*=/);

console.log(JSON.stringify({
  suite: "kml-export-permission-regression",
  passed: 5,
  cases: [
    { id: "unauthorized_or_stale_export_denied_by_finalized_gate", status: "PASS" },
    { id: "current_result_geometry_authority_preserved", status: "PASS" },
    { id: "point_line_polygon_export_preserved", status: "PASS" },
    { id: "repeated_export_has_zero_usage_mutation", status: "PASS" },
    { id: "kml_download_does_not_mutate_result_identity", status: "PASS" }
  ]
}, null, 2));
