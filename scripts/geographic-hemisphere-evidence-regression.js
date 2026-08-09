import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");

function extractFunction(name) {
  const startPattern = `function ${name}`;
  const start = source.indexOf(startPattern);
  assert.notEqual(start, -1, `${name} exists`);
  const signatureEnd = source.indexOf(")", start);
  assert.notEqual(signatureEnd, -1, `${name} has signature`);
  const openBrace = source.indexOf("{", signatureEnd);
  assert.notEqual(openBrace, -1, `${name} has body`);

  let depth = 1;
  for (let index = openBrace + 1; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") depth += 1;
    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return source.slice(start, index + 1);
      }
    }
  }

  throw new Error(`${name} body not closed`);
}

const functionNames = [
  "makeWgs84Point",
  "getDecimalCoordinateEvidence",
  "createEmptyGeographicEvidence",
  "summarizeGeographicEvidence",
  "parseChatCoordinateLine",
  "hasLongitudeLatitudeHeaderContext",
  "parseLonLatTableCoordinateLine",
  "getWgs84TableCoordinatesInfo",
  "getChatCoordinateWarnings",
  "inferGeometry",
  "buildChatCoordinatesKml",
  "getChatCoordinatesInfo"
];

const sandbox = {
  Object,
  Number,
  String,
  Array,
  Set,
  RegExp,
  Math,
  normalizeChatCoordinateText(value = "") {
    return String(value || "").replace(/[，]/g, ",");
  },
  getWgs84ChatRejectionReason() {
    return "";
  },
  escapeKmlText(value = "") {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }
};

vm.createContext(sandbox);
vm.runInContext(functionNames.map(extractFunction).join("\n"), sandbox);

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

test("signed WGS84 chat records signed hemisphere evidence", () => {
  const result = sandbox.getChatCoordinatesInfo("12.319572,-11.178174");
  assert.equal(result.isChatCoordinates, true);
  assert.equal(result.hemisphereEvidence, "signed");
  assert.equal(result.coordinateOrderEvidence, "default_latlon");
  assert.equal(result.ambiguity.hemisphere, false);
  assert.equal(result.ambiguity.reason, null);
});

test("unsigned WGS84 chat records hemisphere ambiguity", () => {
  const result = sandbox.getChatCoordinatesInfo("5.591638,2.790556");
  assert.equal(result.isChatCoordinates, true);
  assert.equal(result.hemisphereEvidence, "absent");
  assert.equal(result.coordinateOrderEvidence, "default_latlon");
  assert.equal(result.ambiguity.hemisphere, true);
  assert.equal(result.ambiguity.reason, "decimal_pair_without_hemisphere");
});

test("equal decimal pair remains parsed but marked as hemisphere ambiguous", () => {
  const result = sandbox.getChatCoordinatesInfo("6.754352,6.754352");
  assert.equal(result.isChatCoordinates, true);
  assert.equal(result.points[0].lat, 6.754352);
  assert.equal(result.points[0].lon, 6.754352);
  assert.equal(result.ambiguity.hemisphere, true);
});

test("explicit longitude latitude table records header order evidence", () => {
  const result = sandbox.getWgs84TableCoordinatesInfo([
    "Longitude | Latitude",
    "-2.790556, 5.591638",
    "-2.775833, 5.591389"
  ].join("\n"));
  assert.equal(result.isWgs84TableCoordinates, true);
  assert.equal(result.hemisphereEvidence, "signed");
  assert.equal(result.coordinateOrderEvidence, "explicit_header_lonlat");
  assert.equal(result.ambiguity.hemisphere, false);
});

test("missing WGS84 table header exposes empty evidence without changing rejection", () => {
  const result = sandbox.getWgs84TableCoordinatesInfo("5.591638,2.790556");
  assert.equal(result.isWgs84TableCoordinates, false);
  assert.equal(result.hemisphereEvidence, "absent");
  assert.equal(result.coordinateOrderEvidence, "");
  assert.equal(result.ambiguity.reason, null);
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("Geographic Hemisphere Evidence Regression: 5/5 PASS");
