import assert from "node:assert/strict";
import fs from "node:fs";
import { transformUtmWgs84Points } from "../server/utm-intent/utm-wgs84-transform.js";

const html = fs.readFileSync(new URL("../index.html", import.meta.url), "utf8");

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `missing ${name}`);
  const paramsStart = html.indexOf("(", start);
  let paramsDepth = 0;
  let bodyStart = -1;
  for (let index = paramsStart; index < html.length; index += 1) {
    if (html[index] === "(") paramsDepth += 1;
    if (html[index] === ")") paramsDepth -= 1;
    if (paramsDepth === 0) {
      bodyStart = html.indexOf("{", index);
      break;
    }
  }
  assert.notEqual(bodyStart, -1, `missing body for ${name}`);
  let depth = 0;
  for (let index = bodyStart; index < html.length; index += 1) {
    if (html[index] === "{") depth += 1;
    if (html[index] === "}") depth -= 1;
    if (depth === 0) return html.slice(start, index + 1);
  }
  throw new Error(`unterminated ${name}`);
}

const intentHelpers = [
  "getWgs84UtmEpsg",
  "normalizeEpsgCode",
  "normalizeWgs84UtmIntent",
  "getTypedCrsIntentFromRecognitionData"
].map(extractFunction).join("\n");
const getIntent = new Function(`${intentHelpers}; return getTypedCrsIntentFromRecognitionData;`)();

const indonesiaIntent = getIntent({
  shadowIntent: {
    projection: "utm",
    datum: "WGS84",
    zone: 50,
    hemisphere: "south",
    epsg: "EPSG:32750",
    confidence: "confirmed",
    conflicts: []
  }
});
assert.deepEqual(indonesiaIntent, {
  coordinateType: "utm_projected_xy",
  datum: "WGS84",
  zone: 50,
  hemisphere: "south",
  epsg: "EPSG:32750",
  source: "图纸 CRS 标注"
});
assert.equal(getIntent({ shadowIntent: { confidence: "unknown" } }), null);
assert.equal(getIntent({
  shadowIntent: {
    projection: "utm",
    datum: "WGS84",
    zone: 50,
    hemisphere: "south",
    epsg: "EPSG:32750",
    confidence: "confirmed",
    conflicts: [{ type: "crs_conflict" }]
  }
}), null);

const projectionHelpers = ["transverseMercatorToWgs84", "utmToWgs84"].map(extractFunction).join("\n");
const frontEndUtmToWgs84 = new Function(`${projectionHelpers}; return utmToWgs84;`)();
const sample = { easting: 779271.176, northing: 9720912.526 };
const frontEndPoint = frontEndUtmToWgs84(sample.easting, sample.northing, 50, false);
const [typedPoint] = transformUtmWgs84Points([sample], {
  coordinateType: "utm_projected_xy",
  datum: "WGS84",
  zone: 50,
  hemisphere: "south",
  epsg: "EPSG:32750"
});
assert.ok(Math.abs(frontEndPoint.longitude - typedPoint.longitude) < 1e-9);
assert.ok(Math.abs(frontEndPoint.latitude - typedPoint.latitude) < 1e-9);

const requiresSource = extractFunction("requiresCrsConfirmationForCurrentInput");
const requiresConfirmation = new Function(
  "activeCoordinatePrecisionMode",
  "projectionType",
  "hasProjectedCoordinateRows",
  "input",
  `${requiresSource}; return requiresCrsConfirmationForCurrentInput();`
);
assert.equal(requiresConfirmation("projected-x-y", { value: "auto" }, () => true, { value: "779271,9720912" }), true);
assert.equal(requiresConfirmation("bftm-projected-x-y", { value: "bftm" }, () => true, { value: "655000,1333600" }), false);
assert.equal(requiresConfirmation("utm30n-projected-x-y", { value: "utm30n" }, () => true, { value: "727250,1219700" }), false);
assert.equal(requiresConfirmation("utm-projected-x-y", { value: "auto" }, () => true, { value: "779271,9720912" }), true);

for (const requiredUiText of [
  "确认使用此坐标系",
  "手动选择坐标系",
  "manualCrsZone",
  "manualCrsHemisphere",
  "manualCrsDatum",
  "crsConfirmationBlocked",
  "CRS_CONFIRMATION_STATUS.BLOCKED",
  "import(\"/server/utm-intent/shadow-resolver.js\")"
]) {
  assert.ok(html.includes(requiredUiText), `missing UI control: ${requiredUiText}`);
}

console.log("CRS Confirmation UI Regression: 4/4 PASS");
console.log("PASS Indonesia UTM50S typed intent confirmation and WGS84 conversion");
console.log("PASS unknown/conflicted CRS remains blocked with manual selection controls");
console.log("PASS BFTM bypass remains unchanged");
console.log("PASS legacy UTM30 bypass remains unchanged while typed UTM requires confirmation");
