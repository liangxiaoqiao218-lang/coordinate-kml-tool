import assert from "node:assert/strict";
import fs from "node:fs";
import { buildSourceCoordinateRepresentation } from "../server/source-coordinate-representation.js";
import { parseManualLongitudeLatitudeText } from "../server/manual-coordinate-input.js";

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}" && --depth === 0) return source.slice(start, index + 1);
  }
  throw new Error(`${functionName} body is not closed`);
}

const indexHtml = fs.readFileSync("index.html", "utf8");
const normalizeManualCoordinateTextForFinalizer = Function(`
  ${extractFunctionSource(indexHtml, "stripLeadingCoordinateLabel")}
  ${extractFunctionSource(indexHtml, "normalizeManualCoordinateTextForFinalizer")}
  return normalizeManualCoordinateTextForFinalizer;
`)();
const swapCoordinateTextOrder = Function(`
  ${extractFunctionSource(indexHtml, "stripLeadingCoordinateLabel")}
  ${extractFunctionSource(indexHtml, "swapCoordinateTextOrder")}
  return swapCoordinateTextOrder;
`)();

const rawRows = [
  "-8.09722200,10.62611100",
  "-8.06027800,10.62611100",
  "-8.06027800,10.52944400",
  "-8.09722200,10.52944400",
  "-8.09722200,10.62611100"
].join("\n");
const formattedRows = [
  "label | WGS84 | KML",
  "1 | -8.097222, 10.626111 | 10.626111,-8.097222,0",
  "2 | -8.060278, 10.626111 | 10.626111,-8.060278,0",
  "3 | -8.060278, 10.529444 | 10.529444,-8.060278,0",
  "4 | -8.097222, 10.529444 | 10.529444,-8.097222,0"
].join("\n");
const engine = {
  coordinate_type: "decimal_latlon",
  precision_mode: "wgs84-chat-coordinates",
  groups: [{
    points: [
      { lat: -8.097222, lon: 10.626111 },
      { lat: -8.060278, lon: 10.626111 },
      { lat: -8.060278, lon: 10.529444 },
      { lat: -8.097222, lon: 10.529444 }
    ]
  }]
};

const source = buildSourceCoordinateRepresentation({
  rawText: rawRows,
  coordinates: formattedRows,
  precisionMode: "wgs84-chat-coordinates"
}, engine);
assert.equal(source.displayText, rawRows, "edit view must preserve source values and precision");
assert.equal(source.rows.length, 5, "source closure row must remain visible");
assert.equal(source.groups.length, 1, "source must not invent groups");
assert.equal(source.axisOrder, "latitude_longitude");
assert.equal(source.sourceEquivalence, "pointwise_decimal_semantic_match_with_source_closure");

const swappedSource = swapCoordinateTextOrder(source.displayText, "latlng");
assert.equal(swappedSource.swappedCount, 5);
assert.equal(swappedSource.displayOrder, "latlng");
assert.equal(swappedSource.text.split("\n")[0], "10.62611100,-8.09722200");
assert.equal(swappedSource.text.split("\n").at(-1), "10.62611100,-8.09722200");

const canonicalSource = normalizeManualCoordinateTextForFinalizer(swappedSource.text, swappedSource.displayOrder);
const sourcePoints = parseManualLongitudeLatitudeText(canonicalSource);
assert.equal(sourcePoints.length, 5);
assert.deepEqual([sourcePoints[0].lon, sourcePoints[0].lat], [-8.097222, 10.626111]);
assert.equal(sourcePoints[0].kmlCoordinate, "-8.097222,10.626111,0");

const swappedBack = swapCoordinateTextOrder(swappedSource.text, swappedSource.displayOrder);
assert.equal(swappedBack.swappedCount, 5);
assert.equal(swappedBack.text, rawRows, "the exchange action must remain reversible");

const platformEngine = {
  coordinate_type: "decimal_latlon",
  precision_mode: "wgs84-platform-lonlat-coordinates",
  source_crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
  groups: [{
    points: [
      { lat: 10.626111, lon: -8.097222 },
      { lat: 10.626111, lon: -8.060278 },
      { lat: 10.529444, lon: -8.060278 },
      { lat: 10.529444, lon: -8.097222 }
    ]
  }]
};
const platformSource = buildSourceCoordinateRepresentation({
  rawText: rawRows,
  coordinates: rawRows,
  precisionMode: "wgs84-platform-lonlat-coordinates"
}, platformEngine);
assert.equal(platformSource.displayText, rawRows);
assert.equal(platformSource.axisOrder, "longitude_latitude");
const platformCanonical = normalizeManualCoordinateTextForFinalizer(platformSource.displayText, "lnglat");
const platformPoints = parseManualLongitudeLatitudeText(platformCanonical);
assert.deepEqual([platformPoints[0].lon, platformPoints[0].lat], [-8.097222, 10.626111]);
assert.equal(platformPoints[0].kmlCoordinate, "-8.097222,10.626111,0");

const swappedFormatted = swapCoordinateTextOrder(formattedRows, "auto");
assert.equal(swappedFormatted.swappedCount, 4);
assert.equal(swappedFormatted.displayOrder, "latlng");
assert.doesNotMatch(swappedFormatted.text, /label|WGS84|KML|\|/i);
assert.equal(swappedFormatted.text.split("\n")[0], "10.626111,-8.097222");
const canonicalFormatted = normalizeManualCoordinateTextForFinalizer(
  swappedFormatted.text,
  swappedFormatted.displayOrder
);
const formattedPoints = parseManualLongitudeLatitudeText(canonicalFormatted);
assert.equal(formattedPoints.length, 4);
assert.deepEqual([formattedPoints[0].lon, formattedPoints[0].lat], [-8.097222, 10.626111]);

const legacyCanonical = normalizeManualCoordinateTextForFinalizer(formattedRows, "auto");
const legacyPoints = parseManualLongitudeLatitudeText(legacyCanonical);
assert.equal(legacyPoints.length, 4, "legacy formatted display remains accepted at finalizer boundary");
assert.deepEqual([legacyPoints[0].lon, legacyPoints[0].lat], [10.626111, -8.097222]);

assert.match(indexHtml, /coordinateOrder\.value = sourceCoordinateRepresentation\.axisOrder === "latitude_longitude"/);
assert.match(indexHtml, /normalizeManualCoordinateTextForFinalizer\(input\.value, coordinateOrder\.value\)/);
assert.match(indexHtml, /agenticCoordinateController\.edit\(input\.value\)/);
assert.match(indexHtml, /地图和 KML 将按交换后的顺序重新计算/);
assert.match(indexHtml, /\["localhost", "127\.0\.0\.1", "::1"\]\.includes\(window\.location\.hostname\)/);
assert.match(indexHtml, /new URLSearchParams\(window\.location\.search\)\.get\("regression"\) === "1"/);
assert.match(indexHtml, /headers\["x-regression-test"\] = "true"/);
assert.match(indexHtml, /class="icon-button undo-button"/);
assert.match(indexHtml, /:not\(\.undo-button\):not\(\.swap-order-button\)/);

console.log(JSON.stringify({
  suite: "wgs84-source-order-swap-regression",
  passed: 32,
  cases: [
    "SOURCE_VALUES_AND_PRECISION_PRESERVED",
    "SOURCE_CLOSURE_ROW_PRESERVED",
    "NO_GROUPS_INVENTED",
    "SOURCE_AXIS_ORDER_EXPLICIT",
    "RAW_SOURCE_SWAP_SEMANTIC",
    "RAW_SOURCE_SWAP_IS_REVERSIBLE",
    "PLATFORM_FORMAT_AUTO_LONGITUDE_LATITUDE",
    "PLATFORM_FORMAT_MAP_LONGITUDE_LATITUDE",
    "PLATFORM_FORMAT_KML_LONGITUDE_LATITUDE",
    "LEGACY_TABLE_SWAP_SEMANTIC",
    "SWAP_OUTPUT_PARSEABLE",
    "MAP_FINALIZER_USES_POST_SWAP_ORDER",
    "KML_FINALIZER_USES_POST_SWAP_ORDER",
    "AGENTIC_WORKSPACE_RECEIVES_POST_SWAP_TEXT",
    "LEGACY_TABLE_FINALIZER_COMPATIBLE",
    "LOCAL_REGRESSION_HOST_REQUIRED",
    "LOCAL_REGRESSION_QUERY_REQUIRED",
    "LOCAL_REGRESSION_HEADER_EXPLICIT",
    "UNDO_BUTTON_PERSISTENT",
    "SWAP_BUTTON_PERSISTENT"
  ]
}, null, 2));
