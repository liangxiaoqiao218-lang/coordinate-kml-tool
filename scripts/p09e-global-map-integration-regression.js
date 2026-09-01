import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { FINALIZED_COORDINATE_CRS, finalizeCoordinateResult } from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";
import { MAP_PREVIEW_GATE, createGeometryRenderPlan, validateMapPreviewObject } from "../assets/spatial-map/geometry-render-plan.js";
import { MapProductController } from "../assets/spatial-map/map-product-controller.js";
import { PROVIDER_STATE } from "../assets/spatial-map/providers.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const clock = () => "2026-08-29T00:00:00.000Z";
const adapter = new MapPreviewAdapter();
const point = { type: "Point", coordinates: [116.391245, 39.907654] };
const line = { type: "LineString", coordinates: [[116.39, 39.90], [116.40, 39.91]] };
const polygon = { type: "Polygon", coordinates: [[[116.39, 39.90], [116.40, 39.90], [116.40, 39.91], [116.39, 39.90]]] };
const multiPolygon = { type: "MultiPolygon", coordinates: [polygon.coordinates, [[[116.41, 39.90], [116.42, 39.90], [116.42, 39.91], [116.41, 39.90]]]] };

function candidate(geometry = polygon, overrides = {}) {
  return {
    resultId: "p09e-result",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "wgs84_decimal",
    precisionMode: "wgs84-table-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry,
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    requiresReview: false,
    kmlReady: true,
    groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
    warnings: [],
    ...overrides
  };
}

function preview(geometry = polygon, overrides = {}) {
  return adapter.adapt(finalizeCoordinateResult(candidate(geometry, overrides), { clock }), { clock });
}

function provider({ state = PROVIDER_STATE.READY, renderError = null, fitError = null } = {}) {
  return {
    initCalls: 0,
    renderCalls: 0,
    fitCalls: 0,
    destroyed: false,
    status: { state: PROVIDER_STATE.IDLE },
    async init() { this.initCalls += 1; this.status = { state, detail: state }; return this.status; },
    async renderGeometry(plan) {
      this.renderCalls += 1;
      if (renderError) throw Object.assign(new Error(renderError), { code: renderError });
      return {
        sourceResultId: plan.sourceResultId,
        sourceRevision: plan.sourceRevision,
        sourceGeometryHash: plan.sourceGeometryHash,
        provider: "TEST",
        displayCoordinateConversionStatus: "TEST_COPY",
        geometryType: plan.geometryType,
        authorityMutationCount: 0
      };
    },
    async fitGeometry() {
      this.fitCalls += 1;
      if (fitError) throw Object.assign(new Error(fitError), { code: fitError });
    },
    destroy() { this.destroyed = true; },
    getStatus() { return this.status; }
  };
}

function fallback() { return { calls: 0, async render() { this.calls += 1; } }; }
function makeController(providerValue = provider(), fallbackValue = fallback()) {
  return { controller: new MapProductController({ provider: providerValue, fallbackRenderer: fallbackValue, timeoutMs: 100 }), provider: providerValue, fallback: fallbackValue };
}

const tests = [];
function test(id, name, run) { tests.push({ id, name, run }); }

test("P09E-01", "only map_preview_object_v1 is accepted", () => {
  assert.equal(validateMapPreviewObject({ schemaVersion: "finalized_coordinate_result_v1" }).ok, false);
  assert.equal(validateMapPreviewObject(preview()).ok, true);
});
test("P09E-02", "review-pending drawable geometry remains Map eligible", () => {
  const value = preview(polygon, { requiresReview: true, confirmationStatus: "pending" });
  assert.equal(value.previewEligibility.allowed, true);
  assert.ok(value.previewWarnings.includes("REVIEW_REQUIRED"));
});
test("P09E-03", "KML-blocked drawable geometry remains Map eligible", () => {
  const value = preview(polygon, { kmlReady: false });
  assert.equal(value.previewEligibility.allowed, true);
  assert.ok(value.previewWarnings.includes("KML_BLOCKED"));
});
test("P09E-04", "accepted drawable geometry remains Map eligible", () => assert.equal(preview().previewEligibility.allowed, true));
test("P09E-05", "invalid geometry is blocked", () => {
  const value = { ...preview(), geometry: { type: "Polygon", coordinates: [] } };
  assert.equal(validateMapPreviewObject(value).reasonCode, "GEOMETRY_NOT_DRAWABLE");
});

for (const [id, name, geometry, expectedLayers] of [
  ["P09E-06", "Point", point, 1],
  ["P09E-07", "LineString", line, 1],
  ["P09E-08", "Polygon", polygon, 2],
  ["P09E-09", "MultiPolygon", multiPolygon, 2]
]) {
  test(id, `${name} creates one canonical GeoJSON source`, () => {
    const plan = createGeometryRenderPlan(geometry);
    assert.equal(plan.source.type, "geojson");
    assert.equal(plan.layers.length, expectedLayers);
    assert.equal(plan.layers.every(layer => layer.source === plan.sourceId), true);
  });
}

test("P09E-10", "Polygon production style remains frozen", () => {
  const plan = createGeometryRenderPlan(polygon);
  assert.equal(plan.layers[0].paint["fill-color"], "#1976D2");
  assert.equal(plan.layers[0].paint["fill-opacity"], 0.15);
  assert.equal(plan.layers[1].paint["line-color"], "#E53935");
});
test("P09E-11", "configuration blocked fails closed to Local SVG", async () => {
  const item = makeController(provider({ state: PROVIDER_STATE.CONFIGURATION_BLOCKED }));
  const result = await item.controller.open(preview());
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.equal(item.fallback.calls, 1);
});
test("P09E-12", "controller calls only the provider-neutral interface", () => {
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/map-product-controller.js"), "utf8");
  assert.doesNotMatch(source, /AMap\.|MapLibre|tileUrl|convertFrom|MapTiler/);
});
test("P09E-13", "Map failure preserves KML and identity authority", async () => {
  const mapPreview = preview(polygon, { kmlReady: false, requiresReview: true });
  const authority = { kmlReady: false, decisionState: "REVIEW_REQUIRED" };
  const item = makeController(provider({ renderError: "SIMULATED_MAP_FAILURE" }));
  const result = await item.controller.open(mapPreview, { authority });
  assert.equal(result.authorityMutationCount, 0);
  assert.deepEqual(authority, { kmlReady: false, decisionState: "REVIEW_REQUIRED" });
});
test("P09E-14", "fit failure is bounded and falls back", async () => {
  const item = makeController(provider({ fitError: "SIMULATED_FIT_FAILURE" }));
  const result = await item.controller.open(preview());
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.equal(item.fallback.calls, 1);
});
test("P09E-15", "render receipt preserves canonical identity", async () => {
  const item = makeController();
  const value = preview();
  const opened = await item.controller.open(value);
  assert.equal(opened.renderReceipt.sourceGeometryHash, value.sourceGeometryHash);
  assert.equal(opened.renderReceipt.sourceRevision, value.sourceRevision);
});
test("P09E-16", "1000 vertices remain one GeoJSON source", () => {
  const points = Array.from({ length: 1000 }, (_, index) => {
    const angle = (index / 1000) * Math.PI * 2;
    return [110 + Math.cos(angle), 30 + Math.sin(angle)];
  });
  points.push(points[0]);
  const plan = createGeometryRenderPlan({ type: "Polygon", coordinates: [points] });
  assert.equal(plan.source.data.geometry.coordinates[0].length, 1001);
});
test("P09E-17", "production HTML exposes the provider-neutral Spatial hooks", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const providerSource = fs.readFileSync(path.join(root, "assets/spatial-map/amap-provider-adapter.js"), "utf8");
  assert.match(html, /id="spatialProviderCanvas"/);
  assert.match(html, /id="mapPreviewAction"[^>]*class="primary-action map-preview-action"[^>]*>查看地图<\/button>/);
  assert.doesNotMatch(html, /data-spatial-map-style=/);
  assert.match(html, /spatial-map-product\.js/);
  assert.match(providerSource, /new this\.runtime\.TileLayer\.Satellite/);
  assert.match(providerSource, /layers: \[this\.satelliteLayer\]/);
  assert.doesNotMatch(providerSource, /mapStyle:\s*["']amap:\/\/styles\/satellite/);
});
test("P09E-18", "390 mobile direct fullscreen task closure is encoded", () => {
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(css, /@media \(width: 390px\)/);
  assert.match(css, /height: 100dvh/);
  assert.match(css, /touch-action: none/);
  assert.match(css, /env\(safe-area-inset-bottom\)/);
  assert.match(html, /class="spatial-result-card"[\s\S]*id="spatialMapFailure"[\s\S]*id="spatialMapRetryAction"/);
  assert.doesNotMatch(css, /spatial-retry-control/);
});
test("P09E-19", "legacy local SVG renderer remains available", () => {
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/maplibre-renderer.js"), "utf8");
  assert.match(source, /export class LocalSvgRenderer/);
  assert.match(source, /"aria-label": "当前地块地图"/);
  assert.match(source, /x: width \* 0\.16[\s\S]*y: height \* 0\.16/);
  assert.doesNotMatch(source, /本地 SVG 预览|本地 Geometry 预览/);
});
test("P09E-20", "direct task back and fit flow preserve result ownership", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  assert.match(html, /onclick="returnToCoordinate\(\)"/);
  assert.match(html, /id="spatialFitGeometryAction"/);
  assert.doesNotMatch(source, /requestFullscreen|exitFullscreen|fullscreenchange/);
  assert.doesNotMatch(source, /activeFinalizedCoordinateResult\s*=/);
});
test("P09E-21", "runtime config route exposes only public readiness", () => {
  const source = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const route = source.match(/app\.get\("\/api\/map-runtime-config"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.match(route, /amapWebJsKey/);
  assert.match(route, /amapSecurityProxyReady/);
  assert.match(route, /Cache-Control", "no-store/);
  assert.doesNotMatch(route, /securityJsCode\s*:/);
});
test("P09E-22", "general Map gate never uses export-grade authority", () => {
  const files = ["geometry-render-plan.js", "map-product-controller.js", "spatial-map-product.js"]
    .map(file => fs.readFileSync(path.join(root, "assets/spatial-map", file), "utf8")).join("\n");
  assert.match(files, new RegExp(MAP_PREVIEW_GATE));
  assert.doesNotMatch(files, /FinalizedResultSpatialGeometryAdapter|AUTO_EXPORT/);
});
test("P09E-23", "production package remains unchanged and MapLibre GL is absent", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  assert.equal(Object.hasOwn(pkg.dependencies, "maplibre-gl"), false);
  assert.equal(pkg.scripts.start, "node server.js");
  assert.equal(Object.hasOwn(pkg.scripts, "p09e-regression"), false);
});
test("P09E-24", "Spatial runtime imports no recognition, KML or V3 implementation", () => {
  const files = fs.readdirSync(path.join(root, "assets/spatial-map"))
    .filter(file => file.endsWith(".js"))
    .map(file => fs.readFileSync(path.join(root, "assets/spatial-map", file), "utf8")).join("\n");
  assert.doesNotMatch(files, /coordinate-engine-v3|recognize-coordinates|downloadKml|kmlReady\s*=|technicalKmlReady\s*=/);
});

test("P09E-25", "Generate Map activates the task view before provider initialization", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const flow = html.match(/async function openSpatialResult\(\)[\s\S]*?function returnToCoordinate/)?.[0] || "";
  assert.ok(flow.indexOf('showPage("spatialResult")') < flow.indexOf("GeoKitSatelliteMap?.open(payload)"));
  assert.ok(flow.indexOf("waitForSpatialLayout") < flow.indexOf("GeoKitSatelliteMap?.open(payload)"));
});

test("P09E-26", "mobile bottom sheet and desktop result card share one bounded result model", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(html, /id="spatialResultCard"/);
  assert.match(html, /id="spatialResultSheetToggle"[^>]*aria-expanded="false"[^>]*aria-controls="spatialResultDetails"/);
  assert.match(css, /width: min\(360px/);
  assert.match(css, /@media \(max-width: 640px\)[\s\S]*bottom: max\(8px, env\(safe-area-inset-bottom\)\)/);
});

test("P09E-27", "KML remains inside details and bound only to server eligibility", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /id="spatialResultDetails"[\s\S]*id="spatialKmlAction"/);
  assert.match(html, /spatialKmlAction\.disabled = payload\?\.kmlEligibility\?\.allowed !== true/);
});

test("P09E-28", "route fail-closed cached reopen and provider teardown are encoded", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  assert.match(html, /route\.pageName === "spatialResult" && !activeMapPreviewResponse/);
  assert.match(html, /activeMapPreviewCacheKey === cacheKey \? activeMapPreviewResponse : null/);
  assert.match(html, /GeoKitSatelliteMap\?\.destroy/);
  assert.match(source, /function destroy\(\)[\s\S]*controller\?\.destroy/);
});

test("P09E-29", "direct HTTP map route redirects without creating map authority", () => {
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const route = server.match(/app\.get\("\/coordinate\/map"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.match(route, /res\.redirect\(302, "\/coordinate"\)/);
  assert.match(route, /Cache-Control", "no-store/);
  assert.doesNotMatch(route, /mapPreview|geometry|resultId|geometryHash|review|confirmation|kml/i);
  const renderRoutes = server.match(/app\.get\(\["\/"[\s\S]*?renderIndexWithMeta\);/)?.[0] || "";
  assert.doesNotMatch(renderRoutes, /\/coordinate\/map/);
});

test("P09E-30", "Coordinate Result presents Map as primary and KML as secondary", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /class="coordinate-result-actions"[\s\S]*id="mapPreviewAction"[^>]*class="primary-action map-preview-action"[\s\S]*class="secondary kml-download-action coordinate-kml-action"/);
  assert.match(html, />查看地图<\/button>[\s\S]*>下载 KML<\/button>/);
});

test("P09E-31", "recognition summary contract remains computed but is hidden from the product surface", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /id="recognitionSummary"[^>]*hidden/);
  assert.match(html, /function setRecognitionSummary[\s\S]*区域概览[\s\S]*recognitionSummary\.hidden = true/);
});

test("P09E-32", "fallback presentation contains one approved failure message and no technical copy", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/maplibre-renderer.js"), "utf8");
  assert.equal((html.match(/卫星地图暂时不可用/g) || []).length, 1);
  assert.doesNotMatch(html + source, /本地 SVG 预览|未请求底图服务|本地 Geometry 预览|Geometry 仅用于显示/);
});

test("P09E-33", "mobile result sheet remains within 40dvh and internally scrollable", () => {
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(css, /@media \(max-width: 640px\)[\s\S]*max-height: min\(40dvh, 420px\)/);
  assert.match(css, /\.spatial-result-details\s*\{[\s\S]*max-height: calc\(40dvh - 50px\)[\s\S]*overflow-y: auto/);
});

test("P09E-34", "sheet transitions recalculate the usable map viewport and refit geometry", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(html, /async function syncSpatialMapViewport[\s\S]*--spatial-sheet-occlusion[\s\S]*GeoKitSatelliteMap\?\.fitGeometry/);
  assert.match(html, /function setSpatialSheetExpanded[\s\S]*syncSpatialMapViewport/);
  assert.match(css, /--spatial-sheet-occlusion: 0px/);
  assert.match(css, /bottom: var\(--spatial-sheet-occlusion\)/);
});

test("P09E-35", "mobile collapsed and expanded states reuse one tappable provider-failure node", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.equal((html.match(/卫星地图暂时不可用/g) || []).length, 1);
  assert.match(source, /function placeProviderFailure[\s\S]*elements\.card\.insertBefore\(elements\.failure, elements\.details\)/);
  assert.match(source, /elements\.details\.insertBefore\(elements\.failure, warning\)/);
  assert.match(source, /dataset\.providerUnavailable = String\(unavailable\)/);
  assert.match(html, /id="spatialMapRetryAction"[^>]*>重试<\/button>/);
  assert.match(css, /\.spatial-result-card > \.spatial-map-failure[\s\S]*font-size: 12px/);
});

assert.equal(tests.length, 35);
let passed = 0;
for (const entry of tests) {
  try {
    await entry.run();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}

console.log(`P09E global map integration regression: ${passed}/${tests.length} PASS`);
console.log("CANONICAL_CONTRACT_ONLY=true");
console.log(`GENERAL_MAP_GATE=${MAP_PREVIEW_GATE}`);
console.log("AUTHORITY_MUTATION_COUNT=0");
console.log("PRODUCTION_PROVIDER_CALLS=0");
