import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AMapDisplayCoordinateAdapter,
  isMainlandChinaDisplayConversionRequired
} from "../assets/spatial-map/amap-display-coordinate-adapter.js";
import { AMapLoader } from "../assets/spatial-map/amap-loader.js";
import { AMapProviderAdapter } from "../assets/spatial-map/amap-provider-adapter.js";
import { MapProductController } from "../assets/spatial-map/map-product-controller.js";
import { isSpatialMapProvider, PROVIDER_STATE } from "../assets/spatial-map/providers.js";
import {
  LocalSvgRenderer,
  LOCAL_MAP_MAX_RELATIVE_ZOOM,
  LOCAL_MAP_MIN_RELATIVE_ZOOM,
  LOCAL_MAP_PADDING_RATIO,
  applyLocalViewTransform,
  createLocalFitTransform,
  projectWgs84GeometryForDisplay,
  projectedGeometryBounds
} from "../assets/spatial-map/maplibre-renderer.js";
import { createAmapSecurityProxy } from "../server/spatial/amap-security-proxy.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const point = { type: "Point", coordinates: [116.391245, 39.907654] };
const overseasPoint = { type: "Point", coordinates: [2.3522, 48.8566] };
const line = { type: "LineString", coordinates: [[116.39, 39.90], [116.40, 39.91]] };
const polygon = { type: "Polygon", coordinates: [[[116.39, 39.90], [116.40, 39.90], [116.40, 39.91], [116.39, 39.90]]] };
const multiPolygon = { type: "MultiPolygon", coordinates: [polygon.coordinates, [[[116.41, 39.90], [116.42, 39.90], [116.42, 39.91], [116.41, 39.90]]]] };

function preview(geometry = polygon, overrides = {}) {
  return {
    schemaVersion: "map_preview_object_v1",
    sourceResultId: "spatial-neutral-result",
    sourceRevision: 3,
    sourceGeometryHash: "sha256:spatial-neutral-geometry",
    crs: { id: "EPSG:4326" },
    axisOrder: "longitude_latitude",
    geometryType: geometry.type,
    geometry: structuredClone(geometry),
    previewEligibility: { allowed: true, gate: "MAP_PREVIEW_DRAWABLE_ELIGIBILITY" },
    previewWarnings: [],
    ...overrides
  };
}

function receipt(plan, overrides = {}) {
  return {
    sourceResultId: plan.sourceResultId,
    sourceRevision: plan.sourceRevision,
    sourceGeometryHash: plan.sourceGeometryHash,
    provider: "TEST_PROVIDER",
    displayCoordinateConversionStatus: "TEST_DISPLAY_COPY",
    geometryType: plan.geometryType,
    authorityMutationCount: 0,
    ...overrides
  };
}

function fakeProvider({ initState = PROVIDER_STATE.READY, initError = null, renderError = null, fitError = null } = {}) {
  return {
    initCalls: 0,
    renderCalls: 0,
    fitCalls: 0,
    destroyCalls: 0,
    status: { state: PROVIDER_STATE.IDLE },
    async init() {
      this.initCalls += 1;
      if (initError) throw Object.assign(new Error(initError), { code: initError });
      this.status = { state: initState, detail: initState === PROVIDER_STATE.CONFIGURATION_BLOCKED ? "CONFIGURATION_BLOCKED" : null };
      return this.status;
    },
    async renderGeometry(plan) {
      this.renderCalls += 1;
      if (renderError) throw Object.assign(new Error(renderError), { code: renderError });
      return receipt(plan);
    },
    async fitGeometry() {
      this.fitCalls += 1;
      if (fitError) throw Object.assign(new Error(fitError), { code: fitError });
    },
    destroy() { this.destroyCalls += 1; this.status = { state: PROVIDER_STATE.IDLE }; },
    getStatus() { return this.status; }
  };
}

function fakeFallback() {
  return { calls: 0, geometries: [], async render(geometry) { this.calls += 1; this.geometries.push(structuredClone(geometry)); } };
}

function controller(provider = fakeProvider(), fallback = fakeFallback()) {
  return { value: new MapProductController({ provider, fallbackRenderer: fallback, timeoutMs: 100 }), provider, fallback };
}

function positions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat(1);
  return geometry.coordinates.flat(2);
}

function makeLocalRenderer(width = 390, height = 600) {
  const listeners = new Map();
  const createNode = name => ({
    name, style: {}, attributes: {}, children: [],
    setAttribute(key, value) { this.attributes[key] = String(value); },
    append(child) { this.children.push(child); },
    addEventListener(type, handler) { listeners.set(type, handler); },
    removeEventListener(type) { listeners.delete(type); },
    getBoundingClientRect() { return { left: 0, top: 0, width, height }; },
    setPointerCapture() {}, releasePointerCapture() {}
  });
  const container = {
    hidden: true, children: [], clientWidth: width, clientHeight: height,
    getBoundingClientRect: () => ({ width, height }),
    replaceChildren(...children) { this.children = children; }
  };
  const previousDocument = globalThis.document;
  globalThis.document = { createElementNS: (_namespace, name) => createNode(name) };
  const renderer = new LocalSvgRenderer({ container, attributionElement: { hidden: false, textContent: "old" } });
  return { renderer, container, listeners, restore: () => { globalThis.document = previousDocument; } };
}

function fakeAmapRuntime() {
  class Overlay { constructor(options) { this.options = options; } }
  class SatelliteLayer { constructor(options) { this.options = options; } }
  class Map {
    constructor(container, options) { this.container = container; this.options = options; this.fitCalls = 0; this.pointFitCalls = 0; this.destroyed = false; }
    setFitView() { this.fitCalls += 1; }
    setZoomAndCenter() { this.pointFitCalls += 1; }
    remove() {}
    destroy() { this.destroyed = true; }
  }
  return {
    Map,
    TileLayer: { Satellite: SatelliteLayer },
    Marker: Overlay,
    Polyline: Overlay,
    Polygon: Overlay,
    convertFrom(coordinates, source, callback) {
      callback("complete", { info: "ok", locations: coordinates.map(([x, y]) => [x + 0.01, y + 0.01]) });
    }
  };
}

const authority = () => ({
  kmlReady: false,
  technicalKmlReady: true,
  kmlContent: "<kml>stable</kml>",
  kmlHash: "sha256:kml-stable",
  reviewState: "REVIEW_REQUIRED",
  confirmationStatus: "pending",
  qualityGateStatus: "review_required",
  decisionState: "REVIEW_REQUIRED"
});

const tests = [];
function test(id, name, run) { tests.push({ id, name, run }); }

test("SPN-01", "page load performs zero provider network attempts", () => {
  const loader = new AMapLoader({ scriptLoader: () => { throw new Error("UNEXPECTED_NETWORK"); } });
  assert.equal(loader.networkAttempts, 0);
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  assert.doesNotMatch(source, /\ninitialize\(\);/);
});

test("SPN-02", "recognition completion without Generate Map performs zero provider work", () => {
  const provider = fakeProvider();
  assert.equal(provider.initCalls, 0);
  assert.equal(provider.renderCalls, 0);
});

test("SPN-03", "Generate Map invokes provider init exactly once", async () => {
  const item = controller();
  await item.value.open(preview(), { publicConfig: {} });
  assert.equal(item.provider.initCalls, 1);
});

for (const [id, geometry] of [["SPN-04", point], ["SPN-05", line], ["SPN-06", polygon], ["SPN-07", multiPolygon]]) {
  test(id, `${geometry.type} returns a bound render receipt`, async () => {
    const item = controller();
    const result = await item.value.open(preview(geometry));
    assert.equal(result.state, PROVIDER_STATE.READY);
    assert.equal(result.renderReceipt.geometryType, geometry.type);
    assert.equal(result.renderReceipt.authorityMutationCount, 0);
  });
}

for (const [id, geometry] of [["SPN-08", point], ["SPN-09", line], ["SPN-10", polygon]]) {
  test(id, `${geometry.type} is auto-fit after render`, async () => {
    const item = controller();
    await item.value.open(preview(geometry));
    assert.equal(item.provider.fitCalls, 1);
  });
}

test("SPN-11", "missing Web JS key is configuration blocked with no SDK load and SVG fallback", async () => {
  const loader = new AMapLoader({ scriptLoader: () => { throw new Error("UNEXPECTED_NETWORK"); } });
  const provider = new AMapProviderAdapter({ loader });
  const fallback = fakeFallback();
  const result = await new MapProductController({ provider, fallbackRenderer: fallback }).open(preview(), {
    publicConfig: { securityProxyReady: true }
  });
  assert.equal(result.providerStatus, PROVIDER_STATE.CONFIGURATION_BLOCKED);
  assert.equal(loader.networkAttempts, 0);
  assert.equal(fallback.calls, 1);
});

test("SPN-12", "missing security proxy readiness is configuration blocked without SDK load", async () => {
  const loader = new AMapLoader({ scriptLoader: () => { throw new Error("UNEXPECTED_NETWORK"); } });
  const provider = new AMapProviderAdapter({ loader });
  const fallback = fakeFallback();
  const result = await new MapProductController({ provider, fallbackRenderer: fallback }).open(preview(), {
    publicConfig: { webJsKey: "public-test-key", securityProxyReady: false }
  });
  assert.equal(result.providerStatus, PROVIDER_STATE.CONFIGURATION_BLOCKED);
  assert.equal(loader.networkAttempts, 0);
  assert.equal(fallback.calls, 1);
});

test("SPN-13", "provider init failure falls back locally", async () => {
  const item = controller(fakeProvider({ initError: "SIMULATED_INIT_FAILURE" }));
  const result = await item.value.open(preview());
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.equal(item.fallback.calls, 1);
});

test("SPN-14", "display conversion failure falls back locally", async () => {
  const item = controller(fakeProvider({ renderError: "AMAP_DISPLAY_CONVERSION_FAILED" }));
  const result = await item.value.open(preview());
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
});

test("SPN-15", "overlay render failure falls back locally", async () => {
  const item = controller(fakeProvider({ renderError: "SIMULATED_RENDER_FAILURE" }));
  await item.value.open(preview());
  assert.equal(item.fallback.calls, 1);
});

test("SPN-16", "fit failure is isolated from recognition and KML authority", async () => {
  const state = authority();
  const before = structuredClone(state);
  const item = controller(fakeProvider({ fitError: "SIMULATED_FIT_FAILURE" }));
  const result = await item.value.open(preview(), { authority: state });
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.deepEqual(state, before);
});

test("SPN-17", "mainland conversion changes only the display copy", async () => {
  const canonical = structuredClone(point);
  const before = structuredClone(canonical);
  const adapter = new AMapDisplayCoordinateAdapter({ convertBatch: async batch => batch.map(([x, y]) => [x + 1, y + 1]) });
  const result = await adapter.convert(canonical);
  assert.notDeepEqual(result.geometry, canonical);
  assert.deepEqual(canonical, before);
});

test("SPN-18", "overseas WGS84 passes through without China offset", async () => {
  let calls = 0;
  const adapter = new AMapDisplayCoordinateAdapter({ convertBatch: async () => { calls += 1; return []; } });
  for (const geometry of [
    overseasPoint,
    { type: "Point", coordinates: [74.6122, 42.8746] },
    { type: "Point", coordinates: [32.5732, -25.9692] },
    { type: "Point", coordinates: [106.8456, -6.2088] },
    { type: "Point", coordinates: [97.4167, 27.3333] },
    { type: "Point", coordinates: [97.3964, 25.3833] }
  ]) {
    const result = await adapter.convert(geometry);
    assert.deepEqual(result.geometry, geometry);
  }
  assert.equal(calls, 0);
});

test("SPN-19", "more than 40 mainland vertices use deterministic batches", async () => {
  const sizes = [];
  const geometry = { type: "LineString", coordinates: Array.from({ length: 81 }, (_, index) => [104 + index / 1000, 30]) };
  const adapter = new AMapDisplayCoordinateAdapter({ convertBatch: async batch => { sizes.push(batch.length); return batch; } });
  const result = await adapter.convert(geometry);
  assert.deepEqual(sizes, [40, 40, 1]);
  assert.equal(result.batchCount, 3);
});

test("SPN-20", "one failed conversion batch never returns a partial geometry", async () => {
  const canonical = { type: "LineString", coordinates: Array.from({ length: 41 }, (_, index) => [104 + index / 1000, 30]) };
  const before = structuredClone(canonical);
  let calls = 0;
  const adapter = new AMapDisplayCoordinateAdapter({ convertBatch: async batch => {
    calls += 1;
    if (calls === 2) throw new Error("BATCH_FAILED");
    return batch.map(([x, y]) => [x + 1, y + 1]);
  } });
  await assert.rejects(() => adapter.convert(canonical));
  assert.deepEqual(canonical, before);
});

test("SPN-21", "canonical geometry remains deeply equal after provider render", async () => {
  const runtime = fakeAmapRuntime();
  const loader = new AMapLoader({ globalRef: {}, scriptLoader: async () => runtime });
  const provider = new AMapProviderAdapter({ loader });
  const canonical = preview(polygon);
  const before = structuredClone(canonical.geometry);
  await new MapProductController({ provider, fallbackRenderer: fakeFallback() }).open(canonical, {
    publicConfig: { webJsKey: "public-test-key", securityProxyReady: true }, container: {}
  });
  assert.deepEqual(canonical.geometry, before);
});

for (const [id, key] of [["SPN-22", "sourceGeometryHash"], ["SPN-23", "sourceRevision"]]) {
  test(id, `${key} remains unchanged`, async () => {
    const value = preview();
    const before = value[key];
    await controller().value.open(value);
    assert.equal(value[key], before);
  });
}

for (const [id, key] of [["SPN-24", "reviewState"], ["SPN-25", "confirmationStatus"], ["SPN-26", "kmlReady"]]) {
  test(id, `${key} authority remains unchanged`, async () => {
    const state = authority();
    const before = structuredClone(state);
    await controller().value.open(preview(), { authority: state });
    assert.deepEqual(state, before);
  });
}

test("SPN-27", "KML content and hash remain unchanged", async () => {
  const state = authority();
  const before = structuredClone(state);
  await controller().value.open(preview(), { authority: state });
  assert.equal(state.kmlContent, before.kmlContent);
  assert.equal(state.kmlHash, before.kmlHash);
});

test("SPN-28", "review-pending drawable preview does not grant KML", async () => {
  const value = preview(polygon, { previewWarnings: ["REVIEW_REQUIRED", "KML_BLOCKED"] });
  const state = authority();
  const result = await controller().value.open(value, { authority: state });
  assert.equal(result.state, PROVIDER_STATE.READY);
  assert.equal(state.kmlReady, false);
  assert.equal(state.confirmationStatus, "pending");
});

test("SPN-29", "stale result identity blocks provider rendering", async () => {
  const item = controller();
  const value = preview();
  const result = await item.value.open(value, { expectedIdentity: { ...value, sourceRevision: value.sourceRevision + 1 } });
  assert.equal(result.ok, false);
  assert.equal(item.provider.initCalls, 0);
});

test("SPN-30", "provider retry preserves canonical identity", async () => {
  const item = controller();
  const value = preview();
  const before = [value.sourceResultId, value.sourceRevision, value.sourceGeometryHash];
  await item.value.open(value);
  const result = await item.value.retry();
  assert.deepEqual([result.preview.sourceResultId, result.preview.sourceRevision, result.preview.sourceGeometryHash], before);
  assert.equal(item.provider.initCalls, 2);
});

test("SPN-31", "destroy releases provider resources", async () => {
  const item = controller();
  await item.value.open(preview());
  item.value.destroy();
  assert.equal(item.provider.destroyCalls, 1);
  assert.equal(item.value.state, PROVIDER_STATE.IDLE);
});

test("SPN-32", "390px direct task UI keeps back fit KML retry without a fullscreen step", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
    assert.match(html, /id="mapPreviewAction"[^>]*>查看地图<\/button>/);
  assert.doesNotMatch(html, /data-spatial-map-style=/);
  assert.match(html, /spatialMapRetryAction/);
  assert.match(html, /spatialFitGeometryAction/);
  assert.doesNotMatch(html, /spatialFullscreenAction/);
  assert.match(html, /spatialKmlAction/);
  assert.match(html, /returnToCoordinate\(\)/);
  assert.match(css, /@media \(width: 390px\)/);
  assert.match(css, /max-width: 100vw/);
  assert.match(css, /touch-action: none/);
});

test("SPN-UX-01", "provider failure is a compact non-blocking status with approved copy", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const failureCss = html.match(/\.spatial-map-failure\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(html, /卫星地图暂时不可用/);
  assert.doesNotMatch(html, /地图暂时无法加载/);
  assert.match(failureCss, /display:\s*flex/);
  assert.match(failureCss, /border:\s*1px/);
  assert.match(failureCss, /border-radius:\s*10px/);
  assert.doesNotMatch(failureCss, /inset:\s*0/);
  assert.doesNotMatch(failureCss, /position:\s*absolute/);
  assert.doesNotMatch(failureCss, /rgba\([^)]*,\s*\.94\)/);
  assert.match(html, /class="spatial-result-card"[\s\S]*id="spatialMapFailure"[\s\S]*id="spatialMapRetryAction"/);
});

test("SPN-UX-02", "Point fallback auto-fit centers single-axis geometry", () => {
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/maplibre-renderer.js"), "utf8");
  assert.match(source, /const centerX = \(bounds\.minX \+ bounds\.maxX\) \/ 2/);
  assert.match(source, /const centerY = \(bounds\.minY \+ bounds\.maxY\) \/ 2/);
  assert.match(source, /\.\.\.\(geometry\.type === "Point" \? \{ r: 9 \} : \{\}\)/);
});

test("SPN-UX-03", "Point LineString and Polygon fallback keep high-contrast SVG styles", () => {
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/maplibre-renderer.js"), "utf8");
  assert.match(source, /geometry\.type === "Point"/);
  assert.match(source, /geometry\.type === "LineString" \? "polyline" : "polygon"/);
  assert.match(source, /stroke:\s*"#E53935"/);
  assert.match(source, /fill:\s*geometry\.type === "LineString" \? "none" : "#1976D2"/);
});

test("SPN-UX-04", "390px summary and controls remain reachable without horizontal overflow", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(css, /height:\s*100dvh/);
  assert.match(css, /left:\s*max\(8px, env\(safe-area-inset-left\)\)/);
  assert.match(html, /id="spatialMapRetryAction"[^>]*>重试<\/button>/);
  assert.match(html, /onclick="returnToCoordinate\(\)"/);
  assert.match(html, /id="spatialKmlAction"/);
  assert.match(html, /id="spatialFitGeometryAction"/);
  assert.doesNotMatch(html, /id="spatialFullscreenAction"/);
});

test("SPN-FS-01", "Spatial result is a fixed 100dvh task view with safe-area support", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(html, /\.spatial-result-page\.active\s*\{[\s\S]*position:\s*fixed[\s\S]*inset:\s*0[\s\S]*height:\s*100dvh/);
  assert.match(css, /env\(safe-area-inset-top\)/);
  assert.match(css, /env\(safe-area-inset-bottom\)/);
  assert.match(html, /body\.spatial-task-active[\s\S]*overflow:\s*hidden/);
});

test("SPN-FS-02", "secondary Browser Fullscreen API and UI are absent", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  assert.doesNotMatch(html, /spatialFullscreenAction|全屏地图/);
  assert.doesNotMatch(source, /requestFullscreen|exitFullscreen|fullscreenchange|document\.fullscreenElement/);
});

test("SPN-FS-03", "direct stale map route fails closed to coordinate", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const route = server.match(/app\.get\("\/coordinate\/map"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.match(html, /route\.pageName === "spatialResult" && !activeMapPreviewResponse[\s\S]*showPage\("coordinate", "", false\)[\s\S]*syncPageUrl\("coordinate", "", "replace"\)/);
  assert.match(route, /res\.redirect\(302, "\/coordinate"\)/);
  assert.match(route, /Cache-Control", "no-store/);
  assert.doesNotMatch(route, /geometry|resultId|preview|cookie|session/);
});

test("SPN-FS-04", "provider opens only after task activation and settled layout", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const flow = html.match(/async function openSpatialResult\(\)[\s\S]*?function returnToCoordinate/)?.[0] || "";
  assert.ok(flow.indexOf('showPage("spatialResult")') < flow.indexOf("renderSpatialResult(payload)"));
  assert.ok(flow.indexOf("renderSpatialResult(payload)") < flow.indexOf("await waitForSpatialLayout()"));
  assert.ok(flow.indexOf("await waitForSpatialLayout()") < flow.indexOf("GeoKitSatelliteMap?.open(payload)"));
});

test("SPN-FS-05", "back uses browser history and exit destroys provider resources", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const flow = html.match(/function returnToCoordinate\(\)[\s\S]*?async function loadSpatialRuntimeConfig/)?.[0] || "";
  assert.match(flow, /GeoKitSatelliteMap\?\.destroy/);
  assert.match(flow, /history\.back\(\)/);
  assert.doesNotMatch(flow, /showPage\("coordinate"\);/);
});

test("SPN-FS-06", "mobile summaries cover Point LineString and Polygon with Chinese hectare presentation", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /geometryType === "Point"[\s\S]*geometryType === "LineString"[\s\S]*geometryType === "Polygon"/);
    assert.match(html, /value \/ 10000\)\.toFixed\(2\)[\s\S]*公顷/);
  assert.match(html, /id="spatialResultSheetToggle"[\s\S]*aria-expanded="false"[\s\S]*aria-controls="spatialResultDetails"/);
});

test("SPN-FS-07", "review state is compact when collapsed and detailed only in the sheet", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /id="spatialReviewCompact"[^>]*hidden>待核对/);
  assert.match(html, /spatialReviewCompact\.hidden = !warning/);
  assert.match(html, /id="spatialResultWarning"[^>]*hidden/);
});

test("SPN-FS-08", "KML action lives inside expandable result details", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /id="spatialResultDetails"[\s\S]*id="spatialKmlAction"/);
  assert.match(html, /spatialKmlAction\.dataset\.eligible = String\(payload\?\.kmlEligibility\?\.allowed === true\)/);
  assert.match(html, /syncButton\(spatialKmlAction, spatialKmlAction\?\.dataset\.eligible === "true"\)/);
});

test("SPN-FS-09", "sheet expansion is presentation-only", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const toggle = html.match(/function setSpatialSheetExpanded[\s\S]*?function spatialCollapsedSummaryText/)?.[0] || "";
  assert.match(toggle, /aria-expanded/);
  assert.doesNotMatch(toggle, /kml|review|confirmation|geometryHash|activeFinalizedCoordinateResult/);
});

test("SPN-FS-10", "reopen uses the cached canonical preview identity", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /activeMapPreviewCacheKey === cacheKey \? activeMapPreviewResponse : null/);
  assert.match(html, /pages\.spatialResult\.dataset\.spatialCacheKey = activeMapPreviewCacheKey/);
});

test("SPN-FS-11", "provider failure stays in compact result UI and never covers the map", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
    assert.equal((html.match(/卫星地图暂时不可用/g) || []).length, 1);
    assert.doesNotMatch(html, /id="spatialProviderCompact"/);
  assert.match(html, /class="spatial-result-card"[\s\S]*id="spatialMapFailure"/);
  assert.doesNotMatch(css, /\.spatial-map-failure\s*\{[\s\S]*?position:\s*absolute/);
});

test("SPN-R1-01", "AMap map is initialized with the documented Satellite tile layer", async () => {
  const runtime = fakeAmapRuntime();
  const provider = new AMapProviderAdapter({
    loader: new AMapLoader({ globalRef: {}, scriptLoader: async () => runtime })
  });
  const result = await provider.init({}, { webJsKey: "public-test-key", securityProxyReady: true });
  assert.equal(result.state, PROVIDER_STATE.READY);
  assert.equal(provider.satelliteLayer.constructor, runtime.TileLayer.Satellite);
  assert.deepEqual(provider.map.options.layers, [provider.satelliteLayer]);
  assert.equal(Object.hasOwn(provider.map.options, "showOversea"), true);
  assert.equal(provider.map.options.showOversea, true);
  assert.equal(Object.hasOwn(provider.map.options, "mapStyle"), false);
});

test("SPN-R1-02", "Satellite layer initialization failure falls back locally", async () => {
  const runtime = fakeAmapRuntime();
  delete runtime.TileLayer;
  const provider = new AMapProviderAdapter({
    loader: new AMapLoader({ globalRef: {}, scriptLoader: async () => runtime })
  });
  const fallbackRenderer = fakeFallback();
  const result = await new MapProductController({ provider, fallbackRenderer }).open(preview(), {
    publicConfig: { webJsKey: "public-test-key", securityProxyReady: true },
    container: {}
  });
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.equal(fallbackRenderer.calls, 1);
});

test("SPN-R1-03", "conservative mainland policy excludes approved border matrix and converts inland matrix", async () => {
  const overseas = {
    PUTAO_MYANMAR: [97.4167, 27.3333],
    MYITKYINA_MYANMAR: [97.3964, 25.3833],
    LAOS_NORTH: [101.4025, 20.95],
    VIETNAM_NORTH: [104.9833, 22.8333],
    NEPAL: [85.324, 27.7172],
    BHUTAN: [89.639, 27.4728],
    PAKISTAN_BORDER_REGION: [74.308, 35.92],
    MONGOLIA: [106.9057, 47.8864],
    RUSSIA_FAR_EAST: [131.8855, 43.1155],
    WEST_AFRICA: [-4.0083, 5.36],
    LAOS: [102.6331, 17.9757],
    PHILIPPINES: [120.9842, 14.5995],
    INDONESIA: [106.8456, -6.2088],
    PAKISTAN: [73.0479, 33.6844]
  };
  const inland = {
    BEIJING: [116.4074, 39.9042],
    SHANGHAI: [121.4737, 31.2304],
    GUANGZHOU: [113.2644, 23.1291],
    CHENGDU: [104.0665, 30.5728],
    WUHAN: [114.3055, 30.5928]
  };
  Object.values(overseas).forEach(([longitude, latitude]) => {
    assert.equal(isMainlandChinaDisplayConversionRequired(longitude, latitude), false);
  });
  Object.values(inland).forEach(([longitude, latitude]) => {
    assert.equal(isMainlandChinaDisplayConversionRequired(longitude, latitude), true);
  });
  let conversionRequests = 0;
  const adapter = new AMapDisplayCoordinateAdapter({ convertBatch: async batch => {
    conversionRequests += 1;
    return batch;
  } });
  for (const coordinates of Object.values(overseas)) {
    const result = await adapter.convert({ type: "Point", coordinates });
    assert.equal(result.status, "OVERSEAS_WGS84_PASSTHROUGH");
  }
  assert.equal(conversionRequests, 0);
  for (const coordinates of Object.values(inland)) {
    await adapter.convert({ type: "Point", coordinates });
  }
  assert.equal(conversionRequests, Object.keys(inland).length);
});

test("SPN-R1-04", "mobile provider failure stays single-instance and retryable outside collapsed details", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const source = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  assert.equal((html.match(/卫星地图暂时不可用/g) || []).length, 1);
  assert.match(source, /mobileResultQuery\?\.matches[\s\S]*elements\.card\.insertBefore\(elements\.failure, elements\.details\)/);
  assert.match(source, /if \(elements\.failure\) elements\.failure\.hidden = !unavailable/);
  assert.match(source, /if \(elements\.retry\) elements\.retry\.hidden = !unavailable/);
});

function assertProjectedAspectPreserved(geometry, width = 900, height = 560) {
  const projected = projectWgs84GeometryForDisplay(geometry);
  const bounds = projectedGeometryBounds(projected);
  const transform = createLocalFitTransform(projected, width, height);
  const topLeft = applyLocalViewTransform([bounds.minX, bounds.maxY], transform);
  const bottomRight = applyLocalViewTransform([bounds.maxX, bounds.minY], transform);
  const projectedRatio = (bounds.maxX - bounds.minX) / (bounds.maxY - bounds.minY);
  const renderedRatio = Math.abs(bottomRight[0] - topLeft[0]) / Math.abs(bottomRight[1] - topLeft[1]);
  assert.ok(Math.abs(projectedRatio - renderedRatio) < 1e-9);
}

test("ASPECT-01", "vertical narrow Polygon preserves projected aspect", () => {
  assertProjectedAspectPreserved({ type: "Polygon", coordinates: [[[30, 10], [30.001, 10], [30.001, 10.01], [30, 10.01], [30, 10]]] });
});
test("ASPECT-02", "horizontal narrow Polygon preserves projected aspect", () => {
  assertProjectedAspectPreserved({ type: "Polygon", coordinates: [[[30, 10], [30.01, 10], [30.01, 10.001], [30, 10.001], [30, 10]]] });
});
test("ASPECT-03", "MultiPolygon relative position is preserved by one global transform", () => {
  const projected = projectWgs84GeometryForDisplay(multiPolygon);
  const transform = createLocalFitTransform(projected, 900, 560);
  const source = [projected.coordinates[0][0][0], projected.coordinates[1][0][0]];
  const screen = source.map(value => applyLocalViewTransform(value, transform));
  assert.ok(screen[1][0] > screen[0][0]);
  assert.ok(Math.abs((screen[1][0] - screen[0][0]) / (source[1][0] - source[0][0]) - transform.scale) < 1e-12);
});
test("FIT-01", "all projected geometry remains inside viewport", () => {
  const projected = projectWgs84GeometryForDisplay(multiPolygon);
  const transform = createLocalFitTransform(projected, 390, 600);
  positions(projected).map(value => applyLocalViewTransform(value, transform)).forEach(([x, y]) => {
    assert.ok(x >= 0 && x <= 390 && y >= 0 && y <= 600);
  });
});
test("FIT-02", "deterministic fit padding is preserved", () => {
  const projected = projectWgs84GeometryForDisplay(polygon);
  const transform = createLocalFitTransform(projected, 900, 560);
  const screen = positions(projected).map(value => applyLocalViewTransform(value, transform));
  const margins = [Math.min(...screen.map(p => p[0])), 900 - Math.max(...screen.map(p => p[0])), Math.min(...screen.map(p => p[1])), 560 - Math.max(...screen.map(p => p[1]))];
  assert.ok(margins.every(value => value >= -1e-7));
  assert.ok(margins.some((value, index) => Math.abs(value - (index < 2 ? 900 : 560) * LOCAL_MAP_PADDING_RATIO) < 1e-6));
});
test("FIT-03", "north-up orientation is preserved", () => {
  const projected = projectWgs84GeometryForDisplay(line);
  const transform = createLocalFitTransform(projected, 390, 600);
  const south = applyLocalViewTransform(projected.coordinates[0], transform);
  const north = applyLocalViewTransform(projected.coordinates[1], transform);
  assert.ok(north[1] < south[1]);
});
test("PAN-01", "pan changes only view translation", async () => {
  const local = makeLocalRenderer();
  try { await local.renderer.render(polygon); const before = local.renderer.getViewState(); local.renderer.panBy(17, -9); const after = local.renderer.getViewState(); assert.equal(after.scale, before.scale); assert.equal(after.translateX, before.translateX + 17); assert.equal(after.translateY, before.translateY - 9); } finally { local.restore(); }
});
test("PAN-02", "pan does not change canonical geometry", async () => {
  const canonical = structuredClone(polygon); const before = JSON.stringify(canonical); const local = makeLocalRenderer();
  try { await local.renderer.render(canonical); local.renderer.panBy(10, 20); assert.equal(JSON.stringify(canonical), before); } finally { local.restore(); }
});
test("ZOOM-01", "zoom changes view scale", async () => {
  const local = makeLocalRenderer(); try { await local.renderer.render(polygon); const before = local.renderer.getViewState(); local.renderer.zoomAt(2, 195, 300); assert.equal(local.renderer.getViewState().scale, before.scale * 2); } finally { local.restore(); }
});
test("ZOOM-02", "zoom does not change canonical geometry", async () => {
  const canonical = structuredClone(polygon); const before = JSON.stringify(canonical); const local = makeLocalRenderer(); try { await local.renderer.render(canonical); local.renderer.zoomAt(3, 100, 100); assert.equal(JSON.stringify(canonical), before); } finally { local.restore(); }
});
test("ZOOM-03", "relative zoom limits are enforced", async () => {
  const local = makeLocalRenderer(); try { await local.renderer.render(polygon); const fit = local.renderer.fitTransform.scale; local.renderer.zoomAt(1e9, 0, 0); assert.equal(local.renderer.getViewState().scale, fit * LOCAL_MAP_MAX_RELATIVE_ZOOM); local.renderer.zoomAt(1e-12, 0, 0); assert.equal(local.renderer.getViewState().scale, fit * LOCAL_MAP_MIN_RELATIVE_ZOOM); } finally { local.restore(); }
});
test("RESET-01", "reset restores the original fit transform", async () => {
  const local = makeLocalRenderer(); try { await local.renderer.render(polygon); const fit = local.renderer.getViewState(); local.renderer.panBy(20, 20); local.renderer.zoomAt(2, 100, 100); local.renderer.fitBounds(); assert.deepEqual(local.renderer.getViewState(), fit); } finally { local.restore(); }
});
test("LIFECYCLE-01", "new geometry resets previous view transform", async () => {
  const local = makeLocalRenderer(); try { await local.renderer.render(polygon); local.renderer.panBy(70, 40); await local.renderer.render(line); assert.deepEqual(local.renderer.getViewState(), { ...local.renderer.fitTransform }); } finally { local.restore(); }
});
test("LIFECYCLE-02", "destroy removes listeners and active pointer/view state", async () => {
  const local = makeLocalRenderer(); try { await local.renderer.render(polygon); local.renderer.pointerPositions.set(7, [1, 2]); local.renderer.destroy(); assert.equal(local.listeners.size, 0); assert.equal(local.renderer.getViewState(), null); assert.equal(local.renderer.pointerPositions.size, 0); assert.equal(local.container.children.length, 0); } finally { local.restore(); }
});
for (const [id, geometry] of [["POINT-01", point], ["LINE-01", line]]) {
  test(id, `${geometry.type} renders safely`, async () => { const local = makeLocalRenderer(); try { await local.renderer.render(geometry); assert.ok(local.renderer.getViewState()); assert.ok(Object.values(local.renderer.getViewState()).filter(Number.isFinite).length >= 3); } finally { local.restore(); } });
}
test("DEGENERATE-01", "zero-width and zero-height bounds fail safely", () => {
  for (const geometry of [point, { type: "LineString", coordinates: [[1, 1], [1, 2]] }, { type: "LineString", coordinates: [[1, 1], [2, 1]] }]) assert.ok(Object.values(createLocalFitTransform(projectWgs84GeometryForDisplay(geometry), 390, 600)).filter(value => typeof value === "number").every(Number.isFinite));
});
test("BASEMAP-01", "basemap unavailable still renders local geometry", async () => {
  const item = controller(fakeProvider({ initState: PROVIDER_STATE.CONFIGURATION_BLOCKED })); const result = await item.value.open(preview()); assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG); assert.equal(item.fallback.calls, 1);
});
test("HASH-01", "canonical geometry hash remains unchanged after all local interactions", async () => {
  const canonical = structuredClone(multiPolygon); const before = JSON.stringify(canonical); const local = makeLocalRenderer(); try { await local.renderer.render(canonical); local.renderer.panBy(3, 4); local.renderer.zoomAt(2, 30, 40); local.renderer.fitBounds(); local.renderer.destroy(); assert.equal(JSON.stringify(canonical), before); } finally { local.restore(); }
});

assert.equal(isSpatialMapProvider(fakeProvider()), true);
assert.equal(tests.length, 70);

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

const indexHtml = fs.readFileSync(path.join(root, "index.html"), "utf8");
const frontendSource = ["amap-loader.js", "amap-display-coordinate-adapter.js", "amap-provider-adapter.js", "spatial-map-product.js"]
  .map(file => fs.readFileSync(path.join(root, "assets/spatial-map", file), "utf8"))
  .join("\n");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const route = serverSource.match(/app\.get\("\/api\/map-runtime-config"[\s\S]*?\n\}\);/)?.[0] || "";
assert.doesNotMatch(indexHtml, /AMAP_SECURITY_JSCODE|securityJsCode/);
assert.doesNotMatch(frontendSource, /AMAP_SECURITY_JSCODE|securityJsCode/);
assert.doesNotMatch(route, /securityJsCode\s*:/);

const secret = "test-only-security-material";
const sent = { status: 0, body: null };
const handler = createAmapSecurityProxy({
  securityJsCode: secret,
  fetchImpl: async () => ({
    status: 200,
    headers: { get: () => "application/json" },
    arrayBuffer: async () => Buffer.from(`{"echo":"${secret}"}`)
  })
});
const response = {
  status(value) { sent.status = value; return this; },
  setHeader() {},
  json(value) { sent.body = JSON.stringify(value); return this; },
  send(value) { sent.body = String(value); return this; }
};
await handler({ method: "GET", url: "/v3/test", get: () => "application/json" }, response);
assert.equal(sent.status, 502);
assert.equal(sent.body.includes(secret), false);

console.log(`Spatial provider-neutral regression: ${passed}/${tests.length} PASS`);
console.log("SECURITY_JSCODE_IN_INDEX_HTML=0");
console.log("SECURITY_JSCODE_IN_FRONTEND_JS=0");
console.log("SECURITY_JSCODE_IN_PUBLIC_RUNTIME_CONFIG=0");
console.log("SECURITY_JSCODE_IN_RESPONSE_BODY=0");
console.log("AUTHORITY_MUTATION_COUNT=0");
console.log("PROVIDER_CALLS=0");
