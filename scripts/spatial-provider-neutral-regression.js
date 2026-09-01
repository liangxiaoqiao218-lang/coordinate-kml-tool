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

test("SPN-32", "390px UI has no switcher and keeps return download fullscreen retry", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const css = fs.readFileSync(path.join(root, "assets/spatial-map/spatial-map.css"), "utf8");
  assert.match(html, />生成地图<\/button>/);
  assert.doesNotMatch(html, /data-spatial-map-style=/);
  assert.match(html, /spatialMapRetryAction/);
  assert.match(html, /spatialFullscreenAction/);
  assert.match(html, /spatialKmlAction/);
  assert.match(html, /returnToCoordinate\(\)/);
  assert.match(css, /@media \(width: 390px\)/);
  assert.match(css, /max-width: 100%/);
  assert.match(css, /touch-action: none/);
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

assert.equal(isSpatialMapProvider(fakeProvider()), true);
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
