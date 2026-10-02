import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  MapProductController,
  createMapFailureDiagnostic
} from "../assets/spatial-map/map-product-controller.js";
import { LocalSvgRenderer } from "../assets/spatial-map/maplibre-renderer.js";
import { PROVIDER_STATE } from "../assets/spatial-map/providers.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const receiptDir = path.resolve(String(process.env.SPATIAL_MAP_FALLBACK_RECEIPT_DIR || "").trim());
if (!process.env.SPATIAL_MAP_FALLBACK_RECEIPT_DIR || existsSync(receiptDir)) {
  throw new Error("SPATIAL_MAP_FALLBACK_NEW_RECEIPT_DIR_REQUIRED");
}
mkdirSync(receiptDir, { recursive: true });

const geometries = Object.freeze({
  Point: { type: "Point", coordinates: [116.391245, 39.907654] },
  MultiPoint: { type: "MultiPoint", coordinates: [[116.391245, 39.907654], [116.392245, 39.908654]] },
  LineString: { type: "LineString", coordinates: [[116.391245, 39.907654], [116.392245, 39.908654]] },
  Polygon: { type: "Polygon", coordinates: [[[116.391245, 39.907654], [116.392245, 39.907654], [116.392245, 39.908654], [116.391245, 39.907654]]] }
});

function preview(geometry = geometries.LineString) {
  return {
    schemaVersion: "map_preview_object_v1",
    sourceResultId: "fallback-diagnostic-result",
    sourceRevision: 7,
    sourceGeometryHash: "sha256:fallback-diagnostic-geometry",
    crs: { id: "EPSG:4326" },
    axisOrder: "longitude_latitude",
    geometryType: geometry.type,
    geometry: structuredClone(geometry),
    previewEligibility: { allowed: true, gate: "MAP_PREVIEW_DRAWABLE_ELIGIBILITY" },
    previewWarnings: ["REVIEW_REQUIRED"]
  };
}

function renderReceipt(plan, overrides = {}) {
  return {
    sourceResultId: plan.sourceResultId,
    sourceRevision: plan.sourceRevision,
    sourceGeometryHash: plan.sourceGeometryHash,
    provider: "MOCK_MAP_PROVIDER",
    geometryType: plan.geometryType,
    authorityMutationCount: 0,
    ...overrides
  };
}

function provider({ initStatus = null, initError = null, fitError = null, receiptOverrides = null } = {}) {
  return {
    status: { state: PROVIDER_STATE.IDLE },
    async init() {
      if (initError) throw initError;
      this.status = initStatus || { state: PROVIDER_STATE.READY, provider: "MOCK_MAP_PROVIDER" };
      return this.status;
    },
    async renderGeometry(plan) { return renderReceipt(plan, receiptOverrides || {}); },
    async fitGeometry() { if (fitError) throw fitError; return true; },
    destroy() { this.status = { state: PROVIDER_STATE.IDLE }; },
    getStatus() { return this.status; }
  };
}

function localRenderer(width = 390, height = 640) {
  const createNode = name => ({
    name, style: {}, attributes: {}, children: [],
    setAttribute(key, value) { this.attributes[key] = String(value); },
    append(child) { this.children.push(child); },
    addEventListener() {}, removeEventListener() {},
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
  return {
    renderer: new LocalSvgRenderer({ container, attributionElement: { hidden: false, textContent: "provider" } }),
    container,
    restore() { globalThis.document = previousDocument; }
  };
}

function failure(reasonCode, stage, resourceCategory, httpStatus = null) {
  return Object.assign(new Error("redacted"), {
    code: reasonCode,
    diagnostic: createMapFailureDiagnostic({
      providerState: reasonCode.includes("TIMEOUT") ? PROVIDER_STATE.TIMEOUT : PROVIDER_STATE.PROVIDER_ERROR,
      reasonCode, stage, resourceCategory, httpStatus,
      url: "https://tiles.example.invalid/style?key=SECRET",
      responseBody: "Bearer SECRET"
    })
  });
}

const tests = [];
function test(name, run) { tests.push({ name, run }); }

test("configured provider style failure falls back with sanitized STYLE diagnostic", async () => {
  const diagnostic = createMapFailureDiagnostic({
    providerState: PROVIDER_STATE.PROVIDER_ERROR,
    reasonCode: "OPENFREEMAP_STYLE_FAILED", stage: "STYLE", resourceCategory: "STYLE", httpStatus: 503
  });
  const fallback = localRenderer();
  try {
    const value = preview();
    const result = await new MapProductController({
      provider: provider({ initStatus: { state: PROVIDER_STATE.PROVIDER_ERROR, detail: diagnostic.reasonCode, diagnostic } }),
      fallbackRenderer: fallback.renderer,
      timeoutMs: 100
    }).open(value, { expectedIdentity: {
      sourceResultId: value.sourceResultId,
      sourceRevision: value.sourceRevision,
      sourceGeometryHash: value.sourceGeometryHash
    } });
    assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
    assert.deepEqual(result.failureDiagnostic, diagnostic);
    assert.equal(fallback.container.children[0]?.name, "svg");
  } finally { fallback.restore(); }
});

for (const [name, error] of [
  ["style timeout", failure("OPENFREEMAP_STYLE_TIMEOUT", "STYLE", "STYLE")],
  ["tile failure", failure("OPENFREEMAP_TILES_FAILED", "TILE", "TILE", 502)],
  ["tile timeout", failure("OPENFREEMAP_TILES_TIMEOUT", "TILE", "TILE")]
]) {
  test(`${name} preserves a structured diagnostic and local geometry`, async () => {
    const fallback = localRenderer();
    try {
      const result = await new MapProductController({
        provider: name.startsWith("style") ? provider({ initError: error }) : provider({ fitError: error }),
        fallbackRenderer: fallback.renderer,
        timeoutMs: 100
      }).open(preview());
      assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
      assert.equal(result.failureDiagnostic.reasonCode, error.diagnostic.reasonCode);
      assert.equal(result.failureDiagnostic.stage, error.diagnostic.stage);
      assert.equal(fallback.container.children[0]?.name, "svg");
    } finally { fallback.restore(); }
  });
}

test("normal provider reaches READY without failure diagnostic", async () => {
  const fallback = localRenderer();
  try {
    const result = await new MapProductController({ provider: provider(), fallbackRenderer: fallback.renderer, timeoutMs: 100 }).open(preview());
    assert.equal(result.state, PROVIDER_STATE.READY);
    assert.equal(result.failureDiagnostic, null);
    assert.equal(fallback.container.children.length, 0);
  } finally { fallback.restore(); }
});

for (const [type, geometry] of Object.entries(geometries)) {
  test(`${type} remains visible in the local fallback`, async () => {
    const fallback = localRenderer();
    try {
      const diagnostic = createMapFailureDiagnostic({
        providerState: PROVIDER_STATE.PROVIDER_ERROR,
        reasonCode: "OPENFREEMAP_STYLE_FAILED", stage: "STYLE", resourceCategory: "STYLE"
      });
      const result = await new MapProductController({
        provider: provider({ initStatus: { state: PROVIDER_STATE.PROVIDER_ERROR, detail: diagnostic.reasonCode, diagnostic } }),
        fallbackRenderer: fallback.renderer,
        timeoutMs: 100
      }).open(preview(geometry));
      assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
      assert.equal(result.preview.geometry.type, type);
      assert.equal(fallback.container.hidden, false);
      assert.equal(fallback.container.children[0]?.attributes?.role, "img");
    } finally { fallback.restore(); }
  });
}

for (const [field, mismatch] of [
  ["sourceResultId", "other-result"],
  ["sourceRevision", 8],
  ["sourceGeometryHash", "sha256:other-geometry"]
]) {
  test(`${field} conflict blocks provider and fallback rendering`, async () => {
    const fallback = localRenderer();
    try {
      const value = preview();
      const result = await new MapProductController({ provider: provider(), fallbackRenderer: fallback.renderer }).open(value, {
        expectedIdentity: {
          sourceResultId: value.sourceResultId,
          sourceRevision: value.sourceRevision,
          sourceGeometryHash: value.sourceGeometryHash,
          [field]: mismatch
        }
      });
      assert.equal(result.ok, false);
      assert.equal(result.reasonCodes[0], "STALE_CANONICAL_IDENTITY");
      assert.equal(fallback.container.children.length, 0);
    } finally { fallback.restore(); }
  });
}

test("render receipt identity conflict blocks instead of exposing a local fallback", async () => {
  const fallback = localRenderer();
  try {
    const result = await new MapProductController({
      provider: provider({ receiptOverrides: { sourceGeometryHash: "sha256:forged-receipt" } }),
      fallbackRenderer: fallback.renderer,
      timeoutMs: 100
    }).open(preview());
    assert.equal(result.ok, false);
    assert.equal(result.state, PROVIDER_STATE.PROVIDER_ERROR);
    assert.equal(result.reasonCodes[0], "RENDER_RECEIPT_IDENTITY_MISMATCH");
    assert.equal(fallback.container.children.length, 0);
  } finally { fallback.restore(); }
});

test("diagnostic allowlist never returns URL, body, credential, query or environment data", () => {
  const result = createMapFailureDiagnostic({
    providerState: "PROVIDER_ERROR",
    reasonCode: "OPENFREEMAP_STYLE_FAILED",
    stage: "STYLE",
    resourceCategory: "STYLE",
    httpStatus: 401,
    url: "https://example.invalid/style?key=SECRET",
    query: "key=SECRET",
    responseBody: "Bearer SECRET",
    cookie: "session=SECRET",
    authorization: "Bearer SECRET",
    environment: { SECRET: "value" }
  });
  assert.deepEqual(Object.keys(result).sort(), ["httpStatus", "providerState", "reasonCode", "resourceCategory", "stage"]);
  assert.doesNotMatch(JSON.stringify(result), /SECRET|https?:|Bearer|session=|environment/i);
});

test("product surface keeps local canvas visible and labels no-basemap fallback", () => {
  const source = readFileSync(path.join(root, "assets/spatial-map/spatial-map-product.js"), "utf8");
  const html = readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(source, /const fallback = result\?\.state === "FALLBACK_LOCAL_SVG"/);
  assert.match(source, /elements\.local\.hidden = !fallback/);
  assert.match(source, /本地几何 · 无底图核对结果/);
  assert.match(source, /无底图核对结果 · 原因编号：/);
  assert.match(html, /\["READY", "FALLBACK_LOCAL_SVG"\]\.includes\(mapResult\.state\)/);
});

test("OpenFreeMap adapter reports only sanitized style/tile diagnostics", () => {
  const source = readFileSync(path.join(root, "assets/spatial-map/openfreemap-provider-adapter.js"), "utf8");
  assert.match(source, /OPENFREEMAP_STYLE_FAILED/);
  assert.match(source, /OPENFREEMAP_STYLE_TIMEOUT/);
  assert.match(source, /OPENFREEMAP_TILES_FAILED/);
  assert.match(source, /OPENFREEMAP_TILES_TIMEOUT/);
  assert.doesNotMatch(source, /cause:\s*event\?\.error/);
  const diagnosticFactory = source.match(/function diagnostic\([\s\S]*?\n\}/)?.[0] || "";
  assert.doesNotMatch(diagnosticFactory, /responseBody|responseText|request\.url|styleUrl|query|cookie|authorization|environment/i);
});

let passed = 0;
const startedAt = new Date().toISOString();
for (const entry of tests) {
  await entry.run();
  passed += 1;
  console.log(`PASS ${passed}/${tests.length} ${entry.name}`);
}

const receipt = {
  schemaVersion: "spatial_map_fallback_diagnostics_regression_v1",
  startedAt,
  completedAt: new Date().toISOString(),
  gitCommit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  gitStatus: execFileSync("git", ["status", "--short"], { cwd: root, encoding: "utf8" }).trim(),
  result: "PASS",
  passed,
  total: tests.length,
  realProviderCalls: 0,
  realMapProviderCalls: 0
};
writeFileSync(path.join(receiptDir, "results.json"), `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
console.log(`Spatial map fallback diagnostics regression: ${passed}/${tests.length} PASS`);
console.log("REAL_PROVIDER_CALLS=0");
console.log("REAL_MAP_PROVIDER_CALLS=0");
