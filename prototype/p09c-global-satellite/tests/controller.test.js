import assert from "node:assert/strict";
import test from "node:test";
import { MapProductController } from "../src/map-product-controller.js";
import { PROVIDER_STATE } from "../src/constants.js";
import { ChinaProviderStub, MapTilerTestProvider } from "../src/providers.js";
import { finalized, geometries } from "./helpers.js";

class FakeFallback {
  calls = 0;
  async render() { this.calls += 1; }
}

test("missing MapTiler key falls back without a Provider call", async () => {
  const fallback = new FakeFallback();
  let providerRenderCalls = 0;
  const controller = new MapProductController({
    provider: new MapTilerTestProvider({ apiKey: "" }),
    renderer: { async render() { providerRenderCalls += 1; } },
    fallbackRenderer: fallback
  });
  const input = await finalized(geometries.Polygon, { kmlReady: false, decisionState: "BLOCKED" });
  const before = structuredClone(input);
  const result = await controller.open(input);
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.equal(providerRenderCalls, 0);
  assert.equal(fallback.calls, 1);
  assert.equal(result.authorityPreserved, true);
  assert.deepEqual(input, before);
});

test("provider deadline is bounded and falls back", async () => {
  const fallback = new FakeFallback();
  const states = [];
  const controller = new MapProductController({
    provider: new MapTilerTestProvider({ apiKey: "test-client-key" }),
    renderer: { render: () => new Promise(() => {}) },
    fallbackRenderer: fallback,
    timeoutMs: 20,
    onState: ({ state }) => states.push(state)
  });
  const result = await controller.open(await finalized(geometries.LineString));
  assert.equal(result.state, PROVIDER_STATE.FALLBACK_LOCAL_SVG);
  assert.deepEqual(states, [PROVIDER_STATE.LOADING, PROVIDER_STATE.TIMEOUT, PROVIDER_STATE.FALLBACK_LOCAL_SVG]);
  assert.equal(fallback.calls, 1);
  assert.equal(result.authorityPreserved, true);
});

test("MapTiler style switching uses only approved read-only style endpoints", () => {
  const provider = new MapTilerTestProvider({ apiKey: "test-client-key" });
  assert.match(provider.styleUrl("satellite"), /\/maps\/satellite-v4\/style\.json\?key=/);
  assert.match(provider.styleUrl("hybrid"), /\/maps\/hybrid-v4\/style\.json\?key=/);
  assert.match(provider.styleUrl("map"), /\/maps\/streets-v4\/style\.json\?key=/);
  assert.throws(() => provider.styleUrl("unknown"), /UNSUPPORTED_MAP_STYLE/);
});

test("China provider is a zero-network pending stub", () => {
  assert.deepEqual(new ChinaProviderStub().resolve(), {
    state: PROVIDER_STATE.PROVIDER_PENDING,
    provider: "AMAP_PENDING_CREDENTIAL",
    networkCalled: false
  });
});
