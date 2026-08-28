import { MapProductController } from "../src/map-product-controller.js";
import { MapTilerTestProvider, ChinaProviderStub } from "../src/providers.js";
import { MapLibreRenderer, LocalSvgRenderer } from "../src/maplibre-renderer.js";
import { createDemoResult, DEMO_POLYGON } from "../src/demo-result.js";

const elements = {
  shell: document.querySelector("#map-shell"),
  map: document.querySelector("#map"),
  attribution: document.querySelector("#attribution"),
  state: document.querySelector("#provider-state"),
  warning: document.querySelector("#authority-warning"),
  styles: [...document.querySelectorAll("[data-style]")],
  fullscreen: document.querySelector("#fullscreen-button"),
  returnButton: document.querySelector("#return-button"),
  china: document.querySelector("#china-status")
};

const runtime = globalThis.__P09C_RUNTIME_CONFIG__ || { mapTilerConfigured: false, mapTilerTestKey: null };
const provider = new MapTilerTestProvider({ apiKey: runtime.mapTilerTestKey });
const renderer = new MapLibreRenderer({
  maplibregl: globalThis.maplibregl,
  container: elements.map,
  attributionElement: elements.attribution
});
const fallbackRenderer = new LocalSvgRenderer({ container: elements.map, attributionElement: elements.attribution });
const controller = new MapProductController({
  provider,
  renderer,
  fallbackRenderer,
  timeoutMs: 8000,
  onState: ({ state }) => { elements.state.textContent = state; }
});

const result = await createDemoResult(DEMO_POLYGON);
const opened = await controller.open(result, { style: "satellite" });
elements.warning.textContent = opened.preview?.warnings?.join(" · ") || "";
elements.china.textContent = new ChinaProviderStub().resolve().state;

elements.styles.forEach(button => button.addEventListener("click", async () => {
  elements.styles.forEach(candidate => candidate.dataset.active = String(candidate === button));
  if (!provider.configured || !renderer.map) return;
  elements.state.textContent = "LOADING";
  try {
    await renderer.switchStyle(provider.styleUrl(button.dataset.style));
    elements.state.textContent = "READY";
  } catch {
    await fallbackRenderer.render(opened.preview.geometry);
    elements.state.textContent = "FALLBACK_LOCAL_SVG";
  }
}));

elements.fullscreen.addEventListener("click", async () => {
  if (!document.fullscreenElement) await elements.shell.requestFullscreen();
  else await document.exitFullscreen();
});

elements.returnButton.addEventListener("click", () => {
  elements.shell.dataset.mode = "result";
  document.querySelector("#result-summary").focus();
});

document.addEventListener("fullscreenchange", () => {
  elements.fullscreen.textContent = document.fullscreenElement ? "退出全屏" : "全屏地图";
  renderer.map?.resize();
});

globalThis.p09cLoadFinalizedResult = async canonicalResult => controller.open(canonicalResult);
