import { AMapLoader } from "./amap-loader.js";
import { AMapProviderAdapter } from "./amap-provider-adapter.js";
import { MapProductController } from "./map-product-controller.js";
import { LocalSvgRenderer } from "./maplibre-renderer.js";

const elements = {
  shell: document.querySelector("#spatialMapShell"),
  providerCanvas: document.querySelector("#spatialProviderCanvas"),
  local: document.querySelector("#spatialLocalMapCanvas"),
  attribution: document.querySelector("#spatialProviderAttribution"),
  state: document.querySelector("#spatialProviderState"),
  compactState: document.querySelector("#spatialProviderCompact"),
  failure: document.querySelector("#spatialMapFailure"),
  retry: document.querySelector("#spatialMapRetryAction"),
  fit: document.querySelector("#spatialFitGeometryAction")
};

let runtimeConfig = Object.freeze({
  webJsKey: "",
  securityProxyReady: false,
  securityServiceHost: "/_AMapService",
  providerTimeoutMs: 8000
});
let controller = null;
let initializationPromise = null;

function updateState({ state, detail = null }) {
  if (elements.state) {
    elements.state.textContent = state === "READY"
      ? "卫星地图已加载"
      : state === "LOADING"
        ? "正在加载卫星地图"
        : state === "FALLBACK_LOCAL_SVG"
          ? "本地 Geometry 预览"
          : "地图预览";
    elements.state.dataset.providerState = state;
    elements.state.dataset.detail = detail || "";
  }
  const unavailable = state === "FALLBACK_LOCAL_SVG";
  if (elements.failure) elements.failure.hidden = !unavailable;
  if (elements.retry) elements.retry.hidden = !unavailable;
  if (elements.compactState) elements.compactState.hidden = !unavailable;
}

async function loadRuntimeConfig() {
  try {
    const response = await fetch("/api/map-runtime-config", { cache: "no-store" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error("MAP_RUNTIME_CONFIG_UNAVAILABLE");
    runtimeConfig = Object.freeze({
      webJsKey: typeof payload.amapWebJsKey === "string" ? payload.amapWebJsKey : "",
      securityProxyReady: payload.amapSecurityProxyReady === true,
      securityServiceHost: "/_AMapService",
      providerTimeoutMs: Number.isFinite(payload.providerTimeoutMs)
        ? Math.max(1000, Math.min(15000, payload.providerTimeoutMs))
        : 8000
    });
  } catch {
    runtimeConfig = Object.freeze({
      webJsKey: "",
      securityProxyReady: false,
      securityServiceHost: "/_AMapService",
      providerTimeoutMs: 8000
    });
  }
  return runtimeConfig;
}

async function initialize() {
  if (controller) return controller;
  if (initializationPromise) return initializationPromise;
  initializationPromise = (async () => {
    await loadRuntimeConfig();
    const provider = new AMapProviderAdapter({ loader: new AMapLoader() });
    const fallbackRenderer = new LocalSvgRenderer({
      container: elements.local,
      attributionElement: elements.attribution
    });
    controller = new MapProductController({
      provider,
      fallbackRenderer,
      timeoutMs: runtimeConfig.providerTimeoutMs,
      onState: updateState
    });
    return controller;
  })();
  try {
    return await initializationPromise;
  } finally {
    initializationPromise = null;
  }
}

function authorityFromPayload(payload) {
  return {
    kmlReady: payload?.kmlEligibility?.kmlReady,
    technicalKmlReady: payload?.kmlEligibility?.technicalKmlReady,
    confirmationStatus: payload?.kmlEligibility?.confirmationStatus,
    qualityGateStatus: payload?.kmlEligibility?.qualityGateStatus,
    decisionState: payload?.kmlEligibility?.decisionState,
    reviewState: payload?.mapPreviewObject?.previewWarnings,
    kmlHash: payload?.kmlEligibility?.kmlHash
  };
}

async function open(payload) {
  const activeController = await initialize();
  elements.providerCanvas.hidden = true;
  elements.local.hidden = false;
  const preview = payload?.mapPreviewObject;
  const expectedIdentity = preview && {
    sourceResultId: preview.sourceResultId,
    sourceRevision: preview.sourceRevision,
    sourceGeometryHash: preview.sourceGeometryHash
  };
  const result = await activeController.open(preview, {
    authority: authorityFromPayload(payload),
    expectedIdentity,
    publicConfig: runtimeConfig,
    container: elements.providerCanvas
  });
  if (result.state === "READY") {
    elements.providerCanvas.hidden = false;
    elements.local.hidden = true;
    if (elements.attribution) {
      elements.attribution.textContent = "卫星地图 · Geometry 仅用于显示";
      elements.attribution.hidden = false;
    }
  }
  elements.shell?.dispatchEvent(new CustomEvent("geokit:spatial-map-opened", { detail: result }));
  return result;
}

async function retry() {
  if (!controller) return null;
  const result = await controller.retry();
  const ready = result?.state === "READY";
  elements.providerCanvas.hidden = !ready;
  elements.local.hidden = ready;
  return result;
}

async function fitGeometry() {
  if (!controller) return false;
  const providerFit = await controller.fitGeometry({ reason: "user-fit" });
  if (providerFit) return true;
  if (!controller.preview) return false;
  await controller.fallbackRenderer.render(controller.preview.geometry);
  return true;
}

function destroy() {
  controller?.destroy();
  if (elements.providerCanvas) elements.providerCanvas.hidden = true;
  if (elements.local) elements.local.hidden = false;
}

elements.retry?.addEventListener("click", retry);
elements.fit?.addEventListener("click", () => fitGeometry().catch(() => {}));

globalThis.GeoKitSatelliteMap = Object.freeze({ open, initialize, retry, fitGeometry, destroy });
