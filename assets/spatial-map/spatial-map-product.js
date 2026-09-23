import { AMapLoader } from "./amap-loader.js";
import { AMapProviderAdapter } from "./amap-provider-adapter.js";
import { MapProductController } from "./map-product-controller.js";
import { LocalSvgRenderer } from "./maplibre-renderer.js";
import { OpenFreeMapProviderAdapter } from "./openfreemap-provider-adapter.js";

const elements = {
  shell: document.querySelector("#spatialMapShell"),
  providerCanvas: document.querySelector("#spatialProviderCanvas"),
  local: document.querySelector("#spatialLocalMapCanvas"),
  attribution: document.querySelector("#spatialProviderAttribution"),
  card: document.querySelector("#spatialResultCard"),
  toggle: document.querySelector("#spatialResultSheetToggle"),
  details: document.querySelector("#spatialResultDetails"),
  state: document.querySelector("#spatialProviderState"),
  failure: document.querySelector("#spatialMapFailure"),
  retry: document.querySelector("#spatialMapRetryAction"),
  fit: document.querySelector("#spatialFitGeometryAction")
};

let runtimeConfig = Object.freeze({
  webJsKey: "",
  securityProxyReady: false,
  securityServiceHost: "/_AMapService",
  openFreeMapEnabled: true,
  openFreeMapStyleUrl: "https://tiles.openfreemap.org/styles/liberty",
  providerTimeoutMs: 8000
});
let controller = null;
let fallbackRenderer = null;
let initializationPromise = null;
const mobileResultQuery = globalThis.matchMedia?.("(max-width: 640px)");

function placeProviderFailure() {
  if (!elements.failure || !elements.details) return;
  if (mobileResultQuery?.matches && elements.card) {
    elements.card.insertBefore(elements.failure, elements.details);
    return;
  }
  const warning = elements.details.querySelector("#spatialResultWarning");
  elements.details.insertBefore(elements.failure, warning);
}

function updateState({ state, detail = null }) {
  if (elements.state) {
    elements.state.textContent = state === "READY"
      ? "地图"
      : state === "LOADING"
        ? "正在加载地图"
        : "地块详情";
    elements.state.dataset.providerState = state;
    elements.state.dataset.detail = detail || "";
  }
  const unavailable = state === "FALLBACK_LOCAL_SVG";
  if (elements.card) elements.card.dataset.providerUnavailable = String(unavailable);
  if (elements.failure) elements.failure.hidden = !unavailable;
  if (elements.retry) elements.retry.hidden = !unavailable;
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
      openFreeMapEnabled: payload.openFreeMapEnabled === true,
      openFreeMapStyleUrl: typeof payload.openFreeMapStyleUrl === "string" ? payload.openFreeMapStyleUrl : "",
      providerTimeoutMs: Number.isFinite(payload.providerTimeoutMs)
        ? Math.max(1000, Math.min(15000, payload.providerTimeoutMs))
        : 8000
    });
  } catch {
    runtimeConfig = Object.freeze({
      webJsKey: "",
      securityProxyReady: false,
      securityServiceHost: "/_AMapService",
      openFreeMapEnabled: false,
      openFreeMapStyleUrl: "",
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
    const provider = runtimeConfig.webJsKey && runtimeConfig.securityProxyReady
      ? new AMapProviderAdapter({ loader: new AMapLoader() })
      : new OpenFreeMapProviderAdapter();
    fallbackRenderer = new LocalSvgRenderer({
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
  elements.local.hidden = true;
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
      elements.attribution.textContent = result.renderReceipt?.provider === "AMAP"
        ? "高德卫星地图"
        : "OpenFreeMap · OpenStreetMap";
      elements.attribution.hidden = false;
    }
  } else {
    elements.providerCanvas.hidden = true;
    elements.local.hidden = true;
  }
  elements.shell?.dispatchEvent(new CustomEvent("geokit:spatial-map-opened", { detail: result }));
  return result;
}

async function retry() {
  if (!controller) return null;
  elements.providerCanvas.hidden = true;
  elements.local.hidden = true;
  const result = await controller.retry();
  const ready = result?.state === "READY";
  elements.providerCanvas.hidden = !ready;
  elements.local.hidden = true;
  return result;
}

async function fitGeometry() {
  if (!controller) return false;
  const providerFit = await controller.fitGeometry({ reason: "user-fit" });
  if (providerFit) return true;
  return false;
}

function destroy() {
  controller?.destroy();
  fallbackRenderer?.destroy();
  if (elements.providerCanvas) elements.providerCanvas.hidden = true;
  if (elements.local) elements.local.hidden = true;
}

elements.retry?.addEventListener("click", retry);
elements.fit?.addEventListener("click", () => fitGeometry().catch(() => {}));
mobileResultQuery?.addEventListener?.("change", placeProviderFailure);
placeProviderFailure();

globalThis.GeoKitSatelliteMap = Object.freeze({ open, initialize, retry, fitGeometry, destroy });
