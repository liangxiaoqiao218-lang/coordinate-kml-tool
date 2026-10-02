import * as maplibregl from "/vendor/maplibre-gl/maplibre-gl.mjs";
import { createMapFailureDiagnostic } from "./map-product-controller.js";
import { PROVIDER_STATE } from "./providers.js";

const SOURCE_ID = "geokit-coordinate-result";
const POINT_LAYER_ID = "geokit-coordinate-points";
const LINE_LAYER_ID = "geokit-coordinate-line";
const FILL_LAYER_ID = "geokit-coordinate-fill";
const OUTLINE_LAYER_ID = "geokit-coordinate-outline";

function status(state, detail = null, diagnostic = null) {
  return Object.freeze({ state, detail, provider: "OPENFREEMAP", diagnostic });
}

function diagnostic(reasonCode, stage, resourceCategory, event = null) {
  const error = event?.error || event || {};
  return createMapFailureDiagnostic({
    providerState: reasonCode.includes("TIMEOUT") ? PROVIDER_STATE.TIMEOUT : PROVIDER_STATE.PROVIDER_ERROR,
    reasonCode,
    stage,
    resourceCategory,
    httpStatus: error?.status ?? error?.statusCode
  });
}

function providerError(reasonCode, stage, resourceCategory, event = null) {
  return Object.assign(new Error(reasonCode), {
    code: reasonCode,
    diagnostic: diagnostic(reasonCode, stage, resourceCategory, event)
  });
}

function positions(value, result = []) {
  if (Array.isArray(value) && value.length >= 2
    && Number.isFinite(Number(value[0])) && Number.isFinite(Number(value[1]))) {
    result.push([Number(value[0]), Number(value[1])]);
    return result;
  }
  if (Array.isArray(value)) value.forEach(item => positions(item, result));
  return result;
}

function waitForMapLoad(map, timeoutMs = 8000) {
  if (map.loaded()) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(providerError("OPENFREEMAP_STYLE_TIMEOUT", "STYLE", "STYLE"));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timer);
      map.off("load", onLoad);
      map.off("error", onError);
    };
    const onLoad = () => {
      cleanup();
      resolve();
    };
    const onError = event => {
      cleanup();
      reject(providerError("OPENFREEMAP_STYLE_FAILED", "STYLE", "STYLE", event));
    };
    map.once("load", onLoad);
    map.once("error", onError);
  });
}

function waitForVisibleTiles(map, timeoutMs = 8000) {
  if (map.loaded() && map.areTilesLoaded?.()) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(providerError("OPENFREEMAP_TILES_TIMEOUT", "TILE", "TILE"));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timer);
      map.off("idle", onIdle);
      map.off("error", onError);
    };
    const onIdle = () => {
      if (typeof map.areTilesLoaded === "function" && !map.areTilesLoaded()) return;
      cleanup();
      resolve();
    };
    const onError = event => {
      cleanup();
      reject(providerError("OPENFREEMAP_TILES_FAILED", "TILE", "TILE", event));
    };
    map.on("idle", onIdle);
    map.once("error", onError);
  });
}

export class OpenFreeMapProviderAdapter {
  constructor() {
    this.map = null;
    this.lastGeometry = null;
    this.timeoutMs = 8000;
    this.providerStatus = status(PROVIDER_STATE.IDLE);
  }

  getStatus() {
    return this.providerStatus;
  }

  async init(container, publicConfig = {}) {
    const styleUrl = String(publicConfig.openFreeMapStyleUrl || "").trim();
    if (publicConfig.openFreeMapEnabled !== true || !styleUrl) {
      this.destroy();
      const failure = createMapFailureDiagnostic({
        providerState: PROVIDER_STATE.CONFIGURATION_BLOCKED,
        reasonCode: "OPENFREEMAP_CONFIGURATION_MISSING",
        stage: "CONFIGURATION",
        resourceCategory: "CONFIG"
      });
      this.providerStatus = status(PROVIDER_STATE.CONFIGURATION_BLOCKED, failure.reasonCode, failure);
      return this.providerStatus;
    }
    if (!container) {
      const failure = createMapFailureDiagnostic({
        providerState: PROVIDER_STATE.PROVIDER_ERROR,
        reasonCode: "MAP_CONTAINER_MISSING",
        stage: "INITIALIZATION",
        resourceCategory: "MAP_RUNTIME"
      });
      this.providerStatus = status(PROVIDER_STATE.PROVIDER_ERROR, failure.reasonCode, failure);
      return this.providerStatus;
    }
    if (this.map && this.providerStatus.state === PROVIDER_STATE.READY) return this.providerStatus;
    this.destroy();
    this.providerStatus = status(PROVIDER_STATE.LOADING);
    this.timeoutMs = Number(publicConfig.providerTimeoutMs) || 8000;
    try {
      this.map = new maplibregl.Map({
        container,
        style: styleUrl,
        center: [0, 0],
        zoom: 1,
        attributionControl: true,
        dragRotate: false,
        pitchWithRotate: false
      });
      this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-left");
      await waitForMapLoad(this.map, Number(publicConfig.providerTimeoutMs) || 8000);
      this.providerStatus = status(PROVIDER_STATE.READY);
    } catch (error) {
      this.destroy();
      const failure = createMapFailureDiagnostic(error?.diagnostic || error, {
        providerState: PROVIDER_STATE.PROVIDER_ERROR,
        reasonCode: error?.code || "OPENFREEMAP_INITIALIZATION_FAILED",
        stage: "INITIALIZATION",
        resourceCategory: "MAP_RUNTIME"
      });
      this.providerStatus = status(PROVIDER_STATE.PROVIDER_ERROR, failure.reasonCode, failure);
    }
    return this.providerStatus;
  }

  removeGeometryLayers() {
    if (!this.map) return;
    [POINT_LAYER_ID, LINE_LAYER_ID, OUTLINE_LAYER_ID, FILL_LAYER_ID].forEach(id => {
      if (this.map.getLayer(id)) this.map.removeLayer(id);
    });
    if (this.map.getSource(SOURCE_ID)) this.map.removeSource(SOURCE_ID);
  }

  async renderGeometry(renderPlan) {
    if (!this.map || this.providerStatus.state !== PROVIDER_STATE.READY) {
      throw Object.assign(new Error("PROVIDER_NOT_READY"), { code: "PROVIDER_NOT_READY" });
    }
    const geometry = structuredClone(renderPlan.canonicalGeometry);
    try {
      this.removeGeometryLayers();
      this.map.addSource(SOURCE_ID, {
        type: "geojson",
        data: { type: "Feature", properties: {}, geometry }
      });
      if (geometry.type === "Point" || geometry.type === "MultiPoint") {
        this.map.addLayer({
        id: POINT_LAYER_ID,
        type: "circle",
        source: SOURCE_ID,
        paint: {
          "circle-radius": 7,
          "circle-color": "#ffffff",
          "circle-stroke-color": "#e53935",
          "circle-stroke-width": 3
        }
        });
      } else if (geometry.type === "LineString" || geometry.type === "MultiLineString") {
        this.map.addLayer({
        id: LINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: { "line-color": "#e53935", "line-width": 4 }
        });
      } else {
        this.map.addLayer({
        id: FILL_LAYER_ID,
        type: "fill",
        source: SOURCE_ID,
        paint: { "fill-color": "#1976d2", "fill-opacity": 0.16 }
        });
        this.map.addLayer({
        id: OUTLINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: { "line-color": "#e53935", "line-width": 4 }
        });
      }
    } catch (error) {
      throw providerError("OPENFREEMAP_RENDER_FAILED", "RENDER", "MAP_RUNTIME", error);
    }
    this.lastGeometry = geometry;
    return Object.freeze({
      sourceResultId: renderPlan.sourceResultId,
      sourceRevision: renderPlan.sourceRevision,
      sourceGeometryHash: renderPlan.sourceGeometryHash,
      provider: "OPENFREEMAP",
      geometryType: geometry.type,
      authorityMutationCount: 0
    });
  }

  async fitGeometry() {
    if (!this.map || !this.lastGeometry) {
      throw Object.assign(new Error("PROVIDER_GEOMETRY_NOT_RENDERED"), { code: "PROVIDER_GEOMETRY_NOT_RENDERED" });
    }
    const points = positions(this.lastGeometry.coordinates);
    if (points.length === 0) throw Object.assign(new Error("GEOMETRY_EMPTY"), { code: "GEOMETRY_EMPTY" });
    try {
      if (points.length === 1) {
        this.map.flyTo({ center: points[0], zoom: 15, essential: false });
        await waitForVisibleTiles(this.map, this.timeoutMs);
        return true;
      }
      const bounds = points.reduce((value, point) => value.extend(point), new maplibregl.LngLatBounds(points[0], points[0]));
      this.map.fitBounds(bounds, { padding: 72, maxZoom: 17, duration: 0 });
      await waitForVisibleTiles(this.map, this.timeoutMs);
    } catch (error) {
      if (error?.diagnostic) throw error;
      throw providerError("OPENFREEMAP_TILES_FAILED", "TILE", "TILE", error);
    }
    return true;
  }

  destroy() {
    try {
      this.removeGeometryLayers();
      this.map?.remove?.();
    } catch (_) {}
    this.map = null;
    this.lastGeometry = null;
    this.providerStatus = status(PROVIDER_STATE.IDLE);
  }
}
