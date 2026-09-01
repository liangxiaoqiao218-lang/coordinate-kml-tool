import { AMapDisplayCoordinateAdapter } from "./amap-display-coordinate-adapter.js";
import { AMapLoader } from "./amap-loader.js";
import { PROVIDER_STATE } from "./providers.js";

function status(state, detail = null) {
  return Object.freeze({ state, detail, provider: "AMAP" });
}

function normalizeConvertedPosition(value) {
  if (Array.isArray(value)) return [Number(value[0]), Number(value[1])];
  if (value && typeof value.getLng === "function" && typeof value.getLat === "function") {
    return [Number(value.getLng()), Number(value.getLat())];
  }
  return [Number.NaN, Number.NaN];
}

export class AMapProviderAdapter {
  constructor({ loader = new AMapLoader(), displayAdapter = null } = {}) {
    this.loader = loader;
    this.displayAdapter = displayAdapter;
    this.runtime = null;
    this.map = null;
    this.satelliteLayer = null;
    this.overlays = [];
    this.lastPoint = null;
    this.providerStatus = status(PROVIDER_STATE.IDLE);
  }

  getStatus() {
    return this.providerStatus;
  }

  async init(container, publicConfig = {}) {
    if (!publicConfig.webJsKey || publicConfig.securityProxyReady !== true) {
      this.destroy();
      this.providerStatus = status(PROVIDER_STATE.CONFIGURATION_BLOCKED, !publicConfig.webJsKey
        ? "AMAP_WEB_JS_KEY_MISSING"
        : "AMAP_SECURITY_PROXY_NOT_READY");
      return this.providerStatus;
    }
    if (this.providerStatus.state === PROVIDER_STATE.READY && this.map) return this.providerStatus;
    this.providerStatus = status(PROVIDER_STATE.LOADING);
    try {
      this.runtime = await this.loader.load(publicConfig);
      if (typeof this.runtime?.TileLayer?.Satellite !== "function") {
        throw Object.assign(new Error("AMAP_SATELLITE_LAYER_UNAVAILABLE"), {
          code: "AMAP_SATELLITE_LAYER_UNAVAILABLE"
        });
      }
      this.satelliteLayer = new this.runtime.TileLayer.Satellite({ zIndex: 1 });
      this.map = new this.runtime.Map(container, {
        viewMode: "2D",
        layers: [this.satelliteLayer],
        showOversea: true,
        zoom: 3,
        center: [105, 35],
        dragEnable: true,
        zoomEnable: true,
        touchZoom: true,
        doubleClickZoom: true
      });
      this.displayAdapter ||= new AMapDisplayCoordinateAdapter({
        convertBatch: coordinates => new Promise((resolve, reject) => {
          this.runtime.convertFrom(coordinates, "gps", (conversionStatus, result) => {
            if (conversionStatus !== "complete" || result?.info !== "ok" || !Array.isArray(result?.locations)) {
              reject(Object.assign(new Error("AMAP_DISPLAY_CONVERSION_FAILED"), {
                code: "AMAP_DISPLAY_CONVERSION_FAILED"
              }));
              return;
            }
            resolve(result.locations.map(normalizeConvertedPosition));
          });
        })
      });
      this.providerStatus = status(PROVIDER_STATE.READY);
    } catch (error) {
      this.providerStatus = status(PROVIDER_STATE.PROVIDER_ERROR, error?.code || "AMAP_INITIALIZATION_FAILED");
    }
    return this.providerStatus;
  }

  async renderGeometry(renderPlan) {
    if (this.providerStatus.state !== PROVIDER_STATE.READY || !this.map || !this.runtime) {
      throw Object.assign(new Error("PROVIDER_NOT_READY"), { code: "PROVIDER_NOT_READY" });
    }
    const conversion = await this.displayAdapter.convert(renderPlan.canonicalGeometry);
    const geometry = conversion.geometry;
    const common = { map: this.map, zIndex: 20 };
    if (this.overlays.length) this.map.remove?.(this.overlays);
    this.overlays = [];
    this.lastPoint = null;
    if (geometry.type === "Point") {
      this.lastPoint = geometry.coordinates;
      this.overlays.push(new this.runtime.Marker({ ...common, position: geometry.coordinates }));
    } else if (geometry.type === "LineString") {
      this.overlays.push(new this.runtime.Polyline({ ...common, path: geometry.coordinates, strokeColor: "#E53935", strokeWeight: 3 }));
    } else {
      const polygons = geometry.type === "Polygon" ? [geometry.coordinates] : geometry.coordinates;
      polygons.forEach(path => this.overlays.push(new this.runtime.Polygon({
        ...common,
        path,
        strokeColor: "#E53935",
        strokeWeight: 3,
        fillColor: "#1976D2",
        fillOpacity: 0.15
      })));
    }
    return Object.freeze({
      sourceResultId: renderPlan.sourceResultId,
      sourceRevision: renderPlan.sourceRevision,
      sourceGeometryHash: renderPlan.sourceGeometryHash,
      provider: "AMAP",
      displayCoordinateConversionStatus: conversion.status,
      geometryType: geometry.type,
      authorityMutationCount: 0
    });
  }

  async fitGeometry() {
    if (!this.map || this.overlays.length === 0) throw Object.assign(new Error("PROVIDER_GEOMETRY_NOT_RENDERED"), {
      code: "PROVIDER_GEOMETRY_NOT_RENDERED"
    });
    if (this.lastPoint) this.map.setZoomAndCenter(15, this.lastPoint, true);
    else this.map.setFitView(this.overlays, false, [56, 56, 56, 56], 17);
  }

  destroy() {
    if (this.map && this.overlays.length) this.map.remove?.(this.overlays);
    this.overlays = [];
    this.lastPoint = null;
    this.map?.destroy?.();
    this.map = null;
    this.satelliteLayer = null;
    this.runtime = null;
    this.providerStatus = status(PROVIDER_STATE.IDLE);
  }
}
