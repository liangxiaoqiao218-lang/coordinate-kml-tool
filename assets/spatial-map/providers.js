export const PROVIDER_STATE = Object.freeze({
  IDLE: "IDLE",
  LOADING: "LOADING",
  READY: "READY",
  CONFIGURATION_BLOCKED: "CONFIGURATION_BLOCKED",
  TIMEOUT: "TIMEOUT",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  PROVIDER_PENDING: "PROVIDER_PENDING",
  FALLBACK_LOCAL_SVG: "FALLBACK_LOCAL_SVG"
});

const PROVIDER_METHODS = Object.freeze([
  "init",
  "renderGeometry",
  "fitGeometry",
  "destroy",
  "getStatus"
]);

export function isSpatialMapProvider(provider) {
  return Boolean(provider) && PROVIDER_METHODS.every(method => typeof provider[method] === "function");
}

export function assertSpatialMapProvider(provider) {
  if (!isSpatialMapProvider(provider)) {
    throw Object.assign(new TypeError("SPATIAL_MAP_PROVIDER_INTERFACE_REQUIRED"), {
      code: "SPATIAL_MAP_PROVIDER_INTERFACE_REQUIRED"
    });
  }
  return provider;
}
