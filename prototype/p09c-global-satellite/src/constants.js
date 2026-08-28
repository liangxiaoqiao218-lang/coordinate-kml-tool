export const FINALIZED_SCHEMA_VERSION = "finalized_coordinate_result_v1";
export const MAP_PREVIEW_GATE = "MAP_PREVIEW_DRAWABLE_ELIGIBILITY";

export const PROVIDER_STATE = Object.freeze({
  IDLE: "IDLE",
  LOADING: "LOADING",
  READY: "READY",
  TIMEOUT: "TIMEOUT",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  PROVIDER_PENDING: "PROVIDER_PENDING",
  FALLBACK_LOCAL_SVG: "FALLBACK_LOCAL_SVG"
});

export const MAP_STYLE = Object.freeze({
  satellite: "satellite-v4",
  hybrid: "hybrid-v4",
  map: "streets-v4"
});

export const GEOMETRY_SOURCE_ID = "p09c-canonical-geometry";

export const DEFAULT_GEOMETRY_STYLE = Object.freeze({
  stroke: "#E53935",
  fill: "#1976D2",
  fillOpacity: 0.15
});
