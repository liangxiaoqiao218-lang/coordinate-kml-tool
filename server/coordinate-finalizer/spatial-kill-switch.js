export const SPATIAL_RESULT_FLAG = "spatial_result_enabled";

export function isSpatialResultEnabled(env = process.env) {
  return String(env.SPATIAL_RESULT_ENABLED || "").trim().toLowerCase() === "true";
}

export function createSpatialExecutionBoundary({ env = process.env, initializeSpatial, requestProvider } = {}) {
  return Object.freeze({
    enabled: isSpatialResultEnabled(env),
    run(context) {
      if (!isSpatialResultEnabled(env)) {
        return Object.freeze({ status: "disabled", initialized: false, providerRequested: false });
      }
      const value = typeof initializeSpatial === "function" ? initializeSpatial(context) : null;
      const providerValue = typeof requestProvider === "function" ? requestProvider(context) : null;
      return Object.freeze({ status: "enabled", initialized: true, providerRequested: typeof requestProvider === "function", value, providerValue });
    }
  });
}

export function enforceSpatialApiEnabled(req, res, next) {
  if (!isSpatialResultEnabled()) {
    return res.status(404).json({ success: false, code: "SPATIAL_RESULT_DISABLED" });
  }
  return next();
}
