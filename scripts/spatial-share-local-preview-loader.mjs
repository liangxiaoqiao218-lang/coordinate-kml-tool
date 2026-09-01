const QUALIFICATION_GATE = "GEOKIT_SHARE_LOCAL_PREVIEW";
const STORE_SPECIFIER = "./server/spatial/sharing/supabase-share-store.js";
const STORE_URL = new URL("./spatial-share-local-preview-store.js", import.meta.url).href;
const SERVER_URL = new URL("../server.js", import.meta.url).href;

function assertQualificationGate() {
  if (process.env[QUALIFICATION_GATE] !== "1" || process.env.NODE_ENV === "production") {
    throw new Error("SHARE_LOCAL_PREVIEW_GATE_REQUIRED");
  }
}

export async function resolve(specifier, context, nextResolve) {
  assertQualificationGate();
  if (specifier === STORE_SPECIFIER && context.parentURL === SERVER_URL) {
    return { url: STORE_URL, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
  assertQualificationGate();
  const loaded = await nextLoad(url, context);
  if (url !== SERVER_URL || loaded.format !== "module") return loaded;
  const source = String(loaded.source);
  const productionCookieName = 'const SPATIAL_SHARE_MANAGER_COOKIE = "__Host-geokit_spatial_share_manager";';
  const productionRecipientCookieName = 'const SPATIAL_SHARE_RECIPIENT_COOKIE = "__Host-geokit_spatial_share_recipient";';
  const productionManagerCookieFlags = "HttpOnly; Secure; SameSite=Strict";
  const productionRecipientCookieFlags = "HttpOnly; Secure; SameSite=Lax";
  if (source.split(productionCookieName).length !== 2
    || source.split(productionRecipientCookieName).length !== 2
    || source.split(productionManagerCookieFlags).length !== 2
    || source.split(productionRecipientCookieFlags).length !== 2) {
    throw new Error("SHARE_LOCAL_PREVIEW_COOKIE_CONTRACT_DRIFT");
  }
  return {
    ...loaded,
    source: source
      .replace(productionCookieName, 'const SPATIAL_SHARE_MANAGER_COOKIE = "geokit_spatial_share_manager_qualification";')
      .replace(productionRecipientCookieName, 'const SPATIAL_SHARE_RECIPIENT_COOKIE = "geokit_spatial_share_recipient_qualification";')
      .replace(productionManagerCookieFlags, "HttpOnly; SameSite=Strict")
      .replace(productionRecipientCookieFlags, "HttpOnly; SameSite=Lax")
  };
}
