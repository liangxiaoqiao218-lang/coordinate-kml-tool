import { register } from "node:module";

if (process.env.GEOKIT_SHARE_LOCAL_PREVIEW !== "1") throw new Error("SHARE_LOCAL_PREVIEW_GATE_REQUIRED");
if (process.env.NODE_ENV === "production") throw new Error("SHARE_LOCAL_PREVIEW_PRODUCTION_FORBIDDEN");
if (process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY) throw new Error("SHARE_LOCAL_PREVIEW_SUPABASE_FORBIDDEN");
if (process.env.ALIYUN_API_KEY || process.env.DASHSCOPE_API_KEY || process.env.AMAP_WEB_JS_KEY || process.env.AMAP_SECURITY_JSCODE) {
  throw new Error("SHARE_LOCAL_PREVIEW_PROVIDER_CREDENTIALS_FORBIDDEN");
}

register("./spatial-share-local-preview-loader.mjs", import.meta.url);
await import("../server.js");
