const DEFAULT_UPSTREAM_ORIGIN = "https://restapi.amap.com";

function safeError(res, status, code) {
  res.setHeader?.("Cache-Control", "no-store, max-age=0");
  res.status(status).json({ success: false, reason: code });
}

export function createAmapSecurityProxy({
  securityJsCode = "",
  fetchImpl = globalThis.fetch,
  upstreamOrigin = DEFAULT_UPSTREAM_ORIGIN
} = {}) {
  const secret = typeof securityJsCode === "string" ? securityJsCode.trim() : "";
  return async function amapSecurityProxy(req, res) {
    if (!secret) return safeError(res, 503, "AMAP_SECURITY_PROXY_NOT_CONFIGURED");
    if (req.method !== "GET") return safeError(res, 405, "AMAP_SECURITY_PROXY_METHOD_NOT_ALLOWED");
    const relative = String(req.url || "/");
    if (!relative.startsWith("/") || relative.startsWith("//") || /[\\\u0000-\u001f]/.test(relative)) {
      return safeError(res, 400, "AMAP_SECURITY_PROXY_PATH_INVALID");
    }

    try {
      const upstream = new URL(relative, upstreamOrigin);
      if (upstream.origin !== new URL(upstreamOrigin).origin) {
        return safeError(res, 400, "AMAP_SECURITY_PROXY_ORIGIN_INVALID");
      }
      upstream.searchParams.set("jscode", secret);
      const response = await fetchImpl(upstream, {
        method: "GET",
        headers: { accept: req.get?.("accept") || "application/json" },
        redirect: "error",
        signal: AbortSignal.timeout(8000)
      });
      const body = Buffer.from(await response.arrayBuffer());
      if (body.includes(Buffer.from(secret))) return safeError(res, 502, "AMAP_SECURITY_PROXY_RESPONSE_REJECTED");
      res.status(response.status);
      res.setHeader("Cache-Control", "no-store, max-age=0");
      res.setHeader("Content-Type", response.headers.get("content-type") || "application/json; charset=utf-8");
      return res.send(body);
    } catch {
      return safeError(res, 502, "AMAP_SECURITY_PROXY_UNAVAILABLE");
    }
  };
}
