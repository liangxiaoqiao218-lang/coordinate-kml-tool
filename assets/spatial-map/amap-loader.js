const DEFAULT_AMAP_SDK_URL = "https://webapi.amap.com/maps?v=2.0";

function configurationError(code) {
  return Object.assign(new Error(code), { code });
}

export class AMapLoader {
  constructor({
    documentRef = globalThis.document,
    globalRef = globalThis,
    sdkUrl = DEFAULT_AMAP_SDK_URL,
    loadTimeoutMs = 8000,
    scriptLoader = null
  } = {}) {
    this.documentRef = documentRef;
    this.globalRef = globalRef;
    this.sdkUrl = sdkUrl;
    this.loadTimeoutMs = loadTimeoutMs;
    this.scriptLoader = scriptLoader;
    this.loadPromise = null;
    this.networkAttempts = 0;
  }

  async load({ webJsKey = "", securityProxyReady = false, securityServiceHost = "/_AMapService" } = {}) {
    const key = typeof webJsKey === "string" ? webJsKey.trim() : "";
    if (!key) throw configurationError("AMAP_WEB_JS_KEY_MISSING");
    if (securityProxyReady !== true) throw configurationError("AMAP_SECURITY_PROXY_NOT_READY");
    if (this.globalRef?.AMap) return this.globalRef.AMap;
    if (this.loadPromise) return this.loadPromise;

    this.globalRef._AMapSecurityConfig = Object.freeze({ serviceHost: securityServiceHost });
    this.networkAttempts += 1;
    const url = `${this.sdkUrl}&key=${encodeURIComponent(key)}`;
    this.loadPromise = this.scriptLoader
      ? Promise.resolve(this.scriptLoader(url, this.globalRef))
      : this.loadBrowserScript(url);
    try {
      const runtime = await this.loadPromise;
      if (!runtime) throw configurationError("AMAP_SDK_RUNTIME_MISSING");
      return runtime;
    } catch (error) {
      this.loadPromise = null;
      throw error;
    }
  }

  loadBrowserScript(url) {
    if (!this.documentRef?.head) return Promise.reject(configurationError("AMAP_DOCUMENT_UNAVAILABLE"));
    return new Promise((resolve, reject) => {
      const script = this.documentRef.createElement("script");
      const timer = setTimeout(() => {
        script.remove();
        reject(configurationError("AMAP_SDK_LOAD_TIMEOUT"));
      }, this.loadTimeoutMs);
      timer.unref?.();
      script.async = true;
      script.src = url;
      script.referrerPolicy = "strict-origin-when-cross-origin";
      script.onload = () => {
        clearTimeout(timer);
        resolve(this.globalRef.AMap);
      };
      script.onerror = () => {
        clearTimeout(timer);
        script.remove();
        reject(configurationError("AMAP_SDK_LOAD_FAILED"));
      };
      this.documentRef.head.append(script);
    });
  }
}
