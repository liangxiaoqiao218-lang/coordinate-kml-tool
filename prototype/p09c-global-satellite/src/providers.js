import { MAP_STYLE, PROVIDER_STATE } from "./constants.js";

export class MapTilerTestProvider {
  constructor({ apiKey }) {
    this.apiKey = typeof apiKey === "string" ? apiKey.trim() : "";
  }

  get configured() {
    return this.apiKey.length > 0;
  }

  styleUrl(styleName = "satellite") {
    if (!this.configured) throw new Error("MAPTILER_TEST_KEY_MISSING");
    const mapId = MAP_STYLE[styleName];
    if (!mapId) throw new Error("UNSUPPORTED_MAP_STYLE");
    return `https://api.maptiler.com/maps/${mapId}/style.json?key=${encodeURIComponent(this.apiKey)}`;
  }
}

export class ChinaProviderStub {
  resolve() {
    return Object.freeze({
      state: PROVIDER_STATE.PROVIDER_PENDING,
      provider: "AMAP_PENDING_CREDENTIAL",
      networkCalled: false
    });
  }
}
