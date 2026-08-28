import { PROVIDER_STATE } from "./constants.js";
import { adaptFinalizedCoordinateResult } from "./canonical-preview-adapter.js";

function timeoutAfter(milliseconds) {
  return new Promise((_, reject) => {
    const timer = setTimeout(() => reject(Object.assign(new Error("PROVIDER_TIMEOUT"), { code: "PROVIDER_TIMEOUT" })), milliseconds);
    timer.unref?.();
  });
}

export class MapProductController {
  constructor({ provider, renderer, fallbackRenderer, timeoutMs = 8000, onState = () => {} }) {
    this.provider = provider;
    this.renderer = renderer;
    this.fallbackRenderer = fallbackRenderer;
    this.timeoutMs = timeoutMs;
    this.onState = onState;
    this.state = PROVIDER_STATE.IDLE;
    this.preview = null;
  }

  transition(state, detail = null) {
    this.state = state;
    this.onState(Object.freeze({ state, detail }));
  }

  async open(finalizedResult, { style = "satellite" } = {}) {
    const authoritySnapshot = structuredClone({
      kmlReady: finalizedResult?.kmlReady,
      technicalKmlReady: finalizedResult?.technicalKmlReady,
      confirmationStatus: finalizedResult?.confirmationStatus,
      qualityGateStatus: finalizedResult?.qualityGateStatus,
      decisionState: finalizedResult?.decisionState,
      resultId: finalizedResult?.resultId,
      resultRevision: finalizedResult?.resultRevision,
      geometryHash: finalizedResult?.geometryHash,
      geometry: finalizedResult?.geometry
    });
    const adapted = await adaptFinalizedCoordinateResult(finalizedResult);
    if (!adapted.ok) {
      this.transition(PROVIDER_STATE.PROVIDER_ERROR, adapted.reasonCodes[0]);
      return Object.freeze({ ok: false, state: this.state, reasonCodes: adapted.reasonCodes });
    }
    this.preview = adapted.preview;

    if (!this.provider.configured) {
      await this.fallbackRenderer.render(this.preview.geometry);
      this.transition(PROVIDER_STATE.FALLBACK_LOCAL_SVG, "MAPTILER_TEST_KEY_MISSING");
      return this.result(authoritySnapshot, finalizedResult);
    }

    this.transition(PROVIDER_STATE.LOADING);
    try {
      await Promise.race([
        this.renderer.render({
          geometry: this.preview.geometry,
          styleUrl: this.provider.styleUrl(style)
        }),
        timeoutAfter(this.timeoutMs)
      ]);
      this.transition(PROVIDER_STATE.READY);
    } catch (error) {
      await this.fallbackRenderer.render(this.preview.geometry);
      this.transition(error?.code === "PROVIDER_TIMEOUT"
        ? PROVIDER_STATE.TIMEOUT
        : PROVIDER_STATE.PROVIDER_ERROR, error?.code || "PROVIDER_INITIALIZATION_FAILED");
      this.transition(PROVIDER_STATE.FALLBACK_LOCAL_SVG, error?.code || "PROVIDER_INITIALIZATION_FAILED");
    }
    return this.result(authoritySnapshot, finalizedResult);
  }

  result(authoritySnapshot, finalizedResult) {
    const currentAuthority = {
      kmlReady: finalizedResult?.kmlReady,
      technicalKmlReady: finalizedResult?.technicalKmlReady,
      confirmationStatus: finalizedResult?.confirmationStatus,
      qualityGateStatus: finalizedResult?.qualityGateStatus,
      decisionState: finalizedResult?.decisionState,
      resultId: finalizedResult?.resultId,
      resultRevision: finalizedResult?.resultRevision,
      geometryHash: finalizedResult?.geometryHash,
      geometry: finalizedResult?.geometry
    };
    const authorityPreserved = JSON.stringify(authoritySnapshot) === JSON.stringify(currentAuthority);
    return Object.freeze({
      ok: true,
      state: this.state,
      authorityPreserved,
      preview: this.preview
    });
  }
}
