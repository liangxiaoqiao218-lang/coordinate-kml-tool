import { createGeometryRenderPlan, validateMapPreviewObject } from "./geometry-render-plan.js";
import { assertSpatialMapProvider, PROVIDER_STATE } from "./providers.js";

function timeoutAfter(milliseconds) {
  return new Promise((_, reject) => {
    const timer = setTimeout(() => reject(Object.assign(new Error("PROVIDER_TIMEOUT"), {
      code: "PROVIDER_TIMEOUT"
    })), milliseconds);
    timer.unref?.();
  });
}

function snapshotAuthority(preview, externalAuthority = {}) {
  return structuredClone({
    sourceResultId: preview?.sourceResultId,
    sourceRevision: preview?.sourceRevision,
    sourceGeometryHash: preview?.sourceGeometryHash,
    geometry: preview?.geometry,
    kmlReady: externalAuthority?.kmlReady,
    kmlContent: externalAuthority?.kmlContent,
    kmlHash: externalAuthority?.kmlHash,
    technicalKmlReady: externalAuthority?.technicalKmlReady,
    confirmationStatus: externalAuthority?.confirmationStatus,
    qualityGateStatus: externalAuthority?.qualityGateStatus,
    reviewState: externalAuthority?.reviewState,
    decisionState: externalAuthority?.decisionState
  });
}

function identityMatches(preview, expectedIdentity) {
  if (!expectedIdentity) return true;
  return preview.sourceResultId === expectedIdentity.sourceResultId
    && preview.sourceRevision === expectedIdentity.sourceRevision
    && preview.sourceGeometryHash === expectedIdentity.sourceGeometryHash;
}

export class MapProductController {
  constructor({ provider, fallbackRenderer, timeoutMs = 8000, onState = () => {} }) {
    this.provider = assertSpatialMapProvider(provider);
    this.fallbackRenderer = fallbackRenderer;
    this.timeoutMs = timeoutMs;
    this.onState = onState;
    this.state = PROVIDER_STATE.IDLE;
    this.preview = null;
    this.authoritySnapshot = null;
    this.lastOpenOptions = null;
    this.renderReceipt = null;
  }

  transition(state, detail = null) {
    this.state = state;
    this.onState(Object.freeze({ state, detail }));
  }

  async fallback(reasonCode) {
    await this.fallbackRenderer.render(this.preview.geometry);
    this.transition(PROVIDER_STATE.FALLBACK_LOCAL_SVG, reasonCode);
  }

  async open(preview, options = {}) {
    const { authority = {}, expectedIdentity = null, publicConfig = {}, container = null } = options;
    const validation = validateMapPreviewObject(preview);
    if (!validation.ok || !identityMatches(preview, expectedIdentity)) {
      const reasonCode = validation.ok ? "STALE_CANONICAL_IDENTITY" : validation.reasonCode;
      this.transition(PROVIDER_STATE.PROVIDER_ERROR, reasonCode);
      return Object.freeze({ ok: false, state: this.state, reasonCodes: [reasonCode], authorityMutationCount: 0 });
    }

    this.preview = structuredClone(preview);
    this.authoritySnapshot = snapshotAuthority(preview, authority);
    this.lastOpenOptions = { preview: structuredClone(preview), options: structuredClone({ ...options, container: null }), container };
    const geometryPlan = createGeometryRenderPlan(this.preview.geometry);
    const renderPlan = Object.freeze({
      ...geometryPlan,
      canonicalGeometry: structuredClone(this.preview.geometry),
      sourceResultId: this.preview.sourceResultId,
      sourceRevision: this.preview.sourceRevision,
      sourceGeometryHash: this.preview.sourceGeometryHash,
      geometryType: this.preview.geometry.type
    });

    this.transition(PROVIDER_STATE.LOADING);
    try {
      const providerStatus = await Promise.race([
        this.provider.init(container, publicConfig),
        timeoutAfter(this.timeoutMs)
      ]);
      if (providerStatus?.state !== PROVIDER_STATE.READY) {
        const reasonCode = providerStatus?.detail || providerStatus?.state || "PROVIDER_INITIALIZATION_FAILED";
        if (providerStatus?.state === PROVIDER_STATE.CONFIGURATION_BLOCKED) {
          this.transition(PROVIDER_STATE.CONFIGURATION_BLOCKED, reasonCode);
        }
        await this.fallback(reasonCode);
        return this.result(preview, authority, providerStatus);
      }
      this.renderReceipt = await Promise.race([
        this.provider.renderGeometry(renderPlan),
        timeoutAfter(this.timeoutMs)
      ]);
      if (this.renderReceipt?.sourceResultId !== preview.sourceResultId
        || this.renderReceipt?.sourceRevision !== preview.sourceRevision
        || this.renderReceipt?.sourceGeometryHash !== preview.sourceGeometryHash
        || this.renderReceipt?.authorityMutationCount !== 0) {
        throw Object.assign(new Error("RENDER_RECEIPT_IDENTITY_MISMATCH"), {
          code: "RENDER_RECEIPT_IDENTITY_MISMATCH"
        });
      }
      await Promise.race([Promise.resolve(this.provider.fitGeometry()), timeoutAfter(this.timeoutMs)]);
      this.transition(PROVIDER_STATE.READY);
    } catch (error) {
      const reasonCode = error?.code || "PROVIDER_RENDER_FAILED";
      this.transition(reasonCode === "PROVIDER_TIMEOUT" ? PROVIDER_STATE.TIMEOUT : PROVIDER_STATE.PROVIDER_ERROR, reasonCode);
      await this.fallback(reasonCode);
    }
    return this.result(preview, authority, this.provider.getStatus());
  }

  async retry() {
    if (!this.lastOpenOptions) return Object.freeze({ ok: false, state: this.state, reasonCode: "NO_RETRY_CONTEXT" });
    const { preview, options, container } = this.lastOpenOptions;
    this.provider.destroy();
    return this.open(preview, { ...options, container });
  }

  async fitGeometry(options = {}) {
    if (this.state !== PROVIDER_STATE.READY) return false;
    await this.provider.fitGeometry(options);
    return true;
  }

  result(preview, authority, providerStatus) {
    const authorityPreserved = JSON.stringify(this.authoritySnapshot) === JSON.stringify(snapshotAuthority(preview, authority));
    return Object.freeze({
      ok: true,
      state: this.state,
      providerStatus: providerStatus?.state || null,
      authorityPreserved,
      authorityMutationCount: authorityPreserved ? 0 : 1,
      renderReceipt: this.renderReceipt ? structuredClone(this.renderReceipt) : null,
      preview: structuredClone(this.preview)
    });
  }

  destroy() {
    this.provider.destroy();
    this.state = PROVIDER_STATE.IDLE;
    this.preview = null;
    this.renderReceipt = null;
  }
}
