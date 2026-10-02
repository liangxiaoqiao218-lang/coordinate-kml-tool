import { createGeometryRenderPlan, validateMapPreviewObject } from "./geometry-render-plan.js";
import { assertSpatialMapProvider, PROVIDER_STATE } from "./providers.js";

const FAILURE_STAGES = new Set(["CONFIGURATION", "INITIALIZATION", "STYLE", "TILE", "RENDER", "TIMEOUT"]);
const RESOURCE_CATEGORIES = new Set(["CONFIG", "MAP_RUNTIME", "STYLE", "TILE", "SPRITE", "GLYPH", "UNKNOWN"]);

function safeReasonCode(value, fallback = "MAP_PROVIDER_UNAVAILABLE") {
  const candidate = String(value || "").trim().toUpperCase();
  return /^[A-Z][A-Z0-9_]{2,80}$/.test(candidate) ? candidate : fallback;
}

function inferFailureStage(reasonCode) {
  if (reasonCode.includes("CONFIGURATION")) return "CONFIGURATION";
  if (reasonCode.includes("STYLE") || reasonCode.includes("LOAD_")) return "STYLE";
  if (reasonCode.includes("TILE")) return "TILE";
  if (reasonCode.includes("TIMEOUT")) return "TIMEOUT";
  if (reasonCode.includes("RENDER") || reasonCode.includes("RECEIPT")) return "RENDER";
  return "INITIALIZATION";
}

export function createMapFailureDiagnostic(value = {}, defaults = {}) {
  const reasonCode = safeReasonCode(value.reasonCode || value.code || value.detail, safeReasonCode(defaults.reasonCode));
  const requestedStage = String(value.stage || defaults.stage || "").trim().toUpperCase();
  const requestedCategory = String(value.resourceCategory || defaults.resourceCategory || "").trim().toUpperCase();
  const numericStatus = Number(value.httpStatus ?? defaults.httpStatus);
  return Object.freeze({
    providerState: safeReasonCode(value.providerState || defaults.providerState || PROVIDER_STATE.PROVIDER_ERROR, PROVIDER_STATE.PROVIDER_ERROR),
    reasonCode,
    stage: FAILURE_STAGES.has(requestedStage) ? requestedStage : inferFailureStage(reasonCode),
    resourceCategory: RESOURCE_CATEGORIES.has(requestedCategory) ? requestedCategory : "UNKNOWN",
    httpStatus: Number.isInteger(numericStatus) && numericStatus >= 100 && numericStatus <= 599 ? numericStatus : null
  });
}

function timeoutAfter(milliseconds, diagnostic) {
  return new Promise((_, reject) => {
    const timer = setTimeout(() => reject(Object.assign(new Error("PROVIDER_TIMEOUT"), {
      code: "PROVIDER_TIMEOUT",
      diagnostic: createMapFailureDiagnostic(diagnostic, { reasonCode: "PROVIDER_TIMEOUT", stage: "TIMEOUT" })
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

  transition(state, detail = null, diagnostic = null) {
    this.state = state;
    this.onState(Object.freeze({ state, detail, diagnostic }));
  }

  async fallback(diagnostic) {
    const safeDiagnostic = createMapFailureDiagnostic(diagnostic);
    await this.fallbackRenderer.render(this.preview.geometry);
    this.transition(PROVIDER_STATE.FALLBACK_LOCAL_SVG, safeDiagnostic.reasonCode, safeDiagnostic);
    return safeDiagnostic;
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
        timeoutAfter(this.timeoutMs, { reasonCode: "PROVIDER_INITIALIZATION_TIMEOUT", stage: "INITIALIZATION", resourceCategory: "MAP_RUNTIME" })
      ]);
      if (providerStatus?.state !== PROVIDER_STATE.READY) {
        const reasonCode = providerStatus?.detail || providerStatus?.state || "PROVIDER_INITIALIZATION_FAILED";
        const failureDiagnostic = createMapFailureDiagnostic(providerStatus?.diagnostic || {}, {
          providerState: providerStatus?.state,
          reasonCode,
          stage: providerStatus?.state === PROVIDER_STATE.CONFIGURATION_BLOCKED ? "CONFIGURATION" : "INITIALIZATION",
          resourceCategory: providerStatus?.state === PROVIDER_STATE.CONFIGURATION_BLOCKED ? "CONFIG" : "MAP_RUNTIME"
        });
        if (providerStatus?.state === PROVIDER_STATE.CONFIGURATION_BLOCKED) {
          this.transition(PROVIDER_STATE.CONFIGURATION_BLOCKED, reasonCode, failureDiagnostic);
        }
        await this.fallback(failureDiagnostic);
        return this.result(preview, authority, providerStatus, failureDiagnostic);
      }
      this.renderReceipt = await Promise.race([
        this.provider.renderGeometry(renderPlan),
        timeoutAfter(this.timeoutMs, { reasonCode: "PROVIDER_RENDER_TIMEOUT", stage: "RENDER", resourceCategory: "MAP_RUNTIME" })
      ]);
      if (this.renderReceipt?.sourceResultId !== preview.sourceResultId
        || this.renderReceipt?.sourceRevision !== preview.sourceRevision
        || this.renderReceipt?.sourceGeometryHash !== preview.sourceGeometryHash
        || this.renderReceipt?.authorityMutationCount !== 0) {
        throw Object.assign(new Error("RENDER_RECEIPT_IDENTITY_MISMATCH"), {
          code: "RENDER_RECEIPT_IDENTITY_MISMATCH"
        });
      }
      await Promise.race([
        Promise.resolve(this.provider.fitGeometry()),
        timeoutAfter(this.timeoutMs, { reasonCode: "PROVIDER_TILES_TIMEOUT", stage: "TILE", resourceCategory: "TILE" })
      ]);
      this.transition(PROVIDER_STATE.READY);
    } catch (error) {
      const reasonCode = error?.code || "PROVIDER_RENDER_FAILED";
      const failureDiagnostic = createMapFailureDiagnostic(error?.diagnostic || error, {
        providerState: reasonCode.includes("TIMEOUT") ? PROVIDER_STATE.TIMEOUT : PROVIDER_STATE.PROVIDER_ERROR,
        reasonCode,
        stage: reasonCode.includes("TILE") ? "TILE" : reasonCode.includes("TIMEOUT") ? "TIMEOUT" : "RENDER",
        resourceCategory: reasonCode.includes("TILE") ? "TILE" : "MAP_RUNTIME"
      });
      this.transition(failureDiagnostic.providerState, reasonCode, failureDiagnostic);
      if (reasonCode === "RENDER_RECEIPT_IDENTITY_MISMATCH") {
        return Object.freeze({
          ok: false,
          state: this.state,
          reasonCodes: [reasonCode],
          failureDiagnostic: structuredClone(failureDiagnostic),
          authorityMutationCount: 0
        });
      }
      await this.fallback(failureDiagnostic);
      return this.result(preview, authority, this.provider.getStatus(), failureDiagnostic);
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
    if (this.state === PROVIDER_STATE.FALLBACK_LOCAL_SVG) {
      return this.fallbackRenderer?.fitBounds?.() === true;
    }
    if (this.state !== PROVIDER_STATE.READY) return false;
    await this.provider.fitGeometry(options);
    return true;
  }

  result(preview, authority, providerStatus, failureDiagnostic = null) {
    const authorityPreserved = JSON.stringify(this.authoritySnapshot) === JSON.stringify(snapshotAuthority(preview, authority));
    return Object.freeze({
      ok: true,
      state: this.state,
      providerStatus: providerStatus?.state || null,
      failureDiagnostic: failureDiagnostic ? structuredClone(failureDiagnostic) : null,
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
