import { arbitrateCoordinateType } from "./arbitration.js";
import { buildCoordinateResultV1 } from "./coordinate-result.js";

function inferArbitrationContext(payload = {}, coordinateEngineV2 = {}) {
  const precisionMode = String(payload.precisionMode || coordinateEngineV2.precision_mode || "");
  return {
    legacyPrecisionMode: precisionMode,
    structuredUtmPriority: precisionMode === "utm-projected-x-y" || precisionMode === "utm-projected-x-y-review"
      ? {
          accepted: precisionMode === "utm-projected-x-y",
          reason: precisionMode === "utm-projected-x-y"
            ? "explicit_crs_structured_projected_xy"
            : "transformation_verification_failed"
        }
      : null,
    crsEvidenceShadow: payload.crsEvidence || null,
    bftmAccepted: precisionMode === "bftm-projected-x-y" || Boolean(payload.bftmLongTable?.isBftm),
    utm30Accepted: precisionMode === "utm30n-projected-x-y",
    mgrs: {
      ...(payload.mgrs || {}),
      isMgrs: Boolean(payload.mgrs?.isMgrs || precisionMode === "mgrs-utm-grid-reference")
    },
    kyrgyzGk: {
      ...(payload.kyrgyzGk || {}),
      isKyrgyzGk: Boolean(payload.kyrgyzGk?.isKyrgyzGk || precisionMode === "kyrgyz-gk-point-x-y")
    },
    cadastralGrid: {
      ...(payload.cadastralGrid || {}),
      isCadastralGrid: Boolean(payload.cadastralGrid?.isCadastralGrid || precisionMode === "cadastral-grid-num-xv-yv")
    },
    dmsGroupedAccepted: precisionMode === "dms-grouped-coordinates",
    frenchPerimeterDms: {
      ...(payload.frenchPerimeterDms || {}),
      isFrenchPerimeterDms: Boolean(payload.frenchPerimeterDms?.isFrenchPerimeterDms || precisionMode === "french-perimeter-dms-prose")
    },
    pointAzDmsTableAccepted: precisionMode === "point-az-dms-table",
    handwrittenDms: {
      isHandwrittenDms: precisionMode === "handwritten-dms-coordinates"
    },
    dmsAccepted: precisionMode === "dms-coordinates" || precisionMode === "preserve-original-decimals-and-parse-dms",
    mozambiqueGeographicTable: {
      ...(payload.mozambiqueGeographicTable || {}),
      isMozambiqueGeographicTable: Boolean(
        payload.mozambiqueGeographicTable?.isMozambiqueGeographicTable
        || precisionMode === "mozambique-geographic-table"
      )
    },
    wgs84TableCoordinates: {
      ...(payload.wgs84TableCoordinates || {}),
      isWgs84TableCoordinates: Boolean(
        payload.wgs84TableCoordinates?.isWgs84TableCoordinates
        || precisionMode === "wgs84-table-coordinates"
      )
    },
    chatCoordinates: {
      ...(payload.chatCoordinates || {}),
      isChatCoordinates: Boolean(payload.chatCoordinates?.isChatCoordinates || precisionMode === "wgs84-chat-coordinates")
    },
    coordinateEngineV2,
    warning: payload.warning || ""
  };
}

function passthroughDecision(payload = {}, coordinateEngineV2 = {}) {
  const requiresReview = Boolean(payload.requires_review || coordinateEngineV2.requires_review);
  const precisionMode = String(payload.precisionMode || coordinateEngineV2.precision_mode || "preserve-original-decimals-and-parse-dms");
  return Object.freeze({
    coordinateType: String(payload.coordinateType || coordinateEngineV2.coordinate_type || "unknown"),
    precisionMode,
    authority: "legacy_compatibility",
    requires_review: requiresReview,
    arbitrationEligible: !requiresReview,
    confirmationStatus: "not_required",
    qualityGateStatus: requiresReview ? "blocked" : "passed",
    kml_allowed: !requiresReview,
    kml_ready: !requiresReview,
    lat_lon_role: "primary",
    reason: "legacy_response_finalized",
    blockedFallbacks: Object.freeze([])
  });
}

export function finalizeCoordinateResponse(payload = {}, { coordinateEngineV2 = null } = {}) {
  const engine = coordinateEngineV2 || payload.coordinateEngineV2 || {};
  let decision = payload.coordinateArbitration;

  if (!decision?.coordinateType || !decision?.precisionMode) {
    decision = arbitrateCoordinateType(inferArbitrationContext(payload, engine));
  }
  if (decision.coordinateType === "unknown" && (payload.precisionMode || engine.coordinate_type)) {
    decision = passthroughDecision(payload, engine);
  }

  const engineReviewAuthoritative = decision.coordinateType === "cote_divoire_geographic_dms_table"
    || decision.coordinateType === "mozambique_geographic_table"
    || decision.authority === "legacy_compatibility";
  const requiresReview = Boolean(
    decision.requires_review
    || payload.requires_review
    || (engineReviewAuthoritative && engine.requires_review)
  );
  const canonicalUtmAwaitingConfirmation = decision.coordinateType === "utm_projected_xy"
    && decision.precisionMode === "utm-projected-x-y"
    && decision.confirmationStatus !== "accepted";
  const confirmationStatus = canonicalUtmAwaitingConfirmation
    ? "awaiting_confirmation"
    : decision.confirmationStatus;
  const kmlReady = Boolean(
    decision.kml_ready
    && !requiresReview
    && confirmationStatus !== "awaiting_confirmation"
    && confirmationStatus !== "blocked"
  );
  const finalizedDecision = Object.freeze({
    ...decision,
    requires_review: requiresReview,
    arbitrationEligible: Boolean(decision.arbitrationEligible && !requiresReview),
    confirmationStatus,
    qualityGateStatus: requiresReview ? "blocked" : decision.qualityGateStatus,
    kml_ready: kmlReady
  });

  return {
    ...payload,
    coordinateType: finalizedDecision.coordinateType,
    precisionMode: finalizedDecision.precisionMode,
    requires_review: finalizedDecision.requires_review,
    arbitrationEligible: finalizedDecision.arbitrationEligible,
    confirmationStatus: finalizedDecision.confirmationStatus,
    qualityGateStatus: finalizedDecision.qualityGateStatus,
    kml_ready: finalizedDecision.kml_ready,
    coordinateArbitration: finalizedDecision,
    coordinateResult: buildCoordinateResultV1(finalizedDecision)
  };
}
