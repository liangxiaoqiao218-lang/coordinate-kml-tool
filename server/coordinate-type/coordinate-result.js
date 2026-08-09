export const COORDINATE_RESULT_SCHEMA_VERSION = "coordinate_result_v1";

export const COORDINATE_RESULT_STATE = Object.freeze({
  AUTO_EXPORT: "AUTO_EXPORT",
  CONFIRM_REQUIRED: "CONFIRM_REQUIRED",
  BLOCKED_REVIEW: "BLOCKED_REVIEW"
});

function normalizeString(value) {
  return String(value || "").trim();
}

function normalizeBoolean(value) {
  return value === true;
}

function hasBlockedReason(reason = "") {
  return /conflict|blocked|failed|without_validated|possible_swapped_lat_lon|review/i.test(reason);
}

function isVerifiedUtmResult(model = {}) {
  return model.coordinateType === "utm_projected_xy"
    && model.precisionMode === "utm-projected-x-y"
    && model.qualityGateStatus === "passed"
    && model.reason !== "utm_transformation_verification_failed"
    && model.reason !== "explicit_utm_crs_without_validated_structured_xy";
}

function isUtmReviewResult(model = {}) {
  return model.coordinateType === "utm_projected_xy"
    && (
      model.precisionMode === "utm-projected-x-y-review"
      || model.reason === "utm_transformation_verification_failed"
      || model.reason === "explicit_utm_crs_without_validated_structured_xy"
    );
}

function isHandwrittenDmsResult(model = {}) {
  return model.precisionMode === "handwritten-dms-coordinates";
}

function classifyCoordinateResult(model = {}) {
  if (isVerifiedUtmResult(model)) {
    return COORDINATE_RESULT_STATE.AUTO_EXPORT;
  }

  if (isHandwrittenDmsResult(model)) {
    return COORDINATE_RESULT_STATE.CONFIRM_REQUIRED;
  }

  if (
    isUtmReviewResult(model)
    || model.confirmationStatus === "blocked"
    || model.qualityGateStatus === "blocked"
    || model.reason === "explicit_crs_conflict"
    || model.reason === "possible_swapped_lat_lon"
    || (model.requiresReview && hasBlockedReason(model.reason))
  ) {
    return COORDINATE_RESULT_STATE.BLOCKED_REVIEW;
  }

  if (model.confirmationStatus === "awaiting_confirmation" || model.confirmationStatus === "required") {
    return COORDINATE_RESULT_STATE.CONFIRM_REQUIRED;
  }

  if (model.kmlReady || model.qualityGateStatus === "passed") {
    return COORDINATE_RESULT_STATE.AUTO_EXPORT;
  }

  return COORDINATE_RESULT_STATE.BLOCKED_REVIEW;
}

function getKmlSource(model = {}, state) {
  if (state === COORDINATE_RESULT_STATE.BLOCKED_REVIEW) return "unavailable";
  if (model.coordinateType === "utm_projected_xy") return "verified_transformation";
  if (model.coordinateType === "bftm_projected_xy" || model.coordinateType === "kyrgyz_gk_projected_xy") return "projected_transformation";
  if (model.coordinateType === "mgrs_utm_grid_reference") return "grid_reference_transformation";
  if (/wgs84|geographic|dms|chat/i.test(`${model.coordinateType} ${model.precisionMode}`)) return "recognized_wgs84";
  return state === COORDINATE_RESULT_STATE.CONFIRM_REQUIRED ? "pending_user_confirmation" : "recognized_coordinates";
}

function getDecisionAction(state) {
  if (state === COORDINATE_RESULT_STATE.AUTO_EXPORT) return "export_kml";
  if (state === COORDINATE_RESULT_STATE.CONFIRM_REQUIRED) return "confirm_result";
  return "review_required";
}

function getReviewLevel(state) {
  if (state === COORDINATE_RESULT_STATE.AUTO_EXPORT) return "none";
  if (state === COORDINATE_RESULT_STATE.CONFIRM_REQUIRED) return "user_confirmation";
  return "blocked";
}

function getUiModel(state) {
  if (state === COORDINATE_RESULT_STATE.AUTO_EXPORT) {
    return Object.freeze({
      panel: "none",
      summary: "识别完成，可生成 KML",
      primaryAction: "生成 KML"
    });
  }
  if (state === COORDINATE_RESULT_STATE.CONFIRM_REQUIRED) {
    return Object.freeze({
      panel: "confirmation",
      summary: "请核对识别结果",
      primaryAction: "确认并继续生成 KML"
    });
  }
  return Object.freeze({
    panel: "review",
    summary: "识别结果需要核对后再生成 KML",
    primaryAction: "需核对后生成 KML"
  });
}

export function buildCoordinateResultV1(input = {}) {
  const arbitration = input.coordinateArbitration || {};
  const model = {
    coordinateType: normalizeString(input.coordinateType || arbitration.coordinateType),
    precisionMode: normalizeString(input.precisionMode || arbitration.precisionMode),
    authority: normalizeString(input.authority || arbitration.authority),
    confirmationStatus: normalizeString(input.confirmationStatus || arbitration.confirmationStatus || "not_required"),
    qualityGateStatus: normalizeString(input.qualityGateStatus || arbitration.qualityGateStatus || "passed"),
    reason: normalizeString(input.reason || arbitration.reason),
    requiresReview: normalizeBoolean(input.requires_review ?? input.requiresReview ?? arbitration.requires_review),
    kmlReady: normalizeBoolean(input.kml_ready ?? arbitration.kml_ready)
  };
  const state = classifyCoordinateResult(model);
  const blockedReason = state === COORDINATE_RESULT_STATE.BLOCKED_REVIEW
    ? (model.reason || model.qualityGateStatus || model.confirmationStatus || "blocked_review")
    : null;
  const reviewRequired = state !== COORDINATE_RESULT_STATE.AUTO_EXPORT;

  return Object.freeze({
    schemaVersion: COORDINATE_RESULT_SCHEMA_VERSION,
    state,
    confidence: Object.freeze({
      recognition: null,
      coordinate: null,
      transform: null
    }),
    coordinate: Object.freeze({
      type: model.coordinateType,
      precision: model.precisionMode,
      authority: model.authority || null
    }),
    decision: Object.freeze({
      action: getDecisionAction(state),
      reason: model.reason || null,
      userActionRequired: state !== COORDINATE_RESULT_STATE.AUTO_EXPORT
    }),
    kml: Object.freeze({
      ready: state === COORDINATE_RESULT_STATE.AUTO_EXPORT,
      source: getKmlSource(model, state),
      blockedReason
    }),
    review: Object.freeze({
      required: reviewRequired,
      level: getReviewLevel(state),
      reason: reviewRequired ? (model.reason || model.confirmationStatus || null) : null,
      canUserConfirm: state === COORDINATE_RESULT_STATE.CONFIRM_REQUIRED
    }),
    ui: getUiModel(state)
  });
}
