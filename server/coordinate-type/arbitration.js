export const COORDINATE_TYPE_PRIORITY = Object.freeze([
  "explicit_crs_evidence",
  "typed_projection",
  "structured_xy",
  "validated_wgs84",
  "dms",
  "chat"
]);

function hasWarning(value, warning) {
  const values = Array.isArray(value) ? value : [value];
  return values.some(item => String(item || "").toLowerCase().includes(warning));
}

function result({
  coordinateType,
  precisionMode,
  authority,
  requiresReview = false,
  kmlAllowed = true,
  arbitrationEligible = kmlAllowed && !requiresReview,
  confirmationStatus = "not_required",
  qualityGateStatus = requiresReview ? "blocked" : "passed",
  latLonRole = "primary",
  reason,
  blockedFallbacks = []
}) {
  const confirmationComplete = confirmationStatus === "accepted" || confirmationStatus === "not_required";
  const qualityGatePassed = qualityGateStatus === "passed";
  return Object.freeze({
    coordinateType,
    precisionMode,
    authority,
    requires_review: Boolean(requiresReview),
    arbitrationEligible: Boolean(arbitrationEligible && !requiresReview),
    confirmationStatus,
    qualityGateStatus,
    kml_allowed: Boolean(kmlAllowed && !requiresReview),
    kml_ready: Boolean(kmlAllowed && !requiresReview && confirmationComplete && qualityGatePassed),
    lat_lon_role: latLonRole,
    reason,
    blockedFallbacks: Object.freeze([...blockedFallbacks])
  });
}

export function arbitrateCoordinateType(context = {}) {
  const {
    structuredUtmPriority = null,
    bftmAccepted = false,
    utm30Accepted = false,
    mgrs = {},
    kyrgyzGk = {},
    cadastralGrid = {},
    dmsGroupedAccepted = false,
    frenchPerimeterDms = {},
    pointAzDmsTableAccepted = false,
    handwrittenDms = {},
    dmsAccepted = false,
    mozambiqueGeographicTable = {},
    wgs84TableCoordinates = {},
    chatCoordinates = {},
    coordinateEngineV2 = {},
    warning = ""
  } = context;

  const crsConflicts = [
    ...(Array.isArray(context.crsEvidenceShadow?.shadowIntent?.conflicts) ? context.crsEvidenceShadow.shadowIntent.conflicts : []),
    ...(Array.isArray(context.explicitCrsConflicts) ? context.explicitCrsConflicts : [])
  ];
  if (crsConflicts.length > 0) {
    return result({
      coordinateType: "crs_conflict",
      precisionMode: "coordinate-type-conflict-review",
      authority: "explicit_crs_evidence",
      requiresReview: true,
      kmlAllowed: false,
      arbitrationEligible: false,
      confirmationStatus: "blocked",
      qualityGateStatus: "blocked",
      reason: "explicit_crs_conflict",
      blockedFallbacks: ["utm_projected_xy", "bftm_projected_xy", "generic_projected_xy", "dms", "wgs84_chat_coordinates"]
    });
  }

  const structuredUtmRouted = Boolean(
    structuredUtmPriority?.accepted
    || structuredUtmPriority?.reason === "transformation_verification_failed"
  );
  if (structuredUtmRouted) {
    const accepted = Boolean(structuredUtmPriority?.accepted);
    return result({
      coordinateType: "utm_projected_xy",
      precisionMode: accepted ? "utm-projected-x-y" : "utm-projected-x-y-review",
      authority: "explicit_crs_evidence",
      requiresReview: !accepted,
      kmlAllowed: accepted,
      arbitrationEligible: accepted,
      confirmationStatus: accepted ? "awaiting_confirmation" : "blocked",
      qualityGateStatus: accepted ? "passed" : "blocked",
      latLonRole: "verification_only",
      reason: accepted
        ? "explicit_utm_crs_and_structured_xy"
        : "utm_transformation_verification_failed",
      blockedFallbacks: [
        "utm30n-projected-x-y",
        "bftm_projected_xy",
        "generic_projected_xy",
        "wgs84_table_coordinates",
        "dms",
        "wgs84_chat_coordinates"
      ]
    });
  }

  const explicitUtmIntent = context.crsEvidenceShadow?.shadowIntent;
  if (explicitUtmIntent?.confidence === "confirmed" && explicitUtmIntent?.projection === "utm") {
    return result({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y-review",
      authority: "explicit_crs_evidence",
      requiresReview: true,
      kmlAllowed: false,
      arbitrationEligible: false,
      confirmationStatus: "blocked",
      qualityGateStatus: "blocked",
      latLonRole: "verification_only",
      reason: "explicit_utm_crs_without_validated_structured_xy",
      blockedFallbacks: ["utm30n-projected-x-y", "bftm_projected_xy", "generic_projected_xy", "dms", "wgs84_chat_coordinates"]
    });
  }

  // Explicit dedicated CRS evidence comes next. Heuristic candidates cannot
  // override the explicit typed UTM decision above.
  if (mgrs?.isMgrs) {
    return result({
      coordinateType: "mgrs_utm_grid_reference",
      precisionMode: "mgrs-utm-grid-reference",
      authority: "explicit_crs_evidence",
      reason: "mgrs_type_lock",
      blockedFallbacks: ["utm_projected_xy", "structured_xy", "wgs84_chat_coordinates"]
    });
  }
  if (bftmAccepted) {
    return result({
      coordinateType: "bftm_projected_xy",
      precisionMode: "bftm-projected-x-y",
      authority: "explicit_crs_evidence",
      reason: "bftm_type_lock",
      blockedFallbacks: ["utm_projected_xy", "generic_projected_xy", "wgs84_chat_coordinates"]
    });
  }
  if (kyrgyzGk?.isKyrgyzGk) {
    return result({
      coordinateType: "kyrgyz_gk_projected_xy",
      precisionMode: "kyrgyz-gk-point-x-y",
      authority: "explicit_crs_evidence",
      reason: "kyrgyz_gk_type_lock",
      blockedFallbacks: ["utm_projected_xy", "generic_projected_xy", "wgs84_chat_coordinates"]
    });
  }

  // Existing UTM30 remains a compatibility route until the alias migration is approved.
  if (utm30Accepted) {
    return result({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm30n-projected-x-y",
      authority: "typed_projection",
      reason: "legacy_utm30_alias",
      latLonRole: "verification_only",
      blockedFallbacks: ["generic_projected_xy", "wgs84_chat_coordinates"]
    });
  }
  if (cadastralGrid?.isCadastralGrid) {
    return result({
      coordinateType: "cadastral_grid",
      precisionMode: "cadastral-grid-num-xv-yv",
      authority: "structured_xy",
      reason: "cadastral_grid_type_lock"
    });
  }
  if (mozambiqueGeographicTable?.isMozambiqueGeographicTable) {
    return result({
      coordinateType: "mozambique_geographic_table",
      precisionMode: "mozambique-geographic-table",
      authority: "validated_wgs84",
      reason: "mozambique_geographic_table_lock"
    });
  }
  // Dedicated DMS structures are type locks. Generic WGS84 table detection may
  // validate them, but it must not rename or flatten their parser contract.
  if (coordinateEngineV2?.coordinate_type === "cote_divoire_geographic_dms_table") {
    const requiresReview = Boolean(coordinateEngineV2.requires_review);
    return result({
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      authority: "dms",
      requiresReview,
      kmlAllowed: !requiresReview,
      reason: requiresReview ? "cote_divoire_dms_requires_review" : "cote_divoire_dms_validated"
    });
  }
  if (dmsGroupedAccepted) {
    return result({ coordinateType: "dms", precisionMode: "dms-grouped-coordinates", authority: "dms", reason: "dms_grouped" });
  }
  if (frenchPerimeterDms?.isFrenchPerimeterDms) {
    return result({ coordinateType: "dms", precisionMode: "french-perimeter-dms-prose", authority: "dms", reason: "french_perimeter_dms" });
  }
  if (pointAzDmsTableAccepted) {
    return result({ coordinateType: "dms", precisionMode: "point-az-dms-table", authority: "dms", reason: "point_az_dms_table" });
  }
  if (handwrittenDms?.isHandwrittenDms) {
    return result({
      coordinateType: "dms",
      precisionMode: "handwritten-dms-coordinates",
      authority: "dms",
      requiresReview: true,
      kmlAllowed: false,
      confirmationStatus: "awaiting_confirmation",
      qualityGateStatus: "blocked",
      reason: "handwritten_dms_requires_review"
    });
  }
  if (wgs84TableCoordinates?.isWgs84TableCoordinates) {
    const swapped = hasWarning([wgs84TableCoordinates.warning, wgs84TableCoordinates.warnings, warning], "possible swapped lat/lon");
    return result({
      coordinateType: "wgs84_geographic_table",
      precisionMode: "wgs84-table-coordinates",
      authority: "validated_wgs84",
      requiresReview: swapped,
      kmlAllowed: !swapped,
      reason: swapped ? "possible_swapped_lat_lon" : "validated_wgs84_table"
    });
  }
  if (dmsAccepted) {
    const precisionMode = context.legacyPrecisionMode === "dms-coordinates"
      ? "dms-coordinates"
      : "preserve-original-decimals-and-parse-dms";
    return result({ coordinateType: "dms", precisionMode, authority: "dms", reason: "validated_dms" });
  }
  if (chatCoordinates?.isChatCoordinates) {
    const swapped = hasWarning([chatCoordinates.warning, chatCoordinates.warnings, warning], "possible swapped lat/lon");
    return result({
      coordinateType: "wgs84_chat_coordinates",
      precisionMode: "wgs84-chat-coordinates",
      authority: "chat",
      requiresReview: swapped,
      kmlAllowed: !swapped,
      reason: swapped ? "possible_swapped_lat_lon" : "wgs84_chat_coordinates"
    });
  }

  return result({
    coordinateType: "unknown",
    precisionMode: "preserve-original-decimals-and-parse-dms",
    authority: "unknown",
    requiresReview: false,
    kmlAllowed: false,
    reason: "no_authoritative_coordinate_type"
  });
}
