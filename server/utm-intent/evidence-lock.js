export function hasExplicitUtmEvidenceLock(crsEvidenceShadow = null) {
  const intent = crsEvidenceShadow?.shadowIntent || crsEvidenceShadow || {};
  const conflicts = Array.isArray(intent.conflicts) ? intent.conflicts : [];
  return Boolean(
    intent.confidence === "confirmed"
    && intent.projection === "utm"
    && intent.datum === "WGS84"
    && Number.isInteger(intent.zone)
    && (intent.hemisphere === "north" || intent.hemisphere === "south")
    && conflicts.length === 0
  );
}

function suppressWgs84TableCandidate(candidate = {}) {
  if (!candidate?.isWgs84TableCoordinates) return candidate;
  return {
    ...candidate,
    isWgs84TableCoordinates: false,
    verificationOnly: true,
    suppressedBy: "explicit_utm_evidence_lock"
  };
}

function suppressChatCandidate(candidate = {}) {
  if (!candidate?.isChatCoordinates) return candidate;
  return {
    ...candidate,
    isChatCoordinates: false,
    verificationOnly: true,
    suppressedBy: "explicit_utm_evidence_lock"
  };
}

function suppressFrenchPerimeterCandidate(candidate = {}) {
  if (!candidate?.isFrenchPerimeterDms) return candidate;
  return {
    ...candidate,
    isFrenchPerimeterDms: false,
    verificationOnly: true,
    suppressedBy: "explicit_utm_evidence_lock"
  };
}

function suppressHandwrittenCandidate(candidate = {}) {
  if (!candidate?.isHandwrittenDms) return candidate;
  return {
    ...candidate,
    isHandwrittenDms: false,
    verificationOnly: true,
    suppressedBy: "explicit_utm_evidence_lock"
  };
}

export function suppressGeographicFallbacksForUtmEvidenceLock(candidates = {}, { enabled = false } = {}) {
  if (!enabled) {
    return {
      ...candidates,
      suppressedFallbacks: []
    };
  }

  const suppressedFallbacks = [];
  if (candidates.dmsGroupedAccepted) suppressedFallbacks.push("DMS_GROUPED");
  if (candidates.frenchPerimeterDms?.isFrenchPerimeterDms) suppressedFallbacks.push("FRENCH_PERIMETER_DMS");
  if (candidates.pointAzDmsTableAccepted) suppressedFallbacks.push("POINT_AZ_DMS_TABLE");
  if (candidates.handwrittenDms?.isHandwrittenDms) suppressedFallbacks.push("HANDWRITTEN_DMS");
  if (candidates.dmsAccepted) suppressedFallbacks.push("DMS");
  if (candidates.wgs84TableCoordinates?.isWgs84TableCoordinates) suppressedFallbacks.push("WGS84_TABLE");
  if (candidates.chatCoordinates?.isChatCoordinates) suppressedFallbacks.push("WGS84_CHAT");

  return {
    ...candidates,
    dmsGroupedAccepted: false,
    frenchPerimeterDms: suppressFrenchPerimeterCandidate(candidates.frenchPerimeterDms),
    pointAzDmsTableAccepted: false,
    handwrittenDms: suppressHandwrittenCandidate(candidates.handwrittenDms),
    dmsAccepted: false,
    wgs84TableCoordinates: suppressWgs84TableCandidate(candidates.wgs84TableCoordinates),
    chatCoordinates: suppressChatCandidate(candidates.chatCoordinates),
    suppressedFallbacks
  };
}
