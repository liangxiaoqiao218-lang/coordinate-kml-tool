const elements = {
  page: document.querySelector("#sharedSpatialPage"),
  shell: document.querySelector("#spatialMapShell"),
  unavailable: document.querySelector("#sharedUnavailable"),
  card: document.querySelector("#spatialResultCard"),
  toggle: document.querySelector("#spatialResultSheetToggle"),
  summary: document.querySelector("#spatialCollapsedSummary"),
  reviewCompact: document.querySelector("#spatialReviewCompact"),
  geometryType: document.querySelector("#spatialGeometryType"),
  warning: document.querySelector("#spatialResultWarning"),
  areaFact: document.querySelector("#spatialAreaFact"),
  area: document.querySelector("#spatialAreaValue"),
  perimeterFact: document.querySelector("#spatialPerimeterFact"),
  perimeter: document.querySelector("#spatialPerimeterValue"),
  pointCount: document.querySelector("#spatialPointCount"),
  coordinates: document.querySelector("#sharedCoordinateText"),
  copyCoordinates: document.querySelector("#sharedCopyCoordinatesAction"),
  edit: document.querySelector("#sharedEditAction"),
  revoke: document.querySelector("#sharedRevokeAction")
};

function shareIdFromPath() {
  const match = location.pathname.match(/^\/s\/([A-Za-z0-9_-]{32})$/);
  return match?.[1] || "";
}

function showUnavailable() {
  globalThis.GeoKitSatelliteMap?.destroy?.();
  elements.shell.hidden = true;
  elements.unavailable.hidden = false;
}

function positionsEqualExact(first, last) {
  return Array.isArray(first) && Array.isArray(last)
    && first.length === last.length
    && first.every((value, index) => value === last[index]);
}

function geometryPositions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") {
    return geometry.coordinates.flatMap(ring => positionsEqualExact(ring[0], ring.at(-1))
      ? ring.slice(0, -1)
      : ring);
  }
  return geometry.coordinates.flat(2);
}

function formatArea(value) {
  return Number.isFinite(value) ? `${(value / 10000).toFixed(2)} 公顷` : "";
}

function formatDistance(value) {
  if (!Number.isFinite(value)) return "";
  return value >= 1000 ? `${(value / 1000).toFixed(2)} km` : `${Math.round(value)} m`;
}

function formatCoordinates(geometry) {
  return geometryPositions(geometry)
    .map((position, index) => `${String(index + 1).padStart(2, "0")}  ${position[1]}, ${position[0]}`)
    .join("\n");
}

function mapPayload(snapshot) {
  const reviewWarnings = snapshot.reviewState?.requiresReview === true ? ["REVIEW_REQUIRED"] : [];
  return {
    mapPreviewObject: {
      schemaVersion: "map_preview_object_v1",
      sourceResultId: snapshot.source.resultId,
      sourceRevision: snapshot.source.resultRevision,
      sourceGeometryHash: snapshot.source.sourceGeometryHash,
      geometry: snapshot.geometry,
      geometryType: snapshot.geometry.type,
      crs: snapshot.crs,
      axisOrder: snapshot.axisOrder,
      previewEligibility: { allowed: true, warning: reviewWarnings.length > 0 },
      previewReasonCodes: [],
      previewWarnings: reviewWarnings,
      createdAt: snapshot.createdAt
    },
    spatialFacts: snapshot.spatialFacts,
    spatialFactsStatus: "available",
    kmlEligibility: {
      allowed: false,
      kmlReady: false,
      decisionState: "SHARED_READ_ONLY"
    }
  };
}

function render(snapshot) {
  const facts = snapshot.spatialFacts || {};
  const area = formatArea(facts.areaMeters2);
  const perimeter = formatDistance(facts.perimeterMeters);
  const points = Number.isFinite(facts.pointCount) ? facts.pointCount : geometryPositions(snapshot.geometry).length;
  elements.geometryType.textContent = snapshot.geometry.type;
  elements.areaFact.hidden = !area;
  elements.area.textContent = area;
  elements.perimeterFact.hidden = !perimeter;
  elements.perimeter.textContent = perimeter;
  elements.pointCount.textContent = `${points} 个`;
  elements.summary.textContent = [area, `${points}点`].filter(Boolean).join(" · ");
  elements.reviewCompact.hidden = snapshot.reviewState?.requiresReview !== true;
  elements.warning.hidden = snapshot.reviewState?.requiresReview !== true;
  elements.warning.textContent = snapshot.reviewState?.requiresReview === true ? "待核对" : "";
  elements.coordinates.textContent = formatCoordinates(snapshot.geometry);
  elements.edit.hidden = snapshot.usagePermission !== "ALLOW_EDIT";
  elements.revoke.hidden = snapshot.canRevoke !== true;
  elements.toggle.setAttribute("aria-expanded", String(!matchMedia("(max-width: 640px)").matches));
}

function createRecipientWorkingCopy(snapshot) {
  return {
    schemaVersion: "shared_recipient_working_copy_v1",
    source: {
      resultId: snapshot.source.resultId,
      resultRevision: snapshot.source.resultRevision,
      sourceGeometryHash: snapshot.source.sourceGeometryHash
    },
    geometry: structuredClone(snapshot.geometry),
    crs: structuredClone(snapshot.crs),
    axisOrder: snapshot.axisOrder
  };
}

let activeSnapshot = null;

elements.edit?.addEventListener("click", () => {
  if (!activeSnapshot || activeSnapshot.usagePermission !== "ALLOW_EDIT") return;
  sessionStorage.setItem("geokit.sharedRecipientWorkingCopy.v1", JSON.stringify(createRecipientWorkingCopy(activeSnapshot)));
  location.assign("/coordinate?from=shared_spatial_result_edit");
});

elements.copyCoordinates?.addEventListener("click", async event => {
  event.preventDefault();
  event.stopPropagation();
  const coordinateText = elements.coordinates.textContent || "";
  if (!coordinateText) return;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(coordinateText);
    } else {
      const target = document.createElement("textarea");
      target.value = coordinateText;
      target.setAttribute("readonly", "");
      target.style.position = "fixed";
      target.style.opacity = "0";
      document.body.appendChild(target);
      target.select();
      const copied = document.execCommand("copy");
      target.remove();
      if (!copied) throw new Error("COPY_FAILED");
    }
    const original = elements.copyCoordinates.textContent;
    elements.copyCoordinates.textContent = "已复制";
    setTimeout(() => { elements.copyCoordinates.textContent = original; }, 1600);
  } catch (_) {}
});

elements.toggle?.addEventListener("click", () => {
  const expanded = elements.toggle.getAttribute("aria-expanded") === "true";
  elements.toggle.setAttribute("aria-expanded", String(!expanded));
  requestAnimationFrame(() => globalThis.GeoKitSatelliteMap?.fitGeometry?.());
});

elements.revoke?.addEventListener("click", async () => {
  elements.revoke.disabled = true;
  try {
    const response = await fetch(`/api/spatial-shares/${shareIdFromPath()}`, { method: "DELETE" });
    if (!response.ok && response.status !== 404) throw new Error("SHARE_REVOKE_FAILED");
    showUnavailable();
  } catch (_) {
    elements.revoke.disabled = false;
  }
});

async function load() {
  const shareId = shareIdFromPath();
  if (!shareId) return showUnavailable();
  try {
    const accessResponse = await fetch(`/api/spatial-shares/${shareId}/access`, {
      method: "POST",
      headers: { "x-geokit-active-share-access": "1" }
    });
    if (!accessResponse.ok) return showUnavailable();
    const response = await fetch(`/api/spatial-shares/${shareId}`, { cache: "no-store" });
    const payload = await response.json().catch(() => ({}));
    const snapshot = payload?.sharedSpatialResult;
    if (!response.ok || snapshot?.schemaVersion !== "shared_spatial_result_v1") return showUnavailable();
    activeSnapshot = snapshot;
    render(snapshot);
    await globalThis.GeoKitSatelliteMap?.open(mapPayload(snapshot));
  } catch (_) {
    showUnavailable();
  }
}

load();
