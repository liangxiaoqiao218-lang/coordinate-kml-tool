import { importKmlFile, exportKml, toGeoJsonFeatureCollection } from "../../kml/kml-local-import.mjs";
import { MapProductController } from "/assets/spatial-map/map-product-controller.js";
import { OpenFreeMapProviderAdapter } from "/assets/spatial-map/openfreemap-provider-adapter.js";
import { LocalSvgRenderer } from "/assets/spatial-map/maplibre-renderer.js";
import {
  maximumRingDifferenceMeters,
  revisionIsCurrent,
  validateIndonesiaFeatureCollection
} from "./indonesia-kml-acceptance-contract.mjs";

const MAX_BYTES = 2 * 1024 * 1024;
const FROZEN_B_REFERENCE_SHA256 = "5bef2ddb0e6f609149b6d5c6c289ee402104e831c00153ddffd548269d3f5697";

const fileInput = document.querySelector("#kmlFileInput");
const baselineInput = document.querySelector("#baselineKmlInput");
const downloadButton = document.querySelector("#downloadKml");
const acceptanceState = document.querySelector("#acceptanceState");
const baselineComparison = document.querySelector("#baselineComparison");
const downloadReceipt = document.querySelector("#downloadReceipt");
const coordinateRows = document.querySelector("#coordinateRows");
const providerCanvas = document.querySelector("#spatialProviderCanvas");
const localCanvas = document.querySelector("#spatialLocalMapCanvas");
const attribution = document.querySelector("#spatialProviderAttribution");
const providerState = document.querySelector("#spatialProviderState");

let sourceBytes = null;
let sourceFileName = null;
let candidateRecord = null;
let baselineRecord = null;
let activeController = null;
let selectionRevision = 0;
let baselineSelectionRevision = 0;

function reject(code) {
  throw Object.assign(new Error(code), { code });
}

function requireCurrentRevision(revision) {
  if (!revisionIsCurrent(revision, selectionRevision)) reject("STALE_FILE_SELECTION");
}

function hex(bytes) {
  return [...new Uint8Array(bytes)].map(value => value.toString(16).padStart(2, "0")).join("");
}

async function sha256(bytes) {
  if (!globalThis.crypto?.subtle) reject("WEB_CRYPTO_UNAVAILABLE");
  return hex(await globalThis.crypto.subtle.digest("SHA-256", bytes));
}

function newController(revision) {
  activeController?.destroy();
  const fallbackRenderer = new LocalSvgRenderer({ container: localCanvas, attributionElement: attribution });
  return new MapProductController({
    provider: new OpenFreeMapProviderAdapter(),
    fallbackRenderer,
    timeoutMs: 8000,
    onState: ({ state, detail }) => {
      if (!revisionIsCurrent(revision, selectionRevision)) return;
      providerState.textContent = `providerState=${state}${detail ? `\ndetail=${detail}` : ""}`;
    }
  });
}

async function readLocalKml(file, revision = selectionRevision) {
  if (!file || !/\.kml$/i.test(file.name)) reject("KML_FILE_REQUIRED");
  if (!Number.isSafeInteger(file.size) || file.size <= 0 || file.size > MAX_BYTES) reject("KML_SIZE_OUT_OF_BOUNDS");
  const bytes = new Uint8Array(await file.arrayBuffer());
  requireCurrentRevision(revision);
  const actualHash = await sha256(bytes);
  requireCurrentRevision(revision);
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  const model = importKmlFile({ name: file.name, text });
  if (exportKml(model) !== text) reject("SOURCE_EXACT_EXPORT_MISMATCH");
  const parsed = validateIndonesiaFeatureCollection(toGeoJsonFeatureCollection(model));
  requireCurrentRevision(revision);
  return {
    bytes,
    fileName: file.name,
    sha256: actualHash,
    geometry: parsed.polygon,
    pointPlacemarkCount: parsed.pointPlacemarkCount,
    pointEvidence: parsed.pointEvidence,
    displayLabels: parsed.displayLabels,
    revision
  };
}

function updateBaselineComparison() {
  if (!baselineRecord) {
    baselineComparison.textContent = "未选择本地基线。";
    return;
  }
  if (!candidateRecord) {
    baselineComparison.textContent = `本地冻结基线已读取：sha256=${baselineRecord.sha256}\n等待候选 KML。`;
    return;
  }
  const candidateRing = candidateRecord.geometry.coordinates[0];
  const baselineRing = baselineRecord.geometry.coordinates[0];
  const maximumDifferenceMeters = maximumRingDifferenceMeters(candidateRing, baselineRing);
  baselineComparison.textContent = [
    "LOCAL_BASELINE_COMPARISON_READY",
    `candidateSha256=${candidateRecord.sha256}`,
    `baselineSha256=${baselineRecord.sha256}`,
    `maximumVertexDifferenceMeters=${maximumDifferenceMeters.toFixed(6)}`,
    "NUMERIC_CORRECTNESS_GRANTED=false",
    "POINT_ID_CORRECTNESS_GRANTED=false",
    "FILES_UPLOADED=0"
  ].join("\n");
}

function renderCandidateStatus(record) {
  const bindingStatus = record.sha256 === FROZEN_B_REFERENCE_SHA256
    ? "FROZEN_B_HASH_MATCH"
    : baselineRecord?.sha256 === FROZEN_B_REFERENCE_SHA256
      ? "LOCAL_FROZEN_B_BASELINE_BOUND"
      : "UNBOUND_REQUIRES_LOCAL_FROZEN_B_BASELINE";
  acceptanceState.textContent = [
    "REVIEW: KML_MAP_AND_DOWNLOAD_EVIDENCE_ONLY",
    `candidateSha256=${record.sha256}`,
    `sampleBinding=${bindingStatus}`,
    `pointPlacemarkCount=${record.pointPlacemarkCount}`,
    `pointEvidence=${record.pointEvidence}`,
    "outerVertexCount=16",
    "polygonCount=1",
    `mapAcceptance=${record.mapAcceptance}`,
    `mapReady=${record.mapAcceptance === "PASS"}`,
    "numericReview=NUMERIC_CORRECTNESS_REVIEW_PENDING",
    "pointIdReview=POINT_ID_REVIEW_PENDING",
    "LOCAL_ACCEPTANCE_DOWNLOAD_ELIGIBLE=true",
    "MODEL_CORRECTNESS=NOT_INFERRED_FROM_MAP_READY",
    "REAL_POSITION_AUTHORITY=NOT_GRANTED_BY_MAP_DISPLAY",
    "KML_UPLOADS=0",
    "MODEL_CALLS=0"
  ].join("\n");
}

function renderCoordinateTable(record) {
  coordinateRows.replaceChildren();
  record.geometry.coordinates[0].slice(0, -1).forEach((position, index) => {
    const row = document.createElement("tr");
    const label = document.createElement("td");
    const longitude = document.createElement("td");
    const latitude = document.createElement("td");
    label.textContent = record.displayLabels[index];
    longitude.textContent = Number(position[0]).toFixed(10);
    latitude.textContent = Number(position[1]).toFixed(10);
    row.append(label, longitude, latitude);
    coordinateRows.append(row);
  });
}

async function acceptSelectedKml(file, revision) {
  const record = await readLocalKml(file, revision);
  requireCurrentRevision(revision);

  sourceBytes = record.bytes;
  sourceFileName = record.fileName;
  candidateRecord = record;
  record.mapAcceptance = "PENDING";
  downloadButton.disabled = false;
  renderCoordinateTable(record);
  updateBaselineComparison();
  renderCandidateStatus(record);

  activeController = newController(revision);
  localCanvas.hidden = true;
  providerCanvas.hidden = false;
  const preview = Object.freeze({
    schemaVersion: "map_preview_object_v1",
    sourceResultId: `offline-kml-${record.sha256.slice(0, 16)}`,
    sourceRevision: 1,
    sourceGeometryHash: record.sha256,
    crs: { id: "EPSG:4326" },
    axisOrder: "longitude_latitude",
    geometry: record.geometry,
    previewEligibility: { allowed: true },
    previewWarnings: ["NON_PRODUCTION_ACCEPTANCE_ONLY", "REAL_POSITION_ACCEPTANCE_REQUIRES_VISIBLE_PROVIDER_READY"]
  });
  const result = await activeController.open(preview, {
    expectedIdentity: {
      sourceResultId: preview.sourceResultId,
      sourceRevision: preview.sourceRevision,
      sourceGeometryHash: preview.sourceGeometryHash
    },
    authority: {
      kmlReady: false,
      technicalKmlReady: true,
      confirmationStatus: "pending",
      qualityGateStatus: "review_required",
      decisionState: "NON_PRODUCTION_REVIEW_REQUIRED",
      kmlHash: record.sha256
    },
    publicConfig: {
      openFreeMapEnabled: true,
      openFreeMapStyleUrl: "https://tiles.openfreemap.org/styles/liberty",
      providerTimeoutMs: 8000
    },
    container: providerCanvas
  });
  requireCurrentRevision(revision);
  if (result.state !== "READY" || result.renderReceipt?.provider !== "OPENFREEMAP"
    || result.authorityPreserved !== true || result.authorityMutationCount !== 0) {
    record.mapAcceptance = "BLOCKED_REAL_GEOGRAPHIC_BASEMAP_NOT_READY";
    providerCanvas.hidden = true;
    localCanvas.hidden = false;
    providerState.textContent += "\nMAP_ACCEPTANCE=BLOCKED_REAL_GEOGRAPHIC_BASEMAP_NOT_READY\nKML_FILE_AND_DOWNLOAD_REMAIN_VALID=true";
    renderCandidateStatus(record);
    return;
  }
  record.mapAcceptance = "PASS";
  renderCandidateStatus(record);
}

fileInput.addEventListener("change", () => {
  const revision = ++selectionRevision;
  sourceBytes = null;
  sourceFileName = null;
  candidateRecord = null;
  downloadButton.disabled = true;
  activeController?.destroy();
  const waitingRow = document.createElement("tr");
  const waitingCell = document.createElement("td");
  waitingCell.colSpan = 3;
  waitingCell.textContent = "正在读取新候选；旧下载与旧地图已失效。";
  waitingRow.append(waitingCell);
  coordinateRows.replaceChildren(waitingRow);
  downloadReceipt.textContent = "尚未发起下载。";
  void acceptSelectedKml(fileInput.files?.[0], revision).catch(error => {
    if (!revisionIsCurrent(revision, selectionRevision)) return;
    if (error?.code === "STALE_FILE_SELECTION") return;
    updateBaselineComparison();
    if (candidateRecord?.revision === revision) {
      candidateRecord.mapAcceptance = "BLOCKED_REAL_GEOGRAPHIC_BASEMAP_EXCEPTION";
      renderCandidateStatus(candidateRecord);
      providerState.textContent = `MAP_ACCEPTANCE=BLOCKED: ${error?.code || "REAL_MAP_FAILED"}\nKML_FILE_AND_DOWNLOAD_REMAIN_VALID=true`;
      return;
    }
    acceptanceState.textContent = `BLOCKED: ${error?.code || "KML_FILE_INVALID"}`;
  });
});

baselineInput.addEventListener("change", () => {
  const baselineRevision = ++baselineSelectionRevision;
  const revision = selectionRevision;
  baselineRecord = null;
  updateBaselineComparison();
  if (candidateRecord) renderCandidateStatus(candidateRecord);
  void readLocalKml(baselineInput.files?.[0], revision).then(record => {
    if (!revisionIsCurrent(baselineRevision, baselineSelectionRevision)) reject("STALE_BASELINE_SELECTION");
    if (record.sha256 !== FROZEN_B_REFERENCE_SHA256) reject("FROZEN_B_BASELINE_SHA256_REQUIRED");
    requireCurrentRevision(revision);
    baselineRecord = record;
    updateBaselineComparison();
    if (candidateRecord) renderCandidateStatus(candidateRecord);
  }).catch(error => {
    if (!revisionIsCurrent(baselineRevision, baselineSelectionRevision)
      || !revisionIsCurrent(revision, selectionRevision)) return;
    if (error?.code === "STALE_BASELINE_SELECTION" || error?.code === "STALE_FILE_SELECTION") return;
    baselineRecord = null;
    baselineComparison.textContent = `BASELINE_BLOCKED: ${error?.code || "BASELINE_KML_INVALID"}`;
  });
});

downloadButton.addEventListener("click", () => {
  if (!sourceBytes || !candidateRecord || candidateRecord.revision !== selectionRevision) return;
  const url = URL.createObjectURL(new Blob([sourceBytes], { type: "application/vnd.google-earth.kml+xml" }));
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = sourceFileName || "candidate-indonesia-16.kml";
  anchor.rel = "noopener";
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
  downloadReceipt.textContent = [
    "BROWSER_DOWNLOAD_DISPATCHED",
    `expectedSha256=${candidateRecord.sha256}`,
    "OS_SAVED_FILE_HASH_VERIFICATION_REQUIRED=true"
  ].join("\n");
});
