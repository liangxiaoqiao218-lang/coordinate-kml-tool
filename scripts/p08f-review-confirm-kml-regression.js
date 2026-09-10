import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  CoordinateConfirmationRuntime,
  FINALIZED_COORDINATE_CRS,
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const clock = () => "2026-08-28T00:00:00.000Z";
const dumbaImageSha256 = "c080f48b23ac2926d622da040601e7bd7458208ce92f2693edce27ddc2be80f4";
const dumbaLine2HumanTruth = `11°43'09.20"N,09°00'56.03"W`;
const dumbaPoint2HumanTruth = Object.freeze([-9.015563888888888, 11.719222222222223]);
assert.equal(dumbaLine2HumanTruth, `11°43'09.20"N,09°00'56.03"W`);
assert.deepEqual(dumbaPoint2HumanTruth, Object.freeze([-9.015563888888888, 11.719222222222223]));
assert.equal(dumbaImageSha256.length, 64);
const dmsPolygon = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([-9.020463888888889, 11.72123611111111]),
    dumbaPoint2HumanTruth,
    Object.freeze([-9.016297222222223, 11.717605555555556]),
    Object.freeze([-9.02090277777778, 11.719805555555556]),
    Object.freeze([-9.020463888888889, 11.72123611111111])
  ])])
});

const acquisitionGroupSizes = Object.freeze([8, 4, 4]);
const acquisitionGroupIdentities = Object.freeze(["SITE:1", "SITE:2", "SITE:3"]);
function acquisitionDeltaGroups() {
  return acquisitionGroupSizes.map((size, groupIndex) => ({
    group_id: `group_${groupIndex + 1}`,
    group_name: `SITES${groupIndex + 1}`,
    geometry: "polygon",
    requires_review: true,
    kml_ready: false,
    points: Array.from({ length: size }, (_, pointIndex) => {
      const angle = (Math.PI * 2 * pointIndex) / size;
      return { label: String(pointIndex + 1), lon: -9 + groupIndex + Math.cos(angle) * 0.1, lat: 11 + groupIndex + Math.sin(angle) * 0.1 };
    })
  }));
}

function candidate(overrides = {}) {
  return {
    resultId: "p08f-review-result",
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    sourceAuthority: "legacy",
    coordinateType: "handwritten_dms_experimental",
    precisionMode: "handwritten-dms-coordinates",
    family: "handwritten_dms_experimental",
    availabilityStatus: "AVAILABLE",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: dmsPolygon,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: true,
    currentAuthorizedGeometryExportable: true,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    warnings: ["手写 DMS 识别结果需要对照原图人工核对。"],
    ...overrides
  };
}

const runtime = new CoordinateConfirmationRuntime({ ttlMs: 60_000, maxResults: 20, now: () => 1_000 });

const pending = finalizeCoordinateResult(candidate(), { clock });
runtime.register(pending);
assert.equal(pending.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED, "review pending stays blocked");
assert.equal(pending.kmlReady, true, "current authorized review-pending geometry is KML-ready with warning");
assert.equal(pending.technicalKmlReady, true, "review pending still has technical KML geometry");
assert.ok(pending.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED));
assert.ok(pending.reasonCodes.includes(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED));

const pendingPreview = new MapPreviewAdapter().adapt(pending, {
  expectedIdentity: {
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash
  },
  clock
});
assert.equal(pendingPreview.previewEligibility.allowed, true, "map preview can draw review-pending geometry");

const accepted = runtime.confirm({
  resultId: pending.resultId,
  resultRevision: pending.resultRevision,
  geometryHash: pending.geometryHash,
  action: "accept"
}).finalizedCoordinateResult;
assert.equal(accepted.confirmationStatus, COORDINATE_CONFIRMATION_STATUS.ACCEPTED);
assert.equal(accepted.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED, "review quality fact is preserved");
assert.equal(accepted.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT, "accepted review can export");
assert.equal(accepted.technicalKmlReady, true);
assert.equal(accepted.kmlReady, true, "accepted review KML allowed");
assert.equal(accepted.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED), false);
assert.equal(accepted.reasonCodes.includes(COORDINATE_GATE_REASON.REVIEW_REQUIRED), false);

const edited = finalizeCoordinateResult(candidate({
  resultRevision: 2,
  currentRevision: 2,
  confirmedRevision: null,
  geometry: {
    type: "Polygon",
    coordinates: [[
      [-9.020463888888889, 11.72123611111111],
      [...dumbaPoint2HumanTruth],
      [-9.016297222222223, 11.717605555555556],
      [-9.021, 11.719805555555556],
      [-9.020463888888889, 11.72123611111111]
    ]]
  }
}), { clock });
runtime.register(edited);
assert.equal(edited.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED, "edited result returns to pending review");
assert.equal(edited.kmlReady, true, "complete new revision remains KML-ready with warning");

const reconfirmed = runtime.confirm({
  resultId: edited.resultId,
  resultRevision: edited.resultRevision,
  geometryHash: edited.geometryHash,
  action: "accept"
}).finalizedCoordinateResult;
assert.equal(reconfirmed.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT, "reconfirmed edit can export");
assert.equal(reconfirmed.kmlReady, true);

const rejected = finalizeCoordinateResult(candidate({
  resultId: "p08f-rejected",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED
}), { clock });
assert.equal(rejected.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "rejected review blocks KML");
assert.equal(rejected.kmlReady, false);
assert.ok(rejected.reasonCodes.includes(COORDINATE_GATE_REASON.CONFIRMATION_REJECTED));

const qualityFailed = finalizeCoordinateResult(candidate({
  resultId: "p08f-quality-failed",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED,
  confirmedRevision: 1,
  qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.FAILED,
  technicalKmlReady: true,
  kmlReady: true
}), { clock });
assert.equal(qualityFailed.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "quality failed remains hard blocked");
assert.equal(qualityFailed.kmlReady, false);

const invalidGeometry = finalizeCoordinateResult(candidate({
  resultId: "p08f-invalid-geometry",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED,
  confirmedRevision: 1,
  geometry: { type: "Point", coordinates: [200, 11] },
  technicalKmlReady: false,
  kmlReady: true
}), { clock });
assert.equal(invalidGeometry.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "invalid geometry remains blocked");
assert.equal(invalidGeometry.kmlReady, false);

const noCoordinate = finalizeCoordinateResult(candidate({
  resultId: "p08f-no-coordinate",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED,
  confirmedRevision: 1,
  geometry: null,
  geometryFailureReason: COORDINATE_GATE_REASON.STRUCTURED_GEOMETRY_MISSING,
  technicalKmlReady: false,
  kmlReady: true
}), { clock });
assert.equal(noCoordinate.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "no coordinate geometry remains blocked");
assert.equal(noCoordinate.kmlReady, false);

const staleAccepted = finalizeCoordinateResult(candidate({
  resultId: "p08f-stale",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.ACCEPTED,
  confirmedRevision: 1,
  resultRevision: 2,
  currentRevision: 2,
  kmlReady: true
}), { clock });
assert.equal(staleAccepted.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "stale accepted revision remains blocked");
assert.equal(staleAccepted.kmlReady, false);

const cleanManual = finalizeCoordinateResult(candidate({
  resultId: "p08f-clean-manual",
  sourceAuthority: "manual_input",
  coordinateType: "standard_dms_table",
  precisionMode: "dms-coordinates",
  family: "standard_dms_table",
  confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
  confirmedRevision: null,
  qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
  technicalKmlReady: true,
  requiresReview: false,
  kmlReady: true,
  groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
  warnings: []
}), { clock });
assert.equal(cleanManual.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT, "clean manual DMS remains direct export");
assert.equal(cleanManual.kmlReady, true);
assert.deepEqual(cleanManual.geometry, dmsPolygon);

const acquisitionDeltaInput = createLegacyFinalizerInput({
  recognitionResult: { precisionMode: "dms-coordinates" },
  coordinateEngineV2: {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    acquisition_delta_provenance: {
      schemaVersion: "dms_grouped_acquisition_delta_v1",
      ownerFamily: "dms_grouped",
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      baselineRowCount: 13,
      retryRowCount: 16,
      addedRowCount: 3,
      baselineRowsPreserved: true,
      groupLocalBaselineRowsPreserved: true,
      baselineGroupCount: 3,
      baselineGroupSizes: [6, 4, 3],
      baselineGroupIdentities: acquisitionGroupIdentities,
      retryGroupCount: 3,
      retryGroupSizes: acquisitionGroupSizes,
      retryGroupIdentities: acquisitionGroupIdentities,
      strongBoundariesProven: true,
      labelsContinuous: true,
      sourceCandidateSeparate: true,
      directCanonicalPromotion: false,
      stage1CandidateSha256: "c".repeat(64),
      retryCandidateSha256: "d".repeat(64)
    },
    groups: acquisitionDeltaGroups()
  },
  verification: { status: "REVIEW", warnings: [] },
  revision: { resultId: "p08f-acquisition-delta", resultRevision: 1 }
});
const acquisitionPending = finalizeCoordinateResult(acquisitionDeltaInput, { clock });
runtime.register(acquisitionPending);
assert.equal(acquisitionPending.kmlReady, false, "expanded reread candidate blocks KML before confirmation");
assert.equal(new MapPreviewAdapter().adapt(acquisitionPending, { clock }).previewEligibility.allowed, false,
  "expanded reread candidate blocks Map before confirmation");
const acquisitionConfirmed = runtime.confirm({
  resultId: acquisitionPending.resultId,
  resultRevision: acquisitionPending.resultRevision,
  geometryHash: acquisitionPending.geometryHash,
  action: "accept"
}).finalizedCoordinateResult;
assert.equal(acquisitionConfirmed.kmlReady, true, "exact confirmed expanded candidate can release KML");
assert.equal(new MapPreviewAdapter().adapt(acquisitionConfirmed, { clock }).previewEligibility.allowed, true,
  "exact confirmed expanded candidate can release Map");
const malformedAcquisitionTopology = finalizeCoordinateResult(createLegacyFinalizerInput({
  recognitionResult: { precisionMode: "dms-coordinates" },
  coordinateEngineV2: {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    acquisition_delta_provenance: acquisitionDeltaInput.familySafetyPolicy.acquisitionDeltaProvenance,
    groups: acquisitionDeltaGroups().slice(0, 2)
  },
  verification: { status: "REVIEW", warnings: [] },
  revision: { resultId: "p08f-acquisition-malformed", resultRevision: 1 }
}), { clock });
assert.equal(malformedAcquisitionTopology.decisionState, COORDINATE_DECISION_STATE.BLOCKED,
  "two-group acquisition provenance cannot reach confirmation or consumers");

const html = await readFile(path.join(repoRoot, "index.html"), "utf8");
function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${functionName} body is not closed`);
}
const finalizedKmlGateSource = extractFunctionSource(html, "shouldBlockFinalizedCoordinateKml");
const downloadKmlSource = extractFunctionSource(html, "downloadKml");
const downloadKmlInternalSource = extractFunctionSource(html, "downloadKmlInternal");
const clearKmlGenerationProgressSource = extractFunctionSource(html, "clearKmlGenerationProgress");
const showKmlCompletionSource = extractFunctionSource(html, "showKmlCompletion");
assert.match(html, /function shouldBlockFinalizedCoordinateKml\(\)/);
assert.match(finalizedKmlGateSource, /activeFinalizedCoordinateResult\.kmlReady !== true/);
assert.doesNotMatch(finalizedKmlGateSource, /activeFinalizedCoordinateResult\.decisionState !== "AUTO_EXPORT"/);
assert.match(html, /fetch\("\/api\/coordinate-confirmation"/);
assert.match(html, /fetch\("\/api\/coordinate-revision"/);
assert.match(html, /if \(activeFinalizedCoordinateResult\) finalizedCoordinateDirty = true/);
assert.match(downloadKmlInternalSource, /showRecognitionProgress\("正在生成 KML 文件\.\.\."\s*,\s*"loading"\)/, "KML download enters explicit generating state");
assert.match(downloadKmlSource, /catch \(error\) \{\s*clearKmlGenerationProgress\(\)/, "exception clears generating state before error UI");
assert.match(downloadKmlSource, /finally \{\s*clearKmlGenerationProgress\(\)/, "all success and blocked returns pass through final cleanup");
assert.match(clearKmlGenerationProgressSource, /classList\.contains\("loading"\)/, "cleanup does not erase a visible success or error state");
assert.match(showKmlCompletionSource, /clearKmlGenerationProgress\(\);\s*showRecognitionProgress\(text, warning \? "warning" : "success"/, "success cleanup is explicit and independent of message wording");
assert.doesNotMatch(downloadKmlInternalSource, /showMessage\(`已[^`]*KML/, "KML success no longer relies on Chinese message classification");
assert.match(downloadKmlInternalSource, /consumeUsage\("convert"\)/, "quota consumption remains at the authorized KML operation boundary");

console.log(JSON.stringify({
  suite: "p08f-review-confirm-kml-regression",
  passed: 28,
  cases: [
    "REVIEW_PENDING_READY_WITH_WARNING",
    "REVIEW_PENDING_MAP_PREVIEW_ALLOWED",
    "REVIEW_ACCEPTED_KML_ALLOWED",
    "QUALITY_REVIEW_FACT_PRESERVED",
    "EDIT_COMPLETE_IDENTITY_READY_WITH_WARNING",
    "RECONFIRM_ALLOWED",
    "REVIEW_REJECTED_BLOCKED",
    "QUALITY_FAILED_BLOCKED",
    "INVALID_GEOMETRY_BLOCKED",
    "NO_COORDINATE_BLOCKED",
    "STALE_RESULT_BLOCKED",
    "DIRECT_MANUAL_FINALIZE_PRESERVED",
    "ACQUISITION_DELTA_PENDING_KML_BLOCKED",
    "ACQUISITION_DELTA_PENDING_MAP_BLOCKED",
    "ACQUISITION_DELTA_CONFIRMED_KML_ALLOWED",
    "ACQUISITION_DELTA_CONFIRMED_MAP_ALLOWED",
    "ACQUISITION_DELTA_MALFORMED_TOPOLOGY_BLOCKED",
    "FRONTEND_CONSUMES_SERVER_KML_READY",
    "CONFIRMATION_UI_BOUND",
    "REVISION_UI_BOUND",
    "EDIT_INVALIDATES_CONFIRMATION",
    "KML_GENERATING_STATE_EXPLICIT",
    "KML_EXCEPTION_CLEANUP",
    "KML_FINALLY_CLEANUP",
    "KML_BLOCKED_RETURN_CLEANUP",
    "KML_SUCCESS_STATE_PRESERVED",
    "KML_SUCCESS_MESSAGE_INDEPENDENT",
    "KML_QUOTA_BOUNDARY_PRESERVED"
  ]
}, null, 2));
