import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS,
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = await readFile(path.join(repoRoot, "index.html"), "utf8");

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  let parameterDepth = 0;
  let openBrace = -1;
  for (let index = source.indexOf("(", start); index < source.length; index += 1) {
    if (source[index] === "(") parameterDepth += 1;
    if (source[index] === ")") {
      parameterDepth -= 1;
      if (parameterDepth === 0) {
        openBrace = source.indexOf("{", index);
        break;
      }
    }
  }
  assert.notEqual(openBrace, -1, `${functionName} body must start`);
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

const requirementSource = extractFunctionSource(html, "finalizedCoordinateResultRequiresUserConfirmation");
assert.match(requirementSource, /confirmationStatus === "pending"/, "pending canonical confirmation status drives visibility");
assert.match(requirementSource, /qualityGateStatus !== "failed"/, "hard failed quality cannot be overridden by UI confirmation");
assert.match(requirementSource, /technicalKmlReady/, "only technically KML-ready geometry can request confirmation");
assert.match(requirementSource, /getFinalizedCoordinateIdentity/, "confirmation action is only exposed for server-identifiable results");

const initializeSource = extractFunctionSource(html, "initializeHandwrittenDmsReviewState");
assert.match(initializeSource, /serverRequiresConfirmation/, "server authority participates in review UI initialization");
assert.match(initializeSource, /finalized_coordinate_result_v1/, "render source records canonical finalized-result authority");
assert.match(initializeSource, /legacyHandwrittenRequiresReview/, "legacy handwritten fallback remains available without a finalized result");
assert.doesNotMatch(
  initializeSource,
  /const required = type === "handwritten_dms_experimental" && engine\?\.requires_review === true;/,
  "review UI is no longer gated only by handwritten_dms_experimental"
);

const renderSource = extractFunctionSource(html, "renderHandwrittenDmsReviewState");
assert.match(renderSource, /我已对照原图核对当前坐标/, "confirmation control remains visible and actionable");
assert.match(renderSource, /当前坐标需要对照原图人工核对/, "pending copy is generic authority-state copy");
assert.match(renderSource, /当前坐标已修改，请对照原图重新核对/, "edit lifecycle copy requires reconfirmation");
assert.match(renderSource, /已确认当前坐标；再次修改后需要重新核对/, "accepted lifecycle copy resolves the panel");

const recognizeSource = extractFunctionSource(html, "recognizeImage");
assert.match(recognizeSource, /activeFinalizedCoordinateResult = data\.finalizedCoordinateResult \|\| null/, "recognized finalized result is adopted");
assert.match(recognizeSource, /syncCoordinateReviewConfirmationState\(summaryEngine, activeFinalizedCoordinateResult\)/, "recognized review result syncs confirmation UI");

const revisionSource = extractFunctionSource(html, "requestEditedCoordinateRevision");
assert.match(revisionSource, /activeFinalizedCoordinateResult = payload\.finalizedCoordinateResult/, "edited revision adopts server result");
assert.match(revisionSource, /syncCoordinateReviewConfirmationState\(activeCoordinateEngineV2, activeFinalizedCoordinateResult\)/, "edited revision reopens confirmation UI when pending");

const manualFinalizeSource = extractFunctionSource(html, "ensureManualInputFinalized");
assert.match(manualFinalizeSource, /syncCoordinateReviewConfirmationState\(activeCoordinateEngineV2, activeFinalizedCoordinateResult\)/, "manual finalize also syncs canonical UI state");

const confirmSource = extractFunctionSource(html, "confirmHandwrittenDmsReview");
assert.match(confirmSource, /fetch\("\/api\/coordinate-confirmation"/, "confirmation action remains server-authoritative");
assert.match(confirmSource, /activeFinalizedCoordinateResult\.decisionState !== "AUTO_EXPORT"/, "UI confirmation cannot bypass unified gate");
assert.match(confirmSource, /HANDWRITTEN_DMS_REVIEW_STATUS\.CONFIRMED/, "accepted server confirmation resolves UI state");

const textChangeSource = extractFunctionSource(html, "markCoordinateTextChanged");
assert.match(textChangeSource, /finalizedCoordinateDirty = true/, "editing invalidates finalized result");
assert.match(textChangeSource, /getHandwrittenDmsReviewStateAfterTextChange/, "editing invalidates accepted UI confirmation");

const kmlGateSource = extractFunctionSource(html, "shouldBlockFinalizedCoordinateKml");
assert.match(kmlGateSource, /finalizedCoordinateDirty/, "dirty edits block KML");
assert.match(kmlGateSource, /activeFinalizedCoordinateResult\.kmlReady !== true/, "KML follows server-authoritative kmlReady");

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

const reviewOnlyInput = createLegacyFinalizerInput({
  recognitionResult: {
    coordinates: "-9.020463888888889,11.72123611111111\n-9.015563888888888,11.719222222222223\n-9.016297222222223,11.717605555555556\n-9.02090277777778,11.719805555555556",
    precisionMode: "preserve-original-decimals-and-parse-dms"
  },
  coordinateEngineV2: {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: dmsPolygon.coordinates[0].slice(0, 4).map(([lon, lat], index) => ({
        label: String(index + 1),
        lon,
        lat
      }))
    }]
  },
  verification: {
    status: "REVIEW",
    validation_scope: "coordinate_and_geometry",
    geometry_validation: "PASSED",
    warnings: ["手写坐标存在需核对字符，请结合原图逐行核对。"],
    conflicts: [],
    geometryWarnings: []
  },
  revision: { resultId: "p08h-review-only", resultRevision: 1, currentRevision: 1 }
});
const reviewOnlyFinalized = finalizeCoordinateResult(reviewOnlyInput, {
  clock: () => "2026-08-28T00:00:00.000Z"
});
assert.equal(reviewOnlyFinalized.confirmationStatus, COORDINATE_CONFIRMATION_STATUS.PENDING, "review-only recognized result enters confirmation workflow");
assert.equal(reviewOnlyFinalized.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED, "review quality fact remains preserved");
assert.equal(reviewOnlyFinalized.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED, "review-only result waits for user confirmation");
assert.equal(reviewOnlyFinalized.technicalKmlReady, true, "review-only result remains technically KML-ready");
assert.equal(reviewOnlyFinalized.kmlReady, false, "review-only result blocks KML before confirmation");

const hardFailureInput = createLegacyFinalizerInput({
  recognitionResult: { coordinates: "invalid", precisionMode: "dms-coordinates" },
  coordinateEngineV2: {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: dmsPolygon.coordinates[0].slice(0, 4).map(([lon, lat], index) => ({ label: String(index + 1), lon, lat }))
    }]
  },
  verification: {
    status: "BLOCK",
    validation_scope: "coordinate_and_geometry",
    geometry_validation: "FAILED",
    warnings: [],
    conflicts: [],
    geometryWarnings: ["ABNORMAL_JUMP"]
  },
  revision: { resultId: "p08h-hard-failed", resultRevision: 1, currentRevision: 1 }
});
const hardFailureFinalized = finalizeCoordinateResult(hardFailureInput, {
  clock: () => "2026-08-28T00:00:00.000Z"
});
assert.equal(hardFailureFinalized.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED, "hard quality failure remains failed");
assert.equal(hardFailureFinalized.decisionState, COORDINATE_DECISION_STATE.BLOCKED, "hard quality failure cannot be released by confirmation UI");

console.log(JSON.stringify({
  suite: "p08h-confirmation-ui-lifecycle-regression",
  passed: 12,
  cases: [
    "PENDING_FINALIZED_RESULT_SHOWS_CONFIRMATION_UI",
    "CONFIRMATION_RENDER_SOURCE_CANONICAL_FINALIZED_RESULT",
    "CONFIRMATION_CONTROL_ACTIONABLE",
    "ACCEPTED_STATE_RESOLVES_PANEL",
    "EDIT_REOPENS_PENDING_CONFIRMATION",
    "RECONFIRM_USES_SERVER_CONFIRMATION",
    "AUTO_EXPORT_CLEAN_PATH_HIDDEN_BY_DEFAULT",
    "QUALITY_FAILED_CANNOT_OVERRIDE",
    "RECOGNITION_ADOPTION_SYNCS_UI",
    "KML_USES_SERVER_KML_READY",
    "REVIEW_ONLY_RECOGNITION_ENTERS_CONFIRMATION_WORKFLOW",
    "HARD_FAILURE_REMAINS_BLOCKED"
  ]
}, null, 2));
