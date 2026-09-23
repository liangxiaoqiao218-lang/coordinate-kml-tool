import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = await readFile(path.join(repoRoot, "index.html"), "utf8");

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const functionStart = source.indexOf(marker);
  assert.notEqual(functionStart, -1, `${functionName} must exist`);
  let start = functionStart;
  const asyncPrefixStart = source.lastIndexOf("async ", functionStart);
  if (asyncPrefixStart >= 0 && source.slice(asyncPrefixStart + "async ".length, functionStart) === "") {
    start = asyncPrefixStart;
  }
  let parameterDepth = 0;
  let openBrace = -1;
  for (let index = source.indexOf("(", functionStart); index < source.length; index += 1) {
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

const dynamicFunctionNames = [
  "hasFiniteFinalizedGeometry",
  "getFinalizedCoordinateIdentity",
  "cloneIdentityBoundValue",
  "deepFreezeIdentityBoundValue",
  "canonicalIdentityBoundSerialization",
  "identityBoundValuesEqual",
  "finalizedResultMatchesIdentityBoundSnapshot",
  "createIdentityBoundSnapshot",
  "rememberLastValidIdentityBoundSnapshot",
  "snapshotMatchesCurrentDraft",
  "revalidateLastValidIdentityBoundSnapshot",
  "revertToLastValidIdentityBoundSnapshot",
  "invalidateSpatialPreview",
  "refreshMapPreviewAction",
  "coordinateKmlVisualState",
  "syncKmlActionVisualState",
  "markCoordinateTextChanged",
  "shouldBlockFinalizedCoordinateKml",
  "getAuthorizedFinalizedGeometryKmlSource",
  "undoLastChange"
];

const dynamicSources = dynamicFunctionNames.map(name => extractFunctionSource(html, name)).join("\n\n");

function createButton(label) {
  return {
    label,
    hidden: false,
    disabled: false,
    dataset: {},
    textContent: "",
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    }
  };
}

const createHarness = new Function(`
  const HANDWRITTEN_DMS_REVIEW_STATUS = Object.freeze({
    NOT_APPLICABLE: "NOT_APPLICABLE",
    PENDING: "PENDING",
    EDITED_PENDING: "EDITED_PENDING",
    CONFIRMED: "CONFIRMED"
  });
  const historyStack = [];
  const input = { value: "", focus() { events.push({ type: "focus" }); } };
  const coordinateOrder = { value: "auto" };
  const pages = { spatialResult: { dataset: {}, classList: { contains() { return false; } } } };
  const mapPreviewAction = (${createButton.toString()})("map");
  const coordinateKmlAction = (${createButton.toString()})("coordinate-kml");
  const spatialKmlAction = (${createButton.toString()})("spatial-kml");
  const events = [];
  let activeCoordinatePrecisionMode = "";
  let agenticCoordinateController = null;
  let activeCoordinateEngineV2 = null;
  let activeCoordinateFieldConflictCount = 0;
  let activeFinalizedCoordinateResult = null;
  let finalizedCoordinateTextSnapshot = "";
  let finalizedCoordinateDirty = false;
  let lastValidIdentityBoundSnapshot = null;
  let kmlGenerationInProgress = false;
  let spatialResultEnabled = true;
  let activeMapPreviewResponse = null;
  let activeMapPreviewCacheKey = "";
  let internalKmlSourceDirty = false;
  let handwrittenDmsReviewState = { required: false, status: "NOT_APPLICABLE", revision: 0, confirmedRevision: null };
  let fetchImpl = async () => { throw new Error("fetch mock not configured"); };
  function projectedCrsSelectionNeedsConfirmation() { return false; }
  function renderProjectedCrsReviewPanel() {}

  function fetch(url, options) { return fetchImpl(url, options); }
  function getSourceHeaders(headers = {}) { return headers; }
  function normalizeManualCoordinateTextForFinalizer(value) { return String(value || "").trim(); }
  function createHandwrittenDmsReviewState(overrides = {}) {
    return { required: false, status: HANDWRITTEN_DMS_REVIEW_STATUS.NOT_APPLICABLE, revision: 0, confirmedRevision: null, source: "none", ...overrides };
  }
  function getHandwrittenDmsReviewStateAfterTextChange(state) {
    if (!state?.required) return createHandwrittenDmsReviewState();
    return createHandwrittenDmsReviewState({
      ...state,
      status: state.status === HANDWRITTEN_DMS_REVIEW_STATUS.CONFIRMED ? HANDWRITTEN_DMS_REVIEW_STATUS.EDITED_PENDING : state.status,
      revision: Number(state.revision || 0) + 1,
      confirmedRevision: state.status === HANDWRITTEN_DMS_REVIEW_STATUS.CONFIRMED ? null : state.confirmedRevision
    });
  }
  function syncCoordinateReviewConfirmationState(engine = activeCoordinateEngineV2 || {}, result = activeFinalizedCoordinateResult) {
    events.push({ type: "syncConfirmation", resultId: result?.resultId || null, engine: engine?.coordinate_type || engine?.coordinateType || null });
    handwrittenDmsReviewState = result?.confirmationStatus === "pending"
      ? createHandwrittenDmsReviewState({ required: true, status: HANDWRITTEN_DMS_REVIEW_STATUS.PENDING, source: "finalized_coordinate_result_v1" })
      : createHandwrittenDmsReviewState();
  }
  function refreshSpatialShareActions() {}
  function renderHandwrittenDmsReviewState() { events.push({ type: "renderReview", status: handwrittenDmsReviewState.status }); }
  function clearRecognitionSummary() {}
  function setCoordinateOrderReviewVisible() {}
  function updateCadastralGridPanel() { events.push({ type: "updateCadastralGridPanel" }); }
  function showMessage(message, isError) { events.push({ type: "message", message, isError: isError === true }); }
  function trackEvent(name) { events.push({ type: "track", name }); }
  function getCoordinateFlowUserMessage(code, fallback) { return code || fallback; }

  ${dynamicSources}

  return {
    input,
    historyStack,
    mapPreviewAction,
    coordinateKmlAction,
    spatialKmlAction,
    setFetch(fn) { fetchImpl = fn; },
    setSpatialEnabled(value) { spatialResultEnabled = value === true; refreshMapPreviewAction(); },
    setActive(result, text, engine = { coordinate_type: "mock_family" }, conflictCount = 0) {
      activeFinalizedCoordinateResult = result;
      activeCoordinateEngineV2 = engine;
      activeCoordinateFieldConflictCount = conflictCount;
      input.value = text;
      finalizedCoordinateTextSnapshot = text;
      finalizedCoordinateDirty = false;
      rememberLastValidIdentityBoundSnapshot(result, text);
      refreshMapPreviewAction();
    },
    setSnapshot(snapshot) { lastValidIdentityBoundSnapshot = snapshot; },
    setInput(value) { input.value = value; },
    markCoordinateTextChanged,
    rememberLastValidIdentityBoundSnapshot,
    createIdentityBoundSnapshot,
    snapshotMatchesCurrentDraft,
    revalidateLastValidIdentityBoundSnapshot,
    revertToLastValidIdentityBoundSnapshot,
    undoLastChange,
    coordinateKmlVisualState,
    shouldBlockFinalizedCoordinateKml,
    getAuthorizedFinalizedGeometryKmlSource,
    refreshMapPreviewAction,
    state() {
      return {
        text: input.value,
        dirty: finalizedCoordinateDirty,
        snapshotText: finalizedCoordinateTextSnapshot,
        activeResult: activeFinalizedCoordinateResult,
        activeIdentity: getFinalizedCoordinateIdentity(),
        lastSnapshot: lastValidIdentityBoundSnapshot,
        engine: activeCoordinateEngineV2,
        conflictCount: activeCoordinateFieldConflictCount,
        kmlVisualState: coordinateKmlVisualState(),
        kmlBlocked: shouldBlockFinalizedCoordinateKml(),
        kmlSourcePresent: Boolean(getAuthorizedFinalizedGeometryKmlSource()),
        mapState: mapPreviewAction.dataset.state,
        mapDisabled: mapPreviewAction.disabled,
        mapText: mapPreviewAction.textContent,
        coordinateKmlState: coordinateKmlAction.dataset.state,
        coordinateKmlText: coordinateKmlAction.textContent,
        events: [...events]
      };
    }
  };
`);

function geometry() {
  return {
    type: "Polygon",
    coordinates: [[
      [104.1, 11.5],
      [104.101, 11.5],
      [104.101, 11.501],
      [104.1, 11.501],
      [104.1, 11.5]
    ]]
  };
}

function finalizedResult(overrides = {}) {
  const revision = overrides.resultRevision ?? 1;
  const hash = overrides.geometryHash || `hash-${revision}`;
  return {
    schemaVersion: "finalized_coordinate_result_v1",
    resultId: overrides.resultId || "result-1",
    resultRevision: revision,
    geometryHash: hash,
    geometry: overrides.geometry || geometry(),
    crs: overrides.crs || { id: "EPSG:4326", axisOrder: "longitude_latitude" },
    coordinateType: overrides.coordinateType || "decimal_latlon",
    precisionMode: overrides.precisionMode || "manual-decimal",
    sourceAuthority: overrides.sourceAuthority || "manual_input",
    decisionState: overrides.decisionState || "AUTO_EXPORT",
    confirmationStatus: overrides.confirmationStatus || "not_required",
    qualityGateStatus: overrides.qualityGateStatus || "passed",
    requiresReview: overrides.requiresReview === true,
    technicalKmlReady: overrides.technicalKmlReady ?? true,
    kmlReady: overrides.kmlReady ?? true,
    reasonCodes: overrides.reasonCodes || [],
    blockingReasons: overrides.blockingReasons || []
  };
}

function jsonResponse(ok, status, body) {
  return {
    ok,
    status,
    async json() {
      return body;
    }
  };
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

const dynamicCases = [];
const staticAssertions = [];
function passDynamic(name) {
  dynamicCases.push(name);
}
function passStatic(name) {
  staticAssertions.push(name);
}

const textA = "104.100000,11.500000\n104.101000,11.500000\n104.101000,11.501000\n104.100000,11.501000";
const textInvalid = "104.100000,11.500000\nNOT_A_COORDINATE";

{
  const h = createHarness();
  const result = finalizedResult();
  h.setActive(result, textA, { coordinate_type: "decimal_latlon", warning: "mock" }, 2);
  const snapshot = h.state().lastSnapshot;
  assert.equal(snapshot.snapshotRole, "RECOVERY_INPUT_AND_UI_CONTEXT");
  assert.equal(snapshot.snapshotAuthority, "NON_AUTHORITATIVE");
  assert.equal(snapshot.resultIdentity.resultId, result.resultId);
  assert.equal(snapshot.resultIdentity.resultRevision, result.resultRevision);
  assert.equal(snapshot.resultIdentity.geometryHash, result.geometryHash);
  assert.equal(snapshot.sourceText, textA);
  assert.equal(snapshot.geometry.coordinates[0][0][0], 104.1);
  assert.equal(snapshot.crs.id, "EPSG:4326");
  assert.equal(snapshot.axisOrder, "longitude_latitude");
  assert.equal(snapshot.recognitionFamily, "decimal_latlon");
  assert.equal(snapshot.engineContext.coordinate_type, "decimal_latlon");
  assert.equal(snapshot.fieldConflictContext.count, 2);
  assert.equal(snapshot.mapEligibility.allowed, true);
  assert.equal(snapshot.kmlEligibility.allowed, true);
  passDynamic("VALID_RESULT_CREATES_NON_AUTHORITY_SNAPSHOT");
  assert.equal(Object.isFrozen(snapshot), true);
  assert.equal(Object.isFrozen(snapshot.geometry), true);
  assert.equal(Object.isFrozen(snapshot.geometry.coordinates), true);
  assert.equal(Object.isFrozen(snapshot.geometry.coordinates[0]), true);
  assert.equal(Object.isFrozen(snapshot.finalizedResult), true);
  assert.equal(Object.isFrozen(snapshot.finalizedResult.geometry.coordinates[0]), true);
  passDynamic("SNAPSHOT_NESTED_STATE_DEEP_IMMUTABLE");

  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  const dirty = h.state();
  assert.equal(dirty.dirty, true);
  passDynamic("EDIT_MARKS_DRAFT_DIRTY");
  assert.equal(dirty.kmlBlocked, true);
  assert.equal(dirty.kmlSourcePresent, false);
  passDynamic("DIRTY_DRAFT_DOES_NOT_CONSUME_OLD_KML");
  assert.equal(dirty.mapState, "validation_required");
  assert.equal(dirty.mapDisabled, false);
  passDynamic("DIRTY_DRAFT_DOES_NOT_CONSUME_OLD_MAP");
  assert.equal(dirty.coordinateKmlState, "validation_required");
  assert.equal(dirty.coordinateKmlText, "下载 KML");
  assert.equal(dirty.mapText, "查看地图");
  passDynamic("DIRTY_UI_STATE_IS_VALIDATION_REQUIRED");
  assert.equal(h.state().lastSnapshot.sourceText, textA);
  passDynamic("INVALID_EDIT_REVISION_FAILURE_PRESERVES_SNAPSHOT");
}

{
  const h = createHarness();
  const result = finalizedResult({ resultRevision: 3, geometryHash: "hash-3" });
  h.setActive(result, textA, { coordinate_type: "decimal_latlon" }, 1);
  h.historyStack.push(textA);
  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  const calls = [];
  h.setFetch(async (url, options) => {
    calls.push({ url, body: JSON.parse(options.body), dirtyAtCall: h.state().dirty, kmlAtCall: h.state().coordinateKmlState, mapAtCall: h.state().mapState });
    return jsonResponse(true, 200, { finalizedCoordinateResult: result });
  });
  await h.undoLastChange();
  assert.equal(h.state().text, textA);
  passDynamic("UNDO_RESTORES_SOURCE_TEXT_BUT_REMAINS_DIRTY_PENDING_SERVER");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/api/coordinate-manual-finalize");
  assert.equal(calls[0].dirtyAtCall, true);
  assert.equal(calls[0].kmlAtCall, "validation_required");
  assert.equal(calls[0].mapAtCall, "validation_required");
  passDynamic("UNDO_CALLS_EXISTING_RECOVERY_ENDPOINT_ONCE");
  assert.equal(calls[0].body.recoveryIdentity.resultId, result.resultId);
  passDynamic("RECOVERY_REQUEST_BINDS_RESULT_ID");
  assert.equal(calls[0].body.recoveryIdentity.resultRevision, result.resultRevision);
  passDynamic("RECOVERY_REQUEST_BINDS_RESULT_REVISION");
  assert.equal(calls[0].body.recoveryIdentity.geometryHash, result.geometryHash);
  passDynamic("RECOVERY_REQUEST_BINDS_GEOMETRY_HASH");
  assert.equal(h.state().activeResult, result);
  assert.equal(h.state().dirty, false);
  passDynamic("SERVER_SUCCESS_ADOPTS_SERVER_RETURNED_RESULT");
  assert.equal(h.state().events.some(event => event.type === "syncConfirmation" && event.resultId === result.resultId), true);
  passDynamic("SERVER_SUCCESS_REBUILDS_CONFIRMATION_STATE");
  assert.equal(h.state().engine.coordinate_type, "decimal_latlon");
  assert.equal(h.state().conflictCount, 1);
  passDynamic("SERVER_SUCCESS_RESTORES_ENGINE_CONFLICT_UI_CONTEXT");
  assert.equal(h.state().coordinateKmlState, "enabled");
  assert.equal(h.state().kmlSourcePresent, true);
  passDynamic("SERVER_SUCCESS_RECOMPUTES_KML_ELIGIBILITY");
}

{
  const h = createHarness();
  const reviewResult = finalizedResult({
    resultId: "result-map-only",
    resultRevision: 1,
    geometryHash: "hash-map-only",
    kmlReady: false,
    decisionState: "REVIEW_REQUIRED",
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    requiresReview: true,
    reasonCodes: ["CONFIRMATION_REQUIRED"],
    blockingReasons: [{ code: "CONFIRMATION_REQUIRED" }]
  });
  h.setActive(reviewResult, textA);
  assert.equal(h.state().lastSnapshot.mapEligibility.allowed, true);
  assert.equal(h.state().lastSnapshot.kmlEligibility.allowed, false);
  h.historyStack.push(textA);
  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  h.setFetch(async () => jsonResponse(true, 200, { finalizedCoordinateResult: reviewResult }));
  await h.undoLastChange();
  assert.equal(h.state().mapState, "enabled");
  assert.equal(h.state().coordinateKmlState, "blocked");
  passDynamic("SERVER_SUCCESS_RECOMPUTES_MAP_ELIGIBILITY_INDEPENDENTLY");
}

async function assertRecoveryFailure(name, fetchImpl, mutateSnapshot = snapshot => snapshot) {
  const h = createHarness();
  const result = finalizedResult({ resultRevision: 4, geometryHash: "hash-4" });
  h.setActive(result, textA);
  const snapshot = mutateSnapshot(clone(h.state().lastSnapshot));
  h.setSnapshot(snapshot);
  h.historyStack.push(textA);
  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  h.setFetch(fetchImpl);
  await h.undoLastChange();
  const state = h.state();
  assert.equal(state.dirty, true, `${name}: dirty remains`);
  assert.equal(state.lastSnapshot !== null, true, `${name}: snapshot preserved`);
  assert.equal(state.kmlSourcePresent, false, `${name}: old KML not consumed`);
  assert.equal(state.coordinateKmlState, "validation_required", `${name}: KML still requires validation`);
  assert.equal(state.mapState, "validation_required", `${name}: map still requires validation`);
  passDynamic(name);
}

await assertRecoveryFailure(
  "HTTP_404_RECOVERY_FAILURE_FAILS_CLOSED",
  async () => jsonResponse(false, 404, { code: "FINALIZED_RESULT_NOT_FOUND" })
);
await assertRecoveryFailure(
  "HTTP_422_RECOVERY_FAILURE_FAILS_CLOSED",
  async () => jsonResponse(false, 422, { code: "INVALID_RECOVERY_IDENTITY" })
);
await assertRecoveryFailure(
  "MALFORMED_JSON_RECOVERY_FAILURE_FAILS_CLOSED",
  async () => ({
    ok: true,
    status: 200,
    async json() {
      throw new SyntaxError("malformed recovery response");
    }
  })
);
await assertRecoveryFailure(
  "STALE_REVISION_REJECTION_REMAINS_DIRTY",
  async () => jsonResponse(false, 409, { code: "STALE_CONFIRMATION_REVISION" })
);
await assertRecoveryFailure(
  "HASH_MISMATCH_REJECTION_REMAINS_DIRTY",
  async () => jsonResponse(false, 409, { code: "GEOMETRY_HASH_MISMATCH" })
);
await assertRecoveryFailure(
  "EXPIRED_OR_MISSING_RESULT_REMAINS_DIRTY",
  async () => jsonResponse(false, 410, { code: "FINALIZED_RESULT_EXPIRED" })
);
await assertRecoveryFailure(
  "RECOVERY_NETWORK_FAILURE_FAILS_CLOSED",
  async () => { throw new Error("network unavailable"); }
);
await assertRecoveryFailure(
  "RESPONSE_IDENTITY_MISMATCH_FAILS_CLOSED",
  async () => jsonResponse(true, 200, { finalizedCoordinateResult: finalizedResult({ resultId: "other-result", resultRevision: 4, geometryHash: "hash-4" }) })
);
await assertRecoveryFailure(
  "GEOMETRY_TAMPER_FAILS_CLOSED",
  async () => jsonResponse(true, 200, { finalizedCoordinateResult: finalizedResult({ resultRevision: 4, geometryHash: "hash-4" }) }),
  snapshot => {
    snapshot.geometry.coordinates[0][0][0] = 105;
    return snapshot;
  }
);
await assertRecoveryFailure(
  "CRS_TAMPER_FAILS_CLOSED",
  async () => jsonResponse(true, 200, { finalizedCoordinateResult: finalizedResult({ resultRevision: 4, geometryHash: "hash-4" }) }),
  snapshot => {
    snapshot.crs.id = "EPSG:3857";
    return snapshot;
  }
);
await assertRecoveryFailure(
  "AXIS_ORDER_TAMPER_FAILS_CLOSED",
  async () => jsonResponse(true, 200, { finalizedCoordinateResult: finalizedResult({ resultRevision: 4, geometryHash: "hash-4" }) }),
  snapshot => {
    snapshot.axisOrder = "latitude_longitude";
    return snapshot;
  }
);

{
  const h = createHarness();
  const resultA = finalizedResult({ resultRevision: 5, geometryHash: "hash-5" });
  h.setActive(resultA, textA);
  h.historyStack.push(textA);
  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  h.setFetch(async () => jsonResponse(true, 200, { finalizedCoordinateResult: resultA }));
  await h.undoLastChange();
  const resultB = finalizedResult({ resultRevision: 6, geometryHash: "hash-6", geometry: {
    type: "Polygon",
    coordinates: [[[104.2, 11.5], [104.201, 11.5], [104.201, 11.501], [104.2, 11.501], [104.2, 11.5]]]
  } });
  const textB = "104.200000,11.500000\n104.201000,11.500000\n104.201000,11.501000\n104.200000,11.501000";
  h.setActive(resultB, textB);
  h.historyStack.push(textB);
  h.setInput(textInvalid);
  h.markCoordinateTextChanged();
  h.setFetch(async () => jsonResponse(true, 200, { finalizedCoordinateResult: resultB }));
  await h.undoLastChange();
  assert.equal(h.state().activeIdentity.resultRevision, 6);
  assert.equal(h.state().dirty, false);
  passDynamic("SECOND_EDIT_REVERT_CYCLE");
}

const recognizeSource = extractFunctionSource(html, "recognizeImage");
assert.match(recognizeSource, /lastValidIdentityBoundSnapshot = null/, "new image clears prior snapshot");
passStatic("NEW_IMAGE_CLEARS_PRIOR_SNAPSHOT");

const sharedCopySource = extractFunctionSource(html, "hydrateSharedRecipientWorkingCopy");
assert.match(sharedCopySource, /lastValidIdentityBoundSnapshot = null/, "shared copy clears prior snapshot");
passStatic("SHARED_COPY_CLEARS_PRIOR_SNAPSHOT");

assert.doesNotMatch(html, /activeFinalizedCoordinateResult = restoredResult/, "client snapshot never directly restores current finalized result");
assert.doesNotMatch(html, /restoreLastValidIdentityBoundSnapshot/, "direct client restore helper is removed");
passStatic("CLIENT_SNAPSHOT_AUTHORITY_REMOVED");

assert.match(html, /fetch\("\/api\/coordinate-manual-finalize"/, "existing recovery endpoint is reused");
assert.doesNotMatch(html, /\/api\/coordinate-recovery/, "no new recovery endpoint is introduced");
passStatic("SERVER_REVALIDATION_ENDPOINT_REUSED");

assert.doesNotMatch(html, /api\/recognize-coordinates.*p0a-edit-recovery-lifecycle/, "P0A regression does not add Provider recognition calls");
passStatic("NO_PROVIDER_REQUEST");

const dynamicPassCount = dynamicCases.length;
const staticPassCount = staticAssertions.length;
const combinedPassCount = dynamicPassCount + staticPassCount;

console.log(JSON.stringify({
  suite: "p0a-edit-recovery-lifecycle-regression",
  dynamic: true,
  actualDynamicPassCount: dynamicPassCount,
  actualDynamicTotalCount: dynamicCases.length,
  actualStaticPassCount: staticPassCount,
  actualStaticTotalCount: staticAssertions.length,
  actualCombinedPassCount: combinedPassCount,
  actualCombinedTotalCount: dynamicCases.length + staticAssertions.length,
  trueDynamicAssertions: dynamicPassCount,
  staticSourceAssertions: staticAssertions.length,
  passed: combinedPassCount,
  dynamicCases,
  staticAssertions,
  cases: [...dynamicCases, ...staticAssertions]
}, null, 2));
