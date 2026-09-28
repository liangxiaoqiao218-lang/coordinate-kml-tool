import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { evaluateUnifiedRecognitionFinalAuthorization } from "../server/recognition/recognition-first-acquisition.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const clock = () => "2026-09-28T00:00:00.000Z";
const polygon = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([-8.01, 10.01]),
    Object.freeze([-8, 10.01]),
    Object.freeze([-8, 10]),
    Object.freeze([-8.01, 10]),
    Object.freeze([-8.01, 10.01])
  ])])
});

function finalized(overrides = {}) {
  return finalizeCoordinateResult({
    resultId: "review-output-contract",
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    sourceAuthority: "legacy",
    coordinateType: "standard_dms_table",
    precisionMode: "dms-coordinates",
    family: "standard_dms_table",
    availabilityStatus: "AVAILABLE",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: polygon,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: true,
    currentAuthorizedGeometryExportable: true,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    warnings: ["请结合原图核对"],
    ...overrides
  }, { clock });
}

const evidence = Object.freeze({
  acquisitionStatus: "COMPLETED",
  candidateCoordinates: Object.freeze([Object.freeze({ label: "1" })]),
  candidateCoordinateGroups: Object.freeze([Object.freeze({ groupId: "group_1" })]),
  visibleCrsEvidence: Object.freeze([]),
  imageEvidence: Object.freeze({ imageCount: 1 })
});

function authorizationFor(result) {
  return evaluateUnifiedRecognitionFinalAuthorization({
    body: {
      success: true,
      requiresReview: true,
      authorizationStatus: "REVIEW_REQUIRED",
      resultStatus: "needs_review",
      mapReady: true,
      kmlReady: result.kmlReady,
      finalizedCoordinateResult: result
    },
    evidence,
    decision: {
      acquisitionStatus: "COMPLETED",
      authorizationStatus: "REVIEW_REQUIRED",
      resultStatus: "needs_review"
    },
    conformance: { status: "CONFORMANT" },
    providerCallCount: 1
  });
}

const review = finalized();
assert.equal(review.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
assert.equal(review.kmlReady, true, "valid current WGS84 review geometry remains technically exportable");
const reviewAuthorization = authorizationFor(review);
assert.equal(reviewAuthorization.authorized, false, "review output never becomes an authorized export");
assert.equal(reviewAuthorization.mapReady, true, "review output keeps its diagnostic map");
assert.equal(reviewAuthorization.provisionalKmlReady, true, "review output exposes only provisional KML");
assert.equal(reviewAuthorization.kmlReady, true, "response keeps provisional KML availability");

for (const [name, result] of [
  ["invalid geometry", finalized({ geometry: { type: "Point", coordinates: [181, 5] } })],
  ["invalid CRS", finalized({ crs: { id: "EPSG:0", axisOrder: "longitude_latitude" } })],
  ["invalid source", finalized({ sourceAuthority: "unknown" })],
  ["stale revision", finalized({ currentRevision: 2 })],
  ["rejected confirmation", finalized({ confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED })],
  ["explicit KML authority block", finalized({ kmlAuthorityBlocked: true })]
]) {
  const authorization = authorizationFor(result);
  assert.equal(authorization.authorized, false, `${name} must not authorize export`);
  assert.equal(authorization.provisionalKmlReady, false, `${name} must not expose provisional KML`);
  assert.equal(authorization.kmlReady, false, `${name} keeps KML closed`);
}

const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const browserContractStart = indexSource.indexOf("function hasCompleteUnifiedRecognitionEvidence");
const browserContractEnd = indexSource.indexOf("function getConvertibleCoordinateGroups", browserContractStart);
assert.notEqual(browserContractStart, -1);
assert.notEqual(browserContractEnd, -1);
const createBrowserAuthorizationState = Function(`
  ${indexSource.slice(browserContractStart, browserContractEnd)}
  return createRecognitionAuthorizationState;
`)();
const browserReview = createBrowserAuthorizationState({
  recognitionAcquisition: evidence,
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  requiresReview: true,
  boundaryBlocked: true,
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: true,
  kmlStatus: "ENABLED",
  finalizedCoordinateResult: review,
  candidateCoordinates: evidence.candidateCoordinates,
  candidateCoordinateGroups: evidence.candidateCoordinateGroups,
  visibleCrsEvidence: evidence.visibleCrsEvidence,
  imageAcquisitionEvidence: evidence.imageEvidence
});
assert.equal(browserReview.authorizationStatus, "REVIEW_REQUIRED");
assert.equal(browserReview.mapStatus, "ENABLED");
assert.equal(browserReview.kmlStatus, "ENABLED");
assert.equal(browserReview.kmlReady, true);

const browserBlocked = createBrowserAuthorizationState({
  recognitionAcquisition: evidence,
  acquisitionStatus: "COMPLETED",
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: false,
  kmlStatus: "CLOSED",
  finalizedCoordinateResult: finalized({ geometry: { type: "Point", coordinates: [181, 5] } }),
  candidateCoordinates: evidence.candidateCoordinates,
  candidateCoordinateGroups: evidence.candidateCoordinateGroups,
  visibleCrsEvidence: evidence.visibleCrsEvidence,
  imageAcquisitionEvidence: evidence.imageEvidence
});
assert.equal(browserBlocked.kmlStatus, "CLOSED");
assert.equal(browserBlocked.kmlReady, false);

assert.match(indexSource, /const displayCoordinates = sourceDisplayText\s*\|\| acquisitionCandidateDisplayText/u,
  "editable coordinates prefer the preserved source rows");
assert.doesNotMatch(indexSource, /const heading = titlePath\.length/u,
  "diagnostic title paths are not inserted into editable coordinate text");
assert.match(indexSource, /if \(provisionalMapReady && provisionalKmlReady\) \{\s*appendDebug\("地图和未确认 KML 已准备，可继续核对"\)/u,
  "recognition details only claim provisional KML when the response enables it");
assert.doesNotMatch(serverSource, /finalizedRequiresFailClose[\s\S]{0,350}finalizedCoordinateResult\.kmlReady === true/u,
  "review-ready KML is not mistaken for an unauthorized AUTO_EXPORT result");

console.log(JSON.stringify({
  suite: "review-output-contract-regression",
  passed: 12,
  providerCalls: 0,
  cases: [
    "VALID_REVIEW_MAP_ENABLED",
    "VALID_REVIEW_UNVERIFIED_KML_ENABLED",
    "REVIEW_STATE_NOT_PROMOTED",
    "INVALID_GEOMETRY_BLOCKED",
    "INVALID_CRS_BLOCKED",
    "INVALID_SOURCE_BLOCKED",
    "STALE_REVISION_BLOCKED",
    "REJECTED_CONFIRMATION_BLOCKED",
    "FRONTEND_REVIEW_KML_ENABLED",
    "FRONTEND_INVALID_KML_BLOCKED",
    "SOURCE_TEXT_PRECEDENCE",
    "DETAIL_STATUS_MATCHES_KML_STATE"
  ]
}, null, 2));
