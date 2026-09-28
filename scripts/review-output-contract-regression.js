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
import {
  evaluateUnifiedRecognitionFinalAuthorization,
  isDirectionBoundDmsProvisionalReviewEligible
} from "../server/recognition/recognition-first-acquisition.js";

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

const directionBoundDmsDecision = Object.freeze({ dmsGeographicReviewEligible: true });
const directionBoundDmsEvidence = Object.freeze({
  candidateCoordinates: Object.freeze([Object.freeze({ format: "DMS" })])
});
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  decision: directionBoundDmsDecision,
  evidence: directionBoundDmsEvidence,
  finalized: review
}), true, "an already REVIEW_REQUIRED direction-bound DMS result is eligible for provisional outputs");
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  providerDmsReviewEvidence: Object.freeze({
    status: "COMPLETE",
    coverageStatus: "COMPLETE",
    coordinateRowCount: 4,
    sourceRowCount: 4,
    blockingRejectedRowCount: 0,
    axisDirectionBound: true,
    axisConflict: false,
    providerAxisConflict: false,
    localAxisConflict: false,
    crossSourceAxisConflict: false
  }),
  finalized: review
}), true, "complete direction-bound Provider DMS evidence independently establishes provisional eligibility");
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  decision: directionBoundDmsDecision,
  evidence: directionBoundDmsEvidence,
  finalized: finalized({
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    requiresReview: false,
    kmlReady: true
  })
}), true, "a safe AUTO_EXPORT DMS result may still be downgraded into the same provisional review contract");

const hardBlockedResults = [
  ["invalid geometry", finalized({ geometry: { type: "Point", coordinates: [181, 5] } })],
  ["invalid CRS", finalized({ crs: { id: "EPSG:0", axisOrder: "longitude_latitude" } })],
  ["invalid source", finalized({ sourceAuthority: "unknown" })],
  ["stale revision", finalized({ currentRevision: 2 })],
  ["rejected confirmation", finalized({ confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED })],
  ["invalid confirmation binding", Object.freeze({
    ...review,
    decisionState: COORDINATE_DECISION_STATE.BLOCKED,
    explicitAuthorityRejected: true,
    technicalKmlReady: false,
    kmlReady: false,
    kmlAuthorityBlocked: true
  })]
];
for (const [name, result] of hardBlockedResults) {
  const authorization = authorizationFor(result);
  assert.equal(authorization.authorized, false, `${name} must not authorize export`);
  assert.equal(authorization.mapReady, false, `${name} must not expose provisional map`);
  assert.equal(authorization.provisionalKmlReady, false, `${name} must not expose provisional KML`);
  assert.equal(authorization.kmlReady, false, `${name} keeps KML closed`);
  assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
    decision: directionBoundDmsDecision,
    evidence: directionBoundDmsEvidence,
    finalized: result
  }), false, `${name} cannot enter the direction-bound DMS provisional path`);
}
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  decision: directionBoundDmsDecision,
  evidence: directionBoundDmsEvidence,
  finalized: Object.freeze({
    ...review,
    technicalKmlReady: false,
    kmlReady: false,
    kmlAuthorityBlocked: true
  })
}), true, "a review-derived legacy KML block cannot override otherwise valid direction-bound DMS evidence");
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  decision: directionBoundDmsDecision,
  evidence: { candidateCoordinates: [{ format: "PROJECTED_XY" }, { format: "DMS" }] },
  finalized: review
}), false, "projected results with DMS reference columns cannot enter the DMS provisional path");
assert.equal(isDirectionBoundDmsProvisionalReviewEligible({
  decision: directionBoundDmsDecision,
  evidence: directionBoundDmsEvidence,
  finalized: finalized({
    coordinateType: "projected_table",
    family: "projected_table",
    precisionMode: "projected-with-dms-crosscheck"
  })
}), false, "a projected final result cannot become DMS-authoritative through reference columns");

const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const candidateSourceEvidenceStart = indexSource.indexOf("function buildRecognitionCandidateSourceEvidence");
const candidateSourceEvidenceEnd = indexSource.indexOf("function compactRecognizedCoordinateDisplayText", candidateSourceEvidenceStart);
const geometryContractStart = indexSource.indexOf("function hasFiniteFinalizedGeometry");
const geometryContractEnd = indexSource.indexOf("function isOrdinaryReviewOnlyFinalizedResult", geometryContractStart);
const browserContractStart = indexSource.indexOf("function hasCompleteUnifiedRecognitionEvidence");
const browserContractEnd = indexSource.indexOf("function getConvertibleCoordinateGroups", browserContractStart);
assert.notEqual(geometryContractStart, -1);
assert.notEqual(geometryContractEnd, -1);
assert.notEqual(browserContractStart, -1);
assert.notEqual(browserContractEnd, -1);
assert.notEqual(candidateSourceEvidenceStart, -1);
assert.notEqual(candidateSourceEvidenceEnd, -1);
const buildRecognitionCandidateSourceEvidence = Function(`
  ${indexSource.slice(candidateSourceEvidenceStart, candidateSourceEvidenceEnd)}
  return buildRecognitionCandidateSourceEvidence;
`)();
const sourceRows = [
  "1 | 05° 34' 42.00\"N | 02° 47' 05.00\"W",
  "2 | 05° 34' 42.00\"N | 02° 46' 19.00\"W",
  "3 | 05° 34' 21.00\"N | 02° 46' 19.00\"W",
  "4 | 05° 34' 21.00\"N | 02° 47' 05.00\"W"
];
const candidateSourceEvidence = buildRecognitionCandidateSourceEvidence({
  candidateCoordinateGroups: [{
    visibleTitle: "[UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE]",
    rows: sourceRows.map((sourceText, index) => ({
      sourceLabel: String(index + 1),
      sourceText,
      latitudeSource: sourceText.split(" | ")[1],
      longitudeSource: sourceText.split(" | ")[2]
    }))
  }]
}, 4);
assert.deepEqual(candidateSourceEvidence.rows, sourceRows,
  "validated candidate rows preserve the exact original text and point order");
assert.equal(candidateSourceEvidence.displayText, sourceRows.join("\n"),
  "editable candidate text contains only original coordinate rows");
assert.doesNotMatch(candidateSourceEvidence.displayText, /UNCLASSIFIED|STRUCTURED COORDINATE EVIDENCE/u,
  "internal diagnostic headings never enter editable coordinate text");
assert.deepEqual(buildRecognitionCandidateSourceEvidence({
  candidateCoordinateGroups: [{ rows: sourceRows.slice(0, 3).map(sourceText => ({ sourceText })) }]
}, 4), { groups: [], rows: [], displayText: "" },
"candidate row fallback stays closed when the validated row count does not match");
const createBrowserAuthorizationState = Function(`
  ${indexSource.slice(geometryContractStart, geometryContractEnd)}
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
  finalizedCoordinateResult: hardBlockedResults[0][1],
  candidateCoordinates: evidence.candidateCoordinates,
  candidateCoordinateGroups: evidence.candidateCoordinateGroups,
  visibleCrsEvidence: evidence.visibleCrsEvidence,
  imageAcquisitionEvidence: evidence.imageEvidence
});
assert.equal(browserBlocked.kmlStatus, "CLOSED");
assert.equal(browserBlocked.kmlReady, false);
assert.equal(browserBlocked.mapStatus, "CLOSED");

const actionContractStart = indexSource.indexOf("function getConvertibleCoordinateGroups");
const actionContractEnd = indexSource.indexOf("function recognitionAuthorizationReasonMessage", actionContractStart);
assert.notEqual(actionContractStart, -1);
assert.notEqual(actionContractEnd, -1);
const createBrowserActionContract = Function(
  "getKmlCoordinateGroups",
  "activeRecognitionAcquisitionResult",
  "activeFinalizedCoordinateResult",
  "getFinalizedCoordinateIdentity",
  `
    ${indexSource.slice(actionContractStart, actionContractEnd)}
    return {
      blocked: coordinateRecognitionActionBlocked,
      provisional: shouldUseProvisionalCoordinateResult
    };
  `
);
const convertibleGroups = () => [[{ longitude: -8.01, latitude: 10.01 }]];
const reviewActions = createBrowserActionContract(
  convertibleGroups,
  browserReview,
  review,
  () => ({ resultId: review.resultId, resultRevision: review.resultRevision })
);
assert.equal(reviewActions.blocked("map"), false, "valid review map action stays enabled");
assert.equal(reviewActions.blocked("kml"), false, "valid review KML action stays enabled");
assert.equal(reviewActions.provisional("map"), true, "valid review uses provisional map flow");
assert.equal(reviewActions.provisional("kml"), true, "valid review uses provisional KML flow");

const blockedActions = createBrowserActionContract(
  convertibleGroups,
  browserBlocked,
  hardBlockedResults[0][1],
  () => null
);
assert.equal(blockedActions.blocked("map"), true, "closed recognition map cannot use parseable text as a bypass");
assert.equal(blockedActions.blocked("kml"), true, "closed recognition KML cannot use parseable text as a bypass");
assert.equal(blockedActions.provisional("map"), false, "closed recognition cannot enter provisional map flow");
assert.equal(blockedActions.provisional("kml"), false, "closed recognition cannot enter provisional KML flow");

for (const [name, result] of hardBlockedResults) {
  const authorization = authorizationFor(result);
  const blockedBrowserState = createBrowserAuthorizationState({
    recognitionAcquisition: evidence,
    acquisitionStatus: "COMPLETED",
    mapReady: authorization.mapReady,
    mapStatus: authorization.mapReady ? "ENABLED" : "CLOSED",
    kmlReady: authorization.kmlReady,
    kmlStatus: authorization.kmlReady ? "ENABLED" : "CLOSED",
    finalizedCoordinateResult: result,
    candidateCoordinates: evidence.candidateCoordinates,
    candidateCoordinateGroups: evidence.candidateCoordinateGroups,
    visibleCrsEvidence: evidence.visibleCrsEvidence,
    imageAcquisitionEvidence: evidence.imageEvidence
  });
  const actions = createBrowserActionContract(convertibleGroups, blockedBrowserState, result, () => null);
  assert.equal(actions.blocked("map"), true, `${name} keeps the frontend map action blocked`);
  assert.equal(actions.blocked("kml"), true, `${name} keeps the frontend KML action blocked`);
  assert.equal(actions.provisional("map"), false, `${name} cannot enter the provisional map flow`);
  assert.equal(actions.provisional("kml"), false, `${name} cannot enter the provisional KML flow`);
}

const manualActions = createBrowserActionContract(convertibleGroups, null, null, () => null);
assert.equal(manualActions.blocked("map"), false, "manual input retains its independent map path");
assert.equal(manualActions.blocked("kml"), false, "manual input retains its independent KML path");

assert.match(indexSource, /const displayCoordinates = sourceDisplayText\s*\|\| acquisitionCandidateDisplayText/u,
  "editable coordinates prefer the preserved source rows");
assert.match(indexSource, /const detailRows = sourceDisplayText[\s\S]+: acquisitionCandidateSourceEvidence\.rows;/u,
  "recognition details use identity-bound candidate rows when the source representation is unavailable");
assert.doesNotMatch(indexSource, /const heading = titlePath\.length/u,
  "diagnostic title paths are not inserted into editable coordinate text");
assert.match(indexSource, /if \(provisionalMapReady && provisionalKmlReady\) \{\s*appendDebug\("地图和未确认 KML 已准备，可继续核对"\)/u,
  "recognition details only claim provisional KML when the response enables it");
assert.match(indexSource, /const reviewMapReady = activeRecognitionAcquisitionResult\?\.mapStatus === "ENABLED"/u,
  "recognition completion message follows the server map state");
assert.match(indexSource, /const reviewKmlReady = activeRecognitionAcquisitionResult\?\.kmlStatus === "ENABLED"/u,
  "recognition completion message follows the server KML state");
assert.match(serverSource, /finalizedRequiresStateAlignment[\s\S]+!finalAuthorization\.mapReady \|\| !finalAuthorization\.kmlReady[\s\S]+coordinateConfirmationRuntime\.register\(Object\.freeze/u,
  "the registered result identity is aligned with the final server map and KML decision");
assert.match(serverSource, /coordinateEvidenceConsistencyStatus[\s\S]+coordinateEvidenceConflict[\s\S]+mapReady: false,[\s\S]+kmlReady: false/u,
  "an explicit cross-source coordinate conflict closes both provisional outputs");
assert.match(serverSource, /isDirectionBoundDmsProvisionalReviewEligible\(\{[\s\S]+technicalKmlReady: true[\s\S]+kmlAuthorityBlocked: false/u,
  "direction-bound DMS is downgraded to provisional review without a hard KML authority block");
assert.match(serverSource, /authorizationStatus: "REVIEW_REQUIRED"[\s\S]+resultStatus: "needs_review"[\s\S]+finalizedCoordinateResult: provisionalDmsReviewResult/u,
  "direction-bound DMS keeps explicit review state while exposing provisional outputs");

console.log(JSON.stringify({
  suite: "review-output-contract-regression",
  passed: 39,
  providerCalls: 0,
  cases: [
    "VALID_REVIEW_MAP_ENABLED",
    "VALID_REVIEW_UNVERIFIED_KML_ENABLED",
    "DMS_REVIEW_REQUIRED_PROVISIONAL_OUTPUT_ELIGIBLE",
    "COMPLETE_PROVIDER_DMS_PROVISIONAL_OUTPUT_ELIGIBLE",
    "DMS_AUTO_EXPORT_DOWNGRADE_REMAINS_ELIGIBLE",
    "REVIEW_STATE_NOT_PROMOTED",
    "INVALID_GEOMETRY_BLOCKED",
    "INVALID_CRS_BLOCKED",
    "INVALID_SOURCE_BLOCKED",
    "STALE_REVISION_BLOCKED",
    "REJECTED_CONFIRMATION_BLOCKED",
    "INVALID_CONFIRMATION_BINDING_BLOCKED",
    "HARD_BLOCKS_CLOSE_MAP_AND_KML",
    "REGISTERED_RESULT_MATCHES_FINAL_MAP_KML_STATE",
    "EXPLICIT_COORDINATE_EVIDENCE_CONFLICT_CLOSES_OUTPUTS",
    "DMS_INVALID_GEOMETRY_PROVISIONAL_PATH_BLOCKED",
    "DMS_INVALID_CRS_PROVISIONAL_PATH_BLOCKED",
    "DMS_INVALID_SOURCE_PROVISIONAL_PATH_BLOCKED",
    "DMS_STALE_REVISION_PROVISIONAL_PATH_BLOCKED",
    "DMS_REJECTED_CONFIRMATION_PROVISIONAL_PATH_BLOCKED",
    "DMS_INVALID_BINDING_PROVISIONAL_PATH_BLOCKED",
    "DMS_REVIEW_DERIVED_LEGACY_KML_BLOCK_IGNORED",
    "PROJECTED_WITH_DMS_REFERENCE_PROVISIONAL_PATH_BLOCKED",
    "PROJECTED_FINAL_RESULT_DMS_CROSSCHECK_NOT_AUTHORITY",
    "FRONTEND_REVIEW_KML_ENABLED",
    "FRONTEND_INVALID_KML_BLOCKED",
    "FRONTEND_VALID_REVIEW_ACTIONS_ENABLED",
    "FRONTEND_CLOSED_ACTIONS_BLOCKED",
    "FRONTEND_CLOSED_PROVISIONAL_FLOW_BLOCKED",
    "MANUAL_INPUT_ACTIONS_REMAIN_INDEPENDENT",
    "SOURCE_TEXT_PRECEDENCE",
    "CANDIDATE_SOURCE_ROWS_PRESERVE_ORIGINAL_TEXT",
    "CANDIDATE_SOURCE_ROWS_PRESERVE_POINT_ORDER",
    "CANDIDATE_SOURCE_ROWS_EXCLUDE_INTERNAL_TITLE",
    "CANDIDATE_SOURCE_ROWS_REQUIRE_COUNT_MATCH",
    "DETAIL_ROWS_USE_VALIDATED_CANDIDATE_FALLBACK",
    "DETAIL_STATUS_MATCHES_KML_STATE",
    "COMPLETION_MESSAGE_MATCHES_MAP_STATE",
    "COMPLETION_MESSAGE_MATCHES_KML_STATE"
  ]
}, null, 2));
