import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { extractProviderProjectedCoordinateEvidence } from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";
import {
  bindProviderRepresentationsToSource,
  extractMultiRepresentationSourceEvidence
} from "../server/recognition/multi-representation-source-evidence.js";
import { normalizeProviderDmsReviewResult } from "../server/recognition/recognition-review-result.js";
import {
  buildRecognitionAcquisitionEvidence,
  evaluateUnifiedRecognitionAcquisition
} from "../server/recognition/recognition-first-acquisition.js";

const sha256 = "7".repeat(64);
const imageIdentity = { image_sha256: sha256 };
const sourceContextProvenance = { image_sha256: sha256, same_request: true, mode: "overview_footer_composite" };
const sourceRows = [
  `8 1 778984,492 9721476,737 2°31'2,794"S 119°30'31,553"E 8`,
  `2 779099,680 9721476,848 2°31'2,783"S 119°30'35,279"E`,
  `3 779099,680 9721110,798 2°31'14,694"S 119°30'35,302"E`,
  `2 4 778875,519 9721110,798 2°31'14,708"S 119°30'28,050"E`,
  `5 778875,519 9721180,576 2°31'12,437"S 119°30'28,046"E`,
  `g 6 778984492 9721180576 2°31'12,430"S 119°30'31,571"E g`
];
const sourceContextText = [
  "No. X Y LATITUDE LONGITUDE",
  ...sourceRows,
  "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S"
].join("\n");

const sourceEvidence = extractMultiRepresentationSourceEvidence({
  sourceContextText,
  sourceContextProvenance,
  imageIdentity
});
assert.equal(sourceEvidence.status, "COMPLETE");
assert.equal(sourceEvidence.crsEvidence.id, "EPSG:32750");
assert.deepEqual(sourceEvidence.labels, ["1", "2", "3", "4", "5", "6"]);
assert.equal(sourceEvidence.rows[5].x, 778984.492);
assert.equal(sourceEvidence.rows[5].y, 9721180.576);

const repeatedOverviewBeforeCompleteTable = [
  "No. X Y LATITUDE LONGITUDE",
  sourceRows[1],
  sourceRows[3],
  ...sourceRows,
  "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S"
].join("\n");
const repeatedOverviewEvidence = extractMultiRepresentationSourceEvidence({
  sourceContextText: repeatedOverviewBeforeCompleteTable,
  sourceContextProvenance,
  imageIdentity
});
assert.equal(repeatedOverviewEvidence.status, "COMPLETE");
assert.deepEqual(repeatedOverviewEvidence.labels, ["1", "2", "3", "4", "5", "6"]);

const dmsOnly = [
  `2°31'2,794"S | 119°30'31,553"E`,
  `2°31'2,783"S | 119°30'35,279"E`,
  `2°31'14,694"S | 119°30'35,302"E`,
  `2°31'14,708"S | 119°30'28,050"E`,
  `2°31'12,437"S | 119°30'28,046"E`,
  `2°31'12,430"S | 119°30'31,571"E`
].join("\n");
const xyOnly = [
  "POINT | X | Y",
  "1 | 778984.492 | 9721476.737",
  "2 | 779099.680 | 9721476.848",
  "3 | 779099.680 | 9721110.798",
  "4 | 778875.519 | 9721110.798",
  "5 | 778875.519 | 9721180.576",
  "6 | 778984.492 | 9721180.576"
].join("\n");
const both = [
  "POINT | X | Y | LATITUDE | LONGITUDE",
  "1 | 778984.492 | 9721476.737 | 2°31'2.794\"S | 119°30'31.553\"E",
  "2 | 779099.680 | 9721476.848 | 2°31'2.783\"S | 119°30'35.279\"E",
  "3 | 779099.680 | 9721110.798 | 2°31'14.694\"S | 119°30'35.302\"E",
  "4 | 778875.519 | 9721110.798 | 2°31'14.708\"S | 119°30'28.050\"E",
  "5 | 778875.519 | 9721180.576 | 2°31'12.437\"S | 119°30'28.046\"E",
  "6 | 778984.492 | 9721180.576 | 2°31'12.430\"S | 119°30'31.571\"E"
].join("\n");

function bind(providerText) {
  return bindProviderRepresentationsToSource({
    providerDmsReviewEvidence: normalizeProviderDmsReviewResult(providerText),
    providerProjectedEvidence: extractProviderProjectedCoordinateEvidence({ sourceText: providerText }),
    sourceContextText,
    sourceContextProvenance,
    imageIdentity
  });
}

const boundDms = bind(dmsOnly);
assert.equal(boundDms.status, "COMPLETE");
assert.equal(boundDms.providerMode, "DMS_ONLY");
assert.deepEqual(boundDms.labels, ["1", "2", "3", "4", "5", "6"]);
assert.match(boundDms.candidateDmsText, /^1 \|/u);
const unifiedDmsEvidence = buildRecognitionAcquisitionEvidence({
  rawText: dmsOnly,
  sourceContextText,
  sourceBoundDmsEvidence: boundDms,
  acquisition: { width: 1600, height: 1129, bytes: 288226, images: [{ role: "overview" }] },
  providerResponseId: "offline-multi-representation-dms"
});
assert.equal(unifiedDmsEvidence.diagnostics.candidateEvidenceSource, "SOURCE_BOUND_MULTI_REPRESENTATION_DMS");
assert.equal(unifiedDmsEvidence.candidateCoordinates.length, 6);
assert.equal(unifiedDmsEvidence.candidateCoordinateGroups.length, 1);
assert.equal(unifiedDmsEvidence.candidateCoordinateGroups[0].sourceLabelsContinuous, true);
assert.ok(!unifiedDmsEvidence.reviewReasons.includes("SOURCE_LABELS_MISSING"));
const unifiedDmsDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: unifiedDmsEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
assert.equal(unifiedDmsDecision.dmsGeographicReviewEligible, true);

const boundProjected = bind(xyOnly);
assert.equal(boundProjected.status, "COMPLETE");
assert.equal(boundProjected.providerMode, "PROJECTED_ONLY");

const boundBoth = bind(both);
assert.equal(boundBoth.status, "COMPLETE");
assert.equal(boundBoth.providerMode, "BOTH");

const longSourceRows = Array.from({ length: 16 }, (_, index) => {
  const label = index + 1;
  const x = (700000 + index * 10).toFixed(3);
  const y = (9000000 - index * 10).toFixed(3);
  const seconds = (10 + index).toFixed(3);
  return `${label} ${x} ${y} 2°31'${seconds}"S 119°30'${seconds}"E`;
});
const longSourceContext = [
  "No. X Y LATITUDE LONGITUDE",
  ...longSourceRows,
  "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S"
].join("\n");
const longProviderDmsRows = longSourceRows.map(row => row.replace(/^\d+\s+\d+(?:\.\d+)?\s+\d+(?:\.\d+)?\s+/u, ""));
const longCompleteBinding = bindProviderRepresentationsToSource({
  providerDmsReviewEvidence: normalizeProviderDmsReviewResult(longProviderDmsRows.join("\n")),
  providerProjectedEvidence: extractProviderProjectedCoordinateEvidence({ sourceText: longProviderDmsRows.join("\n") }),
  sourceContextText: longSourceContext,
  sourceContextProvenance,
  imageIdentity
});
assert.equal(longCompleteBinding.status, "COMPLETE");
assert.equal(longCompleteBinding.rows.length, 16);
const longPartialProviderRows = longProviderDmsRows.filter((_, index) => ![2, 3].includes(index));
const longPartialBinding = bindProviderRepresentationsToSource({
  providerDmsReviewEvidence: normalizeProviderDmsReviewResult(longPartialProviderRows.join("\n")),
  providerProjectedEvidence: extractProviderProjectedCoordinateEvidence({ sourceText: longPartialProviderRows.join("\n") }),
  sourceContextText: longSourceContext,
  sourceContextProvenance,
  imageIdentity
});
assert.equal(longPartialBinding.status, "CONFLICT");
assert.equal(longPartialBinding.reason, "PROVIDER_SOURCE_ROW_COUNT_CONFLICT");
assert.equal(longPartialBinding.sourceRowCount, 16);
assert.equal(longPartialBinding.providerDmsRowCount, 14);
assert.deepEqual(longPartialBinding.missingLabels, ["3", "4"]);
assert.equal(longPartialBinding.orderConflict, false);

const missingPointEvidence = extractMultiRepresentationSourceEvidence({
  sourceContextText: sourceContextText.replace(/^3\s+/mu, ""),
  sourceContextProvenance,
  imageIdentity
});
assert.notEqual(missingPointEvidence.status, "COMPLETE");

const pointOrderConflict = bind(dmsOnly.split("\n").reverse().join("\n"));
assert.equal(pointOrderConflict.status, "CONFLICT");
assert.equal(pointOrderConflict.reason, "PROVIDER_SOURCE_DMS_OR_ORDER_CONFLICT");

const crsConflict = extractMultiRepresentationSourceEvidence({
  sourceContextText: `${sourceContextText}\nUTM ZONE 49S`,
  sourceContextProvenance,
  imageIdentity
});
assert.notEqual(crsConflict.status, "COMPLETE");

const wrongImage = extractMultiRepresentationSourceEvidence({
  sourceContextText,
  sourceContextProvenance: { ...sourceContextProvenance, image_sha256: "8".repeat(64) },
  imageIdentity
});
assert.equal(wrongImage.status, "INCOMPLETE");
assert.equal(wrongImage.reason, "IMAGE_IDENTITY_NOT_BOUND");

const nonContinuousSource = [
  "No. X Y LATITUDE LONGITUDE",
  `10 778984,492 9721476,737 2°31'2,794"S 119°30'31,553"E`,
  `7 779099,680 9721476,848 2°31'2,783"S 119°30'35,279"E`,
  `21 779099,680 9721110,798 2°31'14,694"S 119°30'35,302"E`,
  `3 778875,519 9721110,798 2°31'14,708"S 119°30'28,050"E`,
  `18 778875,519 9721180,576 2°31'12,437"S 119°30'28,046"E`,
  `9 778984,492 9721180,576 2°31'12,430"S 119°30'31,571"E`,
  "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S"
].join("\n");
const preservedSourceOrder = extractMultiRepresentationSourceEvidence({
  sourceContextText: nonContinuousSource,
  sourceContextProvenance,
  imageIdentity
});
assert.equal(preservedSourceOrder.status, "CONFLICT");
assert.equal(preservedSourceOrder.reason, "SOURCE_LABEL_OR_PROJECTED_VALUE_CONFLICT");

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const functionSource = name => {
  const start = indexSource.indexOf(`function ${name}`);
  assert.ok(start >= 0, `${name} missing`);
  const parametersStart = indexSource.indexOf("(", start);
  let parameterDepth = 0;
  let bodyStart = -1;
  for (let index = parametersStart; index < indexSource.length; index += 1) {
    if (indexSource[index] === "(") parameterDepth += 1;
    if (indexSource[index] === ")") parameterDepth -= 1;
    if (parameterDepth === 0) {
      bodyStart = indexSource.indexOf("{", index + 1);
      break;
    }
  }
  assert.ok(bodyStart >= 0, `${name} body missing`);
  let depth = 0;
  for (let index = bodyStart; index < indexSource.length; index += 1) {
    if (indexSource[index] === "{") depth += 1;
    if (indexSource[index] === "}") depth -= 1;
    if (depth === 0) return indexSource.slice(start, index + 1);
  }
  throw new Error(`${name} incomplete`);
};
const context = {
  activeRecognitionAcquisitionResult: null,
  hasFiniteFinalizedGeometry: () => true,
  hasCompleteUnifiedRecognitionEvidence: () => true
};
vm.createContext(context);
vm.runInContext(`${functionSource("createRecognitionAuthorizationState")}; this.createRecognitionAuthorizationState = createRecognitionAuthorizationState;`, context);
const browserState = context.createRecognitionAuthorizationState({
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: false,
  kmlStatus: "CLOSED",
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  requiresReview: true,
  finalizedCoordinateResult: {
    mapReady: false,
    decisionState: "REVIEW_REQUIRED",
    requiresReview: true,
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    sourceAuthority: "legacy",
    explicitAuthorityRejected: false,
    kmlAuthorityBlocked: true,
    crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
    geometry: { type: "Polygon", coordinates: [[[119, -2], [120, -2], [120, -3], [119, -2]]] }
  }
});
assert.equal(browserState.mapStatus, "ENABLED");
assert.equal(browserState.kmlStatus, "CLOSED");

Object.assign(context, {
  activeFinalizedCoordinateResult: {
    resultId: "multi-representation-result",
    resultRevision: 1,
    kmlReady: true,
    geometry: {
      type: "Polygon",
      coordinates: [[[117.01, -2.01], [117.02, -2.01], [117.02, -2.02], [117.01, -2.01]]]
    }
  },
  finalizedCoordinateDirty: false,
  getFinalizedCoordinateIdentity: result => result?.resultId ? { resultId: result.resultId } : null,
  getKmlCoordinateGroups: () => [[{ longitude: 778984.492, latitude: 9721476.737 }]],
  normalizeKmlPair: pair => ({ ...pair }),
  cloneCoordinateGroups: groups => groups.map(group => group.map(pair => ({ ...pair })))
});
vm.runInContext(`
  ${functionSource("getFinalizedGeometryCoordinateSource")}
  ${functionSource("getConvertibleCoordinateGroups")}
  this.getConvertibleCoordinateGroups = getConvertibleCoordinateGroups;
`, context);
const convertibleGroups = context.getConvertibleCoordinateGroups();
assert.equal(convertibleGroups.length, 1, "the server-authoritative WGS84 geometry is usable when editable text remains projected X/Y");
assert.equal(convertibleGroups[0].length, 4);
assert.equal(convertibleGroups[0][0].longitude, 117.01);
assert.equal(convertibleGroups[0][0].latitude, -2.01);

console.log("multi-representation source evidence regression: PASS (6 provider variants; Provider calls: 0)");
