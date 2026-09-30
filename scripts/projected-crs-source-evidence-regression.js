import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Tesseract from "tesseract.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  bindProjectedEvidenceToSourceContext,
  createLocalOcrClassificationImage,
  extractProjectedSourceContext
} from "../server/recognition/projected-source-evidence.js";
import { bindProviderRepresentationsToSource } from "../server/recognition/multi-representation-source-evidence.js";
import { normalizeProviderDmsReviewResult } from "../server/recognition/recognition-review-result.js";
import {
  buildRecognitionAcquisitionEvidence,
  evaluateProjectedCoordinateAuthorizationEvidence,
  evaluateUnifiedRecognitionAcquisition,
  evaluateUnifiedRecognitionFinalAuthorization
} from "../server/recognition/recognition-first-acquisition.js";
import { extractProviderProjectedCoordinateEvidence } from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";
import { FINALIZED_COORDINATE_CRS, finalizeCoordinateResult } from "../server/coordinate-finalizer/index.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const tessdataPath = String(process.env.RECOGNITION_TEST_TESSDATA_PATH || "").trim();
assert.ok(tessdataPath, "RECOGNITION_TEST_TESSDATA_PATH is required for offline OCR regression");
await access(path.join(tessdataPath, "eng.traineddata.gz"));
const fixturePath = path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg");
const fixture = await readFile(fixturePath);
const fixtureSha256 = createHash("sha256").update(fixture).digest("hex");
const imageIdentity = createCoordinateImageIdentity({
  buffer: fixture,
  mimetype: "image/jpeg",
  originalname: "fixture.jpg",
  size: fixture.length
}, { requestId: "projected-crs-source-evidence-regression" });
assert.equal(imageIdentity.image_sha256, fixtureSha256);

for (const [sourceText, expected] of [
  ["No | X | Y\nUTM WGS 1984 ZONA 50S", { id: "EPSG:32750", axisOrder: "easting_northing" }],
  ["POINT | EASTING | NORTHING\nUTM ZONE 31N", { id: "EPSG:32631", axisOrder: "easting_northing" }],
  ["POINT | NORTHING | EASTING\nEPSG:32748", { id: "EPSG:32748", axisOrder: "northing_easting" }]
]) {
  const context = extractProjectedSourceContext(sourceText);
  assert.equal(context.status, "COMPLETE", sourceText);
  assert.equal(context.crsEvidence.id, expected.id, sourceText);
  assert.equal(context.axisOrder, expected.axisOrder, sourceText);
}

assert.equal(extractProjectedSourceContext("POINT | X | Y").status, "INCOMPLETE");
assert.equal(extractProjectedSourceContext("UTM ZONE 31N").status, "INCOMPLETE");
assert.equal(extractProjectedSourceContext("POINT | X | Y\nUTM ZONE 31N\nUTM ZONE 32N").crsConflict, true);
assert.equal(extractProjectedSourceContext("POINT | X | Y\nPOINT | Y | X\nUTM ZONE 31N").axisConflict, true);

const providerText = [
  "[UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE]",
  "1 | 778984.492 | 9721476.737",
  "2 | 779099.680 | 9721476.848",
  "3 | 779099.680 | 9721110.798",
  "4 | 778875.519 | 9721110.798",
  "5 | 778875.519 | 9721180.576",
  "6 | 778984.492 | 9721180.576"
].join("\n");
const providerEvidence = extractProviderProjectedCoordinateEvidence({ sourceText: providerText });
assert.equal(providerEvidence.status, "COMPLETE");
assert.equal(providerEvidence.crsEvidence.status, "UNCONFIRMED");

const providerHeaderText = [
  "No | X | Y",
  "1 | 778807.293 | 9721476.737",
  "2 | 778981.768 | 9721477.288",
  "3 | 778982.700 | 9721182.351"
].join("\n");
const providerHeaderEvidence = extractProviderProjectedCoordinateEvidence({ sourceText: providerHeaderText });
assert.equal(providerHeaderEvidence.status, "COMPLETE");
assert.equal(providerHeaderEvidence.axisOrder, "easting_northing");
assert.deepEqual(providerHeaderEvidence.rows.map(row => [row.label, row.x, row.y]), [
  ["1", "778807.293", "9721476.737"],
  ["2", "778981.768", "9721477.288"],
  ["3", "778982.700", "9721182.351"]
], "Provider projected digits remain byte-for-byte field values after extraction");

const classificationImage = await createLocalOcrClassificationImage({ imageBuffer: fixture, imageIdentity });
assert.equal(classificationImage.layoutSafe, false);
assert.equal(classificationImage.provenance.mode, "overview_footer_composite");
assert.equal(classificationImage.provenance.image_sha256, fixtureSha256);
const worker = await Tesseract.createWorker("eng", 1, {
  langPath: tessdataPath,
  gzip: true,
  cacheMethod: "none",
  logger: () => {},
  errorHandler: () => {}
});
let localContextText = "";
try {
  const result = await worker.recognize(classificationImage.image, {}, { text: true });
  localContextText = String(result?.data?.text || "");
} finally {
  await worker.terminate();
}
const fixtureContext = extractProjectedSourceContext(localContextText);
assert.equal(fixtureContext.status, "COMPLETE");
assert.equal(fixtureContext.crsEvidence.id, "EPSG:32750");
assert.equal(fixtureContext.axisOrder, "easting_northing");

const boundEvidence = bindProjectedEvidenceToSourceContext({
  providerEvidence,
  sourceContextText: localContextText,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(boundEvidence.sourceContextBinding.bound, true);
assert.equal(boundEvidence.crsEvidence.id, "EPSG:32750");
assert.equal(boundEvidence.axisOrder, "easting_northing");
assert.equal(boundEvidence.diagnostics.headerPresent, true);
assert.equal(boundEvidence.rowCount, 6);

const crsOnlySourceContext = "SISTEM KOORDINAT\nUTM WGS 1984 ZONA 50S";
const splitEvidenceBinding = bindProjectedEvidenceToSourceContext({
  providerEvidence: providerHeaderEvidence,
  sourceContextText: crsOnlySourceContext,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(extractProjectedSourceContext(crsOnlySourceContext).status, "INCOMPLETE");
assert.equal(splitEvidenceBinding.sourceContextBinding.bound, true,
  "same-image local CRS and Provider X/Y header establish one bound projected result");
assert.equal(splitEvidenceBinding.crsEvidence.id, "EPSG:32750");
assert.equal(splitEvidenceBinding.axisOrder, "easting_northing");
assert.equal(splitEvidenceBinding.sourceContextBinding.crsSource, "local_ocr");
assert.equal(splitEvidenceBinding.sourceContextBinding.axisSource, "provider_header");

const noHeaderSplitBinding = bindProjectedEvidenceToSourceContext({
  providerEvidence,
  sourceContextText: crsOnlySourceContext,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.notEqual(noHeaderSplitBinding.sourceContextBinding.bound, true,
  "CRS-only context cannot authorize projected rows without an explicit axis header");

const axisConflictBinding = bindProjectedEvidenceToSourceContext({
  providerEvidence: providerHeaderEvidence,
  sourceContextText: "POINT | Y | X\nUTM WGS 1984 ZONA 50S",
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(axisConflictBinding.sourceContextBinding.axisConflict, true);
assert.notEqual(axisConflictBinding.sourceContextBinding.bound, true);

const providerDmsOnlyText = [
  `2°31'2,794" S | 119°30'31,553" E`,
  `2°31'2,783" S | 119°30'35,279" E`,
  `2°31'14,694" S | 119°30'35,302" E`,
  `2°31'14,708" S | 119°30'28,050" E`,
  `2°31'12,437" S | 119°30'28,046" E`,
  `2°31'12,430" S | 119°30'31,571" E`
].join("\n");
const fixtureMultiRepresentationBinding = bindProviderRepresentationsToSource({
  providerDmsReviewEvidence: normalizeProviderDmsReviewResult(providerDmsOnlyText),
  providerProjectedEvidence: extractProviderProjectedCoordinateEvidence({ sourceText: providerDmsOnlyText }),
  sourceContextText: localContextText,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(fixtureMultiRepresentationBinding.status, "COMPLETE");
assert.equal(fixtureMultiRepresentationBinding.providerMode, "DMS_ONLY");
assert.deepEqual(fixtureMultiRepresentationBinding.labels, ["1", "2", "3", "4", "5", "6"]);

const exactMultiSourceText = [
  "No | X | Y | Latitude | Longitude",
  "UTM WGS 1984 ZONA 50S",
  `1 | 778807.293 | 9721476.737 | 2°31'2,805\" S | 119°30'25,820\" E`,
  `2 | 778981.768 | 9721477.288 | 2°31'2,776\" S | 119°30'31,465\" E`,
  `3 | 778982.700 | 9721182.351 | 2°31'12,373\" S | 119°30'31,513\" E`
].join("\n");
const exactProviderProjected = extractProviderProjectedCoordinateEvidence({ sourceText: providerHeaderText });
const exactMultiBinding = bindProviderRepresentationsToSource({
  providerProjectedEvidence: exactProviderProjected,
  sourceContextText: exactMultiSourceText,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(exactMultiBinding.status, "COMPLETE");
assert.deepEqual(exactMultiBinding.labels, ["1", "2", "3"]);

const nearButDifferentProvider = extractProviderProjectedCoordinateEvidence({
  sourceText: providerHeaderText.replace("9721182.351", "9721188.351")
});
const nearButDifferentBinding = bindProviderRepresentationsToSource({
  providerProjectedEvidence: nearButDifferentProvider,
  sourceContextText: exactMultiSourceText,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(nearButDifferentBinding.status, "CONFLICT");
assert.equal(nearButDifferentBinding.reason, "PROVIDER_SOURCE_PROJECTED_OR_ORDER_CONFLICT");
assert.deepEqual(nearButDifferentBinding.rows, [],
  "near-looking digit substitutions never replace same-image source rows");

const shiftedFieldProvider = extractProviderProjectedCoordinateEvidence({
  sourceText: providerHeaderText.replace(
    "2 | 778981.768 | 9721477.288",
    "2 | 9721477.288 | 778981.768"
  )
});
const shiftedFieldBinding = bindProviderRepresentationsToSource({
  providerProjectedEvidence: shiftedFieldProvider,
  sourceContextText: exactMultiSourceText,
  sourceContextProvenance: classificationImage.provenance,
  imageIdentity
});
assert.equal(shiftedFieldBinding.status, "CONFLICT");
assert.deepEqual(shiftedFieldBinding.rows, []);

const acquisition = {
  width: imageIdentity.width,
  height: imageIdentity.height,
  bytes: fixture.length,
  images: [{ role: "overview" }, { role: "detail" }]
};
const unifiedEvidence = buildRecognitionAcquisitionEvidence({
  rawText: providerText,
  sourceContextText: localContextText,
  sourceBoundProjectedEvidence: boundEvidence,
  acquisition,
  providerResponseId: "offline-provider-projected-source-binding"
});
const projectedAuthorization = evaluateProjectedCoordinateAuthorizationEvidence(unifiedEvidence);
assert.equal(projectedAuthorization.eligible, true);
assert.deepEqual(projectedAuthorization.reasons, []);
const decision = evaluateUnifiedRecognitionAcquisition({
  evidence: unifiedEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
assert.equal(decision.projectedAuthorizationEligible, true);

const reviewBody = {
  success: true,
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  requiresReview: true,
  boundaryBlocked: true,
  mapReady: true,
  kmlReady: true,
  coordinateEngineV2: {
    source_crs: { id: "EPSG:32750", axisOrder: "easting_northing" }
  },
  finalizedCoordinateResult: finalizeCoordinateResult({
    resultId: "result_projected_review",
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    decisionState: "REVIEW_REQUIRED",
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    sourceAuthority: "legacy",
    explicitAuthorityRejected: false,
    requiresReview: true,
    technicalKmlReady: true,
    kmlReady: true,
    kmlAuthorityBlocked: false,
    geometry: { type: "Polygon", coordinates: [[[119, -2], [120, -2], [120, -3], [119, -2]]] },
    crs: FINALIZED_COORDINATE_CRS
  })
};
const finalAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: reviewBody,
  evidence: unifiedEvidence,
  decision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
});
assert.equal(finalAuthorization.authorized, false);
assert.equal(finalAuthorization.mapReady, true);
assert.equal(finalAuthorization.kmlReady, true);

for (const invalidContext of [
  "POINT | X | Y",
  "POINT | X | Y\nUTM ZONE 31N\nUTM ZONE 32N",
  "POINT | X | Y\nPOINT | Y | X\nUTM ZONE 31N"
]) {
  const invalidEvidence = bindProjectedEvidenceToSourceContext({
    providerEvidence,
    sourceContextText: invalidContext,
    sourceContextProvenance: classificationImage.provenance,
    imageIdentity
  });
  assert.notEqual(invalidEvidence.sourceContextBinding.bound, true, invalidContext);
  assert.notEqual(invalidEvidence.crsEvidence.status, "EXPLICIT", invalidContext);
}

const html = await readFile(path.join(root, "index.html"), "utf8");
assert.match(html, /showRecognitionProgress\(recognitionAuthorizationReasonMessage\("map"\), "error", 0\)/u,
  "blocked recognition renders the blocking reason instead of a review prompt");
assert.match(html, /!handwrittenDmsReviewState\.required \|\| recognitionReviewActionsBlocked\(\)/u,
  "blocked recovery UI and review acknowledgement are mutually exclusive");
assert.match(html, /原始 CRS：未确认/u);
assert.match(html, /投影区域：\$\{detail\.sourceZone\}/u);
assert.match(html, /投影半球：\$\{detail\.sourceHemisphere\}/u);
assert.match(html, /投影轴序：\$\{detail\.sourceAxisOrder\}/u);

console.log("projected CRS source evidence regression: PASS (Provider calls: 0)");
