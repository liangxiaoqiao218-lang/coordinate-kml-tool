import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Tesseract from "tesseract.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  bindProjectedEvidenceToSourceContext,
  createLocalOcrClassificationImage,
  extractProjectedSourceContext
} from "../server/recognition/projected-source-evidence.js";
import {
  buildRecognitionAcquisitionEvidence,
  evaluateProjectedCoordinateAuthorizationEvidence,
  evaluateUnifiedRecognitionAcquisition,
  evaluateUnifiedRecognitionFinalAuthorization
} from "../server/recognition/recognition-first-acquisition.js";
import { extractProviderProjectedCoordinateEvidence } from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

const classificationImage = await createLocalOcrClassificationImage({ imageBuffer: fixture, imageIdentity });
assert.equal(classificationImage.layoutSafe, false);
assert.equal(classificationImage.provenance.mode, "overview_footer_composite");
assert.equal(classificationImage.provenance.image_sha256, fixtureSha256);
const worker = await Tesseract.createWorker("eng", 1, { logger: () => {}, errorHandler: () => {} });
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
  finalizedCoordinateResult: {
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
    crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" }
  }
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

console.log("projected CRS source evidence regression: PASS (Provider calls: 0)");
