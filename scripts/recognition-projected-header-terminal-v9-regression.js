import assert from "node:assert/strict";
import { randomInt } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildRecognitionAcquisitionEvidence,
  evaluateProjectedCoordinateAuthorizationEvidence,
  evaluateUnifiedRecognitionAcquisition,
  evaluateUnifiedRecognitionFinalAuthorization
} from "../server/recognition/recognition-first-acquisition.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const xOrigin = randomInt(310_000, 690_000);
const yOrigin = randomInt(810_000, 1_910_000);
const offsets = Array.from({ length: 20 }, (_, index) => [
  (index % 5) * 170,
  Math.floor(index / 5) * 230
]);
const acquisition = {
  width: 1000,
  height: 4200,
  bytes: 2_300_000,
  images: [{ role: "overview" }, { role: "detail" }, { role: "detail" }]
};

function providerText(header, { includeCrs = true } = {}) {
  return [
    ...(includeCrs ? ["CONTEXT | Projected coordinate system UTM Zone 32N"] : []),
    "HEADING | Projected boundary",
    header,
    ...offsets.map(([dx, dy], index) => `${index + 1} | ${xOrigin + dx} | ${yOrigin + dy}`)
  ].join("\n");
}

function evidenceFor(header, options = {}) {
  return buildRecognitionAcquisitionEvidence({
    rawText: providerText(header, options),
    acquisition,
    providerResponseId: "offline-provider-v9"
  });
}

const headerVariants = [
  "POINT | X | Y",
  "POINTS | X | Y",
  "VERTEX | X | Y",
  "VERTICES | X | Y",
  "SOMMET | X | Y",
  "SOMMETS | X | Y",
  "Sommets | X (m) | Y (m)",
  "SÓMMETS | X (mètres) | Y (mètres)"
];

for (const header of headerVariants) {
  const evidence = evidenceFor(header);
  assert.equal(evidence.acquisitionStatus, "COMPLETED", header);
  assert.equal(evidence.candidateCoordinates.length, 20, header);
  assert.equal(evidence.candidateCoordinateGroups.length, 1, header);
  assert.equal(evidence.diagnostics.boundRowCount, 20, header);
  assert.equal(evidence.diagnostics.unboundRowCount, 0, header);
  assert.equal(evidence.candidateCoordinates.every(candidate => candidate.format === "PROJECTED_XY"), true, header);
  assert.equal(evidence.candidateCoordinates.every(candidate => candidate.axisOrder === "x_y"), true, header);
  assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(evidence).eligible, true, header);
}

const proseEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "CONTEXT | Projected coordinate system UTM Zone 32N",
    "The points and vertices shown below describe the boundary in X and Y values.",
    "This paragraph mentions sommets, X and Y but is not a coordinate table."
  ].join("\n"),
  acquisition,
  providerResponseId: "offline-provider-v9-prose"
});
assert.equal(proseEvidence.candidateCoordinates.length, 0);
assert.equal(proseEvidence.candidateCoordinateGroups.length, 0);

const completeEvidence = evidenceFor("Sommets | X (m) | Y (m)");
const projectedDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: completeEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
const finalizedBody = {
  success: true,
  requiresReview: false,
  boundaryBlocked: false,
  mapReady: true,
  kmlReady: true,
  coordinateEngineV2: {
    source_crs: {
      id: "EPSG:32632",
      projection: "utm",
      zone: 32,
      hemisphere: "N",
      axisOrder: "easting_northing"
    }
  },
  finalizedCoordinateResult: {
    decisionState: "AUTO_EXPORT",
    qualityGateStatus: "passed",
    requiresReview: false,
    mapReady: true,
    kmlReady: true,
    geometry: { type: "Polygon", coordinates: [[[9, 7], [10, 7], [10, 8], [9, 7]]] },
    crs: { id: "EPSG:4326" }
  }
};
const authorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: finalizedBody,
  evidence: completeEvidence,
  decision: projectedDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
});
assert.equal(authorization.authorized, true);
assert.equal(authorization.finalRequiresReview, false);

const missingCrsEvidence = evidenceFor("Sommets | X | Y", { includeCrs: false });
const missingCrsDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: missingCrsEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
assert.equal(evaluateUnifiedRecognitionFinalAuthorization({
  body: finalizedBody,
  evidence: missingCrsEvidence,
  decision: missingCrsDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
}).authorized, false);

const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const baseStatusRule = indexSource.match(/\.recognition-status::before\s*\{([\s\S]*?)\n\s*\}/u)?.[1] || "";
const loadingStatusRule = indexSource.match(/\.recognition-status\.loading::before\s*\{([\s\S]*?)\n\s*\}/u)?.[1] || "";
assert.match(baseStatusRule, /animation:\s*none/u);
assert.doesNotMatch(baseStatusRule, /infinite/u);
assert.match(loadingStatusRule, /animation:\s*spin\s+0\.8s\s+linear\s+infinite/u);
assert.match(indexSource, /setRecognitionStatus\(\s*`采集完成，等待复核：\$\{recognitionAuthorizationReasonMessage\("map"\)\}`\s*,\s*"warning"\s*\)/u);
assert.match(indexSource, /setRecognitionStatus\("异步任务仍在后台运行，页面已停止等待",\s*"warning"\)/u);

console.log("recognition projected header terminal v9: PASS");
