import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildRecognitionAcquisitionEvidence,
  buildRecognitionAcquisitionLogSummary,
  evaluateUnifiedRecognitionAcquisition
} from "../server/recognition/recognition-first-acquisition.js";
import {
  RECOGNITION_ACQUISITION_JOB_STATUS,
  createRecognitionAcquisitionJobRuntime,
  getRecognitionAcquisitionJobHttpStatus
} from "../server/recognition/recognition-acquisition-job-runtime.js";
import {
  COORDINATE_USAGE_COMMIT_RESULT,
  buildRecognitionAcquisitionReviewUsageAuthority,
  buildUnchargedCoordinateFailureResponse,
  createCoordinateUsageCommitController,
  evaluateCoordinateUsageAuthority
} from "../server/coordinate-usage-atomicity.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requestId = "11111111-1111-4111-8111-111111111111";
const acquisition = {
  width: 1400,
  height: 6200,
  bytes: 2_000_000,
  asyncRecommended: true,
  images: [
    { role: "overview" },
    { role: "detail" },
    { role: "detail" },
    { role: "detail" }
  ]
};

const longRows = Array.from({ length: 20 }, (_, index) => (
  `${index + 1} ${658800 + (index * 13)} ${1364200 + (index * 17)}`
));
const longTable = [
  "CONTEXT | ITRF 2008 / Projection BFTM",
  "HEADING | Boundary",
  "Point X Y",
  ...longRows.slice(0, 10),
  "Point | X | Y",
  ...longRows.slice(10)
].join("\n");
const longEvidence = buildRecognitionAcquisitionEvidence({ rawText: longTable, acquisition, providerResponseId: "provider-long" });
assert.equal(longEvidence.providerCompletionState, "SUCCEEDED");
assert.equal(longEvidence.acquisitionStatus, "COMPLETED");
assert.equal(longEvidence.normalizationStatus, "COMPLETED");
assert.equal(longEvidence.authorizationStatus, "REVIEW_REQUIRED");
assert.equal(longEvidence.candidateCoordinates.length, 20);
assert.equal(longEvidence.candidateCoordinateLines.length, 20);
assert.equal(longEvidence.candidateCoordinateGroups.length, 1);
assert.deepEqual(longEvidence.candidateCoordinates.map(candidate => candidate.sourceLabel), Array.from({ length: 20 }, (_, index) => String(index + 1)));
assert.equal(longEvidence.imageEvidence.imageCount, 4);
assert.equal(longEvidence.imageEvidence.detailTileCount, 3);
assert.ok(longEvidence.visibleCrsEvidence.some(item => /BFTM/iu.test(item.text)));
const longUnifiedDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: longEvidence,
  contractStatus: "CONFORMANT",
  contractReason: "CONTRACT_CONFORMANT"
});
assert.equal(longUnifiedDecision.shouldReturnReview, true);
assert.equal(longUnifiedDecision.mayProceedToGeometryValidation, false);
assert.equal(longUnifiedDecision.authorizationStatus, "REVIEW_REQUIRED");
assert.ok(longUnifiedDecision.contractReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"));

const gkRows = Array.from({ length: 65 }, (_, index) => (
  `${index + 1}\t${13_640_000 + (index * 19)}\t${4_650_000 + (index * 23)}`
));
const gkEvidence = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | Gauss-Kruger Zone 12", "No.\tX\tY", ...gkRows].join("\n"),
  acquisition,
  providerResponseId: "provider-gk"
});
assert.equal(gkEvidence.candidateCoordinates.length, 65);
assert.equal(gkEvidence.candidateCoordinateGroups.length, 1);
assert.deepEqual(gkEvidence.candidateCoordinates.map(candidate => candidate.sourceLabel), Array.from({ length: 65 }, (_, index) => String(index + 1)));
assert.ok(gkEvidence.visibleCrsEvidence.some(item => /GAUSS|GK/iu.test(item.text)));
assert.equal(evaluateUnifiedRecognitionAcquisition({
  evidence: gkEvidence,
  contractStatus: "CONFORMANT"
}).shouldReturnReview, true);

const decimalEvidence = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | WGS 84", "Point | Latitude | Longitude", "1 | 6.752778 | -4.369444"].join("\n")
});
assert.equal(decimalEvidence.candidateCoordinates.length, 1);
assert.equal(decimalEvidence.candidateCoordinates[0].format, "WGS84_DECIMAL");
assert.equal(decimalEvidence.candidateCoordinates[0].sourceLabel, "1");

const utmEvidence = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | UTM Zone 30N", "Point;Easting;Northing", "1;658800;1364200"].join("\n")
});
assert.equal(utmEvidence.candidateCoordinates.length, 1);
assert.equal(utmEvidence.candidateCoordinates[0].format, "PROJECTED_XY");
assert.ok(utmEvidence.visibleCrsEvidence.some(item => /UTM/iu.test(item.text)));

const mgrsEvidence = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | MGRS", "1 | 30N AB 12345 67890"].join("\n")
});
assert.equal(mgrsEvidence.candidateCoordinates.length, 1);
assert.equal(mgrsEvidence.candidateCoordinates[0].format, "MGRS");
assert.equal(mgrsEvidence.candidateCoordinates[0].sourceLabel, "1");

const dmsRow = (label, latSeconds, lonSeconds) => (
  `${label ? `${label} | ` : "| "}6° 45' ${latSeconds}\" N | 4° 22' ${lonSeconds}\" W`
);
const spacedDmsPair = (latSeconds, lonSeconds) => (
  `6\u00b0 45' ${latSeconds}\" N 4\u00b0 22' ${lonSeconds}\" W`
);
const compactDmsPair = (latSeconds, lonSeconds) => (
  `6\u00b045'${latSeconds}\"N 4\u00b022'${lonSeconds}\"W`
);
const separatedDmsPair = (separator, latSeconds, lonSeconds) => (
  `6\u00b0 45' ${latSeconds}\" N${separator}4\u00b0 22' ${lonSeconds}\" W`
);

for (const { row, label } of [
  { row: `POINT 1 ${spacedDmsPair("10", "10")}`, label: "1" },
  { row: `Point 2 ${spacedDmsPair("11", "11")}`, label: "2" },
  { row: `PT 3 ${spacedDmsPair("12", "12")}`, label: "3" },
  { row: `4 ${spacedDmsPair("13", "13")}`, label: "4" },
  { row: spacedDmsPair("14", "14"), label: null },
  { row: `POINT 5 ${compactDmsPair("15", "15")}`, label: "5" },
  { row: `6 | ${separatedDmsPair(" | ", "16", "16")}`, label: "6" },
  { row: `7\t${separatedDmsPair("\t", "17", "17")}`, label: "7" },
  { row: `8;${separatedDmsPair(";", "18", "18")}`, label: "8" }
]) {
  const evidence = buildRecognitionAcquisitionEvidence({
    rawText: ["CONTEXT | WGS 84", row].join("\n"),
    acquisition
  });
  assert.equal(evidence.candidateCoordinates.length, 1, row);
  assert.equal(evidence.candidateCoordinates[0].sourceLabel, label, row);
  assert.equal(evidence.candidateCoordinates[0].sourceLabelInferred, false, row);
  assert.equal(evidence.candidateCoordinates[0].sourceText, row, row);
}

const mixedSpaceSeparatedDms = [
  "CONTEXT | WGS 84",
  "HEADING | Parent",
  "HEADING | Explicit labels",
  "Point Latitude Longitude",
  `POINT 1 ${spacedDmsPair("20", "20")}`,
  `2 ${compactDmsPair("21", "21")}`,
  "HEADING | Parent",
  "HEADING | Visible order only",
  "Point Latitude Longitude",
  spacedDmsPair("22", "22"),
  compactDmsPair("23", "23")
].join("\n");
const mixedSpaceSeparatedEvidence = buildRecognitionAcquisitionEvidence({
  rawText: mixedSpaceSeparatedDms,
  acquisition
});
assert.equal(mixedSpaceSeparatedEvidence.candidateCoordinates.length, 4);
assert.equal(mixedSpaceSeparatedEvidence.candidateCoordinateGroups.length, 2);
assert.deepEqual(mixedSpaceSeparatedEvidence.candidateCoordinateGroups[0].rows.map(row => row.sourceLabel), ["1", "2"]);
assert.ok(mixedSpaceSeparatedEvidence.candidateCoordinateGroups[1].rows.every(row => (
  row.sourceLabel === null && row.sourceLabelInferred === false
)));
assert.ok(mixedSpaceSeparatedEvidence.reviewReasons.includes("SOURCE_LABELS_MISSING"));

const dmsGroup = (parent, title, rows) => [
  `HEADING | ${parent}`,
  `HEADING | ${title}`,
  "Point | Latitude | Longitude",
  ...rows
].join("\n");
const mixedDms = [
  "CONTEXT | WGS 84",
  dmsGroup("Parent A", "Area 1", [dmsRow("1", "10", "10"), dmsRow("2", "11", "11"), dmsRow("3", "12", "12"), dmsRow("4", "13", "13")]),
  dmsGroup("Parent A", "Area 2", [dmsRow("1", "20", "20"), dmsRow("2", "21", "21"), dmsRow("3", "22", "22"), dmsRow("4", "23", "23")]),
  dmsGroup("Parent B", "Area 1", [dmsRow("", "30", "30"), dmsRow("", "31", "31"), dmsRow("", "32", "32"), dmsRow("", "33", "33")]),
  dmsGroup("Parent B", "Area 2", [dmsRow("", "40", "40"), dmsRow("", "41", "41"), dmsRow("", "42", "42"), dmsRow("", "43", "43")])
].join("\n");
const mixedEvidence = buildRecognitionAcquisitionEvidence({ rawText: mixedDms, acquisition, providerResponseId: "provider-dms" });
assert.equal(mixedEvidence.candidateCoordinates.length, 16);
assert.equal(mixedEvidence.candidateCoordinateGroups.length, 4);
assert.deepEqual(mixedEvidence.candidateCoordinateGroups.map(group => group.rows.length), [4, 4, 4, 4]);
assert.ok(mixedEvidence.candidateCoordinateGroups[2].rows.every(row => row.sourceLabel === null && row.sourceLabelInferred === false));
assert.ok(mixedEvidence.reviewReasons.includes("SOURCE_LABELS_MISSING"));
assert.equal(evaluateUnifiedRecognitionAcquisition({
  evidence: mixedEvidence,
  contractStatus: "CONFORMANT"
}).shouldReturnReview, true);

const safeDmsEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "CONTEXT | WGS 84",
    dmsGroup("Parent", "Unique Area", [
      dmsRow("1", "10", "10"),
      dmsRow("2", "11", "11"),
      dmsRow("3", "12", "12"),
      dmsRow("4", "13", "13")
    ])
  ].join("\n"),
  acquisition
});
const safeDmsDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: safeDmsEvidence,
  contractStatus: "CONFORMANT"
});
assert.equal(safeDmsDecision.shouldReturnReview, false);
assert.equal(safeDmsDecision.mayProceedToGeometryValidation, true);
assert.equal(safeDmsDecision.authorizationStatus, "VALIDATION_PENDING");

const multiPair = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | BFTM", "Point | X1 | Y1 | X2 | Y2", "A | 658800 | 1364200 | 658900 | 1364300"].join("\n")
});
assert.equal(multiPair.candidateCoordinates.length, 2);
assert.ok(multiPair.reviewReasons.includes("MULTIPLE_COORDINATES_PER_SOURCE_ROW"));

const duplicateGroupEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "CONTEXT | WGS 84",
    dmsGroup("Page", "Coordinate Table", [dmsRow("1", "10", "10"), dmsRow("2", "11", "11")]),
    dmsGroup("Page", "Coordinate Table", [dmsRow("3", "12", "12"), dmsRow("4", "13", "13")])
  ].join("\n")
});
assert.equal(duplicateGroupEvidence.acquisitionStatus, "COMPLETED");
assert.equal(duplicateGroupEvidence.candidateCoordinates.length, 4);
assert.equal(duplicateGroupEvidence.candidateCoordinateGroups.length, 0);
assert.equal(duplicateGroupEvidence.unboundCandidates.length, 4);
assert.ok(duplicateGroupEvidence.reviewReasons.includes("GROUP_BOUNDARY_AMBIGUOUS"));

const malformedButPreserved = buildRecognitionAcquisitionEvidence({
  rawText: ["CONTEXT | WGS 84", "Point | Latitude | Longitude", dmsRow("1", "10", "10"), `${dmsRow("2", "11", "11")} | 99`].join("\n")
});
assert.equal(malformedButPreserved.acquisitionStatus, "COMPLETED");
assert.equal(malformedButPreserved.normalizationStatus, "PARTIAL");
assert.equal(malformedButPreserved.candidateCoordinates.length, 1);
assert.equal(malformedButPreserved.rejectedRows.length, 1);
assert.match(malformedButPreserved.rawProviderText, /\| 99/u);

for (const nonCoordinate of [
  "",
  "Report 2024 revision 3 area 12",
  "Scale 1:5000 sheet 12",
  "Survey date 2026-09-24 area 4.97 ha",
  "Version 2 section 4 page 7",
  "Explanation of X and Y values\n2026 09 24",
  "Point | X | Y\nReport date 2026-09-24\n1 | 12 | 24",
  "Point | Latitude | Longitude\n1 | 6° 45' 10\" N | incomplete",
  `POINT 1 6\u00b0 45' 10\" N 4\u00b0 22' 10\" S`,
  "Point | X | Y\n1 | 658800 | 1364200 | 99"
]) {
  const evidence = buildRecognitionAcquisitionEvidence({ rawText: nonCoordinate, acquisition });
  assert.notEqual(evidence.acquisitionStatus, "COMPLETED", nonCoordinate);
  assert.equal(evidence.candidateCoordinates.length, 0, nonCoordinate);
  const decision = evaluateUnifiedRecognitionAcquisition({
    evidence,
    contractStatus: "CONFORMANT"
  });
  assert.equal(decision.shouldReturnFailure, true, nonCoordinate);
  assert.equal(decision.authorizationStatus, "NOT_ESTABLISHED", nonCoordinate);
}

const reviewBody = {
  success: true,
  requestId,
  rawText: longEvidence.rawProviderText,
  coordinates: longEvidence.candidateCoordinateLines.map(line => line.text).join("\n"),
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  requiresReview: true,
  kmlReady: false,
  mapReady: false,
  recognitionAcquisition: longEvidence,
  candidateCoordinates: longEvidence.candidateCoordinates,
  candidateCoordinateGroups: longEvidence.candidateCoordinateGroups,
  visibleCrsEvidence: longEvidence.visibleCrsEvidence,
  imageAcquisitionEvidence: longEvidence.imageEvidence,
  reviewReasons: ["FORMAT_CONTRACT_REVIEW_REQUIRED"]
};
reviewBody.recognitionAcquisitionReviewAuthority = buildRecognitionAcquisitionReviewUsageAuthority({
  recognitionRequestId: requestId,
  body: reviewBody
});
assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body: reviewBody }).eligible, true);
assert.equal(evaluateCoordinateUsageAuthority({
  httpStatus: 200,
  body: { ...reviewBody, rawText: `${reviewBody.rawText}\nTAMPERED` }
}).eligible, false);

let prepareCount = 0;
let commitCount = 0;
let recoveredPayload = null;
const atomicityService = {
  async prepare({ responsePayload }) {
    prepareCount += 1;
    recoveredPayload = structuredClone(responsePayload);
    return { result: COORDINATE_USAGE_COMMIT_RESULT.PREPARED };
  },
  async commit() {
    commitCount += 1;
    return { result: COORDINATE_USAGE_COMMIT_RESULT.COMMITTED };
  },
  async recover() {
    return {
      result: COORDINATE_USAGE_COMMIT_RESULT.COMMITTED,
      responsePayload: structuredClone(recoveredPayload)
    };
  }
};
const usageController = createCoordinateUsageCommitController({
  atomicityService,
  recognitionRequestId: requestId,
  userId: "offline-user",
  sessionBindingSha256: `sha256:${"1".repeat(64)}`,
  providerCostState: () => "INCURRED"
});
assert.equal(usageController.schedule({ note: "offline acquisition review" }).success, true);
const usageSettlement = await usageController.settle({ httpStatus: 200, body: reviewBody });
assert.equal(usageSettlement.kind, "USAGE_COMMITTED");
assert.equal(prepareCount, 1);
assert.equal(commitCount, 1);
assert.equal((await atomicityService.recover()).responsePayload.rawText, longEvidence.rawProviderText);
assert.equal((await usageController.settle({ httpStatus: 200, body: reviewBody })).kind, "DUPLICATE_SETTLEMENT");
assert.equal(prepareCount, 1);
assert.equal(commitCount, 1);

const noCharge = buildUnchargedCoordinateFailureResponse({
  body: { success: false, reason: "recognition_failed_closed", code: "COORDINATE_RECOGNITION_FAILED_CLOSED", rawText: "secret" },
  recognitionRequestId: requestId
});
assert.equal(noCharge.usageConsumed, false);
assert.equal(noCharge.recoveryRequired, false);
assert.equal(noCharge.recoveryTerminal, true);
assert.equal(noCharge.rawText, "");

const reviewRuntime = createRecognitionAcquisitionJobRuntime({
  execute: async () => ({ httpStatus: 200, result: reviewBody })
});
const reviewJob = reviewRuntime.enqueue({});
let reviewSnapshot;
while (reviewSnapshot?.completedAt == null) {
  await new Promise(resolve => setImmediate(resolve));
  reviewSnapshot = reviewRuntime.get(reviewJob.jobId, reviewJob.jobAccessToken);
}
assert.equal(reviewSnapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED);
assert.equal(getRecognitionAcquisitionJobHttpStatus(reviewSnapshot), 200);
assert.equal(reviewSnapshot.result.resultStatus, "needs_review");

const failedRuntime = createRecognitionAcquisitionJobRuntime({
  execute: async () => ({ httpStatus: 422, result: noCharge })
});
const failedJob = failedRuntime.enqueue({});
let failedSnapshot;
while (failedSnapshot?.completedAt == null) {
  await new Promise(resolve => setImmediate(resolve));
  failedSnapshot = failedRuntime.get(failedJob.jobId, failedJob.jobAccessToken);
}
assert.equal(failedSnapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
assert.equal(getRecognitionAcquisitionJobHttpStatus(failedSnapshot), 422);
assert.equal(failedSnapshot.result.usageConsumed, false);

const logSummary = buildRecognitionAcquisitionLogSummary({
  evidence: longEvidence,
  providerCallCount: 1,
  contractReason: "FORMAT_CONTRACT_REVIEW_REQUIRED",
  contractReasons: ["COORDINATE_FORMAT_REQUIRES_VALIDATION"],
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  mapStatus: "CLOSED",
  kmlStatus: "CLOSED",
  userUsageConsumed: true,
  recoveryRequired: false,
  finalState: "COMPLETED_REVIEW_REQUIRED"
});
assert.deepEqual(logSummary, {
  providerCompletionState: "SUCCEEDED",
  providerCallCount: 1,
  imageCount: 4,
  detailTileCount: 3,
  providerOutputLength: longTable.length,
  candidatePointCount: 20,
  candidateGroupCount: 1,
  boundRowCount: 20,
  unboundRowCount: 0,
  contractReason: "FORMAT_CONTRACT_REVIEW_REQUIRED",
  contractReasons: ["FORMAT_CONTRACT_REVIEW_REQUIRED", "COORDINATE_FORMAT_REQUIRES_VALIDATION"],
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  mapStatus: "CLOSED",
  kmlStatus: "CLOSED",
  userUsageConsumed: true,
  recoveryRequired: false,
  finalState: "COMPLETED_REVIEW_REQUIRED"
});
assert.equal(JSON.stringify(logSummary).includes("658800"), false);

const candidateSource = fs.readFileSync(path.join(root, "server/recognition/recognition-candidate-evidence.js"), "utf8").toLowerCase();
for (const forbidden of ["kyrgyz", "cote", "ivory", ".jpg", ".png"]) {
  assert.equal(candidateSource.includes(forbidden), false, `generic extractor must not contain ${forbidden}`);
}
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
assert.equal((serverSource.match(/acquisitionEvidence\.acquisitionStatus === "COMPLETED"/gu) || []).length, 2);
assert.equal((serverSource.match(/recognitionAcquisitionReviewAuthority = buildRecognitionAcquisitionReviewUsageAuthority/gu) || []).length, 3);
assert.equal((serverSource.match(/registerUnifiedRecognitionAcquisition\(\{/gu) || []).length, 2);
assert.equal((serverSource.match(/returnUnifiedRecognitionAcquisitionTerminal\(\{/gu) || []).length, 3);
assert.match(serverSource, /const wgs84UnifiedAcquisitionEvidence = buildRecognitionAcquisitionEvidence[\s\S]+if \(wgs84PrimaryConformance\.status !==/u);
assert.match(serverSource, /const unifiedAcquisitionEvidence = buildRecognitionAcquisitionEvidence[\s\S]+if \(oneShotAcquisitionConformance\.status !==/u);
assert.match(serverSource, /Recognition acquisition final state:/u);
assert.match(serverSource, /recovered\.result === COORDINATE_USAGE_COMMIT_RESULT\.NOT_FOUND/);
assert.doesNotMatch(serverSource, /res\.status\(acquisitionCompleted \? 200 : 422\)\.json/);

console.log("recognition-first acquisition-evidence v4 regression: PASS (unified conformant/nonconformant routing, 20-row, 65-row, 4-group DMS, evidence retention, async, usage/recovery, fail-close)");
