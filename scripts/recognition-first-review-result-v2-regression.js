import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACQUISITION_REVIEW_STATUS,
  formatProviderDmsReviewCoordinates,
  normalizeProviderDmsReviewResult
} from "../server/recognition/recognition-review-result.js";
import { buildRecognitionAcquisitionEvidence } from "../server/recognition/recognition-first-acquisition.js";
import { finalizeCoordinateResult } from "../server/coordinate-finalizer/finalized-coordinate-result-v1.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const row = (label, latSeconds, lonSeconds, latDirection = "N", lonDirection = "W") => (
  `${label ? `${label} | ` : "| "}6° 45' ${latSeconds}" ${latDirection} | 4° 22' ${lonSeconds}" ${lonDirection}`
);
const group = (parent, title, rows) => [
  `HEADING | ${parent}`,
  `HEADING | ${title}`,
  "Point | Latitude | Longitude",
  ...rows
].join("\n");

for (const nonCoordinateText of [
  "Report 2024 revision 3 area 12",
  "Scale 1:5000 sheet 12",
  "Survey date 2026-09-24 area 4.97 ha",
  "Version 2 section 4 page 7",
  "CONTEXT | Map scale 1:10000, surveyed 2025-04-12, area 18.7 ha",
  "2026 | 09 | 24",
  "Explanation of X and Y values\n2026 09 24"
]) {
  const evidence = buildRecognitionAcquisitionEvidence({ rawText: nonCoordinateText });
  assert.equal(evidence.status, "NO_COORDINATE_EVIDENCE");
  assert.equal(evidence.candidateCoordinateLines.length, 0);
}

const strictCoordinateEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "CONTEXT | WGS 84 / UTM ZONE 50S",
    "No. | X | Y | Latitude | Longitude",
    "1 778984.492 9721476.737 2° 31' 2.794\" S 119° 30' 31.553\" E",
    "2 | 779099.680 | 9721476.848 | 2° 31' 2.783\" S | 119° 30' 35.279\" E"
  ].join("\n")
});
assert.equal(strictCoordinateEvidence.status, "COMPLETED");
assert.equal(strictCoordinateEvidence.candidateCoordinateLines.length, 2);

const headerBoundProjectedEvidence = buildRecognitionAcquisitionEvidence({
  rawText: ["Point X Y", "1 778984 9721476", "2 779099 9721476"].join("\n")
});
assert.equal(headerBoundProjectedEvidence.candidateCoordinateLines.length, 2);

const mgrsEvidence = buildRecognitionAcquisitionEvidence({ rawText: "1 | 33 U XP 12345 67890" });
assert.equal(mgrsEvidence.status, "COMPLETED");
assert.equal(mgrsEvidence.candidateCoordinateLines.length, 1);

const mixedFourGroupText = [
  "CONTEXT | WGS 84",
  group("Parent Alpha", "Area 1", [row("1", "10", "10"), row("2", "11", "11"), row("3", "12", "12"), row("4", "13", "13")]),
  group("Parent Alpha", "Area 2", [row("1", "20", "20"), row("2", "21", "21"), row("3", "22", "22"), row("4", "23", "23")]),
  group("Parent Beta", "Area 1", [row("", "30", "30"), row("", "31", "31"), row("", "32", "32"), row("", "33", "33")]),
  group("Parent Beta", "Area 2", [row("", "40", "40"), row("", "41", "41"), row("", "42", "42"), row("", "43", "43")])
].join("\n");

const mixed = normalizeProviderDmsReviewResult(mixedFourGroupText);
assert.equal(mixed.status, ACQUISITION_REVIEW_STATUS.REVIEW_REQUIRED);
assert.equal(mixed.candidatePointCount, 16);
assert.equal(mixed.candidateGroupCount, 4);
assert.equal(mixed.boundRowCount, 16);
assert.equal(mixed.unboundRowCount, 0);
assert.deepEqual(mixed.candidateGroups.map(candidate => candidate.rows.length), [4, 4, 4, 4]);
assert.deepEqual(mixed.candidateGroups.map(candidate => candidate.sourceLabelState), [
  "CONTINUOUS", "CONTINUOUS", "MISSING", "MISSING"
]);
assert.ok(mixed.candidateGroups[2].rows.every(candidate => candidate.sourceLabel === null));
assert.ok(mixed.candidateGroups[2].rows.every((candidate, index) => candidate.candidateOrder === index + 1));
assert.ok(mixed.reviewReasons.includes("SOURCE_LABELS_MISSING"));
assert.equal(formatProviderDmsReviewCoordinates(mixed).split(/\n/u).filter(Boolean).length, 16);

const fullyLabelledText = mixedFourGroupText.replace(/^\| /gmu, (_, offset, source) => {
  const preceding = source.slice(0, offset);
  const priorHeading = preceding.lastIndexOf("HEADING | Area");
  const localRows = preceding.slice(priorHeading).split(/\n/u).filter(line => /^\| /u.test(line)).length;
  return `${localRows + 1} | `;
});
const fullyLabelled = normalizeProviderDmsReviewResult(fullyLabelledText);
assert.equal(fullyLabelled.status, ACQUISITION_REVIEW_STATUS.AUTHORIZATION_CANDIDATE);
assert.equal(fullyLabelled.authorizationCandidate, true);
assert.equal(fullyLabelled.candidateGroupCount, 4);
assert.ok(fullyLabelled.candidateGroups.every(candidate => candidate.sourceLabelsContinuous));

const duplicateTitlePaths = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84",
  group("Repeated Parent", "Repeated Area", [row("1", "10", "10"), row("2", "11", "11"), row("3", "12", "12")]),
  group("Repeated Parent", "Repeated Area", [row("1", "20", "20"), row("2", "21", "21"), row("3", "22", "22")])
].join("\n"));
assert.equal(duplicateTitlePaths.status, ACQUISITION_REVIEW_STATUS.REVIEW_REQUIRED);
assert.equal(duplicateTitlePaths.authorizationCandidate, false);
assert.equal(duplicateTitlePaths.candidatePointCount, 6);
assert.equal(duplicateTitlePaths.candidateGroupCount, 0);
assert.equal(duplicateTitlePaths.unboundRowCount, 6);
assert.ok(duplicateTitlePaths.reviewReasons.includes("GROUP_TITLE_PATH_DUPLICATE"));

const repeatedPageHeading = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84",
  group("Page Heading", "Coordinate Table", [row("1", "10", "10"), row("2", "11", "11")]),
  group("Page Heading", "Coordinate Table", [row("3", "12", "12"), row("4", "13", "13")])
].join("\n"));
assert.equal(repeatedPageHeading.authorizationCandidate, false);
assert.equal(repeatedPageHeading.candidateGroupCount, 0);
assert.ok(repeatedPageHeading.reviewReasons.includes("GROUP_TITLE_PATH_DUPLICATE"));

const repeatedHeaderContinuation = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84",
  "HEADING | Parent",
  "HEADING | Area",
  "Point | Latitude | Longitude",
  row("1", "10", "10"), row("2", "11", "11"),
  "Point | Latitude | Longitude",
  row("3", "12", "12"), row("4", "13", "13")
].join("\n"));
assert.equal(repeatedHeaderContinuation.authorizationCandidate, false);
assert.equal(repeatedHeaderContinuation.candidatePointCount, 4);
assert.equal(repeatedHeaderContinuation.candidateGroupCount, 0);
assert.equal(repeatedHeaderContinuation.unboundRowCount, 4);
assert.ok(repeatedHeaderContinuation.reviewReasons.includes("REPEATED_HEADER_CONTINUATION_UNRESOLVED"));

const noHeading = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84",
  row("1", "10", "10"), row("2", "11", "11"), row("3", "12", "12"), row("4", "13", "13"),
  "",
  row("1", "20", "20"), row("2", "21", "21"), row("3", "22", "22"), row("4", "23", "23")
].join("\n"));
assert.equal(noHeading.status, ACQUISITION_REVIEW_STATUS.REVIEW_REQUIRED);
assert.equal(noHeading.candidatePointCount, 8);
assert.equal(noHeading.candidateGroupCount, 0);
assert.equal(noHeading.unboundRowCount, 8);
assert.ok(noHeading.reviewReasons.includes("GROUP_BOUNDARY_UNRESOLVED"));

const incomplete = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84", "HEADING | Area", "Point | Latitude | Longitude",
  row("1", "10", "10"), row("2", "11", "11"), row("3", "12", "12"),
  "4 | 6° 45' 13\" N | incomplete"
].join("\n"));
assert.equal(incomplete.candidatePointCount, 3);
assert.equal(incomplete.rejectedRows.length, 1);
assert.ok(incomplete.reviewReasons.includes("DMS_ROW_MALFORMED"));
assert.equal(incomplete.authorizationCandidate, false);

const extraNumber = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84", "HEADING | Area", "Point | Latitude | Longitude",
  row("1", "10", "10"), `${row("2", "11", "11")} | 99`, row("3", "12", "12"), row("4", "13", "13")
].join("\n"));
assert.equal(extraNumber.rejectedRows.length, 1);
assert.ok(extraNumber.reviewReasons.includes("DMS_EXTRA_CONTENT"));
assert.equal(extraNumber.authorizationCandidate, false);

const directionConflict = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84", "HEADING | Area", "Point | Latitude | Longitude",
  row("1", "10", "10"), "2 | 6° 45' 11\" N | 4° 22' 11\" S", row("3", "12", "12"), row("4", "13", "13")
].join("\n"));
assert.equal(directionConflict.rejectedRows.length, 1);
assert.ok(directionConflict.reviewReasons.includes("DMS_DIRECTION_CONFLICT"));

const duplicate = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84", "HEADING | Area", "Point | Latitude | Longitude",
  row("1", "10", "10"), row("2", "11", "11"), row("2", "12", "12"), row("4", "13", "13")
].join("\n"));
assert.ok(duplicate.reviewReasons.includes("SOURCE_LABELS_DUPLICATE"));
assert.equal(duplicate.authorizationCandidate, false);

const gap = normalizeProviderDmsReviewResult([
  "CONTEXT | WGS 84", "HEADING | Area", "Point | Latitude | Longitude",
  row("1", "10", "10"), row("2", "11", "11"), row("4", "12", "12"), row("5", "13", "13")
].join("\n"));
assert.ok(gap.reviewReasons.includes("SOURCE_LABELS_NONCONTIGUOUS"));
assert.equal(gap.authorizationCandidate, false);

const noCrs = normalizeProviderDmsReviewResult(group("Parent", "Area", [
  row("1", "10", "10"), row("2", "11", "11"), row("3", "12", "12"), row("4", "13", "13")
]));
assert.ok(noCrs.reviewReasons.includes("CRS_EVIDENCE_MISSING"));
assert.equal(noCrs.authorizationCandidate, false);

const reviewOnlyFinalized = finalizeCoordinateResult({
  sourceAuthority: "legacy",
  coordinateType: "dms",
  precisionMode: "provider-dms-candidate-review",
  crs: { type: "geographic", epsg: 4326, axisOrder: "longitude_latitude" },
  geometry: {
    type: "MultiPoint",
    coordinates: mixed.candidateGroups.flatMap(candidate => candidate.rows)
      .map(candidate => [candidate.longitude, candidate.latitude])
  },
  confirmationStatus: "pending",
  qualityGateStatus: "REVIEW_REQUIRED",
  technicalKmlReady: false,
  currentAuthorizedGeometryExportable: false,
  mapReady: false,
  requiresReview: true,
  kmlReady: false
});
assert.equal(reviewOnlyFinalized.kmlReady, false);
assert.equal(reviewOnlyFinalized.mapReady, false);
const reviewPreview = new MapPreviewAdapter().adapt(reviewOnlyFinalized);
assert.equal(reviewPreview.previewEligibility.allowed, false);
assert.deepEqual(reviewPreview.previewReasonCodes, ["REVIEW_RESULT_NOT_MAP_READY"]);

const selfIntersectingFinalized = finalizeCoordinateResult({
  sourceAuthority: "legacy",
  coordinateType: "dms",
  precisionMode: "dms-grouped-coordinates",
  crs: { type: "geographic", epsg: 4326, axisOrder: "longitude_latitude" },
  geometry: {
    type: "Polygon",
    coordinates: [[
      [0, 0], [1, 1], [0, 1], [1, 0], [0, 0]
    ]]
  },
  confirmationStatus: "accepted",
  qualityGateStatus: "PASSED",
  technicalKmlReady: true,
  currentAuthorizedGeometryExportable: true,
  requiresReview: false,
  kmlReady: true
});
assert.equal(selfIntersectingFinalized.decisionState, "BLOCKED");
assert.equal(selfIntersectingFinalized.kmlReady, false);

const reviewSource = fs.readFileSync(path.join(root, "server", "recognition", "recognition-review-result.js"), "utf8");
for (const forbidden of ["indonesia", "kyrgyz", "cote", "ivory", ".jpg", ".png"]) {
  assert.equal(reviewSource.toLowerCase().includes(forbidden), false, `generic review normalizer must not contain ${forbidden}`);
}

const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const deadlineSource = fs.readFileSync(path.join(root, "server", "coordinate-finalizer", "recognition-deadline.js"), "utf8");
const uiSource = fs.readFileSync(path.join(root, "index.html"), "utf8");
assert.equal((serverSource.match(/acquisitionEvidence\.acquisitionStatus === "COMPLETED"/gu) || []).length, 2);
assert.equal((serverSource.match(/if \(!acquisitionCompleted\) return res\.status\(422\)\.json/gu) || []).length, 2);
assert.equal((serverSource.match(/return res\.status\(200\)\.json\(reviewOnlyResponse\)/gu) || []).length, 2);
assert.match(serverSource, /const consumeResult = acquisitionCompleted\s*\n\s*\? await consumeCoordinateUsage/);
assert.match(serverSource, /rawText,\s*\n\s*coordinates:\s*formatProviderDmsReviewCoordinates/);
assert.equal((serverSource.match(/recognitionAcquisitionReviewAuthority = buildRecognitionAcquisitionReviewUsageAuthority/gu) || []).length, 2);
assert.equal((serverSource.match(/candidateCoordinates:\s*acquisitionEvidence\.candidateCoordinates/gu) || []).length, 4);
assert.match(serverSource, /candidateCoordinateGroups:\s*groupedProviderDmsEvidence\.candidateGroups/);
assert.match(serverSource, /visibleCrsEvidence:\s*groupedProviderDmsEvidence\.visibleCrsEvidence/);
assert.match(serverSource, /imageAcquisitionEvidence:\s*groupedAcquisitionEvidence\.imageEvidence/);
assert.match(serverSource, /source_crs:\s*reviewResult\?\.geographicCrsExplicit === true \? "EPSG:4326" : null/);
assert.match(serverSource, /keepRecognizedCoordinatesAsPointReview/);
assert.match(serverSource, /consumeCoordinateUsage\(\{\s*note:[^}]*Provider acquisition completed for review/s);
assert.match(serverSource, /providerReviewCandidatePointCount/);
assert.match(serverSource, /providerReviewCandidateGroupCount/);
assert.match(serverSource, /providerReviewBoundRowCount/);
assert.match(serverSource, /providerReviewUnboundRowCount/);
assert.match(serverSource, /isCoordinateEngineV2SelfIntersecting/);
assert.match(deadlineSource, /"ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED"/);
assert.match(uiSource, /采集完成，等待复核/);
assert.match(uiSource, /候选点数/);
assert.match(uiSource, /候选组数/);

console.log("recognition-first review-result v2 regression: PASS (strict coordinate evidence, unique grouping, ambiguity fail-close, response/log/UI contracts)");
