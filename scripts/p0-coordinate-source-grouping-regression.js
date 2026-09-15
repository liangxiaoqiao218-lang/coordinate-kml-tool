import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildDmsGroupedRetryFailClosedPatch,
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  buildPartialMultisiteSafeVisionRouting,
  classifyDmsRetryOwnership,
  createDmsGroupedRetryOrchestrator,
  DMS_RETRY_ROUTE_CLASSIFICATION,
  IMAGE_DMS_SELECTED_ROUTE,
  evaluateDmsGroupedAcquisitionExpansion,
  evaluateDmsGroupedRoutePriority,
  evaluateDmsGroupedRetryCoverage,
  evaluateDmsGroupedRetryEligibility,
  evaluateDmsWeakPartialMultisiteRecovery,
  evaluateDmsNormalizedPointwiseEquivalence,
  evaluateImageDmsAcquisitionCompleteness,
  evaluateStage1FullMultisitePromotion,
  evaluateStage1FullMultisiteSafety,
  extractDmsSourceStructure,
  hasExplicitDmsMultiRegionEvidence,
  isStage1FullMultisiteConfirmationPending,
  parseDmsSourceCoordinateRow,
  reconstructDmsGroupsFromNormalizedCoordinates,
  resolveDmsRetryTrustBoundary,
  resolveDmsEngineGroupNames,
  STAGE1_FULL_MULTISITE_POLICY_ID
} from "../server/recognition/dms-source-structure.js";
import { buildSourceCoordinateRepresentation } from "../server/source-coordinate-representation.js";
import { getDmsDocumentEvidence, getPrintedProjectedDmsReference } from "../server/recognition/family-primary-routing.js";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";
import {
  buildStage1FullMultisiteConfirmationPolicy,
  CoordinateConfirmationRuntime,
  finalizeCoordinateResult,
  releaseConfirmedFamilySafetyPolicy,
  STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
} from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const server = await readFile(path.join(root, "server.js"), "utf8");
const html = await readFile(path.join(root, "index.html"), "utf8");

const dynamicCases = [];
const staticAssertions = [];
function dynamicCase(name, fn) { dynamicCases.push({ name, fn }); }
function staticAssertion(name, fn) { staticAssertions.push({ name, fn }); }

const dmsRows = [
  `1. 12°00'36.9"N, 9°09'40.8"E`,
  `2. 12°00'34.0"N, 9°09'22.0"E`,
  `3. 12°00'48.1"N, 9°08'32.7"E`,
  `4. 12°00'38.4"N, 9°08'08.4"E`,
  `5. 12°00'28.7"N, 9°08'12.4"E`,
  `6. 12°00'39.1"N, 9°08'37.3"E`,
  `7. 12°00'25.1"N, 9°09'30.2"E`,
  `8. 12°00'32.3"N, 9°09'41.7"E`,
  `1. 11°59'46.7"N, 9°07'27.0"E`,
  `2. 12°00'54.7"N, 9°05'59.9"E`,
  `3. 12°00'36.1"N, 9°05'56.5"E`,
  `4. 11°59'35.0"N, 9°07'16.5"E`,
  `1. 11°59'38.4"N, 9°05'58.2"E`,
  `2. 11°58'51.2"N, 9°06'35.9"E`,
  `3. 11°58'43.7"N, 9°06'21.8"E`,
  `4. 11°59'20.4"N, 9°05'57.6"E`
];
const flatRows = dmsRows.map((row, index) => row.replace(/^\d+\./, `${index + 1}.`));
const stage1PartialEightText = [
  "Recognition hint: handwritten DMS",
  ...dmsRows.slice(0, 8)
].join("\n");

function decimalRowsFromDms(rows) {
  return rows.map(row => {
    const parsed = parseDmsSourceCoordinateRow(row);
    assert.ok(parsed, `DMS row must parse: ${row}`);
    return `${parsed.latitude.toFixed(8)},${parsed.longitude.toFixed(8)}`;
  });
}

function productionNormalizedRowsFromDms(rows) {
  return rows.map(row => {
    const parsed = parseDmsSourceCoordinateRow(row);
    assert.ok(parsed, `DMS row must parse: ${row}`);
    return `${parsed.longitude.toFixed(8)},${parsed.latitude.toFixed(8)}`;
  });
}

function pointsFromDms(rows) {
  return rows.map(row => {
    const parsed = parseDmsSourceCoordinateRow(row);
    return {
      label: parsed.label,
      latitude: parsed.latitude,
      longitude: parsed.longitude
    };
  });
}

const decimalRows = decimalRowsFromDms(flatRows);
const productionNormalizedRows = productionNormalizedRowsFromDms(flatRows);
const flatEngine = {
  coordinate_type: "wgs84_chat_coordinates",
  precision_mode: "wgs84-chat-coordinates",
  source_crs: { axisOrder: "latitude_longitude" },
  groups: [{ group_id: "group_1", points: pointsFromDms(flatRows) }]
};

const flat = buildSourceCoordinateRepresentation({
  rawText: flatRows.join("\n"),
  coordinates: decimalRows.join("\n"),
  precisionMode: "wgs84-chat-coordinates"
}, flatEngine);

dynamicCase("flat OCR displays original DMS only when semantically equivalent", () => {
  assert.equal(flat.displayText, flatRows.join("\n"));
});
dynamicCase("normalized decimals stay internal after semantic source recovery", () => {
  assert.equal(flat.displayText.includes(decimalRows[1]), false);
});
dynamicCase("all semantically verified source rows remain editable", () => {
  assert.equal(flat.rows.length, 16);
});

const structuredText = [
  "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 8), "",
  "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
  "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12)
].join("\n");
const headerlessStructuredText = structuredText
  .split("\n")
  .filter(line => line !== "POINT | LATITUDE | LONGITUDE")
  .join("\n");
const stage1OmittedIndexes = new Set([3, 7, 15]);
const stage1ThirteenText = structuredText.split("\n")
  .filter(line => !dmsRows.some((row, index) => stage1OmittedIndexes.has(index) && line === row))
  .join("\n");
const stage1FourFourFourOneText = [
  "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4), "",
  "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(4, 8).map((row, index) => row.replace(/^\d+\./, `${index + 1}.`)), "",
  "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
  "SITES4", "POINT | LATITUDE | LONGITUDE", dmsRows[12]
].join("\n");
const weakPartialBaselineRows = [
  ...dmsRows.slice(0, 4),
  ...dmsRows.slice(8, 12),
  ...dmsRows.slice(12, 15)
];
const weakPartialStage1Text = [
  "Recognition hint: handwritten DMS",
  "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4),
  `5. 12°00'00.0"N, 9°00'00.0"N`, "",
  "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12),
  `5. 11°59'00.0"N, 9°01'00.0"N`, "",
  "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 15),
  `4. 11°58'00.0"N, 9°02'00.0"N`
].join("\n");
const weakPartialStage1Coordinates = productionNormalizedRowsFromDms(weakPartialBaselineRows).join("\n");
const syntheticWeakPartialImageBuffer = Buffer.from("synthetic-weak-partial-image-v1");
const acquisitionTopologyProvenance = Object.freeze({
  groupLocalBaselineRowsPreserved: true,
  baselineGroupCount: 3,
  baselineGroupSizes: Object.freeze([6, 4, 3]),
  baselineGroupIdentities: Object.freeze(["SITE:1", "SITE:2", "SITE:3"]),
  retryGroupCount: 3,
  retryGroupSizes: Object.freeze([8, 4, 4]),
  retryGroupIdentities: Object.freeze(["SITE:1", "SITE:2", "SITE:3"])
});
const structure = extractDmsSourceStructure(structuredText);

dynamicCase("Stage-1 full multi-site risk signal is narrow and never proves topology", () => {
  const fullPrinted = getDmsDocumentEvidence(structuredText);
  const fifteenPrinted = getDmsDocumentEvidence([
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 15)
  ].join("\n"));
  const explicitHandwritten = getDmsDocumentEvidence(`handwritten DMS\n${structuredText}`);
  assert.equal(fullPrinted.dmsPairLineCount, 16);
  assert.equal(fullPrinted.stage1FullMultisiteRiskSignal, true);
  assert.equal(fifteenPrinted.stage1FullMultisiteRiskSignal, false);
  assert.equal(explicitHandwritten.explicitHandwrittenSignal, true);
  assert.equal(explicitHandwritten.stage1FullMultisiteRiskSignal, false);
  assert.equal(explicitHandwritten.stage1FullMultisiteStructureSignal, true);
  assert.equal(resolveDmsRetryTrustBoundary({
    documentEvidence: explicitHandwritten,
    trustedHandwrittenSignal: false
  }).stage1FullMultisiteRiskSignal, true);
  assert.equal(resolveDmsRetryTrustBoundary({
    documentEvidence: explicitHandwritten,
    trustedHandwrittenSignal: true
  }).stage1FullMultisiteRiskSignal, false);
});

dynamicCase("Stage-1 direct 16-point multi-site DMS requires exact ordered 8/4/4 confirmation", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.gateRequired, true);
  assert.equal(safety.acceptedForReview, true);
  assert.equal(safety.requiresConfirmation, true);
  assert.equal(safety.mapKmlBlockedUntilConfirmation, true);
  assert.deepEqual(safety.groupSizes, [8, 4, 4]);
  assert.deepEqual(safety.groupedCoordinates.split(/\n\n/).map(group => group.split("\n").length), [8, 4, 4]);
});

dynamicCase("Direct-16 rejects equal-count normalized coordinates with unrelated values", () => {
  const unrelated = productionNormalizedRows.map((row, index) => index === 0 ? "70.00000000,40.00000000" : row);
  const semantic = evaluateDmsNormalizedPointwiseEquivalence({
    structureText: structuredText,
    normalizedCoordinates: unrelated.join("\n")
  });
  assert.deepEqual(semantic, { accepted: false, reason: "NORMALIZED_DMS_POINT_VALUE_MISMATCH" });
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: unrelated.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.groupedCoordinates, "");
});

dynamicCase("Direct-16 rejects normalized latitude-longitude axis swap", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: decimalRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_POINT_VALUE_MISMATCH");
});

dynamicCase("Direct-16 rejects source axis order that contradicts its table header", () => {
  const sourceAxisSwap = structuredText.replace(
    `1. 12°00'36.9"N, 9°09'40.8"E`,
    `1. 9°09'40.8"E, 12°00'36.9"N`
  );
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: sourceAxisSwap,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_AXIS_ORDER_MISMATCH");
});

dynamicCase("Direct-16 preserves an explicit longitude-latitude table when every row matches", () => {
  const longitudeLatitudeTable = structuredText
    .replaceAll("POINT | LATITUDE | LONGITUDE", "POINT | LONGITUDE | LATITUDE")
    .split("\n")
    .map(line => {
      const point = parseDmsSourceCoordinateRow(line);
      if (!point) return line;
      const body = line.replace(/^\s*\d+[.)、:\-]?\s*/, "");
      const parts = body.split(/\s*,\s*/);
      return `${point.label}. ${parts[1]}, ${parts[0]}`;
    })
    .join("\n");
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: longitudeLatitudeTable,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, true);
  assert.equal(safety.failClosed, false);
  assert.deepEqual(safety.groupSizes, [8, 4, 4]);
});

dynamicCase("Direct-16 accepts consistent latitude-longitude source rows without table headers", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: headerlessStructuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, true);
  assert.equal(safety.failClosed, false);
  assert.deepEqual(safety.groupSizes, [8, 4, 4]);
});

dynamicCase("Direct-16 rejects one swapped source row without table headers", () => {
  const oneSwapped = headerlessStructuredText.replace(
    `1. 12°00'36.9"N, 9°09'40.8"E`,
    `1. 9°09'40.8"E, 12°00'36.9"N`
  );
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: oneSwapped,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_AXIS_ORDER_MISMATCH");
});

dynamicCase("Direct-16 rejects all swapped source rows without table headers", () => {
  const allSwapped = headerlessStructuredText
    .split("\n")
    .map(line => {
      const point = parseDmsSourceCoordinateRow(line);
      if (!point) return line;
      const body = line.replace(/^\s*\d+[.)、:\-]?\s*/, "");
      const parts = body.split(/\s*,\s*/);
      return `${point.label}. ${parts[1]}, ${parts[0]}`;
    })
    .join("\n");
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: allSwapped,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_AXIS_ORDER_MISMATCH");
});

dynamicCase("Direct-16 preserves legal zero-degree hemisphere semantics", () => {
  const zeroSource = structuredText.replace(
    `1. 12°00'36.9"N, 9°09'40.8"E`,
    `1. 0°00'00.0"N, 0°00'00.0"E`
  );
  const zeroNormalized = [...productionNormalizedRows];
  zeroNormalized[0] = "0.00000000,0.00000000";
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: zeroSource,
    normalizedCoordinates: zeroNormalized.join("\n")
  });
  assert.equal(safety.acceptedForReview, true);
  assert.equal(safety.failClosed, false);
  assert.deepEqual(safety.groupSizes, [8, 4, 4]);
});

dynamicCase("Direct-16 rejects hemisphere mismatch", () => {
  const wrongHemisphere = structuredText.replace(`12°00'36.9"N`, `12°00'36.9"S`);
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: wrongHemisphere,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_HEMISPHERE_MISMATCH");
});

dynamicCase("Direct-16 rejects group-local label mismatch", () => {
  const wrongLabel = structuredText.replace(`SITES2\nPOINT | LATITUDE | LONGITUDE\n1.`, `SITES2\nPOINT | LATITUDE | LONGITUDE\n2.`);
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: wrongLabel,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_LABEL_ORDER_MISMATCH");
});

dynamicCase("Direct-16 rejects normalized point-order mutation", () => {
  const wrongOrder = [...productionNormalizedRows];
  [wrongOrder[0], wrongOrder[1]] = [wrongOrder[1], wrongOrder[0]];
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: wrongOrder.join("\n")
  });
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "NORMALIZED_DMS_POINT_VALUE_MISMATCH");
});

dynamicCase("Stage-1 direct 16 promotion requires both grouped safety and image source completeness", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const completeness = evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n"),
    selectedRoute: IMAGE_DMS_SELECTED_ROUTE.DMS_GROUPED
  });
  assert.equal(completeness.allowed, true);
  assert.equal(completeness.failClosed, false);
  assert.deepEqual(completeness.groupSizes, [8, 4, 4]);
  assert.deepEqual(evaluateStage1FullMultisitePromotion({
    stage1Safety: safety,
    imageDmsSourceCompleteness: completeness
  }), {
    allowed: true,
    failClosed: false,
    reason: "STAGE1_FULL_MULTISITE_READY_FOR_CONFIRMATION"
  });
});

dynamicCase("Stage-1 direct 16 cannot override an image source completeness blocker", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const completeness = evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n"),
    selectedRoute: IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(completeness.allowed, false);
  assert.equal(completeness.failClosed, true);
  assert.deepEqual(evaluateStage1FullMultisitePromotion({
    stage1Safety: safety,
    imageDmsSourceCompleteness: completeness
  }), {
    allowed: false,
    failClosed: true,
    reason: "STAGE1_FULL_MULTISITE_IMAGE_SOURCE_BLOCKED"
  });
});

dynamicCase("Stage-1 direct 16 cannot promote after a grouped retry boundary failure", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const completeness = evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n"),
    selectedRoute: IMAGE_DMS_SELECTED_ROUTE.DMS_GROUPED
  });
  assert.deepEqual(evaluateStage1FullMultisitePromotion({
    stage1Safety: safety,
    imageDmsSourceCompleteness: completeness,
    retryBoundaryFailed: true
  }), {
    allowed: false,
    failClosed: true,
    reason: "STAGE1_FULL_MULTISITE_RETRY_BOUNDARY_BLOCKED"
  });
});

dynamicCase("Stage-1 direct 16 rejects missing image source completeness evidence", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.deepEqual(evaluateStage1FullMultisitePromotion({ stage1Safety: safety }), {
    allowed: false,
    failClosed: true,
    reason: "STAGE1_FULL_MULTISITE_IMAGE_SOURCE_BLOCKED"
  });
});

dynamicCase("Stage-1 direct 16-point DMS without group proof fails closed", () => {
  const ungrouped = flatRows.join("\n");
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: ungrouped,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(extractDmsSourceStructure(ungrouped).groupCount, 1);
  assert.equal(safety.gateRequired, true);
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.groupedCoordinates, "");
  assert.equal(safety.reason, "STAGE1_FULL_MULTISITE_GROUPING_UNPROVEN");
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: ungrouped }),
    stage1FullMultisiteSafety: safety,
    nonHandwrittenDmsCandidateSignal: true
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED);
  assert.equal(ownership.retryOwner, null);
});

dynamicCase("Stage-1 16-row risk cannot be bypassed by a weaker structure parse", () => {
  const incompleteStructureText = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 15)
  ].join("\n");
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: incompleteStructureText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.equal(extractDmsSourceStructure(incompleteStructureText).rowCount, 15);
  assert.equal(safety.gateRequired, true);
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.reason, "STAGE1_FULL_MULTISITE_GROUPING_UNPROVEN");
});

dynamicCase("Stage-1 direct 16-point DMS with wrong group order fails closed", () => {
  const wrongOrder = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(4, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12)
  ].join("\n");
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: wrongOrder,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  assert.deepEqual(extractDmsSourceStructure(wrongOrder).groups.map(group => group.rows.length), [4, 8, 4]);
  assert.equal(safety.gateRequired, true);
  assert.equal(safety.acceptedForReview, false);
  assert.equal(safety.failClosed, true);
  assert.equal(safety.groupedCoordinates, "");
  assert.equal(safety.reason, "STAGE1_FULL_MULTISITE_GROUPING_UNPROVEN");
});

dynamicCase("Stage-1 full multi-site result cannot bypass confirmation", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const engine = {
    ...groupedEngine,
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: safety.requiresConfirmation,
    groups: groupedEngine.groups.map(group => ({
      ...group,
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: group.points.map(point => ({ label: point.label, lat: point.latitude, lon: point.longitude }))
    }))
  };
  const response = buildCoordinateVerificationResponse({
    rawText: structuredText,
    coordinates: safety.groupedCoordinates,
    precisionMode: "dms-coordinates",
    stage1FullMultisiteSafety: safety
  }, engine);
  assert.equal(response.verification.status, "REVIEW");
  const pending = finalizeCoordinateResult({
    ...response.finalizedCoordinateResult,
    confirmationStatus: "pending",
    confirmedRevision: null,
    requiresReview: true,
    kmlReady: false,
    familySafetyPolicy: buildStage1FullMultisiteConfirmationPolicy({
      finalizedResult: response.finalizedCoordinateResult,
      verification: response.verification,
      stage1Safety: safety,
      confirmationSource: STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
    }),
    groups: response.finalizedCoordinateResult.groups.map(group => ({
      ...group,
      requiresReview: true,
      kmlReady: false
    }))
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  assert.equal(pending.kmlReady, false);
  assert.equal(isStage1FullMultisiteConfirmationPending(pending), true);
  assert.equal(pending.familySafetyPolicy.effectiveState, "REVIEW_REQUIRED_UNTIL_EXACT_IDENTITY_CONFIRMED");
  assert.equal(pending.familySafetyPolicy.exportEligible, false);
  assert.equal(pending.groups.every(group => group.requiresReview === true && group.kmlReady === false), true);

  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const wrongIdentity = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: "0".repeat(64),
    action: "accept"
  });
  assert.equal(wrongIdentity.ok, false);
  const stillPending = runtime.validateIdentity({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash
  }).result;
  assert.equal(stillPending.kmlReady, false);
  assert.equal(stillPending.familySafetyPolicy.effectiveState, "REVIEW_REQUIRED_UNTIL_EXACT_IDENTITY_CONFIRMED");
  assert.equal(stillPending.familySafetyPolicy.exportEligible, false);

  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.kmlReady, true);
  assert.equal(Object.hasOwn(confirmed, "kmlAuthorityBlocked"), false);
  assert.equal(isStage1FullMultisiteConfirmationPending(confirmed), false);
  assert.equal(confirmed.familySafetyPolicy.effectiveState, "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES");
  assert.equal(confirmed.familySafetyPolicy.exportEligible, true);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.requiresReview, false);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.kmlReady, true);
  assert.equal(confirmed.groups.every(group => group.requiresReview === false && group.kmlReady === true), true);
  assert.equal(new MapPreviewAdapter().adapt(confirmed).previewEligibility.allowed, true);
});

dynamicCase("Stage-1 policy without bound confirmation scope and readiness fails closed", () => {
  const released = releaseConfirmedFamilySafetyPolicy({
    technicalKmlReady: true,
    qualityGateStatus: "review_required",
    reasonCodes: ["QUALITY_GATE_REVIEW_REQUIRED", "REVIEW_REQUIRED"],
    blockingReasons: [{ code: "KML_NOT_READY" }],
    groups: [{ groupId: "site-1" }, { groupId: "site-2" }, { groupId: "site-3" }],
    familySafetyPolicy: {
      policyId: STAGE1_FULL_MULTISITE_POLICY_ID,
      policyVersion: "1",
      applied: true
    }
  });
  assert.equal(released.requiresReview, true);
  assert.equal(released.kmlReady, false);
  assert.equal(released.kmlAuthorityBlocked, true);
  assert.equal(released.familySafetyPolicy.exportEligible, false);
});

dynamicCase("Stage-1 policy with forged readiness and scope cannot release", () => {
  const released = releaseConfirmedFamilySafetyPolicy({
    resultId: "forged-result",
    resultRevision: 1,
    geometryHash: "1".repeat(64),
    groups: [{ groupId: "site-1" }, { groupId: "site-2" }, { groupId: "site-3" }],
    familySafetyPolicy: {
      policyId: STAGE1_FULL_MULTISITE_POLICY_ID,
      policyVersion: "1",
      applied: true,
      confirmationScope: "STAGE1_FULL_MULTISITE_GROUPING",
      confirmationSource: STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE,
      underlyingReadinessProven: true,
      underlyingReadiness: {
        requiresReview: false,
        kmlReady: true,
        groups: [
          { groupId: "site-1", requiresReview: false, kmlReady: true },
          { groupId: "site-2", requiresReview: false, kmlReady: true },
          { groupId: "site-3", requiresReview: false, kmlReady: true }
        ]
      },
      authorityBinding: {
        resultId: "forged-result",
        resultRevision: 1,
        geometryHash: "1".repeat(64)
      },
      integrity: { algorithm: "HMAC-SHA256", signature: "0".repeat(64) }
    }
  });
  assert.equal(released.requiresReview, true);
  assert.equal(released.kmlReady, false);
  assert.equal(released.kmlAuthorityBlocked, true);
  assert.equal(released.familySafetyPolicy.exportEligible, false);
});

dynamicCase("Confirmation Runtime cannot promote a forged Stage-1 readiness policy", () => {
  const geometry = {
    type: "Polygon",
    coordinates: [[[9, 12], [9.1, 12], [9.1, 12.1], [9, 12]]]
  };
  const base = finalizeCoordinateResult({
    sourceAuthority: "legacy",
    coordinateType: "standard_dms_table",
    crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
    geometry,
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    technicalKmlReady: true,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "site-1" }, { groupId: "site-2" }, { groupId: "site-3" }]
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  const forgedPolicy = {
    policyId: STAGE1_FULL_MULTISITE_POLICY_ID,
    policyVersion: "1",
    applied: true,
    confirmationScope: "STAGE1_FULL_MULTISITE_GROUPING",
    confirmationSource: STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE,
    underlyingReadinessProven: true,
    underlyingReadiness: {
      requiresReview: false,
      kmlReady: true,
      groups: base.groups.map(group => ({ ...group, requiresReview: false, kmlReady: true }))
    },
    authorityBinding: {
      resultId: base.resultId,
      resultRevision: base.resultRevision,
      geometryHash: base.geometryHash
    },
    integrity: { algorithm: "HMAC-SHA256", signature: "0".repeat(64) }
  };
  const pending = finalizeCoordinateResult({
    ...base,
    familySafetyPolicy: forgedPolicy,
    currentRevision: base.resultRevision,
    confirmationStatus: "pending",
    requiresReview: true,
    kmlReady: false
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.confirmationStatus, "accepted");
  assert.equal(confirmed.decisionState, "BLOCKED");
  assert.equal(confirmed.kmlReady, false);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
  assert.equal(confirmed.familySafetyPolicy.exportEligible, false);
});

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent quality blocker", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const engine = {
    ...groupedEngine,
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    groups: groupedEngine.groups.map(group => ({
      ...group,
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: group.points.map(point => ({ label: point.label, lat: point.latitude, lon: point.longitude }))
    }))
  };
  const response = buildCoordinateVerificationResponse({
    rawText: structuredText,
    coordinates: safety.groupedCoordinates,
    precisionMode: "dms-coordinates",
    stage1FullMultisiteSafety: safety
  }, engine);
  const pending = finalizeCoordinateResult({
    ...response.finalizedCoordinateResult,
    confirmationStatus: "pending",
    confirmedRevision: null,
    qualityGateStatus: "failed",
    requiresReview: true,
    kmlReady: false,
    familySafetyPolicy: buildStage1FullMultisiteConfirmationPolicy({
      finalizedResult: { ...response.finalizedCoordinateResult, qualityGateStatus: "failed" },
      verification: response.verification,
      stage1Safety: safety,
      confirmationSource: STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
    }),
    groups: response.finalizedCoordinateResult.groups.map(group => ({
      ...group,
      requiresReview: true,
      kmlReady: false
    }))
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.confirmationStatus, "accepted");
  assert.equal(confirmed.decisionState, "BLOCKED");
  assert.equal(confirmed.kmlReady, false);
  assert.equal(confirmed.familySafetyPolicy.effectiveState, "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES");
  assert.equal(confirmed.familySafetyPolicy.exportEligible, false);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.requiresReview, true);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.kmlReady, false);
  assert.equal(confirmed.groups.every(group => group.requiresReview === true && group.kmlReady === false), true);
});

function confirmStage1FullMultisiteWithOverrides(overrides = {}, verificationOverrides = {}) {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const engine = {
    ...groupedEngine,
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    groups: groupedEngine.groups.map(group => ({
      ...group,
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: group.points.map(point => ({ label: point.label, lat: point.latitude, lon: point.longitude }))
    }))
  };
  const response = buildCoordinateVerificationResponse({
    rawText: structuredText,
    coordinates: safety.groupedCoordinates,
    precisionMode: "dms-coordinates",
    stage1FullMultisiteSafety: safety
  }, engine);
  const candidateWithOverrides = finalizeCoordinateResult({
    ...response.finalizedCoordinateResult,
    ...overrides
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  const pending = finalizeCoordinateResult({
    ...candidateWithOverrides,
    confirmationStatus: "pending",
    confirmedRevision: null,
    requiresReview: true,
    kmlReady: false,
    familySafetyPolicy: buildStage1FullMultisiteConfirmationPolicy({
      finalizedResult: candidateWithOverrides,
      verification: { ...response.verification, ...verificationOverrides },
      stage1Safety: safety,
      confirmationSource: STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE
    }),
    groups: response.finalizedCoordinateResult.groups.map(group => ({
      ...group,
      requiresReview: true,
      kmlReady: false
    }))
  }, { clock: () => "2026-09-12T00:00:00.000Z" });
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  return runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
}

function assertStage1IndependentBlockerPreserved(confirmed) {
  assert.equal(confirmed.confirmationStatus, "accepted");
  assert.equal(confirmed.decisionState, "BLOCKED");
  assert.equal(confirmed.kmlReady, false);
  assert.equal(confirmed.familySafetyPolicy.effectiveState, "CONFIRMED_SUBJECT_TO_INDEPENDENT_GATES");
  assert.equal(confirmed.familySafetyPolicy.exportEligible, false);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.requiresReview, true);
  assert.equal(confirmed.familySafetyPolicy.underlyingReadiness.kmlReady, false);
  assert.equal(confirmed.groups.every(group => group.requiresReview === true && group.kmlReady === false), true);
}

function validMultiPolygonWithGroupSizes(groupSizes) {
  return {
    type: "MultiPolygon",
    coordinates: groupSizes.map((size, groupIndex) => {
      const ring = Array.from({ length: size }, (_, pointIndex) => [
        9 + groupIndex + (pointIndex % 3) * 0.01,
        12 + Math.floor(pointIndex / 3) * 0.01
      ]);
      return [[...ring, [...ring[0]]]];
    })
  };
}

function expectedStage1MultiPolygon() {
  const positions = productionNormalizedRows.map(row => row.split(",").map(Number));
  let offset = 0;
  return {
    type: "MultiPolygon",
    coordinates: [8, 4, 4].map(size => {
      const ring = positions.slice(offset, offset + size).map(position => [...position]);
      offset += size;
      return [[...ring, [...ring[0]]]];
    })
  };
}

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent CRS blocker", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    crs: { id: "EPSG:3857", axisOrder: "longitude_latitude" }
  });
  assertStage1IndependentBlockerPreserved(confirmed);
});

dynamicCase("Stage-1 grouping confirmation cannot clear an independent review-required quality reason", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({}, {
    conflicts: [{ severity: "medium", field: "independent_quality_review" }]
  });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.qualityGateStatus, "review_required");
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 grouping confirmation rejects an independent reason code even with generic REVIEW readiness", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({}, {
    reasonCode: "INDEPENDENT_REVIEW_REQUIRED"
  });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.qualityGateStatus, "review_required");
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent geometry blocker", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    geometry: { type: "MultiPolygon", coordinates: [] }
  });
  assertStage1IndependentBlockerPreserved(confirmed);
});

dynamicCase("Stage-1 confirmation rejects valid three-by-three-by-three geometry topology", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    geometry: validMultiPolygonWithGroupSizes([3, 3, 3])
  });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 confirmation rejects valid eight-four-three geometry topology", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    geometry: validMultiPolygonWithGroupSizes([8, 4, 3])
  });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 confirmation rejects swapped equal-size polygon order", () => {
  const geometry = expectedStage1MultiPolygon();
  geometry.coordinates = [geometry.coordinates[0], geometry.coordinates[2], geometry.coordinates[1]];
  const confirmed = confirmStage1FullMultisiteWithOverrides({ geometry });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 confirmation rejects cross-group point exchange with preserved eight-four-four sizes", () => {
  const geometry = expectedStage1MultiPolygon();
  const firstGroupPoint = geometry.coordinates[0][0][1];
  geometry.coordinates[0][0][1] = geometry.coordinates[1][0][1];
  geometry.coordinates[1][0][1] = firstGroupPoint;
  const confirmed = confirmStage1FullMultisiteWithOverrides({ geometry });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent KML blocker", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    technicalKmlReady: true,
    kmlAuthorityBlocked: true
  });
  assertStage1IndependentBlockerPreserved(confirmed);
  assert.equal(confirmed.technicalKmlReady, true);
  assert.equal(confirmed.kmlAuthorityBlocked, true);
});

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent source authority blocker", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    sourceAuthority: "untrusted_source"
  });
  assertStage1IndependentBlockerPreserved(confirmed);
});

dynamicCase("Stage-1 full multi-site confirmation cannot override an independent availability blocker", () => {
  const confirmed = confirmStage1FullMultisiteWithOverrides({
    availabilityStatus: "BLOCKED_BY_PROVIDER"
  });
  assertStage1IndependentBlockerPreserved(confirmed);
});

dynamicCase("Stage-1 full multi-site geometry never creates cross-group edges", () => {
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: productionNormalizedRows.join("\n")
  });
  const groups = safety.groupedCoordinates.split(/\n\n/).map(group => group.split("\n"));
  assert.deepEqual(groups.map(group => group.length), [8, 4, 4]);
  const edges = groups.flatMap(group => group.map((point, index) => (
    `${point}->${group[(index + 1) % group.length]}`
  )));
  assert.equal(edges.length, 16);
  assert.equal(edges.includes(`${groups[0].at(-1)}->${groups[1][0]}`), false);
  assert.equal(edges.includes(`${groups[1].at(-1)}->${groups[2][0]}`), false);
  assert.equal(edges.includes(`${groups[2].at(-1)}->${groups[0][0]}`), false);
});

dynamicCase("three visible DMS tables stay separate", () => assert.equal(structure.groupCount, 3));
dynamicCase("8/4/4 group sizes stay exact", () => assert.deepEqual(structure.groups.map(group => group.rows.length), [8, 4, 4]));
dynamicCase("visible section names survive", () => assert.deepEqual(structure.groups.map(group => group.name), ["SITES1", "SITES2", "SITES3"]));
dynamicCase("all sixteen structured rows survive", () => assert.equal(structure.rowCount, 16));

const groupedEngine = {
  ...flatEngine,
  groups: [dmsRows.slice(0, 8), dmsRows.slice(8, 12), dmsRows.slice(12)].map((rows, groupIndex) => ({
    group_id: `group_${groupIndex + 1}`,
    group_name: `SITES${groupIndex + 1}`,
    points: pointsFromDms(rows)
  }))
};
const grouped = buildSourceCoordinateRepresentation({
  rawText: structuredText,
  coordinates: [decimalRows.slice(0, 8).join("\n"), decimalRows.slice(8, 12).join("\n"), decimalRows.slice(12).join("\n\n")].join("\n\n")
}, groupedEngine);

dynamicCase("source representation exposes exact 8/4/4 groups", () => {
  assert.deepEqual(grouped.groups.map(group => group.length), [8, 4, 4]);
});
dynamicCase("source representation exposes group names", () => {
  assert.deepEqual(grouped.groupNames, ["SITES1", "SITES2", "SITES3"]);
});
dynamicCase("editable display keeps group separators", () => {
  assert.match(grouped.displayText, /^SITES1[\s\S]*\n\nSITES2[\s\S]*\n\nSITES3/);
});

dynamicCase("source representation keeps Stage-1 and structured reread as separate candidates", () => {
  const stage1Raw = stage1ThirteenText;
  const stage1Coordinates = decimalRows.filter((_, index) => !stage1OmittedIndexes.has(index)).join("\n");
  const retryCoordinates = [decimalRows.slice(0, 8).join("\n"), decimalRows.slice(8, 12).join("\n"), decimalRows.slice(12).join("\n")].join("\n\n");
  const recognition = {
    rawText: stage1Raw,
    coordinates: stage1Coordinates,
    acquisitionExpansionCandidate: {
      rawText: structuredText,
      coordinates: retryCoordinates,
      precisionMode: "dms-coordinates",
      provenance: {
        schemaVersion: "dms_grouped_acquisition_delta_v1",
        ownerFamily: "dms_grouped",
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        baselineRowCount: 13,
        retryRowCount: 16,
        addedRowCount: 3,
        baselineRowsPreserved: true,
        ...acquisitionTopologyProvenance,
        strongBoundariesProven: true,
        labelsContinuous: true,
        sourceCandidateSeparate: true,
        directCanonicalPromotion: false,
        stage1CandidateSha256: createHash("sha256").update(JSON.stringify({ rawText: stage1Raw, coordinates: stage1Coordinates })).digest("hex"),
        retryCandidateSha256: createHash("sha256").update(JSON.stringify({ rawText: structuredText, coordinates: retryCoordinates })).digest("hex")
      }
    }
  };
  const result = buildSourceCoordinateRepresentation(recognition, groupedEngine);
  assert.equal(result.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(result.sourceCandidates.stage1.rowCount, 13);
  assert.equal(result.sourceCandidates.structuredReread.rowCount, 16);
  assert.deepEqual(result.groups.map(group => group.length), [8, 4, 4]);
  assert.equal(recognition.rawText, stage1Raw);
  assert.equal(recognition.coordinates, stage1Coordinates);

  const tampered = buildSourceCoordinateRepresentation({
    ...recognition,
    acquisitionExpansionCandidate: {
      ...recognition.acquisitionExpansionCandidate,
      provenance: { ...recognition.acquisitionExpansionCandidate.provenance, retryCandidateSha256: "0".repeat(64) }
    }
  }, groupedEngine);
  assert.equal(tampered.sourceCandidates, null);
  assert.equal(tampered.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");

  const malformedTopology = buildSourceCoordinateRepresentation({
    ...recognition,
    acquisitionExpansionCandidate: {
      ...recognition.acquisitionExpansionCandidate,
      provenance: {
        ...recognition.acquisitionExpansionCandidate.provenance,
        retryGroupCount: 2,
        retryGroupSizes: [8, 8],
        retryGroupIdentities: ["SITE:1", "SITE:2"]
      }
    }
  }, groupedEngine);
  assert.equal(malformedTopology.sourceCandidates, null);
  assert.equal(malformedTopology.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");
});

dynamicCase("production verification keeps 16-point candidate blocked until exact confirmation", () => {
  const stage1Coordinates = decimalRows.filter((_, index) => !stage1OmittedIndexes.has(index)).join("\n");
  const retryCoordinates = [decimalRows.slice(0, 8).join("\n"), decimalRows.slice(8, 12).join("\n"), decimalRows.slice(12).join("\n")].join("\n\n");
  const provenance = {
    schemaVersion: "dms_grouped_acquisition_delta_v1",
    ownerFamily: "dms_grouped",
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    baselineRowCount: 13,
    retryRowCount: 16,
    addedRowCount: 3,
    baselineRowsPreserved: true,
    ...acquisitionTopologyProvenance,
    strongBoundariesProven: true,
    labelsContinuous: true,
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    stage1CandidateSha256: createHash("sha256").update(JSON.stringify({ rawText: stage1ThirteenText, coordinates: stage1Coordinates })).digest("hex"),
    retryCandidateSha256: createHash("sha256").update(JSON.stringify({ rawText: structuredText, coordinates: retryCoordinates })).digest("hex")
  };
  const payload = {
    rawText: structuredText,
    coordinates: retryCoordinates,
    precisionMode: "dms-coordinates",
    stage1Candidate: { rawText: stage1ThirteenText, coordinates: stage1Coordinates },
    acquisitionExpansionCandidate: { rawText: structuredText, coordinates: retryCoordinates, precisionMode: "dms-coordinates", provenance },
    acquisitionDeltaProvenance: provenance
  };
  const engine = {
    ...groupedEngine,
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    acquisition_delta_provenance: provenance,
    groups: groupedEngine.groups.map(group => ({
      ...group,
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: group.points.map(point => ({ label: point.label, lat: point.latitude, lon: point.longitude }))
    }))
  };
  const response = buildCoordinateVerificationResponse(payload, engine);
  const pending = response.finalizedCoordinateResult;
  assert.equal(response.verification.status, "REVIEW");
  assert.equal(response.sourceCoordinateRepresentation.sourceCandidates.stage1.rowCount, 13);
  assert.equal(response.sourceCoordinateRepresentation.sourceCandidates.structuredReread.rowCount, 16);
  assert.equal(pending.kmlReady, false);
  assert.equal(new MapPreviewAdapter().adapt(pending).previewEligibility.allowed, false);

  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.decisionState, "AUTO_EXPORT");
  assert.equal(confirmed.kmlReady, true);
  assert.equal(new MapPreviewAdapter().adapt(confirmed).previewEligibility.allowed, true);
});

dynamicCase("same row count with changed coordinate fails closed to normalized display", () => {
  const wrongEngine = {
    ...flatEngine,
    groups: [{ group_id: "group_1", points: flatEngine.groups[0].points.map((point, index) => (
      index === 3 ? { ...point, longitude: point.longitude + 0.01 } : point
    )) }]
  };
  const result = buildSourceCoordinateRepresentation({ rawText: flatRows.join("\n"), coordinates: decimalRows.join("\n") }, wrongEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "point_value_mismatch");
});

dynamicCase("same row count with row order mismatch fails closed", () => {
  const labelledRows = [
    `A 12°00'36.9"N, 9°09'40.8"E`,
    `B 12°00'34.0"N, 9°09'22.0"E`
  ];
  const labelledDecimals = decimalRowsFromDms(labelledRows);
  const labelledEngine = {
    ...flatEngine,
    groups: [{ group_id: "group_1", points: pointsFromDms(labelledRows) }]
  };
  const result = buildSourceCoordinateRepresentation({
    rawText: [...labelledRows].reverse().join("\n"),
    coordinates: labelledDecimals.join("\n")
  }, labelledEngine);
  assert.equal(result.displayText, labelledDecimals.join("\n"));
  assert.equal(result.sourceEquivalence, "label_order_mismatch");
});

dynamicCase("source labels and engine labels must be symmetrically present", () => {
  const unlabeledEngine = {
    ...flatEngine,
    groups: [{ group_id: "group_1", points: flatEngine.groups[0].points.map(point => ({
      latitude: point.latitude,
      longitude: point.longitude
    })) }]
  };
  const result = buildSourceCoordinateRepresentation({ rawText: flatRows.join("\n"), coordinates: decimalRows.join("\n") }, unlabeledEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "label_order_mismatch");
});

dynamicCase("same row count with group count mismatch fails closed", () => {
  const result = buildSourceCoordinateRepresentation({
    rawText: structuredText,
    coordinates: decimalRows.join("\n")
  }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "group_count_mismatch");
});

dynamicCase("same row count with group size mismatch fails closed", () => {
  const wrongGroupedEngine = {
    ...flatEngine,
    groups: [dmsRows.slice(0, 7), dmsRows.slice(7, 12), dmsRows.slice(12)].map((rows, groupIndex) => ({
      group_id: `group_${groupIndex + 1}`,
      points: pointsFromDms(rows)
    }))
  };
  const result = buildSourceCoordinateRepresentation({
    rawText: structuredText,
    coordinates: decimalRows.join("\n")
  }, wrongGroupedEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "group_size_mismatch");
});

dynamicCase("hemisphere mismatch fails closed", () => {
  const badRows = [...flatRows];
  badRows[0] = badRows[0].replace(`9°09'40.8"E`, `9°09'40.8"W`);
  const result = buildSourceCoordinateRepresentation({ rawText: badRows.join("\n"), coordinates: decimalRows.join("\n") }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "hemisphere_mismatch");
});

dynamicCase("unparseable no-hemisphere DMS fails closed", () => {
  const noHemisphereRows = flatRows.map(row => row.replace(/[NE]/g, ""));
  const result = buildSourceCoordinateRepresentation({ rawText: noHemisphereRows.join("\n"), coordinates: decimalRows.join("\n") }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "missing_rows_or_engine_points");
});

dynamicCase("declared latitude-longitude rejects longitude-latitude source order", () => {
  const swappedRows = flatRows.map(row => {
    const parsed = parseDmsSourceCoordinateRow(row);
    return `${parsed.label}. ${parsed.tokens[1]}, ${parsed.tokens[0]}`;
  });
  const result = buildSourceCoordinateRepresentation({ rawText: swappedRows.join("\n"), coordinates: decimalRows.join("\n") }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "axis_order_mismatch");
});

dynamicCase("declared longitude-latitude rejects latitude-longitude source order", () => {
  const result = buildSourceCoordinateRepresentation({ rawText: flatRows.join("\n"), coordinates: decimalRows.join("\n") }, {
    ...flatEngine,
    source_crs: { axisOrder: "longitude_latitude" }
  });
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "axis_order_mismatch");
});

dynamicCase("raw DMS with more rows than engine fails closed", () => {
  const result = buildSourceCoordinateRepresentation({
    rawText: [...flatRows, `17. 12°00'00.0"N, 9°00'00.0"E`].join("\n"),
    coordinates: decimalRows.join("\n")
  }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "group_size_mismatch");
});

dynamicCase("raw DMS with fewer rows than engine fails closed", () => {
  const result = buildSourceCoordinateRepresentation({
    rawText: flatRows.slice(0, 15).join("\n"),
    coordinates: decimalRows.join("\n")
  }, flatEngine);
  assert.equal(result.displayText, decimalRows.join("\n"));
  assert.equal(result.sourceEquivalence, "group_size_mismatch");
});

dynamicCase("multi-region evidence requires structural evidence, not row count alone", () => {
  assert.equal(hasExplicitDmsMultiRegionEvidence(flatRows.slice(0, 8).join("\n")), false);
  assert.equal(hasExplicitDmsMultiRegionEvidence(structuredText), true);
});

dynamicCase("retry coverage accepts exact same multi-group DMS structure", () => {
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText: structuredText });
  assert.equal(coverage.accepted, true);
  assert.equal(coverage.rowCount, 16);
  assert.equal(coverage.groupCount, 3);
});

dynamicCase("explicit multi-region evidence with failed retry cannot replace baseline", () => {
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText: "NO COORDINATES" });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "NO_RETRY_DMS_ROWS");
});

dynamicCase("retry coverage rejects incomplete rows", () => {
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText: structuredText.replace(dmsRows[15], "") });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "ROW_COUNT_MISMATCH");
});

dynamicCase("retry coverage rejects extra rows", () => {
  const coverage = evaluateDmsGroupedRetryCoverage({
    baselineText: structuredText,
    retryText: `${structuredText}\n17. 12°00'00.0"N, 9°00'00.0"E`
  });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "ROW_COUNT_MISMATCH");
});

dynamicCase("retry coverage rejects flattened groups", () => {
  const coverage = evaluateDmsGroupedRetryCoverage({
    baselineText: structuredText,
    retryText: flatRows.join("\n")
  });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "GROUP_COUNT_MISMATCH");
});

dynamicCase("retry coverage rejects coordinate reordering", () => {
  const retryRows = [...dmsRows];
  [retryRows[0], retryRows[1]] = [retryRows[1], retryRows[0]];
  const coverage = evaluateDmsGroupedRetryCoverage({
    baselineText: structuredText,
    retryText: [
      "SITES1", ...retryRows.slice(0, 8), "",
      "SITES2", ...retryRows.slice(8, 12), "",
      "SITES3", ...retryRows.slice(12)
    ].join("\n")
  });
  assert.equal(coverage.accepted, false);
  assert.ok(["GROUP_COUNT_MISMATCH", "LABEL_ORDER_MISMATCH", "POINT_VALUE_MISMATCH"].includes(coverage.reason));
});

dynamicCase("retry coverage rejects hemisphere mutation", () => {
  const retryRows = [...dmsRows];
  retryRows[2] = retryRows[2].replace(`9°08'32.7"E`, `9°08'32.7"W`);
  const coverage = evaluateDmsGroupedRetryCoverage({
    baselineText: structuredText,
    retryText: [
      "SITES1", ...retryRows.slice(0, 8), "",
      "SITES2", ...retryRows.slice(8, 12), "",
      "SITES3", ...retryRows.slice(12)
    ].join("\n")
  });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "HEMISPHERE_MISMATCH");
});

dynamicCase("retry coverage rejects a missing baseline row label", () => {
  const retryText = structuredText.replace(dmsRows[0], dmsRows[0].replace(/^1\.\s*/, ""));
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "LABEL_ORDER_MISMATCH");
});

dynamicCase("retry coverage rejects a changed baseline row label", () => {
  const retryText = structuredText.replace(dmsRows[0], dmsRows[0].replace(/^1\./, "01."));
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "LABEL_ORDER_MISMATCH");
});

dynamicCase("retry coverage rejects changed group identity and number", () => {
  const retryText = structuredText.replace("SITES1", "AREA 9").replace("SITES2", "AREA 8").replace("SITES3", "AREA 7");
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "GROUP_IDENTITY_MISMATCH");
});

dynamicCase("retry cannot widen baseline tolerance by reducing candidate precision", () => {
  const retryText = structuredText.replace(`12°00'36.9"N`, `12°00'37"N`);
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "POINT_VALUE_MISMATCH");
});

dynamicCase("production retry eligibility rejects a single DMS group", () => {
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: flatRows.slice(0, 8).join("\n"),
    isImageInput: true,
    familyRetryAllowed: true
  });
  assert.equal(eligibility.allowed, false);
  assert.equal(eligibility.failClosed, false);
});

dynamicCase("source representation preserves verified 8-to-16 recovery identity without Direct-16 promotion", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: structuredText,
    allowPartialMultisiteRecovery: true
  });
  const stage1Coordinates = productionNormalizedRows.slice(0, 8).join("\n");
  const candidate = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: stage1PartialEightText,
    stage1Coordinates,
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped"
  });
  const result = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    partialMultisiteRecoveryProvenance: candidate.provenance,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    },
    sourceCandidates: {
      stage1: {
        ...candidate.stage1Candidate,
        rowCount: candidate.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: candidate.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: candidate.rawText,
        coordinates: candidate.normalizedCoordinates,
        rowCount: candidate.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: candidate.provenance.retryCandidateSha256
      }
    }
  }, groupedEngine);
  assert.equal(result.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(result.sourceCandidateSeparate, true);
  assert.equal(result.directCanonicalPromotion, false);
  assert.equal(result.partialMultisiteRecoveryProvenance.recoverySource, "dms_grouped");
  assert.equal(result.sourceCandidates.stage1.rowCount, 8);
  assert.equal(result.sourceCandidates.stage1.candidateRole, "STAGE1_ACQUISITION_CANDIDATE");
  assert.equal(result.sourceCandidates.stage1.candidateSha256, candidate.provenance.stage1CandidateSha256);
  assert.equal(result.sourceCandidates.structuredReread.rowCount, 16);
  assert.equal(result.sourceCandidates.structuredReread.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(result.sourceCandidates.structuredReread.candidateSha256, candidate.provenance.retryCandidateSha256);
  assert.equal(result.acquisitionDeltaProvenance, null);

  const forged = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates.replace(/[^\n]+$/, "99,12"),
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    }
  }, groupedEngine);
  assert.equal(forged.sourceCandidates, null);
  assert.equal(forged.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");

  const forgedWithDeclaredSources = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    partialMultisiteRecoveryProvenance: candidate.provenance,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: `${candidate.normalizedCoordinates}\nNOT_A_COORDINATE`,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    },
    sourceCandidates: {
      stage1: {
        ...candidate.stage1Candidate,
        rowCount: candidate.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: candidate.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: candidate.rawText,
        coordinates: candidate.normalizedCoordinates,
        rowCount: candidate.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: candidate.provenance.retryCandidateSha256
      }
    }
  }, groupedEngine);
  assert.equal(forgedWithDeclaredSources.sourceCandidates, null);
  assert.equal(forgedWithDeclaredSources.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");

  const contradictoryProvenance = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: { ...candidate.provenance, strongBoundariesProven: false }
    }
  }, groupedEngine);
  assert.equal(contradictoryProvenance.sourceCandidates, null);
  assert.equal(contradictoryProvenance.partialMultisiteRecoveryProvenance, null);
  assert.equal(contradictoryProvenance.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");

  const contradictorySourceBinding = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    partialMultisiteRecoveryProvenance: candidate.provenance,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    },
    sourceCandidates: {
      stage1: {
        ...candidate.stage1Candidate,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: "forged-stage1-binding"
      },
      structuredReread: {
        rawText: candidate.rawText,
        coordinates: candidate.normalizedCoordinates,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: candidate.provenance.retryCandidateSha256
      }
    }
  }, groupedEngine);
  assert.equal(contradictorySourceBinding.sourceCandidates, null);
  assert.equal(contradictorySourceBinding.partialMultisiteRecoveryProvenance, null);
  assert.equal(contradictorySourceBinding.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");

  const driftedIdentity = buildSourceCoordinateRepresentation({
    rawText: stage1PartialEightText,
    coordinates: stage1Coordinates,
    stage1Candidate: candidate.stage1Candidate,
    candidateRole: "CURRENT_RECOGNITION_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    partialMultisiteRecoveryProvenance: candidate.provenance,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    },
    sourceCandidates: {
      stage1: {
        ...candidate.stage1Candidate,
        rowCount: 999,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: candidate.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: candidate.rawText,
        coordinates: candidate.normalizedCoordinates,
        rowCount: 1,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: candidate.provenance.retryCandidateSha256
      }
    }
  }, groupedEngine);
  assert.equal(driftedIdentity.sourceCandidates, null);
  assert.equal(driftedIdentity.candidateRole, "CURRENT_RECOGNITION_CANDIDATE");
});

dynamicCase("ordinary ordered eight-point single-site DMS does not acquire a recovery owner", () => {
  const rawText = dmsRows.slice(0, 8).join("\n");
  const evidence = getDmsDocumentEvidence(rawText);
  const trust = resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: rawText }),
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    partialMultisiteRecoveryCandidateSignal: trust.partialMultisiteRecoveryCandidateSignal,
    handwrittenShapeRetryCandidate: false
  });
  assert.equal(trust.partialMultisiteRecoveryCandidateSignal, true);
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
  assert.equal(evaluateDmsGroupedRetryEligibility({
    rawText,
    isImageInput: true,
    familyRetryAllowed: true,
    partialMultisiteRecoveryEligible: false
  }).allowed, false);
});

dynamicCase("production retry eligibility requires the family retry budget", () => {
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: structuredText,
    isImageInput: true,
    familyRetryAllowed: false
  });
  assert.equal(eligibility.allowed, false);
  assert.equal(eligibility.failClosed, true);
  assert.equal(eligibility.reason, "DMS_GROUPED_RETRY_BUDGET_BLOCKED");
});

dynamicCase("production retry eligibility authorizes explicit multi-region evidence once", () => {
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: structuredText,
    isImageInput: true,
    familyRetryAllowed: true
  });
  assert.equal(eligibility.allowed, true);
  assert.equal(eligibility.reason, "DMS_GROUPED_RETRY_AUTHORIZED");
});

dynamicCase("production fail-closed patch clears every geometry-facing retry state immediately", () => {
  const patch = buildDmsGroupedRetryFailClosedPatch("ROW_COUNT_MISMATCH", ["OCR"]);
  assert.equal(patch.coordinates, "");
  assert.equal(patch.dmsGroupedAccepted, false);
  assert.equal(patch.dmsAccepted, false);
  assert.equal(patch.resetChatCoordinates, true);
  assert.equal(patch.resetWgs84TableCoordinates, true);
  assert.deepEqual(patch.parserTrace, ["OCR", "DMS_GROUPED:boundary_unresolved_review_required"]);
});

dynamicCase("production retry orchestrator enforces one owner and blocks every claim after fail-closed", () => {
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: ["OCR"] });
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.claim("wgs84_table"), false);
  const patch = orchestrator.failClose("ROW_COUNT_MISMATCH");
  assert.equal(patch.coordinates, "");
  assert.equal(orchestrator.claim("dms_grouped"), false);
  assert.deepEqual(orchestrator.snapshot(), {
    activeFamilyOwner: "dms_grouped",
    reservedFamilyOwner: null,
    failed: true,
    failureReason: "ROW_COUNT_MISMATCH"
  });
});

dynamicCase("area titles create groups", () => {
  const areaStructure = extractDmsSourceStructure([
    "Area One", "POINT LATITUDE LONGITUDE", dmsRows[0], dmsRows[1],
    "Area 2", "POINT LATITUDE LONGITUDE", dmsRows[2], dmsRows[3]
  ].join("\n"));
  assert.deepEqual(areaStructure.groups.map(group => group.rows.length), [2, 2]);
});

dynamicCase("row-number restart creates headerless groups", () => {
  const restartedRows = extractDmsSourceStructure([
    ...dmsRows.slice(0, 3),
    ...dmsRows.slice(8, 11)
  ].join("\n"));
  assert.deepEqual(restartedRows.groups.map(group => group.rows.length), [3, 3]);
});

dynamicCase("named source group identity mismatch fails closed", () => {
  const changed = structuredText.replace("SITES1", "AREA 9");
  const result = buildSourceCoordinateRepresentation({ rawText: changed, coordinates: grouped.displayText }, groupedEngine);
  assert.equal(result.sourceEquivalence, "group_identity_mismatch");
});

dynamicCase("named source requires matching engine group identity", () => {
  const engineWithoutNames = { ...groupedEngine, groups: groupedEngine.groups.map(({ group_name, ...group }) => group) };
  const result = buildSourceCoordinateRepresentation({ rawText: structuredText, coordinates: decimalRows.join("\n") }, engineWithoutNames);
  assert.equal(result.sourceEquivalence, "group_identity_mismatch");
});

dynamicCase("zero latitude hemisphere cannot be inferred and fails closed", () => {
  const raw = `1. 0°00'00.0"S, 9°00'00.0"E`;
  const engine = { ...flatEngine, groups: [{ points: [{ label: "1", latitude: 0, longitude: 9 }] }] };
  const result = buildSourceCoordinateRepresentation({ rawText: raw, coordinates: "0.00000000,9.00000000" }, engine);
  assert.equal(result.sourceEquivalence, "hemisphere_mismatch");
});

dynamicCase("zero longitude hemisphere cannot be inferred and fails closed", () => {
  const raw = `1. 9°00'00.0"N, 0°00'00.0"W`;
  const engine = { ...flatEngine, groups: [{ points: [{ label: "1", latitude: 9, longitude: 0 }] }] };
  const result = buildSourceCoordinateRepresentation({ rawText: raw, coordinates: "9.00000000,0.00000000" }, engine);
  assert.equal(result.sourceEquivalence, "hemisphere_mismatch");
});

dynamicCase("third DMS token is rejected", () => {
  assert.equal(parseDmsSourceCoordinateRow(`1. 12°00'00.0"N, 9°00'00.0"E, 8°00'00.0"E`), null);
});

dynamicCase("single table blank line does not authorize retry", () => {
  const text = [...flatRows.slice(0, 3), "", ...flatRows.slice(3, 6)].join("\n");
  assert.equal(hasExplicitDmsMultiRegionEvidence(text), false);
  assert.equal(evaluateDmsGroupedRetryEligibility({ rawText: text, isImageInput: true, familyRetryAllowed: true }).allowed, false);
});

dynamicCase("intra-site blank lines do not split titled 8 4 4 groups", () => {
  const withIntraSiteBlanks = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4), "", ...dmsRows.slice(4, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 10), "", ...dmsRows.slice(10, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 14), "", ...dmsRows.slice(14)
  ].join("\n");
  const structure = extractDmsSourceStructure(withIntraSiteBlanks);
  assert.deepEqual(structure.groups.map(group => group.rows.length), [8, 4, 4]);
  assert.deepEqual(structure.groups.map(group => group.boundaryProvenance), ["section_title", "section_title", "section_title"]);
  assert.equal(hasExplicitDmsMultiRegionEvidence(withIntraSiteBlanks), true);
});

dynamicCase("printed multi-site route suppresses damaged-token handwritten retry without changing shared evidence", () => {
  const damagedPrintedTable = structuredText.replace(dmsRows[0], `1 | 12°00.369'N | 9°09'40.8\"E`);
  const evidence = getDmsDocumentEvidence(damagedPrintedTable);
  assert.equal(evidence.printedTableSignal, true);
  assert.equal(evidence.damagedDmsSignal, true);
  assert.equal(evidence.explicitHandwrittenSignal, false);
  assert.equal(evidence.handwrittenPositiveSignal, true);
  assert.equal(evidence.printedDmsCandidateSignal, true);
  assert.equal(evidence.nonHandwrittenDmsCandidateSignal, true);
  assert.ok(evidence.dmsPairLineCount >= 3);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedTableSignal,
    explicitHandwrittenSignal: evidence.explicitHandwrittenSignal,
    structureText: damagedPrintedTable
  });
  assert.equal(route.typedDmsGrouped, true);
  assert.equal(route.suppressHandwrittenRetry, true);
});

dynamicCase("proven printed multi-site structure reserves dms_grouped as the only retry owner", () => {
  const evidence = getDmsDocumentEvidence(stage1ThirteenText);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    explicitHandwrittenSignal: false,
    structureText: stage1ThirteenText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    nonHandwrittenDmsCandidateSignal: evidence.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true
  });
  assert.deepEqual(ownership, {
    classification: DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_ONLY,
    retryOwner: "dms_grouped",
    failClosed: false,
    reason: DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_ONLY
  });
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  assert.equal(orchestrator.claim("handwritten_dms"), false);
  assert.equal(orchestrator.claim("generic_ocr"), false);
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.snapshot().reservedFamilyOwner, "dms_grouped");
});

dynamicCase("Provider pseudo-handwritten wording cannot hijack a proven 13-point dms_grouped reread", () => {
  const providerText = `Recognition hint: handwritten DMS\n${stage1ThirteenText}`;
  const evidence = getDmsDocumentEvidence(providerText);
  assert.equal(evidence.explicitHandwrittenSignal, true);
  assert.equal(evidence.dmsPairLineCount, 13);
  assert.equal(evidence.dmsStructureCandidateSignal, true);
  const trust = resolveDmsRetryTrustBoundary({
    documentEvidence: evidence,
    trustedHandwrittenSignal: false
  });
  assert.equal(trust.explicitHandwrittenSignal, false);
  assert.equal(trust.printedDmsCandidateSignal, true);
  assert.equal(trust.nonHandwrittenDmsCandidateSignal, true);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: trust.printedDmsCandidateSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    structureText: providerText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true
  });
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: providerText,
    dmsGroupedInfo: { output: "stage1-normalized-present" },
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: ownership.retryOwner === "dms_grouped"
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_ONLY);
  assert.equal(eligibility.allowed, true);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  let providerCalls = 0;
  for (const owner of ["handwritten_dms", "generic_ocr", "wgs84_table", "french_perimeter_dms"]) {
    if (orchestrator.claim(owner)) providerCalls += 1;
  }
  if (orchestrator.claim("dms_grouped")) providerCalls += 1;
  assert.equal(providerCalls, 1);
  assert.equal(orchestrator.snapshot().activeFamilyOwner, "dms_grouped");
});

dynamicCase("runtime-equivalent 4 4 4 1 Stage-1 evidence gives the only second call to dms_grouped", () => {
  const providerText = `识别提示：手写坐标\n${stage1FourFourFourOneText}`;
  const structure = extractDmsSourceStructure(providerText);
  assert.equal(structure.rowCount, 13);
  assert.deepEqual(structure.groups.map(group => group.rows.length), [4, 4, 4, 1]);
  const evidence = getDmsDocumentEvidence(providerText);
  const trust = resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: trust.printedDmsCandidateSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    structureText: providerText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true
  });
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: providerText,
    dmsGroupedInfo: { output: "stage1-normalized-present" },
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: ownership.retryOwner === "dms_grouped"
  });
  assert.equal(route.typedDmsGrouped, true);
  assert.equal(ownership.retryOwner, "dms_grouped");
  assert.equal(eligibility.allowed, true);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  assert.equal(orchestrator.claim("handwritten_dms"), false);
  assert.equal(orchestrator.claim("generic_ocr"), false);
  assert.equal(orchestrator.claim("wgs84_table"), false);
  assert.equal(orchestrator.claim("dms_grouped"), true);
});

dynamicCase("pseudo-handwritten multi-site DMS with unproven boundaries fails before every second Provider route", () => {
  const ambiguous = [
    "Recognition hint: handwritten coordinates",
    ...dmsRows.slice(0, 3),
    ...dmsRows.slice(8, 11),
    "",
    ...flatRows.slice(6, 9)
  ].join("\n");
  const evidence = getDmsDocumentEvidence(ambiguous);
  const trust = resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: trust.printedDmsCandidateSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    structureText: ambiguous
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  orchestrator.failClose(ownership.reason);
  for (const owner of ["handwritten_dms", "generic_ocr", "wgs84_table", "dms_grouped"]) {
    assert.equal(orchestrator.claim(owner), false);
  }
});

dynamicCase("eight-row morphology risk reserves one evidence-only dms_grouped recovery", () => {
  const evidence = getDmsDocumentEvidence(stage1PartialEightText);
  const trust = resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: trust.printedDmsCandidateSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    structureText: stage1PartialEightText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true,
    partialMultisiteRecoveryCandidateSignal: trust.partialMultisiteRecoveryCandidateSignal
  });
  assert.equal(evidence.explicitHandwrittenSignal, true);
  assert.equal(trust.explicitHandwrittenSignal, false);
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_PARTIAL_RECOVERY_ONLY);
  assert.equal(ownership.retryOwner, "dms_grouped");
  assert.equal(ownership.failClosed, false);
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: stage1PartialEightText,
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: false,
    partialMultisiteRecoveryEligible: true
  });
  assert.deepEqual(eligibility, {
    allowed: true,
    failClosed: false,
    reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_AUTHORIZED"
  });
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  assert.equal(orchestrator.claim("handwritten_dms"), false);
  assert.equal(orchestrator.claim("generic_ocr"), false);
  assert.equal(orchestrator.claim("wgs84_table"), false);
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.snapshot().activeFamilyOwner, "dms_grouped");
});

dynamicCase("partial multi-site recovery fails closed when the single retry budget is unavailable", () => {
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: stage1PartialEightText,
    isImageInput: true,
    familyRetryAllowed: false,
    typedRouteEligible: false,
    partialMultisiteRecoveryEligible: true
  });
  assert.equal(eligibility.allowed, false);
  assert.equal(eligibility.failClosed, true);
  assert.equal(eligibility.reason, "DMS_GROUPED_RETRY_BUDGET_BLOCKED");
});

dynamicCase("partial recovery ownership fails closed when the eight-row single-group qualification is absent", () => {
  for (const rawText of [
    dmsRows.slice(0, 7).join("\n"),
    ["SITES1", ...dmsRows.slice(0, 4), "", "SITES2", ...dmsRows.slice(4, 8)].join("\n")
  ]) {
    const eligibility = evaluateDmsGroupedRetryEligibility({
      rawText,
      isImageInput: true,
      familyRetryAllowed: true,
      typedRouteEligible: false,
      partialMultisiteRecoveryEligible: true
    });
    assert.deepEqual(eligibility, {
      allowed: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_STRUCTURE_UNPROVEN"
    });
  }
});

dynamicCase("weak partial 11-row three-group evidence reserves only dms_grouped", () => {
  const evidence = getDmsDocumentEvidence(weakPartialStage1Text);
  const trust = resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const weak = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    projectedTableSignal: evidence.projectedTableSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    candidateSignal: trust.weakPartialMultisiteRecoveryCandidateSignal,
    structureText: weakPartialStage1Text,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(evidence.explicitHandwrittenSignal, true);
  assert.equal(trust.explicitHandwrittenSignal, false);
  assert.equal(weak.accepted, true);
  assert.equal(weak.failClosed, false);
  assert.deepEqual(weak.groupSizes, [4, 4, 3]);
  assert.deepEqual(weak.rejectedEvidence, {
    sourceStage: "STAGE1",
    parserVersion: "DMS_SOURCE_STRICT_V1",
    candidateLineCount: 14,
    validRowCount: 11,
    rejectedLineCount: 3,
    repairableRejectedLineCount: 3,
    unknownRejectedLineCount: 0,
    rejectionReasonCounts: {
      DMS_PAIR_AXIS_CONFLICT: 3,
      DMS_PAIR_HEMISPHERE_UNRESOLVED: 0
    }
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({
      isImageInput: true,
      printedTableSignal: trust.printedDmsCandidateSignal,
      explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
      structureText: weakPartialStage1Text
    }),
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: false,
    weakPartialMultisiteRecovery: weak
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY);
  assert.equal(ownership.retryOwner, "dms_grouped");
  assert.equal(ownership.failClosed, false);
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: weakPartialStage1Text,
    isImageInput: true,
    familyRetryAllowed: true,
    imageInputBuffer: syntheticWeakPartialImageBuffer,
    weakPartialMultisiteRecovery: { ...weak, claimed: true }
  });
  assert.deepEqual(eligibility, {
    allowed: true,
    failClosed: false,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_AUTHORIZED"
  });
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  for (const owner of ["handwritten_dms", "generic_ocr", "wgs84_table"]) {
    assert.equal(orchestrator.claim(owner), false);
  }
  assert.equal(orchestrator.claim("dms_grouped"), true);
});

dynamicCase("weak partial recovery eligibility is bounded and fails closed after owner claim", () => {
  const minimumShape = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4), `5. 12°00'00.0"N, 9°00'00.0"N`, "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 10), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 15)
  ].join("\n");
  const minimumEvaluation = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: minimumShape,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(minimumEvaluation.accepted, true);
  assert.deepEqual(minimumEvaluation.groupSizes, [4, 2, 3]);
  const missingHemisphereEvaluation = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: minimumShape.replace(`12°00'00.0"N, 9°00'00.0"N`, `12°00'00.0", 9°00'00.0"`),
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(missingHemisphereEvaluation.accepted, true);
  assert.equal(missingHemisphereEvaluation.rejectedEvidence.rejectionReasonCounts.DMS_PAIR_HEMISPHERE_UNRESOLVED, 1);
  const cleanThreeGroupEleven = weakPartialStage1Text.split("\n")
    .filter(line => !/9°0[012]'00\.0"N/.test(line))
    .join("\n");
  const selfConsistentButUnattestedInputEvidence = {
    schemaVersion: "dms_grouped_weak_partial_input_evidence_v2",
    sourceStage: "STAGE1",
    inputModality: "IMAGE",
    imageInputEvidenceSource: "REQUEST_FILE_PRESENT",
    projectedTableSignal: false,
    independentHandwrittenSignal: false,
    candidateSignal: true,
    stage1SanitizedTextSha256: createHash("sha256").update(cleanThreeGroupEleven).digest("hex"),
    runtimeAttestationId: "f".repeat(48)
  };
  const selfConsistentButUnattestedExpansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: cleanThreeGroupEleven,
    retryText: structuredText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialQualification: {
      schemaVersion: "dms_grouped_weak_partial_qualification_v1",
      inputModality: "IMAGE",
      projectedTableSignal: false,
      independentHandwrittenSignal: false,
      candidateSignal: true,
      accepted: true,
      failClosed: false,
      reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ELIGIBLE"
    },
    weakPartialRejectedEvidence: {
      sourceStage: "STAGE1",
      parserVersion: "DMS_SOURCE_STRICT_V1",
      candidateLineCount: 12,
      validRowCount: 11,
      rejectedLineCount: 1,
      repairableRejectedLineCount: 1,
      unknownRejectedLineCount: 0,
      rejectionReasonCounts: {
        DMS_PAIR_AXIS_CONFLICT: 1,
        DMS_PAIR_HEMISPHERE_UNRESOLVED: 0
      }
    },
    weakPartialInputEvidence: selfConsistentButUnattestedInputEvidence
  });
  assert.equal(selfConsistentButUnattestedExpansion.accepted, false);
  const unknownRejected = `${cleanThreeGroupEleven}\n4. 11°58'00.0"N`;
  const invalidRangeRejected = `${cleanThreeGroupEleven}\n4. 11°99'00.0"N, 9°02'00.0"E`;
  const tooManyRepairableRejects = `${cleanThreeGroupEleven}\n${Array.from({ length: 12 }, (_, index) => `${index + 4}. 11°58'00.0"N, 9°02'00.0"N`).join("\n")}`;
  const groupBelowMinimum = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 3), `4. 12°00'00.0"N, 9°00'00.0"N`, "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12, 16)
  ].join("\n");
  const groupAboveMaximum = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 8), `9. 12°00'00.0"N, 9°00'00.0"N`, "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 13), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(14, 16)
  ].join("\n");
  const invalidInputs = [
    { text: dmsRows.slice(0, 11).map((row, index) => row.replace(/^\d+\./, `${index + 1}.`)).join("\n"), reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SHAPE_INVALID" },
    { text: cleanThreeGroupEleven, reason: "DMS_GROUPED_WEAK_PARTIAL_REJECTION_COUNT_INVALID" },
    { text: stage1FourFourFourOneText, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SHAPE_INVALID" },
    { text: weakPartialStage1Text.replace(dmsRows[14], dmsRows[14].replace(/^3\./, "4.")), reason: "DMS_GROUPED_WEAK_PARTIAL_LABEL_SEQUENCE_INVALID" },
    { text: unknownRejected, reason: "DMS_GROUPED_WEAK_PARTIAL_UNKNOWN_REJECTION" },
    { text: invalidRangeRejected, reason: "DMS_GROUPED_WEAK_PARTIAL_UNKNOWN_REJECTION" },
    { text: tooManyRepairableRejects, reason: "DMS_GROUPED_WEAK_PARTIAL_REJECTION_COUNT_INVALID" },
    { text: groupBelowMinimum, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SIZE_INVALID" },
    { text: groupAboveMaximum, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SIZE_INVALID" }
  ];
  for (const entry of invalidInputs) {
    const weak = evaluateDmsWeakPartialMultisiteRecovery({
      isImageInput: true,
      candidateSignal: true,
      structureText: entry.text,
      imageInputBuffer: syntheticWeakPartialImageBuffer
    });
    assert.equal(weak.accepted, false);
    assert.equal(weak.failClosed, true);
    assert.equal(weak.reason, entry.reason);
    assert.deepEqual(evaluateDmsGroupedRetryEligibility({
      rawText: entry.text,
      isImageInput: true,
      familyRetryAllowed: true,
      weakPartialMultisiteRecovery: { ...weak, claimed: true }
    }), {
      allowed: false,
      failClosed: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_STRUCTURE_UNPROVEN"
    });
  }
  for (const flags of [
    { isImageInput: false },
    { isImageInput: true, projectedTableSignal: true },
    { isImageInput: true, explicitHandwrittenSignal: true }
  ]) {
    const weak = evaluateDmsWeakPartialMultisiteRecovery({
      ...flags,
      candidateSignal: true,
      structureText: weakPartialStage1Text,
      imageInputBuffer: syntheticWeakPartialImageBuffer
    });
    assert.equal(weak.accepted, false);
    assert.equal(weak.reason, "DMS_GROUPED_WEAK_PARTIAL_INPUT_NOT_ELIGIBLE");
  }
  const validWeak = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: weakPartialStage1Text,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.deepEqual(evaluateDmsGroupedRetryEligibility({
    rawText: weakPartialStage1Text,
    isImageInput: true,
    familyRetryAllowed: false,
    imageInputBuffer: syntheticWeakPartialImageBuffer,
    weakPartialMultisiteRecovery: { ...validWeak, claimed: true }
  }), {
    allowed: false,
    failClosed: true,
    reason: "DMS_GROUPED_RETRY_BUDGET_BLOCKED"
  });
  for (const signal of [
    { projectedTableSignal: true },
    { explicitHandwrittenSignal: true }
  ]) {
    assert.deepEqual(evaluateDmsGroupedRetryEligibility({
      rawText: weakPartialStage1Text,
      isImageInput: true,
      familyRetryAllowed: true,
      imageInputBuffer: syntheticWeakPartialImageBuffer,
      weakPartialMultisiteRecovery: { ...validWeak, claimed: true },
      ...signal
    }), {
      allowed: false,
      failClosed: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_STRUCTURE_UNPROVEN"
    });
  }
});

dynamicCase("independent trusted handwritten evidence preserves the genuine handwritten reread", () => {
  const providerText = `Recognition hint: handwritten DMS\n${dmsRows.slice(0, 4).join("\n")}`;
  const evidence = getDmsDocumentEvidence(providerText);
  const trust = resolveDmsRetryTrustBoundary({
    documentEvidence: evidence,
    trustedHandwrittenSignal: true
  });
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: trust.printedDmsCandidateSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    structureText: providerText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true
  });
  assert.equal(trust.nonHandwrittenDmsCandidateSignal, false);
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.HANDWRITTEN_DMS_ONLY);
  assert.equal(ownership.retryOwner, "handwritten_dms");
});

dynamicCase("unproven printed multi-site structure fails closed before any alternate Provider retry", () => {
  const ambiguous = [
    ...dmsRows.slice(0, 3),
    ...dmsRows.slice(8, 11),
    "",
    ...flatRows.slice(6, 9)
  ].join("\n");
  const evidence = getDmsDocumentEvidence(ambiguous);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    explicitHandwrittenSignal: false,
    structureText: ambiguous
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    nonHandwrittenDmsCandidateSignal: evidence.nonHandwrittenDmsCandidateSignal
  });
  assert.equal(route.documentHasStrongMultiRegionEvidence, true);
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED);
  assert.equal(ownership.failClosed, true);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  orchestrator.failClose(ownership.reason);
  let providerCalls = 0;
  for (const owner of ["handwritten_dms", "generic_ocr", "wgs84_table", "dms_grouped"]) {
    if (orchestrator.claim(owner)) providerCalls += 1;
  }
  assert.equal(providerCalls, 0);
});

dynamicCase("non-explicit handwritten-shaped DMS cannot acquire handwritten or generic retry ownership", () => {
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: dmsRows.slice(0, 4).join("\n") }),
    nonHandwrittenDmsCandidateSignal: true,
    handwrittenShapeRetryCandidate: true
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED);
  assert.equal(ownership.retryOwner, null);
  assert.equal(ownership.failClosed, true);
});

dynamicCase("explicit handwritten evidence alone retains handwritten retry ownership", () => {
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({
      isImageInput: true,
      printedTableSignal: true,
      explicitHandwrittenSignal: true,
      structureText: structuredText
    }),
    explicitHandwrittenSignal: true,
    nonHandwrittenDmsCandidateSignal: false,
    handwrittenShapeRetryCandidate: true
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.HANDWRITTEN_DMS_ONLY);
  assert.equal(ownership.retryOwner, "handwritten_dms");
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.reserve(ownership.retryOwner), true);
  assert.equal(orchestrator.claim("dms_grouped"), false);
  assert.equal(orchestrator.claim("handwritten_dms"), true);
});

dynamicCase("non-DMS image leaves retry ownership unreserved", () => {
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: "ordinary image text" })
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
  assert.equal(ownership.failClosed, false);
});

dynamicCase("retry routing classification is sanitized finite metadata only", () => {
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: { documentHasStrongMultiRegionEvidence: true, hasUnprovenBoundary: true },
    nonHandwrittenDmsCandidateSignal: true
  });
  assert.deepEqual(Object.keys(ownership), ["classification", "retryOwner", "failClosed", "reason"]);
  assert.doesNotMatch(JSON.stringify(ownership), /12°|SITES1|latitude|longitude|raw|image/i);
});

dynamicCase("projected X Y table with damaged DMS reference cannot be captured by dms_grouped", () => {
  const projectedReferenceRows = [
    `1 | 500001 | 1200001 | 12°00'36.9"N | 9°09'40.8"E`,
    `2 | 500002 | 1200002 | 12°00'34.0"N | 9°09'22.0"E`,
    `3 | 500003 | 1200003 | 12°00'48.1"N | 9°08'32.7"E`,
    `1 | 500004 | 1200004 | 11°59'46.7"N | 9°07'27.0"E`,
    `2 | 500005 | 1200005 | INVALID_DMS | 9°05'59.9"E`,
    `3 | 500006 | 1200006 | 12°00'36.1"N | 9°05'56.5"E`
  ];
  const projectedMultiSite = [
    "SITES1", "POINT | X | Y | LATITUDE | LONGITUDE", ...projectedReferenceRows.slice(0, 3),
    "SITES2", "POINT | X | Y | LATITUDE | LONGITUDE", ...projectedReferenceRows.slice(3)
  ].join("\n");
  assert.equal(getPrintedProjectedDmsReference(projectedMultiSite), null);
  const evidence = getDmsDocumentEvidence(projectedMultiSite);
  assert.equal(evidence.projectedTableSignal, true);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    projectedTableSignal: evidence.projectedTableSignal,
    structureText: projectedMultiSite
  });
  assert.equal(route.documentHasStrongMultiRegionEvidence, true);
  assert.equal(route.typedDmsGrouped, false);
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    projectedTableSignal: evidence.projectedTableSignal,
    nonHandwrittenDmsCandidateSignal: evidence.nonHandwrittenDmsCandidateSignal
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.snapshot().reservedFamilyOwner, null);
  assert.equal(orchestrator.claim("projected_xy"), true);
});

dynamicCase("whitespace projected X Y rows cannot be captured by dms_grouped", () => {
  const projectedWhitespaceMultiSite = [
    "SITES1", "POINT X Y LATITUDE LONGITUDE",
    `10001 500001 1200001 12°00'36.9\"N 9°09'40.8\"E`,
    `10002 500002 1200002 12°00'34.0\"N 9°09'22.0\"E`,
    `10003 500003 1200003 12°00'48.1\"N 9°08'32.7\"E`,
    "SITES2", "POINT X Y LATITUDE LONGITUDE",
    `20001 500004 1200004 11°59'46.7\"N 9°07'27.0\"E`,
    `20002 500005 1200005 INVALID_DMS 9°05'59.9\"E`,
    `20003 500006 1200006 12°00'36.1\"N 9°05'56.5\"E`
  ].join("\n");
  assert.equal(getPrintedProjectedDmsReference(projectedWhitespaceMultiSite), null);
  const evidence = getDmsDocumentEvidence(projectedWhitespaceMultiSite);
  assert.equal(evidence.projectedTableSignal, true);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    projectedTableSignal: evidence.projectedTableSignal,
    structureText: projectedWhitespaceMultiSite
  });
  assert.equal(route.documentHasStrongMultiRegionEvidence, true);
  assert.equal(route.typedDmsGrouped, false);
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    projectedTableSignal: evidence.projectedTableSignal,
    nonHandwrittenDmsCandidateSignal: evidence.nonHandwrittenDmsCandidateSignal
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.snapshot().reservedFamilyOwner, null);
  assert.equal(orchestrator.claim("projected_xy"), true);
});

dynamicCase("projected evidence outranks explicit handwritten text for retry ownership", () => {
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: true,
    projectedTableSignal: true,
    explicitHandwrittenSignal: true,
    structureText: structuredText
  });
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    projectedTableSignal: true,
    explicitHandwrittenSignal: true,
    handwrittenShapeRetryCandidate: true
  });
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.claim("projected_xy"), true);
});

dynamicCase("headerless named multi-site DMS keeps printed priority after Stage-1 drops table headers", () => {
  const headerless = [
    "SITES1", ...dmsRows.slice(0, 8),
    "SITES2", ...dmsRows.slice(8, 12),
    "SITES3", ...dmsRows.slice(12)
  ].join("\n");
  const evidence = getDmsDocumentEvidence(headerless);
  assert.equal(evidence.printedDmsCandidateSignal, false);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    explicitHandwrittenSignal: false,
    structureText: headerless
  });
  assert.equal(route.printedEvidenceEstablished, true);
  assert.equal(route.typedDmsGrouped, true);
  assert.equal(route.suppressHandwrittenRetry, true);
});

dynamicCase("headerless 13-point Stage-1 routes exactly one dms_grouped reread and zero handwritten rereads", () => {
  const headerlessStage1 = stage1ThirteenText.split("\n")
    .filter(line => !/POINT\s*\|\s*LATITUDE\s*\|\s*LONGITUDE/i.test(line))
    .join("\n");
  const evidence = getDmsDocumentEvidence(headerlessStage1);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedDmsCandidateSignal,
    explicitHandwrittenSignal: false,
    structureText: headerlessStage1
  });
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: headerlessStage1,
    dmsGroupedInfo: { output: "stage1-normalized-present" },
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: route.typedDmsGrouped
  });
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  let handwrittenCalls = 0;
  let dmsGroupedCalls = 0;
  if (!route.suppressHandwrittenRetry) handwrittenCalls += 1;
  if (eligibility.allowed && orchestrator.claim("dms_grouped")) dmsGroupedCalls += 1;
  assert.equal(extractDmsSourceStructure(headerlessStage1).rowCount, 13);
  assert.equal(route.typedDmsGrouped, true);
  assert.equal(eligibility.allowed, true);
  assert.equal(handwrittenCalls, 0);
  assert.equal(dmsGroupedCalls, 1);
  assert.equal(orchestrator.snapshot().activeFamilyOwner, "dms_grouped");
});

dynamicCase("headerless numbered restarts establish multi-site printed priority", () => {
  const headerless = dmsRows.join("\n");
  const structure = extractDmsSourceStructure(headerless);
  assert.deepEqual(structure.groups.map(group => group.rows.length), [8, 4, 4]);
  assert.deepEqual(structure.groups.map(group => group.boundaryProvenance), ["document_start", "number_restart", "number_restart"]);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: false,
    explicitHandwrittenSignal: false,
    structureText: headerless
  });
  assert.equal(route.typedDmsGrouped, true);
});

dynamicCase("blank-separated groups require independent continuous numbering before gaining priority", () => {
  const proven = [
    ...dmsRows.slice(0, 3), "",
    ...dmsRows.slice(8, 11), "",
    ...dmsRows.slice(12, 15)
  ].join("\n");
  const structure = extractDmsSourceStructure(proven);
  assert.deepEqual(structure.groups.map(group => group.rows.length), [3, 3, 3]);
  assert.equal(structure.hasProvenBlankLineBoundary, true);
  assert.equal(structure.allBoundariesProven, true);
  assert.equal(hasExplicitDmsMultiRegionEvidence(proven), true);
  assert.equal(evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: false,
    explicitHandwrittenSignal: false,
    structureText: proven
  }).typedDmsGrouped, true);

  const unproven = [...flatRows.slice(0, 3), "", ...flatRows.slice(3, 6)].join("\n");
  const unprovenStructure = extractDmsSourceStructure(unproven);
  assert.equal(unprovenStructure.hasProvenBlankLineBoundary, false);
  assert.equal(unprovenStructure.allBoundariesProven, false);
  assert.equal(evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: false,
    explicitHandwrittenSignal: false,
    structureText: unproven
  }).typedDmsGrouped, false);
});

dynamicCase("printed table signal cannot bypass a mixed proven and unproven group boundary", () => {
  const mixedBoundaries = [
    ...dmsRows.slice(0, 3),
    ...dmsRows.slice(8, 11),
    "",
    ...flatRows.slice(6, 9)
  ].join("\n");
  const structure = extractDmsSourceStructure(mixedBoundaries);
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: true,
    explicitHandwrittenSignal: false,
    structureText: mixedBoundaries
  });
  assert.deepEqual(structure.groups.map(group => group.boundaryProvenance), [
    "document_start",
    "number_restart",
    "blank_line"
  ]);
  assert.equal(structure.documentHasStrongMultiRegionEvidence, true);
  assert.equal(structure.allBoundariesProven, false);
  assert.equal(structure.hasUnprovenBoundary, true);
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(route.suppressHandwrittenRetry, false);
  const ownership = classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: route,
    nonHandwrittenDmsCandidateSignal: getDmsDocumentEvidence(mixedBoundaries).nonHandwrittenDmsCandidateSignal
  });
  assert.equal(ownership.classification, DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED);
});

dynamicCase("explicit handwritten context still retains the handwritten retry path", () => {
  const headerless = [
    "SITES1", ...dmsRows.slice(0, 8),
    "SITES2", ...dmsRows.slice(8, 12),
    "SITES3", ...dmsRows.slice(12)
  ].join("\n");
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: false,
    explicitHandwrittenSignal: true,
    structureText: headerless
  });
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(route.suppressHandwrittenRetry, false);
});

dynamicCase("incomplete 13-point structured result remains fail closed", () => {
  const incomplete = structuredText.replace(dmsRows[3], "").replace(dmsRows[7], "").replace(dmsRows[15], "");
  const coverage = evaluateDmsGroupedRetryCoverage({ baselineText: structuredText, retryText: incomplete });
  assert.equal(coverage.accepted, false);
  assert.equal(coverage.reason, "ROW_COUNT_MISMATCH");
});

dynamicCase("Stage-1 13 points and structured reread 16 points form a review-only acquisition delta", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({ baselineText: stage1ThirteenText, retryText: structuredText });
  assert.equal(extractDmsSourceStructure(stage1ThirteenText).rowCount, 13);
  assert.equal(expansion.accepted, true);
  assert.equal(expansion.baselineRowCount, 13);
  assert.equal(expansion.retryRowCount, 16);
  assert.equal(expansion.addedRowCount, 3);
  assert.deepEqual(expansion.groupSizes, [8, 4, 4]);
  assert.equal(expansion.normalizedCoordinates.replace(/\n\n/g, "\n").split("\n").length, 16);
});

dynamicCase("Stage-1 eight-row partial multi-site recovery accepts only preserved ordered 8 4 4 evidence", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: structuredText,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(expansion.accepted, true);
  assert.equal(expansion.recoveryMode, "STAGE1_PARTIAL_MULTISITE_8_TO_16");
  assert.equal(expansion.baselineRowCount, 8);
  assert.equal(expansion.retryRowCount, 16);
  assert.deepEqual(expansion.groupSizes, [8, 4, 4]);
  const candidate = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: stage1PartialEightText,
    stage1Coordinates: productionNormalizedRows.slice(0, 8).join("\n"),
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped"
  });
  assert.equal(candidate.accepted, true);
  assert.equal(candidate.failClosed, false);
  assert.equal(candidate.provenance.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(candidate.provenance.sourceCandidateSeparate, true);
  assert.equal(candidate.provenance.directCanonicalPromotion, false);
  assert.equal(candidate.provenance.recoverySource, "dms_grouped");
  assert.equal(candidate.provenance.pointwiseBaselineEquivalenceProven, true);
  assert.equal(candidate.provenance.pointwiseAcquisitionDeltaProven, true);
  assert.equal(Object.hasOwn(candidate.provenance, "stage1RejectedEvidence"), false);
  assert.notEqual(candidate.provenance.stage1CandidateSha256, candidate.provenance.retryCandidateSha256);
  const safety = evaluateStage1FullMultisiteSafety({
    isImageInput: true,
    riskSignal: true,
    structureText: structuredText,
    normalizedCoordinates: expansion.normalizedCoordinates
  });
  assert.equal(safety.acceptedForReview, true);
  assert.equal(safety.failClosed, false);
  assert.deepEqual(safety.groupSizes, [8, 4, 4]);
  const completeness = evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: structuredText,
    normalizedCoordinates: expansion.normalizedCoordinates,
    selectedRoute: IMAGE_DMS_SELECTED_ROUTE.DMS_GROUPED
  });
  assert.equal(evaluateStage1FullMultisitePromotion({
    stage1Safety: safety,
    imageDmsSourceCompleteness: completeness
  }).allowed, true);
});

dynamicCase("weak partial 11-to-16 recovery is bound to ordered baseline content and remains non-authoritative", () => {
  const attestedStage1 = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    candidateSignal: true,
    structureText: weakPartialStage1Text,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(attestedStage1.accepted, true);
  assert.equal(evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: weakPartialStage1Text,
    imageInputBuffer: Buffer.from("synthetic-weak-partial-image-B"),
    weakPartialInputEvidence: attestedStage1.inputEvidence
  }).reason, "DMS_GROUPED_WEAK_PARTIAL_INPUT_ATTESTATION_INVALID");
  assert.equal(evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: weakPartialStage1Text,
    weakPartialInputEvidence: attestedStage1.inputEvidence
  }).reason, "DMS_GROUPED_WEAK_PARTIAL_INPUT_ATTESTATION_INVALID");
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: weakPartialStage1Text,
    retryText: structuredText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialInputEvidence: attestedStage1.inputEvidence,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(expansion.accepted, true);
  assert.equal(expansion.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.equal(expansion.baselineRowCount, 11);
  assert.equal(expansion.retryRowCount, 16);
  assert.equal(expansion.addedRowCount, 5);
  assert.deepEqual(expansion.baselineGroupSizes, [4, 4, 3]);
  assert.deepEqual(expansion.groupSizes, [8, 4, 4]);
  assert.equal(expansion.stage1RejectedEvidence.rejectedLineCount, 3);
  assert.equal(expansion.weakPartialQualification.inputModality, "IMAGE");
  assert.equal(expansion.weakPartialQualification.projectedTableSignal, false);
  assert.equal(expansion.weakPartialQualification.independentHandwrittenSignal, false);
  for (const invalidIdentity of [
    {},
    { isImageInput: false, projectedTableSignal: false, explicitHandwrittenSignal: false, weakPartialCandidateSignal: true },
    { isImageInput: true, projectedTableSignal: true, explicitHandwrittenSignal: false, weakPartialCandidateSignal: true },
    { isImageInput: true, projectedTableSignal: false, explicitHandwrittenSignal: true, weakPartialCandidateSignal: true },
    { isImageInput: true, projectedTableSignal: false, explicitHandwrittenSignal: false, weakPartialCandidateSignal: false }
  ]) {
    const rejected = evaluateDmsGroupedAcquisitionExpansion({
      baselineText: weakPartialStage1Text,
      retryText: structuredText,
      allowWeakPartialMultisiteRecovery: true,
      ...invalidIdentity
    });
    assert.equal(rejected.accepted, false);
  }
  const projectedWeakPartialText = [
    "POINT | X | Y",
    "P1 | 500001 | 1200001",
    "P2 | 500002 | 1200002",
    "P3 | 500003 | 1200003",
    weakPartialStage1Text
  ].join("\n");
  assert.equal(getDmsDocumentEvidence(projectedWeakPartialText).projectedTableSignal, true);
  const projectedMasquerade = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: projectedWeakPartialText,
    retryText: structuredText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true
  });
  assert.equal(projectedMasquerade.accepted, false);
  const candidate = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: weakPartialStage1Text,
    stage1Coordinates: weakPartialStage1Coordinates,
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped",
    imageInputBuffer: syntheticWeakPartialImageBuffer,
    allowUnsanitizedWeakPartialStage1: true
  });
  assert.equal(candidate.accepted, true);
  assert.equal(candidate.failClosed, false);
  assert.equal(candidate.provenance.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.equal(candidate.provenance.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(candidate.provenance.sourceCandidateSeparate, true);
  assert.equal(candidate.provenance.directCanonicalPromotion, false);
  assert.deepEqual(candidate.provenance.stage1RejectedEvidence, expansion.stage1RejectedEvidence);
  assert.deepEqual(candidate.provenance.weakPartialQualification, expansion.weakPartialQualification);
  assert.notEqual(candidate.stage1Candidate.rawText, weakPartialStage1Text);
  assert.equal(candidate.stage1Candidate.rawText.includes("Recognition hint: handwritten DMS"), false);
  assert.equal(candidate.stage1Candidate.rawText.includes(`9°00'00.0"N`), false);
  assert.equal(candidate.stage1Candidate.rawText.includes(`9°01'00.0"N`), false);
  assert.equal(candidate.stage1Candidate.rawText.includes(`9°02'00.0"N`), false);
  assert.notEqual(candidate.provenance.stage1CandidateSha256, candidate.provenance.retryCandidateSha256);
  const replayRetryText = structuredText.replace(dmsRows[4], dmsRows[4].replace("36.9", "38.9"));
  const replayExpansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: weakPartialStage1Text,
    retryText: replayRetryText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialInputEvidence: attestedStage1.inputEvidence,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(replayExpansion.accepted, true);
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: weakPartialStage1Text,
    stage1Coordinates: weakPartialStage1Coordinates,
    retryRawText: replayRetryText,
    retryCoordinates: replayExpansion.normalizedCoordinates,
    expansion: replayExpansion,
    ownerFamily: "dms_grouped",
    imageInputBuffer: syntheticWeakPartialImageBuffer,
    allowUnsanitizedWeakPartialStage1: true
  }).reason, "DMS_GROUPED_WEAK_PARTIAL_RUNTIME_ATTESTATION_REPLAY_OR_BINDING_INVALID");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: weakPartialStage1Text,
    stage1Coordinates: weakPartialStage1Coordinates,
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped"
  }).reason, "DMS_GROUPED_WEAK_PARTIAL_REJECTED_RAW_EVIDENCE_FORBIDDEN");
  const recognitionCandidate = {
    rawText: candidate.rawText,
    coordinates: candidate.normalizedCoordinates,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    stage1Candidate: candidate.stage1Candidate,
    partialMultisiteRecoveryCandidate: {
      rawText: candidate.rawText,
      coordinates: candidate.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: candidate.provenance
    },
    sourceCandidates: {
      stage1: {
        ...candidate.stage1Candidate,
        rowCount: candidate.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: candidate.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: candidate.rawText,
        coordinates: candidate.normalizedCoordinates,
        rowCount: candidate.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: candidate.provenance.retryCandidateSha256
      }
    },
    partialMultisiteRecoveryProvenance: candidate.provenance,
    partialMultisiteRecoveryInputEvidence: candidate.weakPartialInputEvidence
  };
  const sourceRepresentation = buildSourceCoordinateRepresentation(recognitionCandidate, {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    groups: [
      { group_id: "group_1", points: pointsFromDms(dmsRows.slice(0, 8)) },
      { group_id: "group_2", points: pointsFromDms(dmsRows.slice(8, 12)) },
      { group_id: "group_3", points: pointsFromDms(dmsRows.slice(12)) }
    ]
  });
  assert.equal(sourceRepresentation.partialMultisiteRecoveryProvenance.recoveryMode,
    "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.equal(sourceRepresentation.partialMultisiteRecoveryProvenance.stage1RejectedEvidence.rejectedLineCount, 3);
  assert.deepEqual(sourceRepresentation.partialMultisiteRecoveryInputEvidence, candidate.weakPartialInputEvidence);
  assert.equal(sourceRepresentation.sourceCandidates.stage1.rawText, candidate.stage1Candidate.rawText);
  assert.equal(sourceRepresentation.sourceCandidates.stage1.rawText.includes(`9°00'00.0"N`), false);

  const rejectedRawSentinels = [
    `5. 12°00'00.0"N, 9°00'00.0"N`,
    `5. 11°59'00.0"N, 9°01'00.0"N`,
    `4. 11°58'00.0"N, 9°02'00.0"N`
  ];
  const safeVisionRouting = buildPartialMultisiteSafeVisionRouting({
    handwrittenVisionRouting: { retrySuccess: false },
    generalVisionRawText: weakPartialStage1Text,
    handwrittenVisionRawText: weakPartialStage1Text,
    finalRawText: weakPartialStage1Text,
    partialMultisiteRecoveryCandidate: candidate
  });
  const verificationResponse = buildCoordinateVerificationResponse({
    ...recognitionCandidate,
    handwrittenVisionRouting: safeVisionRouting
  }, {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    groups: [
      { group_id: "group_1", geometry: "polygon", requires_review: true, kml_ready: false, points: pointsFromDms(dmsRows.slice(0, 8)) },
      { group_id: "group_2", geometry: "polygon", requires_review: true, kml_ready: false, points: pointsFromDms(dmsRows.slice(8, 12)) },
      { group_id: "group_3", geometry: "polygon", requires_review: true, kml_ready: false, points: pointsFromDms(dmsRows.slice(12)) }
    ]
  });
  const serializedVerificationResponse = JSON.stringify(verificationResponse);
  assert.equal(safeVisionRouting.generalVisionRawText, candidate.stage1Candidate.rawText);
  assert.equal(safeVisionRouting.handwrittenVisionRawText, "");
  assert.equal(safeVisionRouting.finalRawText, candidate.rawText);
  for (const rejectedRawSentinel of rejectedRawSentinels) {
    assert.equal(serializedVerificationResponse.includes(rejectedRawSentinel), false);
    assert.equal(verificationResponse.evidence.items.some(item => String(item.raw_text || "").includes(rejectedRawSentinel)), false);
    assert.equal(verificationResponse.verification.conflicts.some(conflict => JSON.stringify(conflict).includes(rejectedRawSentinel)), false);
  }
  assert.equal(verificationResponse.partialMultisiteRecoveryProvenance.stage1RejectedEvidence.rejectedLineCount, 3);
  assert.deepEqual(
    verificationResponse.partialMultisiteRecoveryProvenance.stage1RejectedEvidence.rejectionReasonCounts,
    candidate.provenance.stage1RejectedEvidence.rejectionReasonCounts
  );

  for (const forgedQualification of [
    { ...candidate.provenance.weakPartialQualification, inputModality: "MANUAL" },
    { ...candidate.provenance.weakPartialQualification, projectedTableSignal: true },
    { ...candidate.provenance.weakPartialQualification, independentHandwrittenSignal: true },
    { ...candidate.provenance.weakPartialQualification, candidateSignal: false },
    null,
    "IMAGE"
  ]) {
    const forgedProvenance = {
      ...candidate.provenance,
      weakPartialQualification: forgedQualification
    };
    const forgedRepresentation = buildSourceCoordinateRepresentation({
      ...recognitionCandidate,
      partialMultisiteRecoveryCandidate: {
        ...recognitionCandidate.partialMultisiteRecoveryCandidate,
        provenance: forgedProvenance
      },
      partialMultisiteRecoveryProvenance: forgedProvenance
    }, {
      coordinate_type: "standard_dms_table",
      precision_mode: "dms-coordinates",
      groups: [
        { group_id: "group_1", points: pointsFromDms(dmsRows.slice(0, 8)) },
        { group_id: "group_2", points: pointsFromDms(dmsRows.slice(8, 12)) },
        { group_id: "group_3", points: pointsFromDms(dmsRows.slice(12)) }
      ]
    });
    assert.equal(forgedRepresentation.partialMultisiteRecoveryProvenance, null);
    assert.equal(forgedRepresentation.sourceCandidates, null);
  }

  for (const forgedInputEvidence of [
    null,
    "IMAGE",
    { ...candidate.weakPartialInputEvidence, inputModality: "MANUAL" },
    { ...candidate.weakPartialInputEvidence, projectedTableSignal: true },
    { ...candidate.weakPartialInputEvidence, independentHandwrittenSignal: true },
    { ...candidate.weakPartialInputEvidence, stage1SanitizedTextSha256: "0".repeat(64) },
    { ...candidate.weakPartialInputEvidence, runtimeAttestationId: "f".repeat(48) }
  ]) {
    const forgedRepresentation = buildSourceCoordinateRepresentation({
      ...recognitionCandidate,
      partialMultisiteRecoveryInputEvidence: forgedInputEvidence
    }, {
      coordinate_type: "standard_dms_table",
      precision_mode: "dms-coordinates",
      groups: [
        { group_id: "group_1", points: pointsFromDms(dmsRows.slice(0, 8)) },
        { group_id: "group_2", points: pointsFromDms(dmsRows.slice(8, 12)) },
        { group_id: "group_3", points: pointsFromDms(dmsRows.slice(12)) }
      ]
    });
    assert.equal(forgedRepresentation.partialMultisiteRecoveryProvenance, null);
    assert.equal(forgedRepresentation.sourceCandidates, null);
  }

  const leakedRejectedRawText = `${candidate.stage1Candidate.rawText}\n5. 9°00'00.0"N, 19°00'00.0"N`;
  const leakedRepresentation = buildSourceCoordinateRepresentation({
    ...recognitionCandidate,
    stage1Candidate: {
      ...candidate.stage1Candidate,
      rawText: leakedRejectedRawText
    },
    sourceCandidates: {
      ...recognitionCandidate.sourceCandidates,
      stage1: {
        ...recognitionCandidate.sourceCandidates.stage1,
        rawText: leakedRejectedRawText
      }
    }
  }, {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    groups: [
      { group_id: "group_1", points: pointsFromDms(dmsRows.slice(0, 8)) },
      { group_id: "group_2", points: pointsFromDms(dmsRows.slice(8, 12)) },
      { group_id: "group_3", points: pointsFromDms(dmsRows.slice(12)) }
    ]
  });
  assert.equal(leakedRepresentation.partialMultisiteRecoveryProvenance, null);
  assert.equal(leakedRepresentation.sourceCandidates, null);
});

dynamicCase("weak partial reread rejects incomplete malformed duplicate changed reordered and cross-group results", () => {
  const attestedStage1 = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: weakPartialStage1Text,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(attestedStage1.accepted, true);
  const evaluate = retryText => evaluateDmsGroupedAcquisitionExpansion({
    baselineText: weakPartialStage1Text,
    retryText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialInputEvidence: attestedStage1.inputEvidence,
    imageInputBuffer: syntheticWeakPartialImageBuffer
  });
  assert.equal(evaluate(structuredText.replace(dmsRows[15], "")).accepted, false);
  assert.equal(evaluate(structuredText.replace(dmsRows[0], dmsRows[0].replace("36.9", "37.9"))).reason,
    "WEAK_PARTIAL_BASELINE_POINT_CHANGED_MISSING_REORDERED_OR_CROSS_GROUP");
  const reordered = structuredText.replace(
    `${dmsRows[0]}\n${dmsRows[1]}`,
    `${dmsRows[1]}\n${dmsRows[0]}`
  );
  assert.equal(evaluate(reordered).reason, "RETRY_LABEL_SEQUENCE_INVALID");
  const relabeledBaselinePoint = structuredText.replace(dmsRows[0], dmsRows[0].replace(/^1\./, "01."));
  assert.equal(evaluate(relabeledBaselinePoint).reason,
    "WEAK_PARTIAL_BASELINE_POINT_CHANGED_MISSING_REORDERED_OR_CROSS_GROUP");
  const duplicate = structuredText.replace(dmsRows[4], dmsRows[0].replace(/^1\./, "5."));
  assert.equal(evaluate(duplicate).reason, "WEAK_PARTIAL_RETRY_DUPLICATE_POINT");
  const crossGroup = structuredText.replace(dmsRows[12], dmsRows[0]);
  assert.equal(evaluate(crossGroup).reason, "WEAK_PARTIAL_RETRY_DUPLICATE_POINT");
  const residualReject = `${structuredText}\n5. 11°58'00.0"N, 9°02'00.0"N`;
  assert.equal(evaluate(residualReject).reason, "RETRY_REJECTED_DMS_ROW_PRESENT");
  const malformedCoordinates = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: weakPartialStage1Text,
    stage1Coordinates: `${weakPartialStage1Coordinates}\nNOT_A_COORDINATE`,
    retryRawText: structuredText,
    retryCoordinates: evaluate(structuredText).normalizedCoordinates,
    expansion: evaluate(structuredText),
    ownerFamily: "dms_grouped",
    imageInputBuffer: syntheticWeakPartialImageBuffer,
    allowUnsanitizedWeakPartialStage1: true
  });
  assert.equal(malformedCoordinates.reason, "DMS_GROUPED_PARTIAL_MULTISITE_STAGE1_POINTWISE_MISMATCH");
});

dynamicCase("partial recovery candidate cannot masquerade as Stage-1 Direct-16", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: structuredText,
    allowPartialMultisiteRecovery: true
  });
  const invalid = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: stage1PartialEightText,
    stage1Coordinates: productionNormalizedRows.slice(0, 8).join("\n"),
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion: { ...expansion, recoveryMode: "STAGE1_DIRECT_16" },
    ownerFamily: "dms_grouped"
  });
  assert.deepEqual(invalid, {
    accepted: false,
    failClosed: true,
    reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_EXPANSION_BINDING_MISMATCH"
  });
});

dynamicCase("partial recovery proof is recomputed and rejects forged coordinate content", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: structuredText,
    allowPartialMultisiteRecovery: true
  });
  const common = {
    stage1RawText: stage1PartialEightText,
    stage1Coordinates: productionNormalizedRows.slice(0, 8).join("\n"),
    retryRawText: structuredText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped"
  };
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    stage1Coordinates: common.stage1Coordinates.replace(productionNormalizedRows[0], "99,12")
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_STAGE1_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryCoordinates: common.retryCoordinates.replace(/[^\n]+$/, "99,12")
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    stage1Coordinates: `${common.stage1Coordinates}\nNOT_A_COORDINATE`
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_STAGE1_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryCoordinates: `${common.retryCoordinates}\nNOT_A_COORDINATE`
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryRawText: `${common.retryRawText}\n5. 99°XX'YY\"N, 44°00'00\"E`
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_MALFORMED_SOURCE_ROW");
  const swappedAxisCoordinates = common.retryCoordinates.replace(
    /^([^,\n]+),([^\n]+)$/m,
    "$2,$1"
  );
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryCoordinates: swappedAxisCoordinates
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH");
  const reorderedCoordinates = common.retryCoordinates.split("\n");
  [reorderedCoordinates[0], reorderedCoordinates[1]] = [reorderedCoordinates[1], reorderedCoordinates[0]];
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryCoordinates: reorderedCoordinates.join("\n")
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryRawText: common.retryRawText.replace(dmsRows[8], dmsRows[8].replace('"N', '"S'))
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    retryRawText: common.retryRawText.replace(dmsRows[9], dmsRows[9].replace(/^2\./, "3."))
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_CONTENT_UNPROVEN");
  assert.equal(buildDmsGroupedPartialMultisiteRecoveryCandidate({
    ...common,
    expansion: { ...expansion, groupSizes: [4, 8, 4] }
  }).reason, "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_EXPANSION_BINDING_MISMATCH");
});

dynamicCase("partial multi-site recovery remains disabled without its explicit evidence-only authorization", () => {
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: structuredText
  });
  assert.equal(expansion.accepted, false);
  assert.equal(expansion.reason, "UNSUPPORTED_ACQUISITION_DELTA_SIZE");
});

dynamicCase("partial multi-site recovery rejects changed reordered and incomplete baselines", () => {
  const changed = structuredText.replace(`12°00'36.9"N`, `12°00'37.9"N`);
  const reordered = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", dmsRows[1], dmsRows[0], ...dmsRows.slice(2, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12)
  ].join("\n");
  const incomplete = structuredText.replace(dmsRows[15], "not a coordinate row");
  const changedResult = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: changed,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(changedResult.accepted, false);
  assert.equal(changedResult.reason, "PARTIAL_BASELINE_POINT_CHANGED_MISSING_OR_REORDERED");
  const reorderedResult = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: reordered,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(reorderedResult.accepted, false);
  assert.equal(reorderedResult.reason, "RETRY_LABEL_SEQUENCE_INVALID");
  const incompleteResult = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: incomplete,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(incompleteResult.accepted, false);
  assert.equal(incompleteResult.reason, "UNSUPPORTED_ACQUISITION_DELTA_SIZE");
});

dynamicCase("partial multi-site recovery rejects malformed and wrong 8 4 4 topology", () => {
  const malformed = structuredText.replace(dmsRows[15], "4. malformed DMS row");
  const wrongGrouping = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 4), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(4, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(12)
  ].join("\n");
  assert.equal(evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: malformed,
    allowPartialMultisiteRecovery: true
  }).accepted, false);
  const wrong = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1PartialEightText,
    retryText: wrongGrouping,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(wrong.accepted, false);
  assert.equal(wrong.reason, "RETRY_ORDERED_8_4_4_GROUPING_REQUIRED");
});

dynamicCase("expanded reread rejects a changed Stage-1 point", () => {
  const changedRetry = structuredText.replace(`12°00'36.9"N`, `12°00'37.9"N`);
  const expansion = evaluateDmsGroupedAcquisitionExpansion({ baselineText: stage1ThirteenText, retryText: changedRetry });
  assert.equal(expansion.accepted, false);
  assert.equal(expansion.reason, "BASELINE_GROUP_POINT_CHANGED_OR_MISSING");
});

dynamicCase("two strong groups totaling 16 cannot satisfy the three-group acquisition contract", () => {
  const twoGroupRetry = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(0, 8),
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...dmsRows.slice(8).map((row, index) => row.replace(/^\d+\./, `${index + 1}.`))
  ].join("\n");
  const twoGroupBaseline = twoGroupRetry.split("\n")
    .filter(line => ![dmsRows[3], dmsRows[7], dmsRows[15].replace(/^4\./, "8.")].includes(line))
    .join("\n");
  const expansion = evaluateDmsGroupedAcquisitionExpansion({ baselineText: twoGroupBaseline, retryText: twoGroupRetry });
  assert.equal(expansion.accepted, false);
  assert.equal(expansion.reason, "THREE_GROUP_BOUNDARY_CONTRACT_REQUIRED");
});

dynamicCase("Stage-1 group identity or order mismatch fails closed", () => {
  const swappedBaseline = stage1ThirteenText
    .replace("SITES1", "SITE99")
    .replace("SITES2", "SITES1")
    .replace("SITE99", "SITES2");
  const expansion = evaluateDmsGroupedAcquisitionExpansion({ baselineText: swappedBaseline, retryText: structuredText });
  assert.equal(expansion.accepted, false);
  assert.equal(expansion.reason, "BASELINE_GROUP_IDENTITY_OR_ORDER_MISMATCH");
});

dynamicCase("matching values moved across named groups cannot satisfy group-local preservation", () => {
  const firstSiteRow = dmsRows[0];
  const secondSiteRow = dmsRows[8];
  const crossGroupRetry = structuredText
    .replace(firstSiteRow, "__FIRST_SITE_ROW__")
    .replace(secondSiteRow, firstSiteRow.replace(/^1\./, "1."))
    .replace("__FIRST_SITE_ROW__", secondSiteRow.replace(/^1\./, "1."));
  const expansion = evaluateDmsGroupedAcquisitionExpansion({ baselineText: stage1ThirteenText, retryText: crossGroupRetry });
  assert.equal(expansion.accepted, false);
  assert.equal(expansion.reason, "BASELINE_GROUP_POINT_CHANGED_OR_MISSING");
});

dynamicCase("duplicate and decrement labels do not prove a number-restart boundary", () => {
  const duplicated = [...dmsRows.slice(0, 4), dmsRows[3], ...dmsRows.slice(4, 8)].join("\n");
  const decremented = [...dmsRows.slice(0, 4), dmsRows[2], ...dmsRows.slice(4, 8)].join("\n");
  assert.equal(extractDmsSourceStructure(duplicated).groupCount, 1);
  assert.equal(extractDmsSourceStructure(decremented).groupCount, 1);
});

dynamicCase("explicit handwritten multi-SITE evidence does not enter printed dms_grouped priority", () => {
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: true,
    explicitHandwrittenSignal: true,
    structureText: structuredText
  });
  assert.equal(route.typedDmsGrouped, false);
  assert.equal(route.suppressHandwrittenRetry, false);
});

dynamicCase("typed dms_grouped retry consumes one owner claim and one Provider call", () => {
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: true,
    explicitHandwrittenSignal: false,
    structureText: stage1ThirteenText
  });
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: stage1ThirteenText,
    dmsGroupedInfo: { output: "stage1-normalized-present" },
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: route.typedDmsGrouped
  });
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  let providerCalls = 0;
  if (eligibility.allowed && orchestrator.claim("dms_grouped")) providerCalls += 1;
  assert.equal(providerCalls, 1);
  assert.equal(orchestrator.snapshot().activeFamilyOwner, "dms_grouped");
});

dynamicCase("complete 16-point structured result does not expand Provider budget", () => {
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: true,
    explicitHandwrittenSignal: false,
    structureText: structuredText
  });
  const eligibility = evaluateDmsGroupedRetryEligibility({
    rawText: structuredText,
    dmsGroupedInfo: { output: decimalRows.join("\n") },
    isImageInput: true,
    familyRetryAllowed: true,
    typedRouteEligible: route.typedDmsGrouped
  });
  assert.equal(eligibility.allowed, false);
  assert.equal(eligibility.reason, "DMS_GROUPED_RETRY_NOT_REQUIRED");
});

dynamicCase("normalized points reconstruct exact 8 4 4 without retry values", () => {
  const canonical = decimalRows.join("\n");
  const reconstructed = reconstructDmsGroupsFromNormalizedCoordinates({ structureText: structuredText, normalizedCoordinates: canonical });
  assert.equal(reconstructed.accepted, true);
  assert.deepEqual(reconstructed.groupSizes, [8, 4, 4]);
  assert.equal(reconstructed.output.replace(/\n\n/g, "\n"), canonical);
  assert.equal(reconstructed.output.includes("12°"), false);
});

dynamicCase("retry values never enter normalized reconstructed geometry", () => {
  const retryWithDifferentValues = structuredText.replace(`12°00'36.9"N`, `12°00'37.9"N`);
  const reconstructed = reconstructDmsGroupsFromNormalizedCoordinates({ structureText: retryWithDifferentValues, normalizedCoordinates: decimalRows.join("\n") });
  assert.equal(reconstructed.accepted, true);
  assert.equal(reconstructed.output.replace(/\n\n/g, "\n"), decimalRows.join("\n"));
});

dynamicCase("normalized reconstruction count mismatch fails closed", () => {
  const reconstructed = reconstructDmsGroupsFromNormalizedCoordinates({ structureText: structuredText, normalizedCoordinates: decimalRows.slice(0, 15).join("\n") });
  assert.equal(reconstructed.accepted, false);
  assert.equal(reconstructed.reason, "NORMALIZED_ROW_COUNT_MISMATCH");
});

dynamicCase("request retry owner is shared across success timeout and fallback phases", () => {
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.claim("dms_grouped"), true);
  assert.equal(orchestrator.claim("wgs84_table"), false);
  assert.equal(orchestrator.claim("bftm"), false);
});

dynamicCase("production group-name resolver binds exact SITES identities to matching 8 4 4 blocks", () => {
  assert.deepEqual(resolveDmsEngineGroupNames({
    structureText: structuredText,
    groupSizes: [8, 4, 4]
  }), ["SITES1", "SITES2", "SITES3"]);
});

dynamicCase("production group-name resolver fails closed on block coverage mismatch", () => {
  assert.deepEqual(resolveDmsEngineGroupNames({
    structureText: structuredText,
    groupSizes: [8, 3, 5]
  }), []);
});

dynamicCase("handwritten owner conflict permits zero Provider calls", () => {
  const orchestrator = createDmsGroupedRetryOrchestrator({ parserTrace: [] });
  assert.equal(orchestrator.claim("wgs84_table"), true);
  let providerCalls = 0;
  const attemptHandwrittenProviderRetry = () => {
    if (!orchestrator.claim("handwritten_dms")) return false;
    providerCalls += 1;
    return true;
  };
  assert.equal(attemptHandwrittenProviderRetry(), false);
  assert.equal(providerCalls, 0);
});

staticAssertion("server calls production DMS grouped retry eligibility", () => {
  assert.match(server, /evaluateDmsGroupedRetryEligibility\(\{/);
  assert.match(server, /if \(!dmsGroupedRetryBoundaryFailure && dmsGroupedRetryEligibility\.allowed\)/);
});
staticAssertion("server applies production fail-closed patch before downstream family retries", () => {
  const applyIndex = server.indexOf("applyDmsGroupedRetryFailClosed(dmsGroupedRetryEligibility.reason)");
  const downstreamIndex = server.indexOf("claimDownstreamFamilyRetry(\"french_perimeter_dms\")");
  assert.ok(applyIndex >= 0 && downstreamIndex > applyIndex);
  assert.match(server, /const requestRetryOrchestrator = createDmsGroupedRetryOrchestrator\(\{ parserTrace \}\)/);
  assert.match(server, /const claimDownstreamFamilyRetry = targetOwner => \{\s*return claimRequestRetry\(targetOwner\)/);
});
staticAssertion("server binds DMS grouped retry to the family dispatch budget", () => {
  assert.match(server, /authorizeFamilyRetryDispatch\(\{[\s\S]{0,120}targetOwner: dmsGroupedRetryOwner/);
  assert.match(server, /familyRetryAllowed: dmsGroupedRetryDispatch\.allowed/);
});
staticAssertion("retry no longer triggers from image size plus six source rows", () => {
  assert.doesNotMatch(server, /smallDocumentImage\s*&&\s*sourceRows\s*>=\s*6/);
});
staticAssertion("retry acceptance no longer uses row-count-only greater-than baseline", () => {
  assert.doesNotMatch(server, /countCoordinateRows\(dmsGroupedRetryInfo\.output\)\s*>=\s*dmsGroupedBaselineRows/);
});
staticAssertion("retry rejection no longer logs raw provider preview", () => {
  const providerHelperStart = server.indexOf("async function callAliyunVision(");
  const providerHelperEnd = server.indexOf("function getHandwrittenDmsTranscriptionPrompt", providerHelperStart);
  const providerHelper = server.slice(providerHelperStart, providerHelperEnd > providerHelperStart ? providerHelperEnd : undefined);
  const routeStart = server.indexOf('app.post("/api/recognize-coordinates"');
  const routeEnd = server.indexOf("app.use((error, req, res, next) =>", routeStart);
  assert.ok(routeStart >= 0, "recognition route start marker must exist");
  assert.ok(routeEnd > routeStart, "recognition route end marker must follow its start");
  const recognitionRoute = server.slice(routeStart, routeEnd > routeStart ? routeEnd : undefined);
  assert.ok(providerHelperStart >= 0 && providerHelperEnd > providerHelperStart);
  assert.doesNotMatch(providerHelper, /error\.message|errorMessage|responseBody|error\.details|JSON\.stringify\(data\)\.slice|data\?\.error\?\.message|data\?\.message|requestId\s*:/);
  assert.match(providerHelper, /classification:\s*"NETWORK_ERROR"/);
  assert.match(providerHelper, /const classification = response\.status === 401/);
  assert.doesNotMatch(recognitionRoute, /[A-Za-z0-9_.]*(?:rawText|coordinates|RawText|layoutText)\.slice\(\s*0\s*,\s*(?:300|500|1000)\s*\)|(?:preview|ocrPreview):\s*[A-Za-z0-9_.]*(?:rawText|coordinates|RawText)/);
  assert.doesNotMatch(recognitionRoute, /console\.log\(rawText\)|console\.log\(coordinates\)/);
  assert.doesNotMatch(recognitionRoute, /console\.(?:log|error)\([^\n]*(?:Error|error)\.message|retryError:\s*[^\n]*\.message/);
  assert.doesNotMatch(recognitionRoute, /console\.error\(fallbackError\)|(?:message|details|formatted):\s*(?:error|fallbackError)(?:\.|\b)|error:\s*`[^`]*(?:errorMessage|fallbackError\.message)/);
  const coordinateLogs = recognitionRoute.match(/console\.(?:log|warn|error)\([\s\S]*?\);/g) || [];
  assert.equal(coordinateLogs.some(log => /fileName:\s*(?:req\.file|uploadedFileName)|console\.(?:log|warn|error)\([^,]+,\s*(?:req\.file[^)]*originalname|uploadedFileName)\s*\)/.test(log)), false);
});
staticAssertion("server reconstructs grouped geometry only from normalized coordinates", () => {
  assert.match(server, /reconstructDmsGroupsFromNormalizedCoordinates\(\{[\s\S]{0,180}normalizedCoordinates: canonicalDmsCoordinates/);
  assert.doesNotMatch(server, /coordinates\s*=\s*dmsGrouped(?:Retry)?Info\.output/);
});
staticAssertion("production group builder consumes only structurally verified DMS identities", () => {
  assert.match(server, /const verifiedDmsGroupNames = resolveDmsEngineGroupNames\(\{[\s\S]{0,180}groupSizes: blocks\.map\(block => block\.length\)/);
  assert.match(server, /group_name: verifiedDmsGroupNames\[groupIndex\] \|\| `矿地\$\{groupIndex \+ 1\}`/);
});
staticAssertion("handwritten Provider retry requires a successful request-owner claim first", () => {
  const start = server.indexOf("if (handwrittenVisionRouting.shouldRetry)");
  const end = server.indexOf("let cadastralGrid", start);
  const path = server.slice(start, end);
  const claim = path.indexOf("if (!claimRequestRetry(RETRY_OWNER_FAMILY.HANDWRITTEN_DMS))");
  const provider = path.indexOf("await readHandwrittenDmsWithPrompt");
  assert.ok(claim >= 0 && provider > claim);
  assert.match(path, /ownerError\.code = "RETRY_OWNER_BLOCKED"/);
  assert.doesNotMatch(path.slice(provider), /claimRequestRetry\(RETRY_OWNER_FAMILY\.HANDWRITTEN_DMS\)/);
});
staticAssertion("structured multi-site DMS computes typed priority before handwritten retry and claims only at execution", () => {
  const priority = server.indexOf("const structuredDmsRoutePriority");
  const ownership = server.indexOf("const dmsRetryOwnership", priority);
  const handwritten = server.indexOf("if (handwrittenVisionRouting.shouldRetry)");
  const downstream = server.indexOf("const dmsGroupedRetryEligibility");
  assert.ok(priority >= 0 && ownership > priority && handwritten > ownership && downstream > handwritten);
  assert.doesNotMatch(server.slice(priority, handwritten), /claimRequestRetry\(dmsGroupedRetryOwner\)/);
  assert.match(server.slice(priority, handwritten), /reason:\s*"structured_dms_priority"/);
  const execution = server.indexOf("if (!dmsGroupedRetryBoundaryFailure && dmsGroupedRetryEligibility.allowed)");
  const provider = server.indexOf("await callAliyunVision", execution);
  assert.ok(execution > downstream && provider > execution);
  assert.match(server.slice(execution, provider), /claimDownstreamFamilyRetry\(dmsGroupedRetryOwner\)/);
});
staticAssertion("server trusts only independent handwritten upload evidence while prioritizing proven printed structure", () => {
  const priority = server.indexOf("const structuredDmsRoutePriority");
  const handwritten = server.indexOf("if (handwrittenVisionRouting.shouldRetry)", priority);
  const path = server.slice(priority, handwritten);
  assert.match(server.slice(server.indexOf("const initialDmsDocumentEvidence"), priority), /resolveDmsRetryTrustBoundary\(\{[\s\S]*trustedHandwrittenSignal:\s*handwrittenDmsUploadContext/);
  assert.match(path, /explicitHandwrittenSignal:\s*dmsRetryTrustBoundary\.explicitHandwrittenSignal/g);
  assert.match(path, /nonHandwrittenDmsCandidateSignal:\s*dmsRetryTrustBoundary\.nonHandwrittenDmsCandidateSignal/);
  assert.match(path, /partialMultisiteRecoveryCandidateSignal:\s*dmsRetryTrustBoundary\.partialMultisiteRecoveryCandidateSignal/);
  assert.match(path, /riskSignal:\s*dmsRetryTrustBoundary\.stage1FullMultisiteRiskSignal/);
  assert.doesNotMatch(path, /initialDmsDocumentEvidence\.explicitHandwrittenSignal\s*\|\|/);
  assert.match(path, /explicitHandwrittenSignal:\s*handwrittenDmsUploadContext/);
  assert.match(path, /if \(dmsRetryOwnership\.retryOwner === dmsGroupedRetryOwner\)/);
});
staticAssertion("projected evidence is wired through both route layers and ownership classification authorizes typed dms_grouped retry", () => {
  const priority = server.indexOf("const structuredDmsRoutePriority");
  const handwritten = server.indexOf("if (handwrittenVisionRouting.shouldRetry)", priority);
  const routePath = server.slice(priority, handwritten);
  assert.match(routePath, /projectedTableSignal:\s*initialDmsDocumentEvidence\.projectedTableSignal/g);
  const eligibility = server.indexOf("const dmsGroupedRetryEligibility", handwritten);
  const eligibilityEnd = server.indexOf("if (!dmsGroupedRetryBoundaryFailure && dmsGroupedRetryEligibility.failClosed)", eligibility);
  const eligibilityPath = server.slice(eligibility, eligibilityEnd);
  assert.match(eligibilityPath, /typedRouteEligible:\s*dmsRetryOwnership\.classification[\s\S]{0,100}DMS_GROUPED_ONLY/);
  assert.doesNotMatch(eligibilityPath, /typedRouteEligible:\s*structuredDmsRoutePriority\.typedDmsGrouped/);
});
staticAssertion("server freezes sanitized retry ownership before any Stage-2 Provider branch", () => {
  const priority = server.indexOf("const structuredDmsRoutePriority");
  const classification = server.indexOf("const dmsRetryOwnership = classifyDmsRetryOwnership", priority);
  const trace = server.indexOf("DMS_RETRY_ROUTE:${dmsRetryOwnership.classification}", classification);
  const reserve = server.indexOf("requestRetryOrchestrator.reserve(dmsRetryOwnership.retryOwner)", classification);
  const failClosed = server.indexOf("requestRetryOrchestrator.failClose(dmsRetryOwnership.reason)", classification);
  const handwritten = server.indexOf("if (handwrittenVisionRouting.shouldRetry)", classification);
  assert.ok(classification > priority && trace > classification);
  assert.ok(reserve > classification && reserve < handwritten);
  assert.ok(failClosed > classification && failClosed < handwritten);
  assert.doesNotMatch(server.slice(classification, handwritten), /rawText|coordinates|response|imageItems/);
});
staticAssertion("partial multi-site recovery is wired to one grouped retry and re-enters the full safety gate", () => {
  assert.match(server, /partialMultisiteRecoveryEligible:\s*dmsRetryOwnership\.classification[\s\S]{0,100}DMS_GROUPED_PARTIAL_RECOVERY_ONLY/);
  assert.match(server, /allowPartialMultisiteRecovery:\s*dmsRetryOwnership\.classification[\s\S]{0,100}DMS_GROUPED_PARTIAL_RECOVERY_ONLY/);
  const start = server.indexOf('"STAGE1_PARTIAL_MULTISITE_8_TO_16",', server.indexOf("dmsGroupedExpansionCoverage.accepted"));
  const end = server.indexOf('} else if (dmsGroupedExpansionCoverage.accepted === true)', start);
  const partialRecovery = server.slice(start, end);
  assert.match(partialRecovery, /buildDmsGroupedPartialMultisiteRecoveryCandidate\(\{/);
  assert.match(partialRecovery, /stage1RawText:\s*rawText/);
  assert.match(partialRecovery, /stage1Coordinates:\s*dmsGroupedStage1Coordinates/);
  assert.match(partialRecovery, /stage1FullMultisiteSafety = evaluateStage1FullMultisiteSafety\(\{[\s\S]*riskSignal:\s*true/);
  assert.match(server, /DMS_GROUPED:partial_multisite_recovery_review_required/);
});
staticAssertion("weak partial multi-site recovery has a separate owner and clears Stage-1 authority before reread", () => {
  assert.match(server, /evaluateDmsWeakPartialMultisiteRecovery\(\{/);
  assert.match(server, /candidateSignal:\s*dmsRetryTrustBoundary\.weakPartialMultisiteRecoveryCandidateSignal/);
  assert.match(server, /DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY/);
  const clearStart = server.indexOf("DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY");
  const retryStart = server.indexOf("const dmsGroupedRetryEligibility", clearStart);
  const path = server.slice(clearStart, retryStart);
  assert.match(path, /coordinates\s*=\s*""/);
  assert.match(path, /dmsGroupedAccepted\s*=\s*false/);
  assert.match(path, /dmsAccepted\s*=\s*false/);
  assert.match(path, /getChatCoordinatesInfo\(""\)/);
  assert.match(path, /getWgs84TableCoordinatesInfo\(""\)/);
  assert.match(server, /const dmsGroupedRetryEligibility = evaluateDmsGroupedRetryEligibility\(\{[\s\S]{0,800}imageInputBuffer:\s*req\.file\?\.buffer/);
  assert.match(server, /allowWeakPartialMultisiteRecovery:\s*dmsRetryOwnership\.classification[\s\S]{0,140}DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY/);
  assert.match(server, /const dmsGroupedExpansionCoverage = evaluateDmsGroupedAcquisitionExpansion\(\{[\s\S]{0,1100}imageInputBuffer:\s*req\.file\?\.buffer/);
  assert.match(server, /const partialRecoveryCandidate = buildDmsGroupedPartialMultisiteRecoveryCandidate\(\{[\s\S]{0,700}imageInputBuffer:\s*req\.file\?\.buffer/);
  assert.match(server, /DMS_GROUPED:weak_partial_multisite_recovery_review_required/);
});
staticAssertion("partial recovery response preserves separate non-authoritative candidates and cannot claim Direct-16 identity", () => {
  assert.match(server, /partialMultisiteRecoveryCandidate:[\s\S]{0,500}candidateRole:\s*"NONAUTHORITATIVE_REVIEW_CANDIDATE"/);
  assert.match(server, /sourceCandidates:\s*dmsGroupedPartialMultisiteRecovery[\s\S]{0,1200}stage1:[\s\S]{0,700}structuredReread:/);
  assert.match(server, /candidateRole:\s*dmsGroupedPartialMultisiteRecovery[\s\S]{0,180}"NONAUTHORITATIVE_REVIEW_CANDIDATE"[\s\S]{0,180}"CURRENT_RECOGNITION_CANDIDATE"/);
  assert.match(server, /evidenceSource:\s*dmsGroupedPartialMultisiteRecovery\s*\?\s*"dms_grouped"\s*:\s*"stage1"/);
  assert.match(server, /directCanonicalPromotion:\s*dmsGroupedPartialMultisiteRecovery\s*\?\s*false\s*:\s*null/);
  assert.match(server, /partialMultisiteRecoveryInputEvidence:\s*dmsGroupedPartialMultisiteRecovery[\s\S]{0,160}weakPartialInputEvidence/);
  assert.match(server, /handwrittenVisionRouting:\s*buildPartialMultisiteSafeVisionRouting\(\{[\s\S]{0,260}partialMultisiteRecoveryCandidate:\s*dmsGroupedPartialMultisiteRecovery/);
});
staticAssertion("handwritten Stage-2 routing requires explicit handwritten evidence and shape", () => {
  const start = server.indexOf("function getHandwrittenDmsVisionRoutingEvidence");
  const end = server.indexOf("function shouldRetryHandwrittenDmsOnTimeout", start);
  const path = server.slice(start, end);
  assert.match(path, /explicitHandwrittenContext = options\.explicitHandwrittenSignal === true/);
  assert.doesNotMatch(path, /explicitHandwrittenContext = documentEvidence\.explicitHandwrittenSignal/);
  assert.match(path, /evidence\.shapeRetryCandidate = evidence\.score >= 5/);
  assert.match(path, /evidence\.shouldRetry = explicitHandwrittenContext && evidence\.shapeRetryCandidate/);
});
staticAssertion("handwritten timeout retry requires independent upload evidence", () => {
  const start = server.indexOf("function getHandwrittenDmsTimeoutRoutingEvidence");
  const end = server.indexOf("function getHandwrittenDmsVisionRoutingEvidence", start);
  const path = server.slice(start, end);
  assert.match(path, /explicitHandwrittenContext = highConfidenceNameOrHint/);
  assert.doesNotMatch(path, /explicitHandwrittenContext = highConfidenceNameOrHint\s*\|\|/);
  assert.match(path, /\|\| !explicitHandwrittenContext\) \{/);
  assert.match(path, /evidence\.shouldRetry = explicitHandwrittenContext && evidence\.score >= 5/);
});
staticAssertion("Provider-returned handwriting wording remains observation-only at the retry trust boundary", () => {
  assert.match(server, /resolveDmsRetryTrustBoundary\(\{[\s\S]{0,180}documentEvidence:\s*initialDmsDocumentEvidence,[\s\S]{0,120}trustedHandwrittenSignal:\s*handwrittenDmsUploadContext/);
  assert.doesNotMatch(server, /trustedHandwrittenSignal:\s*initialDmsDocumentEvidence\.explicitHandwrittenSignal/);
  assert.match(server, /printedTableSignal:\s*dmsRetryTrustBoundary\.printedDmsCandidateSignal/);
});
staticAssertion("final handwritten identity requires independent trusted context", () => {
  const start = server.indexOf("function getHandwrittenDmsInfo");
  const end = server.indexOf("function getHandwrittenDmsTimeoutRoutingEvidence", start);
  const path = server.slice(start, end);
  assert.match(path, /explicitHandwrittenEvidence = hasExplicitHandwrittenDmsContext/);
  assert.doesNotMatch(path, /explicitHandwrittenEvidence = hasExplicitHandwrittenDmsContext\s*\|\|/);
  assert.match(path, /const isHandwrittenDms = isOcrImage\s*&& explicitHandwrittenEvidence/);
});
staticAssertion("all timeout and fallback provider retries claim the request owner", () => {
  assert.match(server, /handwrittenTimeoutRoutingEvidence\?\.shouldRetry[\s\S]{0,120}claimRequestRetry\(RETRY_OWNER_FAMILY\.HANDWRITTEN_DMS\)/);
  assert.match(server, /hasKyrgyzTimeoutEvidence[\s\S]{0,120}claimRequestRetry\("kyrgyz_gk"\)/);
  assert.match(server, /shouldRetryWgs84TableOnTimeout[\s\S]{0,160}claimRequestRetry\("wgs84_table"\)/);
  assert.match(server, /bftmIncompleteWarning[\s\S]{0,160}claimRequestRetry\("bftm"\)/);
});
staticAssertion("failed multi-region retry blocks cross-group geometry consumption", () => {
  assert.match(server, /dmsGroupedRetryBoundaryFailure[\s\S]*coordinates\s*=\s*""[\s\S]*DMS_GROUPED:boundary_unresolved_review_required/);
});
staticAssertion("Stage-1 full multi-site safety is applied before response and re-registers a pending final authority", () => {
  const safety = server.indexOf("let stage1FullMultisiteSafety = evaluateStage1FullMultisiteSafety");
  const promotion = server.indexOf("const stage1FullMultisitePromotion = evaluateStage1FullMultisitePromotion", safety);
  const engine = server.indexOf("let coordinateEngineV2 = buildCoordinateEngineV2ShadowResult", safety);
  const response = server.indexOf("let verificationResponse = buildCoordinateVerificationResponse", engine);
  const pending = server.indexOf("const pendingStage1FullMultisiteResult = coordinateConfirmationRuntime.register(finalizeCoordinateResult", response);
  const send = server.indexOf("res.json(dmsGroupedReviewCandidate", pending);
  assert.ok(safety >= 0 && promotion > safety && engine > promotion && response > engine && pending > response && send > pending);
  assert.match(server.slice(promotion, engine), /imageDmsSourceCompleteness:\s*imageDmsAcquisitionCompleteness/);
  assert.match(server.slice(promotion, engine), /retryBoundaryFailed:\s*Boolean\(dmsGroupedRetryBoundaryFailure\)/);
  assert.match(server.slice(promotion, engine), /failClosed:\s*stage1FullMultisitePromotion\.failClosed\s*===\s*true/);
  assert.match(server.slice(promotion, engine), /requiresConfirmation:\s*stage1FullMultisiteReviewCandidate/);
  assert.match(server.slice(engine, response), /forceRequiresReview:\s*Boolean\([\s\S]*stage1FullMultisiteSafety\.gateRequired[\s\S]*imageDmsFailClosedPatch\?\.forceRequiresReview/);
  assert.match(server.slice(pending, send), /confirmationStatus:\s*"pending"/);
  assert.match(server.slice(pending, send), /requiresReview:\s*true/);
  assert.match(server.slice(pending, send), /kmlReady:\s*false/);
  assert.match(server.slice(response, pending), /buildStage1FullMultisiteConfirmationPolicy\(\{[\s\S]*verification:\s*verificationResponse\.verification[\s\S]*stage1Safety:\s*stage1FullMultisiteSafety[\s\S]*confirmationSource:\s*STAGE1_FULL_MULTISITE_CONFIRMATION_SOURCE/);
  assert.match(server.slice(pending, send), /familySafetyPolicy:\s*stage1FullMultisiteConfirmationPolicy/);
  const mapRoute = server.slice(server.indexOf('app.post("/api/map-preview"'), server.indexOf('app.post("/api/spatial-shares"'));
  const confirmationGuard = mapRoute.indexOf("isStage1FullMultisiteConfirmationPending(current.result)");
  const adapter = mapRoute.indexOf("mapPreviewAdapter.adapt(current.result");
  assert.ok(confirmationGuard >= 0 && adapter > confirmationGuard);
  assert.match(mapRoute.slice(confirmationGuard, adapter), /STAGE1_FULL_MULTISITE_CONFIRMATION_REQUIRED/);
});
staticAssertion("13-point Stage-1 candidate is snapshotted before downstream routing can mutate working variables", () => {
  const acquisitionBranch = server.indexOf("} else if (dmsGroupedExpansionCoverage.accepted === true) {");
  const snapshot = server.indexOf("const stage1Candidate = Object.freeze({ rawText, coordinates })", acquisitionBranch);
  const downstream = server.indexOf('claimDownstreamFamilyRetry("french_perimeter_dms")');
  const response = server.indexOf("const finalRecognitionCandidate", snapshot);
  assert.ok(snapshot >= 0 && downstream > snapshot && response > downstream);
  assert.match(server.slice(snapshot, response), /stage1CandidateSha256[\s\S]*JSON\.stringify\(stage1Candidate\)/);
  assert.match(server.slice(response, response + 900), /stage1Candidate:\s*Object\.freeze\([\s\S]*dmsGroupedReviewCandidate\.stage1Candidate\.rawText/);
});
staticAssertion("retry prompt preserves SITE grouping instructions", () => {
  assert.match(server, /SITES1 \/ SITE 1 \/ SITES2 \/ SITE 2/);
  assert.match(server, /Keep every visible SITE, SITES, AREA, or Mining Area table as a separate group/);
  assert.match(server, /必须逐字保留每个区域标题及编号、每次重复表头、每个可见行标签和组间空行/);
});
staticAssertion("coordinate page uses copy action", () => {
  assert.match(html, /id="coordinateCopyAction"[^>]+onclick="copyContent\(\)"[^>]*>复制坐标<\/button>/);
});
staticAssertion("coordinate page no longer exposes share action", () => {
  assert.doesNotMatch(html, /id="shareResultAction"|id="coordinateCopyAction"[^>]+onclick="openSpatialShareDialog\(\)"/);
});
staticAssertion("sharing remains on reviewed spatial result", () => {
  assert.match(html, /id="spatialShareCardAction"[^>]+onclick="openSpatialShareDialog\(\)"/);
});

let dynamicPassed = 0;
for (const entry of dynamicCases) {
  await entry.fn();
  dynamicPassed += 1;
}

let staticPassed = 0;
for (const entry of staticAssertions) {
  await entry.fn();
  staticPassed += 1;
}

const totalPassed = dynamicPassed + staticPassed;
const totalCount = dynamicCases.length + staticAssertions.length;

console.log("P0 coordinate source/grouping regression:");
console.log(`ACTUAL_DYNAMIC_PASS_COUNT=${dynamicPassed}`);
console.log(`ACTUAL_DYNAMIC_TOTAL_COUNT=${dynamicCases.length}`);
console.log(`ACTUAL_STATIC_PASS_COUNT=${staticPassed}`);
console.log(`ACTUAL_STATIC_TOTAL_COUNT=${staticAssertions.length}`);
console.log(`ACTUAL_COMBINED_PASS_COUNT=${totalPassed}`);
console.log(`ACTUAL_COMBINED_TOTAL_COUNT=${totalCount}`);
