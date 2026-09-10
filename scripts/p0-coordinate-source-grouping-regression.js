import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildDmsGroupedRetryFailClosedPatch,
  createDmsGroupedRetryOrchestrator,
  evaluateDmsGroupedAcquisitionExpansion,
  evaluateDmsGroupedRoutePriority,
  evaluateDmsGroupedRetryCoverage,
  evaluateDmsGroupedRetryEligibility,
  extractDmsSourceStructure,
  hasExplicitDmsMultiRegionEvidence,
  parseDmsSourceCoordinateRow,
  reconstructDmsGroupsFromNormalizedCoordinates,
  resolveDmsEngineGroupNames
} from "../server/recognition/dms-source-structure.js";
import { buildSourceCoordinateRepresentation } from "../server/source-coordinate-representation.js";
import { getDmsDocumentEvidence } from "../server/recognition/family-primary-routing.js";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";
import { CoordinateConfirmationRuntime } from "../server/coordinate-finalizer/index.js";
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

function decimalRowsFromDms(rows) {
  return rows.map(row => {
    const parsed = parseDmsSourceCoordinateRow(row);
    assert.ok(parsed, `DMS row must parse: ${row}`);
    return `${parsed.latitude.toFixed(8)},${parsed.longitude.toFixed(8)}`;
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
const stage1OmittedIndexes = new Set([3, 7, 15]);
const stage1ThirteenText = structuredText.split("\n")
  .filter(line => !dmsRows.some((row, index) => stage1OmittedIndexes.has(index) && line === row))
  .join("\n");
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
  const route = evaluateDmsGroupedRoutePriority({
    isImageInput: true,
    printedTableSignal: evidence.printedTableSignal,
    explicitHandwrittenSignal: evidence.explicitHandwrittenSignal,
    structureText: damagedPrintedTable
  });
  assert.equal(route.typedDmsGrouped, true);
  assert.equal(route.suppressHandwrittenRetry, true);
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
  assert.match(server, /if \(dmsGroupedRetryEligibility\.allowed\)/);
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
  const handwritten = server.indexOf("if (handwrittenVisionRouting.shouldRetry)");
  const downstream = server.indexOf("const dmsGroupedRetryEligibility");
  assert.ok(priority >= 0 && handwritten > priority && downstream > handwritten);
  assert.doesNotMatch(server.slice(priority, handwritten), /claimRequestRetry\(dmsGroupedRetryOwner\)/);
  assert.match(server.slice(priority, handwritten), /reason:\s*"structured_dms_priority"/);
  const execution = server.indexOf("if (dmsGroupedRetryEligibility.allowed)");
  const provider = server.indexOf("await callAliyunVision", execution);
  assert.ok(execution > downstream && provider > execution);
  assert.match(server.slice(execution, provider), /claimDownstreamFamilyRetry\(dmsGroupedRetryOwner\)/);
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
staticAssertion("13-point Stage-1 candidate is snapshotted before downstream routing can mutate working variables", () => {
  const snapshot = server.indexOf("const stage1Candidate = Object.freeze({ rawText, coordinates })");
  const downstream = server.indexOf('claimDownstreamFamilyRetry("french_perimeter_dms")');
  const response = server.indexOf("const finalRecognitionCandidate", snapshot);
  assert.ok(snapshot >= 0 && downstream > snapshot && response > downstream);
  assert.match(server.slice(snapshot, response), /stage1CandidateSha256[\s\S]*JSON\.stringify\(stage1Candidate\)/);
  assert.match(server.slice(response, response + 900), /stage1Candidate:\s*Object\.freeze\([\s\S]*dmsGroupedAcquisitionExpansion\.stage1Candidate\.rawText/);
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
