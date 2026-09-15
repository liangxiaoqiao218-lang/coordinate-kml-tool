import { createHash, randomBytes } from "node:crypto";
import { authorizeFamilyRetryDispatch } from "./family-retry-policy.js";
import { getDmsDocumentEvidence } from "./family-primary-routing.js";

const DMS_COMPONENT_PATTERN = /[-+]?\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?/gi;

export const DMS_RETRY_ROUTE_CLASSIFICATION = Object.freeze({
  DMS_GROUPED_ONLY: "DMS_GROUPED_ONLY",
  DMS_GROUPED_PARTIAL_RECOVERY_ONLY: "DMS_GROUPED_PARTIAL_RECOVERY_ONLY",
  DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY",
  HANDWRITTEN_DMS_ONLY: "HANDWRITTEN_DMS_ONLY",
  FAIL_CLOSED: "FAIL_CLOSED_UNPROVEN_DMS_STRUCTURE",
  NONE: "NO_DMS_RETRY_OWNER"
});

export const IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE = Object.freeze({
  NOT_APPLICABLE: "IMAGE_DMS_ACQUISITION_NOT_APPLICABLE",
  TYPED_FAMILY_PROVEN: "IMAGE_DMS_TYPED_FAMILY_PROVEN",
  COMPLETE_GENERIC_TABLE: "IMAGE_DMS_GENERIC_TABLE_COMPLETE",
  FAIL_CLOSED: "IMAGE_DMS_SOURCE_STRUCTURE_UNPROVEN"
});

export const IMAGE_DMS_SELECTED_ROUTE = Object.freeze({
  NONE: "NO_DMS_ROUTE_SELECTED",
  GENERIC_DMS: "GENERIC_DMS",
  DMS_GROUPED: "DMS_GROUPED",
  FRENCH_PERIMETER_DMS: "FRENCH_PERIMETER_DMS",
  POINT_AZ_DMS_TABLE: "POINT_AZ_DMS_TABLE",
  HANDWRITTEN_DMS: "HANDWRITTEN_DMS"
});

export function buildPartialMultisiteSafeVisionRouting({
  handwrittenVisionRouting = {},
  generalVisionRawText = "",
  handwrittenVisionRawText = "",
  finalRawText = "",
  partialMultisiteRecoveryCandidate = null
} = {}) {
  if (!partialMultisiteRecoveryCandidate) {
    return {
      ...handwrittenVisionRouting,
      generalVisionRawText,
      handwrittenVisionRawText,
      finalRawText
    };
  }

  return {
    ...handwrittenVisionRouting,
    generalVisionRawText: String(partialMultisiteRecoveryCandidate?.stage1Candidate?.rawText || ""),
    handwrittenVisionRawText: "",
    finalRawText: String(partialMultisiteRecoveryCandidate?.rawText || "")
  };
}

const IMAGE_DMS_TYPED_ROUTE_ALLOWLIST = new Set([
  IMAGE_DMS_SELECTED_ROUTE.DMS_GROUPED,
  IMAGE_DMS_SELECTED_ROUTE.FRENCH_PERIMETER_DMS,
  IMAGE_DMS_SELECTED_ROUTE.POINT_AZ_DMS_TABLE,
  IMAGE_DMS_SELECTED_ROUTE.HANDWRITTEN_DMS
]);

function sourceLines(text) {
  return String(text || "").replace(/\r\n/g, "\n").split("\n");
}

function boundaryName(line) {
  const value = String(line || "").trim();
  const match = value.match(/^\s*(sites?|mining\s+areas?|areas?)\s*(one|two|three|four|\d+)?\s*[:#\-]?\s*$/i);
  return match ? value : "";
}

export function normalizeDmsBoundaryIdentity(value) {
  const match = String(value || "").trim().match(/^\s*(sites?|mining\s+areas?|areas?)\s*(one|two|three|four|\d+)?/i);
  if (!match) return "";
  const kind = /^sites?$/i.test(match[1]) ? "SITE" : /^mining/i.test(match[1]) ? "MINING_AREA" : "AREA";
  const numberWords = { one: "1", two: "2", three: "3", four: "4" };
  const ordinal = numberWords[String(match[2] || "").toLowerCase()] || String(match[2] || "");
  return `${kind}:${ordinal || "UNNUMBERED"}`;
}

function isTableHeader(line) {
  const value = String(line || "").toLowerCase();
  return /\bpoint\b/.test(value) && /\blatitude\b/.test(value) && /\blongitude\b/.test(value);
}

function explicitGenericDmsTableHeaderAxisOrder(line) {
  const value = String(line || "").trim();
  const identityPattern = /^(?:point|vertex|no\.?|number|id)$/i;
  const latitudePattern = /^(?:latitude|lat|parall[eè]le)$/i;
  const longitudePattern = /^(?:longitude|lon|m[eé]ridien)$/i;
  let cells = [];
  if (/[|;,\t]/.test(value)) {
    cells = value.split(/\s*(?:\||;|,|\t)\s*/).map(cell => cell.trim()).filter(Boolean);
  } else {
    const match = value.match(/^\s*(point|vertex|no\.?|number|id)\s+(latitude|lat|parall[eè]le|longitude|lon|m[eé]ridien)\s+(latitude|lat|parall[eè]le|longitude|lon|m[eé]ridien)\s*$/i);
    if (match) cells = match.slice(1);
  }
  if (cells.length !== 3 || !identityPattern.test(cells[0])) return "";
  if (latitudePattern.test(cells[1]) && longitudePattern.test(cells[2])) return "latitude_longitude";
  if (longitudePattern.test(cells[1]) && latitudePattern.test(cells[2])) return "longitude_latitude";
  return "";
}

function leadingRowNumber(line) {
  const match = String(line || "").match(/^\s*(\d{1,3})\s*(?:[.|):\-]|\s)/);
  return match ? Number(match[1]) : null;
}

function hasContinuousNumberedRows(rows = []) {
  if (!Array.isArray(rows) || rows.length < 3) return false;
  const labels = rows.map(leadingRowNumber);
  return labels.every((label, index) => Number.isInteger(label) && label === index + 1);
}

function hasSequentialLabelsFromOne(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return false;
  const labels = rows.map(leadingRowNumber);
  return labels.every((label, index) => Number.isInteger(label) && label === index + 1);
}

function normalizeHemisphere(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (["N", "NORD", "NORTH"].includes(normalized)) return "N";
  if (["S", "SUD", "SOUTH"].includes(normalized)) return "S";
  if (["E", "EST", "EAST"].includes(normalized)) return "E";
  if (["W", "O", "OUEST", "WEST"].includes(normalized)) return "W";
  return "";
}

function normalizeSeconds(value) {
  return String(value || "").replace(",", ".");
}

function parseDmsComponent(token) {
  const value = String(token || "").trim();
  const match = value.match(/^\s*([-+]?\d{1,3})\s*[°º]\s*(\d{1,2})\s*['′’]\s*(\d{1,2}(?:[.,]\d+)?)\s*["″”]?\s*(N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?\s*$/i);
  if (!match) return null;

  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const secondsText = normalizeSeconds(match[3]);
  const seconds = Number(secondsText);
  const hemisphere = normalizeHemisphere(match[4]);

  if (!Number.isFinite(degrees) || !Number.isFinite(minutes) || !Number.isFinite(seconds)) return null;
  if (minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60) return null;
  if (!hemisphere) return null;

  const decimals = Math.max(0, (secondsText.split(".")[1] || "").length);
  const absolute = Math.abs(degrees) + (minutes / 60) + (seconds / 3600);
  const sign = ["S", "W"].includes(hemisphere) ? -1 : 1;
  const axis = ["N", "S"].includes(hemisphere) ? "latitude" : "longitude";

  return Object.freeze({
    token: value,
    axis,
    hemisphere,
    value: sign * absolute,
    secondsDecimals: decimals
  });
}

function pointLabelFromLine(line) {
  const value = String(line || "").trim();
  const numbered = value.match(/^\s*(\d{1,3})\s*(?:[.|):\-]|\s)/);
  if (numbered) return numbered[1];
  const labelled = value.match(/^\s*([A-Z]{1,4}\d{0,3})\s+(?=\d{1,3}\s*[°º])/i);
  return labelled ? labelled[1].toUpperCase() : "";
}

function dmsTolerance(components) {
  const decimals = Math.max(0, ...components.map(component => component.secondsDecimals || 0));
  return Math.max(1e-8, (0.5 * (10 ** -decimals)) / 3600);
}

function componentTolerance(component) {
  const decimals = Math.max(0, Number(component?.secondsDecimals) || 0);
  return Math.max(1e-8, (0.5 * (10 ** -decimals)) / 3600);
}

export function parseDmsSourceCoordinateRow(line) {
  const value = String(line || "").trim();
  const tokens = [...value.matchAll(DMS_COMPONENT_PATTERN)]
    .map(match => match[0].trim())
    .filter(Boolean);

  if (tokens.length !== 2) return null;

  const parsed = tokens.map(parseDmsComponent);
  if (parsed.some(component => !component)) return null;

  const latitude = parsed.find(component => component.axis === "latitude");
  const longitude = parsed.find(component => component.axis === "longitude");
  if (!latitude || !longitude) return null;
  if (Math.abs(latitude.value) > 90 || Math.abs(longitude.value) > 180) return null;

  return Object.freeze({
    sourceLine: value,
    label: pointLabelFromLine(value),
    latitude: latitude.value,
    longitude: longitude.value,
    latitudeHemisphere: latitude.hemisphere,
    longitudeHemisphere: longitude.hemisphere,
    axisOrder: parsed[0].axis === "latitude" ? "latitude_longitude" : "longitude_latitude",
    tolerance: dmsTolerance(parsed),
    latitudeTolerance: componentTolerance(latitude),
    longitudeTolerance: componentTolerance(longitude),
    tokens: Object.freeze(tokens)
  });
}

export function isDmsCoordinateSourceRow(line) {
  return Boolean(parseDmsSourceCoordinateRow(line));
}

export function hasDmsGroupBoundaryContext(text) {
  const value = String(text || "");
  const titleCount = sourceLines(value).filter(line => boundaryName(line)).length;
  const headerCount = sourceLines(value).filter(line => isTableHeader(line)).length;
  return titleCount >= 2 || headerCount >= 2;
}

export function extractDmsSourceStructure(text) {
  const strongDocumentBoundaryContext = hasDmsGroupBoundaryContext(text);
  const groups = [];
  let currentRows = [];
  let currentDisplayRows = [];
  let currentName = "";
  let currentBoundaryProvenance = "document_start";
  let previousRowNumber = null;
  let pendingBlankBoundary = false;
  let reason = "";

  const closeGroup = closeReason => {
    if (currentRows.length) {
      groups.push(Object.freeze({
        name: currentName || null,
        rows: Object.freeze([...currentRows]),
        displayRows: Object.freeze([...currentDisplayRows]),
        boundaryProvenance: currentBoundaryProvenance
      }));
      reason ||= closeReason;
    }
    currentRows = [];
    currentDisplayRows = [];
    currentName = "";
    previousRowNumber = null;
    pendingBlankBoundary = false;
  };

  for (const sourceLine of sourceLines(text)) {
    const line = sourceLine.trim();
    if (!line) {
      pendingBlankBoundary = currentRows.length > 0;
      continue;
    }

    const name = boundaryName(line);
    if (name) {
      closeGroup("section_title");
      currentName = name;
      currentBoundaryProvenance = "section_title";
      continue;
    }

    if (isTableHeader(line)) {
      if (currentRows.length) {
        closeGroup("repeated_table_header");
        currentBoundaryProvenance = "repeated_table_header";
      }
      continue;
    }

    if (!isDmsCoordinateSourceRow(line)) continue;

    const rowNumber = leadingRowNumber(line);
    // A blank line is layout whitespace, not site identity. In particular it
    // must not split a titled/repeated-header table into invented sites.
    if (currentRows.length && pendingBlankBoundary && !strongDocumentBoundaryContext) {
      closeGroup("blank_line");
      currentBoundaryProvenance = "blank_line";
    } else if (currentRows.length && !currentName && rowNumber === 1
      && Number.isInteger(previousRowNumber) && previousRowNumber >= 3
      && hasContinuousNumberedRows(currentRows)) {
      closeGroup("number_restart");
      currentBoundaryProvenance = "number_restart";
    }

    if (currentRows.length && pendingBlankBoundary) currentDisplayRows.push("");
    currentRows.push(line);
    currentDisplayRows.push(line);
    previousRowNumber = Number.isInteger(rowNumber) ? rowNumber : previousRowNumber;
    pendingBlankBoundary = false;
  }
  closeGroup(reason || "single_group");

  const rows = groups.flatMap(group => group.rows);
  const laterGroups = groups.slice(1);
  const laterBoundaries = laterGroups.map(group => group.boundaryProvenance);
  const hasProvenBlankLineBoundary = groups.length > 1
    && laterBoundaries.includes("blank_line")
    && groups.every(group => group.rows.length >= 3 && hasContinuousNumberedRows(group.rows))
    && laterGroups.every(group => group.boundaryProvenance !== "blank_line"
      || parseDmsSourceCoordinateRow(group.rows[0])?.label === "1");
  const allBoundariesProven = groups.length > 1 && laterBoundaries.every(value => [
    "section_title",
    "repeated_table_header",
    "number_restart"
  ].includes(value) || (value === "blank_line" && hasProvenBlankLineBoundary));
  const hasProvenNumberRestart = groups.some((group, index) => index > 0
    && group.boundaryProvenance === "number_restart"
    && hasContinuousNumberedRows(group.rows));
  const documentHasStrongMultiRegionEvidence = hasDmsGroupBoundaryContext(text)
    || hasProvenNumberRestart
    || hasProvenBlankLineBoundary;
  const displayText = groups.map(group => {
    const lines = [...(group.name ? [group.name] : []), ...group.displayRows];
    while (lines[0] === "") lines.shift();
    while (lines.at(-1) === "") lines.pop();
    return lines.join("\n");
  }).join("\n\n");
  return Object.freeze({
    groups: Object.freeze(groups),
    rows: Object.freeze(rows),
    rowCount: rows.length,
    groupCount: groups.length,
    reason: groups.length > 1 ? reason : "single_group",
    boundaryProvenance: Object.freeze(groups.map(group => group.boundaryProvenance)),
    documentHasStrongMultiRegionEvidence,
    allBoundariesProven,
    hasProvenBlankLineBoundary,
    hasUnprovenBoundary: groups.length > 1 && !allBoundariesProven,
    displayText
  });
}

function closeEnough(actual, expected, tolerance) {
  return Number.isFinite(actual)
    && Number.isFinite(expected)
    && Math.abs(actual - expected) <= Math.max(Number(tolerance) || 0, 1e-8);
}

function normalizeLabel(value) {
  return String(value || "").trim().toUpperCase();
}

export function hasExplicitDmsMultiRegionEvidence(text, dmsGroupedInfo = {}) {
  const sourceStructure = extractDmsSourceStructure(text);
  const laterBoundariesProven = sourceStructure.allBoundariesProven;
  const namedIdentities = sourceStructure.groups
    .map(group => normalizeDmsBoundaryIdentity(group.name))
    .filter(Boolean);
  const strongNumberRestart = laterBoundariesProven
    && sourceStructure.groupCount > 1
    && sourceStructure.groups.slice(1).every(group => group.boundaryProvenance === "number_restart")
    && sourceStructure.groups.every(group => group.rows.length >= 3
      && parseDmsSourceCoordinateRow(group.rows[0])?.label === "1");
  const strongBlankLineBoundary = laterBoundariesProven
    && sourceStructure.hasProvenBlankLineBoundary;
  return Boolean(
    (laterBoundariesProven && hasDmsGroupBoundaryContext(text))
    || (laterBoundariesProven && namedIdentities.length >= 2)
    || strongNumberRestart
    || strongBlankLineBoundary
  );
}

export function evaluateDmsGroupedRoutePriority({
  isImageInput = false,
  printedTableSignal = false,
  projectedTableSignal = false,
  explicitHandwrittenSignal = false,
  structureText = ""
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const printedEvidenceEstablished = Boolean(printedTableSignal
    || (structure.allBoundariesProven && structure.documentHasStrongMultiRegionEvidence));
  const typedDmsGrouped = Boolean(isImageInput
    && !projectedTableSignal
    && printedEvidenceEstablished
    && !explicitHandwrittenSignal
    && structure.rowCount > 0
    && structure.allBoundariesProven
    && structure.documentHasStrongMultiRegionEvidence);
  return Object.freeze({
    typedDmsGrouped,
    suppressHandwrittenRetry: typedDmsGrouped,
    documentHasStrongMultiRegionEvidence: structure.documentHasStrongMultiRegionEvidence,
    printedEvidenceEstablished,
    allBoundariesProven: structure.allBoundariesProven,
    hasUnprovenBoundary: structure.hasUnprovenBoundary,
    reason: typedDmsGrouped ? "PRINTED_MULTI_SITE_DMS" : "DMS_GROUPED_PRIORITY_NOT_ESTABLISHED"
  });
}

export function evaluateImageDmsAcquisitionCompleteness({
  isImageInput = false,
  rawText = "",
  normalizedCoordinates = "",
  selectedRoute = IMAGE_DMS_SELECTED_ROUTE.NONE
} = {}) {
  const structure = extractDmsSourceStructure(rawText);
  const normalizedRows = String(normalizedCoordinates || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const match = line.match(/^([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)$/);
      if (!match) return null;
      const longitude = Number(match[1]);
      const latitude = Number(match[2]);
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude)
        || Math.abs(longitude) > 180 || Math.abs(latitude) > 90) return null;
      return Object.freeze({ longitude, latitude });
    });
  const route = String(selectedRoute || "").trim();
  const groupSizes = Object.freeze(structure.groups.map(group => group.rows.length));
  const base = {
    sourceRowCount: structure.rowCount,
    normalizedCoordinateRowCount: normalizedRows.length,
    groupCount: structure.groupCount,
    groupSizes,
    selectedRoute: Object.values(IMAGE_DMS_SELECTED_ROUTE).includes(route)
      ? route
      : "UNLISTED_DMS_ROUTE"
  };

  if (!isImageInput || route === IMAGE_DMS_SELECTED_ROUTE.NONE) {
    return Object.freeze({
      ...base,
      allowed: true,
      failClosed: false,
      state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.NOT_APPLICABLE,
      reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.NOT_APPLICABLE
    });
  }

  if (IMAGE_DMS_TYPED_ROUTE_ALLOWLIST.has(route)) {
    return Object.freeze({
      ...base,
      allowed: true,
      failClosed: false,
      state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.TYPED_FAMILY_PROVEN,
      reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.TYPED_FAMILY_PROVEN
    });
  }

  if (route !== IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS) {
    return Object.freeze({
      ...base,
      allowed: false,
      failClosed: true,
      state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED,
      reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED
    });
  }

  if (structure.rowCount === 0) {
    return Object.freeze({
      ...base,
      allowed: false,
      failClosed: true,
      state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED,
      reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED
    });
  }

  const headerOrders = sourceLines(rawText)
    .map(explicitGenericDmsTableHeaderAxisOrder)
    .filter(Boolean);
  const singleGroup = structure.groupCount === 1;
  const sourcePoints = singleGroup
    ? structure.groups[0].rows.map(parseDmsSourceCoordinateRow)
    : [];
  const exactCoverage = structure.rowCount === normalizedRows.length
    && normalizedRows.every(Boolean)
    && sourcePoints.every(Boolean);
  const continuousRows = singleGroup && hasContinuousNumberedRows(structure.groups[0]?.rows || []);
  const axisOrderMatchesHeader = headerOrders.length === 1
    && sourcePoints.every(point => point?.axisOrder === headerOrders[0]);
  const semanticRowsMatch = exactCoverage && sourcePoints.every((point, index) => {
    const normalized = normalizedRows[index];
    return closeEnough(normalized?.latitude, point?.latitude, point?.latitudeTolerance)
      && closeEnough(normalized?.longitude, point?.longitude, point?.longitudeTolerance);
  });
  if (singleGroup && exactCoverage && continuousRows && axisOrderMatchesHeader && semanticRowsMatch) {
    return Object.freeze({
      ...base,
      allowed: true,
      failClosed: false,
      state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.COMPLETE_GENERIC_TABLE,
      reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.COMPLETE_GENERIC_TABLE
    });
  }

  return Object.freeze({
    ...base,
    allowed: false,
    failClosed: true,
    state: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED,
    reason: IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED
  });
}

// Provider-returned wording is not an identity authority. Only independent
// upload evidence may authorize the handwritten retry path; structural DMS
// evidence remains available to reserve dms_grouped or fail closed.
export function resolveDmsRetryTrustBoundary({
  documentEvidence = {},
  trustedHandwrittenSignal = false
} = {}) {
  const projectedTableSignal = documentEvidence?.projectedTableSignal === true;
  const explicitHandwrittenSignal = trustedHandwrittenSignal === true;
  return Object.freeze({
    explicitHandwrittenSignal,
    printedDmsCandidateSignal: !explicitHandwrittenSignal
      && !projectedTableSignal
      && documentEvidence?.printedDmsStructureSignal === true,
    nonHandwrittenDmsCandidateSignal: !explicitHandwrittenSignal
      && !projectedTableSignal
      && documentEvidence?.dmsStructureCandidateSignal === true,
    stage1FullMultisiteRiskSignal: !explicitHandwrittenSignal
      && !projectedTableSignal
      && documentEvidence?.stage1FullMultisiteStructureSignal === true,
    partialMultisiteRecoveryCandidateSignal: !explicitHandwrittenSignal
      && !projectedTableSignal
      && documentEvidence?.partialMultisiteRecoveryCandidateSignal === true,
    weakPartialMultisiteRecoveryCandidateSignal: !explicitHandwrittenSignal
      && !projectedTableSignal
      && documentEvidence?.weakPartialMultisiteRecoveryCandidateSignal === true
  });
}

export const STAGE1_FULL_MULTISITE_POLICY_ID = "DMS_GROUPED_STAGE1_FULL_MULTISITE_POLICY";
const STAGE1_FULL_MULTISITE_ROW_COUNT = 16;
const STAGE1_FULL_MULTISITE_GROUP_SIZES = Object.freeze([8, 4, 4]);

export function isStage1FullMultisiteConfirmationPending(finalizedResult = {}) {
  return finalizedResult?.familySafetyPolicy?.policyId === STAGE1_FULL_MULTISITE_POLICY_ID
    && finalizedResult?.familySafetyPolicy?.policyVersion === "1"
    && finalizedResult?.familySafetyPolicy?.applied === true
    && finalizedResult?.confirmationStatus !== "accepted";
}

export function evaluateStage1FullMultisiteSafety({
  isImageInput = false,
  riskSignal = false,
  structureText = "",
  normalizedCoordinates = ""
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  // The independent 16-row risk detector opens the gate. A weaker structure
  // parse must therefore fail closed below; it must never disable the gate.
  const gateRequired = Boolean(isImageInput && riskSignal);
  const base = {
    gateRequired,
    requiresConfirmation: gateRequired,
    mapKmlBlockedUntilConfirmation: gateRequired,
    expectedGroupSizes: STAGE1_FULL_MULTISITE_GROUP_SIZES,
    rowCount: structure.rowCount,
    groupCount: structure.groupCount,
    groupSizes: Object.freeze(structure.groups.map(group => group.rows.length))
  };
  if (!gateRequired) {
    return Object.freeze({
      ...base,
      acceptedForReview: false,
      failClosed: false,
      reason: "STAGE1_FULL_MULTISITE_GATE_NOT_REQUIRED",
      groupedCoordinates: ""
    });
  }

  const exactOrderedGrouping = structure.allBoundariesProven
    && !structure.hasUnprovenBoundary
    && hasExplicitDmsMultiRegionEvidence(structureText)
    && structure.rowCount === STAGE1_FULL_MULTISITE_ROW_COUNT
    && structure.groupCount === STAGE1_FULL_MULTISITE_GROUP_SIZES.length
    && STAGE1_FULL_MULTISITE_GROUP_SIZES.every((size, index) => structure.groups[index]?.rows.length === size);
  if (!exactOrderedGrouping) {
    return Object.freeze({
      ...base,
      acceptedForReview: false,
      failClosed: true,
      reason: "STAGE1_FULL_MULTISITE_GROUPING_UNPROVEN",
      groupedCoordinates: ""
    });
  }

  const semanticEquivalence = evaluateDmsNormalizedPointwiseEquivalence({
    structureText,
    normalizedCoordinates
  });
  if (!semanticEquivalence.accepted) {
    return Object.freeze({
      ...base,
      acceptedForReview: false,
      failClosed: true,
      reason: semanticEquivalence.reason,
      groupedCoordinates: ""
    });
  }

  const reconstructed = reconstructDmsGroupsFromNormalizedCoordinates({
    structureText,
    normalizedCoordinates
  });
  if (!reconstructed.accepted
    || reconstructed.groupSizes.length !== STAGE1_FULL_MULTISITE_GROUP_SIZES.length
    || STAGE1_FULL_MULTISITE_GROUP_SIZES.some((size, index) => reconstructed.groupSizes[index] !== size)) {
    return Object.freeze({
      ...base,
      acceptedForReview: false,
      failClosed: true,
      reason: reconstructed.reason || "STAGE1_FULL_MULTISITE_NORMALIZED_GROUPING_REJECTED",
      groupedCoordinates: ""
    });
  }

  return Object.freeze({
    ...base,
    acceptedForReview: true,
    failClosed: false,
    reason: "STAGE1_FULL_MULTISITE_REVIEW_REQUIRED",
    groupedCoordinates: reconstructed.output
  });
}

export function evaluateStage1FullMultisitePromotion({
  stage1Safety = {},
  imageDmsSourceCompleteness = {},
  retryBoundaryFailed = false
} = {}) {
  if (stage1Safety?.gateRequired !== true) {
    return Object.freeze({
      allowed: false,
      failClosed: false,
      reason: "STAGE1_FULL_MULTISITE_PROMOTION_NOT_REQUIRED"
    });
  }
  if (stage1Safety?.acceptedForReview !== true || stage1Safety?.failClosed !== false) {
    return Object.freeze({
      allowed: false,
      failClosed: true,
      reason: "STAGE1_FULL_MULTISITE_SAFETY_REJECTED"
    });
  }
  if (imageDmsSourceCompleteness?.allowed !== true
    || imageDmsSourceCompleteness?.failClosed !== false) {
    return Object.freeze({
      allowed: false,
      failClosed: true,
      reason: "STAGE1_FULL_MULTISITE_IMAGE_SOURCE_BLOCKED"
    });
  }
  if (retryBoundaryFailed === true) {
    return Object.freeze({
      allowed: false,
      failClosed: true,
      reason: "STAGE1_FULL_MULTISITE_RETRY_BOUNDARY_BLOCKED"
    });
  }
  return Object.freeze({
    allowed: true,
    failClosed: false,
    reason: "STAGE1_FULL_MULTISITE_READY_FOR_CONFIRMATION"
  });
}

export function classifyDmsRetryOwnership({
  isImageInput = false,
  routePriority = {},
  stage1FullMultisiteSafety = {},
  projectedTableSignal = false,
  explicitHandwrittenSignal = false,
  nonHandwrittenDmsCandidateSignal = false,
  handwrittenShapeRetryCandidate = false,
  partialMultisiteRecoveryCandidateSignal = false,
  weakPartialMultisiteRecovery = {}
} = {}) {
  let classification = DMS_RETRY_ROUTE_CLASSIFICATION.NONE;
  let retryOwner = null;
  let failClosed = false;

  if (!isImageInput || projectedTableSignal) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.NONE;
  } else if (stage1FullMultisiteSafety?.failClosed === true) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED;
    failClosed = true;
  } else if (explicitHandwrittenSignal) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.HANDWRITTEN_DMS_ONLY;
    retryOwner = "handwritten_dms";
  } else if (weakPartialMultisiteRecovery?.accepted === true
    && weakPartialMultisiteRecovery?.failClosed === false
    && nonHandwrittenDmsCandidateSignal === true) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY;
    retryOwner = "dms_grouped";
  } else if (routePriority?.typedDmsGrouped === true) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_ONLY;
    retryOwner = "dms_grouped";
  } else if (partialMultisiteRecoveryCandidateSignal === true
    && nonHandwrittenDmsCandidateSignal === true
    && handwrittenShapeRetryCandidate === true) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_PARTIAL_RECOVERY_ONLY;
    retryOwner = "dms_grouped";
  } else if (
    handwrittenShapeRetryCandidate === true
    || (nonHandwrittenDmsCandidateSignal === true && (
      routePriority?.documentHasStrongMultiRegionEvidence === true
      || routePriority?.hasUnprovenBoundary === true
    ))
  ) {
    classification = DMS_RETRY_ROUTE_CLASSIFICATION.FAIL_CLOSED;
    failClosed = true;
  }

  return Object.freeze({
    classification,
    retryOwner,
    failClosed,
    reason: classification
  });
}

function parsedGroupsFromStructure(structure) {
  return structure.groups.map(group => Object.freeze({
    name: group.name || null,
    identity: normalizeDmsBoundaryIdentity(group.name),
    points: Object.freeze(group.rows.map(parseDmsSourceCoordinateRow))
  }));
}

function normalizedCoordinateRows(text) {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map(line => line.trim())
    .filter(line => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?(?:\s*,\s*[-+]?\d+(?:\.\d+)?)?$/.test(line));
}

function nonEmptyNormalizedCoordinateRows(text) {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean);
}

function hasRejectedDmsCandidateLine(text) {
  return sourceLines(text).some(sourceLine => {
    const line = sourceLine.trim();
    if (!line || isDmsCoordinateSourceRow(line) || boundaryName(line) || isTableHeader(line)) return false;
    return /\d{1,3}\s*[°º]|["'′″”]\s*(?:N|S|E|W|O)\b|^\d{1,4}\s*[.)\-:]\s*/i.test(line);
  });
}

const WEAK_PARTIAL_MULTISITE_PARSER_VERSION = "DMS_SOURCE_STRICT_V1";
const WEAK_PARTIAL_REPAIRABLE_REJECTIONS = Object.freeze([
  "DMS_PAIR_AXIS_CONFLICT",
  "DMS_PAIR_HEMISPHERE_UNRESOLVED"
]);
const WEAK_PARTIAL_GROUP_MINIMUMS = Object.freeze([4, 2, 2]);
const WEAK_PARTIAL_GROUP_MAXIMUMS = Object.freeze([8, 4, 4]);
const WEAK_PARTIAL_QUALIFICATION_SCHEMA = "dms_grouped_weak_partial_qualification_v1";
const WEAK_PARTIAL_INPUT_EVIDENCE_SCHEMA = "dms_grouped_weak_partial_input_evidence_v2";
const WEAK_PARTIAL_RUNTIME_ATTESTATION_LIMIT = 4096;
const weakPartialRuntimeAttestations = new Map();
const WEAK_PARTIAL_QUALIFICATION_FIELDS = Object.freeze([
  "schemaVersion",
  "inputModality",
  "projectedTableSignal",
  "independentHandwrittenSignal",
  "candidateSignal",
  "accepted",
  "failClosed",
  "reason"
]);
const WEAK_PARTIAL_REJECTED_EVIDENCE_FIELDS = Object.freeze([
  "sourceStage",
  "parserVersion",
  "candidateLineCount",
  "validRowCount",
  "rejectedLineCount",
  "repairableRejectedLineCount",
  "unknownRejectedLineCount",
  "rejectionReasonCounts"
]);
const WEAK_PARTIAL_INPUT_EVIDENCE_FIELDS = Object.freeze([
  "schemaVersion",
  "sourceStage",
  "inputModality",
  "imageInputEvidenceSource",
  "projectedTableSignal",
  "independentHandwrittenSignal",
  "candidateSignal",
  "stage1SanitizedTextSha256",
  "runtimeAttestationId"
]);

function inspectWeakPartialMultisiteRejectedEvidence(text) {
  let validRowCount = 0;
  const rejectionReasonCounts = {
    DMS_PAIR_AXIS_CONFLICT: 0,
    DMS_PAIR_HEMISPHERE_UNRESOLVED: 0
  };
  let unknownRejectedLineCount = 0;
  for (const sourceLine of sourceLines(text)) {
    const line = sourceLine.trim();
    if (!line || boundaryName(line) || isTableHeader(line)) continue;
    if (isDmsCoordinateSourceRow(line)) {
      validRowCount += 1;
      continue;
    }
    const dmsComponents = [...line.matchAll(DMS_COMPONENT_PATTERN)];
    const looksLikeDmsCandidate = dmsComponents.length > 0
      || /\d{1,3}\s*[°º]|["'′″”]\s*(?:N|S|E|W|O)\b|^\d{1,4}\s*[.)\-:]\s*/i.test(line);
    if (!looksLikeDmsCandidate) continue;
    let repairableReason = "";
    if (dmsComponents.length === 2) {
      const tokens = dmsComponents.map(match => match[0].trim());
      const looseComponents = tokens.map(token => {
        const match = token.match(/^\s*([-+]?\d{1,3})\s*[°º]\s*(\d{1,2})\s*['′’]\s*(\d{1,2}(?:[.,]\d+)?)\s*["″”]?\s*(N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?\s*$/i);
        if (!match) return null;
        const degrees = Math.abs(Number(match[1]));
        const minutes = Number(match[2]);
        const seconds = Number(normalizeSeconds(match[3]));
        if (!Number.isFinite(degrees) || degrees > 180
          || !Number.isFinite(minutes) || minutes >= 60
          || !Number.isFinite(seconds) || seconds >= 60) return null;
        return Object.freeze({ hemisphere: normalizeHemisphere(match[4]) });
      });
      const parsedComponents = tokens.map(parseDmsComponent);
      const parsedRangesValid = parsedComponents.every(component => component
        && (component.axis !== "latitude" || Math.abs(component.value) <= 90)
        && (component.axis !== "longitude" || Math.abs(component.value) <= 180));
      if (looseComponents.every(Boolean) && looseComponents.some(component => !component.hemisphere)) {
        repairableReason = "DMS_PAIR_HEMISPHERE_UNRESOLVED";
      } else if (parsedRangesValid && parsedComponents[0].axis === parsedComponents[1].axis) {
        repairableReason = "DMS_PAIR_AXIS_CONFLICT";
      }
    }
    if (WEAK_PARTIAL_REPAIRABLE_REJECTIONS.includes(repairableReason)) {
      rejectionReasonCounts[repairableReason] += 1;
    } else {
      unknownRejectedLineCount += 1;
    }
  }
  const repairableRejectedLineCount = WEAK_PARTIAL_REPAIRABLE_REJECTIONS
    .reduce((count, reason) => count + rejectionReasonCounts[reason], 0);
  const rejectedLineCount = repairableRejectedLineCount + unknownRejectedLineCount;
  return Object.freeze({
    sourceStage: "STAGE1",
    parserVersion: WEAK_PARTIAL_MULTISITE_PARSER_VERSION,
    candidateLineCount: validRowCount + rejectedLineCount,
    validRowCount,
    rejectedLineCount,
    repairableRejectedLineCount,
    unknownRejectedLineCount,
    rejectionReasonCounts: Object.freeze({ ...rejectionReasonCounts })
  });
}

function weakPartialMultisiteBoundariesProven(structure) {
  if (structure?.groupCount !== 3) return false;
  return structure.groups.slice(1).every(group => [
    "section_title",
    "repeated_table_header",
    "number_restart"
  ].includes(group.boundaryProvenance)
    || (group.boundaryProvenance === "blank_line" && structure.hasProvenBlankLineBoundary === true));
}

function exactObjectKeys(value, fields) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === fields.length
    && fields.every(field => Object.hasOwn(value, field)));
}

function weakPartialQualificationValid(value) {
  return exactObjectKeys(value, WEAK_PARTIAL_QUALIFICATION_FIELDS)
    && value.schemaVersion === WEAK_PARTIAL_QUALIFICATION_SCHEMA
    && value.inputModality === "IMAGE"
    && value.projectedTableSignal === false
    && value.independentHandwrittenSignal === false
    && value.candidateSignal === true
    && value.accepted === true
    && value.failClosed === false
    && value.reason === "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ELIGIBLE";
}

function weakPartialRejectedEvidenceValid(value, validRowCount) {
  if (!exactObjectKeys(value, WEAK_PARTIAL_REJECTED_EVIDENCE_FIELDS)
    || !exactObjectKeys(value.rejectionReasonCounts, WEAK_PARTIAL_REPAIRABLE_REJECTIONS)) return false;
  const integerFields = [
    "candidateLineCount", "validRowCount", "rejectedLineCount",
    "repairableRejectedLineCount", "unknownRejectedLineCount"
  ];
  if (value.sourceStage !== "STAGE1"
    || value.parserVersion !== WEAK_PARTIAL_MULTISITE_PARSER_VERSION
    || integerFields.some(field => !Number.isInteger(value[field]) || value[field] < 0)
    || WEAK_PARTIAL_REPAIRABLE_REJECTIONS.some(reason => (
      !Number.isInteger(value.rejectionReasonCounts[reason])
      || value.rejectionReasonCounts[reason] < 0
    ))) return false;
  const repairableCount = WEAK_PARTIAL_REPAIRABLE_REJECTIONS
    .reduce((count, reason) => count + value.rejectionReasonCounts[reason], 0);
  return value.validRowCount === validRowCount
    && value.candidateLineCount === value.validRowCount + value.rejectedLineCount
    && value.rejectedLineCount === value.repairableRejectedLineCount + value.unknownRejectedLineCount
    && value.repairableRejectedLineCount === repairableCount
    && value.unknownRejectedLineCount === 0
    && value.repairableRejectedLineCount >= 1
    && value.repairableRejectedLineCount <= Math.min(16, validRowCount);
}

function weakPartialQualificationMatches(left, right) {
  return weakPartialQualificationValid(left)
    && weakPartialQualificationValid(right)
    && WEAK_PARTIAL_QUALIFICATION_FIELDS.every(field => left[field] === right[field]);
}

function weakPartialRejectedEvidenceMatches(left, right, validRowCount) {
  return weakPartialRejectedEvidenceValid(left, validRowCount)
    && weakPartialRejectedEvidenceValid(right, validRowCount)
    && WEAK_PARTIAL_REJECTED_EVIDENCE_FIELDS
      .filter(field => field !== "rejectionReasonCounts")
      .every(field => left[field] === right[field])
    && WEAK_PARTIAL_REPAIRABLE_REJECTIONS.every(reason => (
      left.rejectionReasonCounts[reason] === right.rejectionReasonCounts[reason]
    ));
}

function createWeakPartialQualification() {
  return Object.freeze({
    schemaVersion: WEAK_PARTIAL_QUALIFICATION_SCHEMA,
    inputModality: "IMAGE",
    projectedTableSignal: false,
    independentHandwrittenSignal: false,
    candidateSignal: true,
    accepted: true,
    failClosed: false,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ELIGIBLE"
  });
}

function createWeakPartialInputEvidence({
  isImageInput = false,
  structureText = "",
  explicitHandwrittenSignal = false,
  imageInputBuffer = null,
  rejectedEvidence = null
} = {}) {
  const documentEvidence = getDmsDocumentEvidence(structureText);
  const sanitizedText = sanitizeWeakPartialMultisiteStage1Text(structureText);
  let runtimeAttestationId = "";
  if (isImageInput === true
    && Buffer.isBuffer(imageInputBuffer)
    && imageInputBuffer.length > 0
    && hasRejectedDmsCandidateLine(structureText)
    && weakPartialRejectedEvidenceValid(rejectedEvidence, extractDmsSourceStructure(structureText).rowCount)) {
    runtimeAttestationId = randomBytes(24).toString("hex");
    weakPartialRuntimeAttestations.set(runtimeAttestationId, {
      imageSha256: createHash("sha256").update(imageInputBuffer).digest("hex"),
      originalStage1TextSha256: createHash("sha256").update(String(structureText)).digest("hex"),
      sanitizedStage1TextSha256: createHash("sha256").update(sanitizedText).digest("hex"),
      rejectedEvidenceSha256: createHash("sha256").update(JSON.stringify(rejectedEvidence)).digest("hex"),
      state: "ISSUED",
      boundRetryCandidateSha256: null
    });
    while (weakPartialRuntimeAttestations.size > WEAK_PARTIAL_RUNTIME_ATTESTATION_LIMIT) {
      weakPartialRuntimeAttestations.delete(weakPartialRuntimeAttestations.keys().next().value);
    }
  }
  return Object.freeze({
    schemaVersion: WEAK_PARTIAL_INPUT_EVIDENCE_SCHEMA,
    sourceStage: "STAGE1",
    inputModality: isImageInput === true ? "IMAGE" : "UNPROVEN_INPUT_MODALITY",
    imageInputEvidenceSource: isImageInput === true
      ? "REQUEST_FILE_PRESENT"
      : "REQUEST_FILE_NOT_PROVEN",
    projectedTableSignal: documentEvidence.projectedTableSignal === true,
    independentHandwrittenSignal: explicitHandwrittenSignal === true,
    candidateSignal: documentEvidence.weakPartialMultisiteRecoveryCandidateSignal === true,
    stage1SanitizedTextSha256: createHash("sha256").update(sanitizedText).digest("hex"),
    runtimeAttestationId
  });
}

function weakPartialInputEvidenceValid(value, stage1RawText, rejectedEvidence, imageInputBuffer = null) {
  if (!exactObjectKeys(value, WEAK_PARTIAL_INPUT_EVIDENCE_FIELDS)) return false;
  const sanitizedText = sanitizeWeakPartialMultisiteStage1Text(stage1RawText);
  const derivedDocumentEvidence = getDmsDocumentEvidence(stage1RawText);
  const attestation = weakPartialRuntimeAttestations.get(String(value.runtimeAttestationId || ""));
  const rejectedEvidenceSha256 = createHash("sha256").update(JSON.stringify(rejectedEvidence || null)).digest("hex");
  const suppliedImageSha256 = Buffer.isBuffer(imageInputBuffer) && imageInputBuffer.length > 0
    ? createHash("sha256").update(imageInputBuffer).digest("hex")
    : null;
  const imageIdentityValid = suppliedImageSha256
    ? attestation?.imageSha256 === suppliedImageSha256
    : attestation?.state === "BOUND";
  return value.schemaVersion === WEAK_PARTIAL_INPUT_EVIDENCE_SCHEMA
    && value.sourceStage === "STAGE1"
    && value.inputModality === "IMAGE"
    && value.imageInputEvidenceSource === "REQUEST_FILE_PRESENT"
    && value.projectedTableSignal === false
    && value.independentHandwrittenSignal === false
    && value.candidateSignal === true
    && derivedDocumentEvidence.projectedTableSignal !== true
    && derivedDocumentEvidence.weakPartialMultisiteRecoveryCandidateSignal === true
    && /^[a-f0-9]{48}$/.test(value.runtimeAttestationId)
    && Boolean(attestation)
    && imageIdentityValid
    && value.stage1SanitizedTextSha256 === createHash("sha256").update(sanitizedText).digest("hex")
    && attestation.sanitizedStage1TextSha256 === value.stage1SanitizedTextSha256
    && attestation.rejectedEvidenceSha256 === rejectedEvidenceSha256
    && (!hasRejectedDmsCandidateLine(stage1RawText)
      || attestation.originalStage1TextSha256 === createHash("sha256").update(String(stage1RawText)).digest("hex"));
}

function weakPartialInputEvidenceMatches(
  left,
  right,
  stage1RawText,
  rejectedEvidence,
  imageInputBuffer = null
) {
  return weakPartialInputEvidenceValid(left, stage1RawText, rejectedEvidence, imageInputBuffer)
    && weakPartialInputEvidenceValid(right, stage1RawText, rejectedEvidence, imageInputBuffer)
    && WEAK_PARTIAL_INPUT_EVIDENCE_FIELDS.every(field => left[field] === right[field]);
}

function bindOrValidateWeakPartialRuntimeAttestation({
  inputEvidence = {},
  imageInputBuffer = null,
  stage1RawText = "",
  rejectedEvidence = null,
  retryCandidate = null
} = {}) {
  const attestation = weakPartialRuntimeAttestations.get(String(inputEvidence?.runtimeAttestationId || ""));
  if (!attestation || !retryCandidate) return false;
  const retryCandidateSha256 = createHash("sha256")
    .update(JSON.stringify(retryCandidate))
    .digest("hex");
  if (hasRejectedDmsCandidateLine(stage1RawText)) {
    if (!Buffer.isBuffer(imageInputBuffer) || imageInputBuffer.length === 0
      || attestation.state !== "ISSUED"
      || attestation.imageSha256 !== createHash("sha256").update(imageInputBuffer).digest("hex")
      || attestation.originalStage1TextSha256
        !== createHash("sha256").update(String(stage1RawText)).digest("hex")
      || attestation.rejectedEvidenceSha256
        !== createHash("sha256").update(JSON.stringify(rejectedEvidence || null)).digest("hex")) {
      return false;
    }
    attestation.state = "BOUND";
    attestation.boundRetryCandidateSha256 = retryCandidateSha256;
    return true;
  }
  return attestation.state === "BOUND"
    && attestation.boundRetryCandidateSha256 === retryCandidateSha256;
}

function weakPartialInputEvidenceSha256(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

export function sanitizeWeakPartialMultisiteStage1Text(text = "") {
  return sourceLines(text)
    .filter(sourceLine => {
      const line = sourceLine.trim();
      return !line || Boolean(boundaryName(line)) || isTableHeader(line) || isDmsCoordinateSourceRow(line);
    })
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function evaluateSanitizedWeakPartialMultisiteRecovery({
  structureText = "",
  qualification = {},
  rejectedEvidence = {},
  inputEvidence = {},
  imageInputBuffer = null
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const groupSizes = Object.freeze(structure.groups.map(group => group.rows.length));
  const base = { rowCount: structure.rowCount, groupCount: structure.groupCount, groupSizes, rejectedEvidence };
  const derivedDocumentEvidence = getDmsDocumentEvidence(structureText);
  if (!weakPartialQualificationValid(qualification)
    || !weakPartialInputEvidenceValid(inputEvidence, structureText, rejectedEvidence, imageInputBuffer)
    || qualification.inputModality !== inputEvidence.inputModality
    || qualification.projectedTableSignal !== inputEvidence.projectedTableSignal
    || qualification.independentHandwrittenSignal !== inputEvidence.independentHandwrittenSignal
    || qualification.candidateSignal !== inputEvidence.candidateSignal
    || derivedDocumentEvidence.projectedTableSignal === true
    || derivedDocumentEvidence.weakPartialMultisiteRecoveryCandidateSignal !== true
    || hasRejectedDmsCandidateLine(structureText)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_QUALIFICATION_INVALID" });
  }
  if (structure.rowCount < 9 || structure.rowCount > 15 || structure.groupCount !== 3) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SHAPE_INVALID" });
  }
  if (!WEAK_PARTIAL_GROUP_MINIMUMS.every((minimum, index) => groupSizes[index] >= minimum)
    || !WEAK_PARTIAL_GROUP_MAXIMUMS.every((maximum, index) => groupSizes[index] <= maximum)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SIZE_INVALID" });
  }
  if (!weakPartialMultisiteBoundariesProven(structure)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_BOUNDARY_UNPROVEN" });
  }
  if (!structure.groups.every(group => hasSequentialLabelsFromOne(group.rows))) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_LABEL_SEQUENCE_INVALID" });
  }
  if (!weakPartialRejectedEvidenceValid(rejectedEvidence, structure.rowCount)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_REJECTED_EVIDENCE_INVALID" });
  }
  return Object.freeze({
    ...base,
    qualification: createWeakPartialQualification(),
    inputEvidence: Object.freeze({ ...inputEvidence }),
    accepted: true,
    failClosed: false,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ELIGIBLE"
  });
}

export function validateWeakPartialMultisiteLifecycleEvidence({
  stage1RawText = "",
  qualification = {},
  rejectedEvidence = {},
  inputEvidence = {}
} = {}) {
  const evaluation = evaluateSanitizedWeakPartialMultisiteRecovery({
    structureText: stage1RawText,
    qualification,
    rejectedEvidence,
    inputEvidence
  });
  return Object.freeze({
    valid: evaluation.accepted === true && evaluation.failClosed === false,
    reason: evaluation.accepted === true
      ? "DMS_GROUPED_WEAK_PARTIAL_LIFECYCLE_EVIDENCE_VALID"
      : evaluation.reason
  });
}

export function evaluateDmsWeakPartialMultisiteRecovery({
  isImageInput = false,
  projectedTableSignal = false,
  explicitHandwrittenSignal = false,
  candidateSignal = false,
  structureText = "",
  imageInputBuffer = null,
  weakPartialInputEvidence = null
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const rejectedEvidence = inspectWeakPartialMultisiteRejectedEvidence(structureText);
  const derivedDocumentEvidence = getDmsDocumentEvidence(structureText);
  const groupSizes = Object.freeze(structure.groups.map(group => group.rows.length));
  const base = {
    rowCount: structure.rowCount,
    groupCount: structure.groupCount,
    groupSizes,
    rejectedEvidence
  };
  if (!isImageInput || projectedTableSignal || explicitHandwrittenSignal || candidateSignal !== true
    || derivedDocumentEvidence.projectedTableSignal === true
    || derivedDocumentEvidence.weakPartialMultisiteRecoveryCandidateSignal !== true) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_INPUT_NOT_ELIGIBLE" });
  }
  if (structure.rowCount < 9 || structure.rowCount > 15 || structure.groupCount !== 3) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SHAPE_INVALID" });
  }
  if (!WEAK_PARTIAL_GROUP_MINIMUMS.every((minimum, index) => groupSizes[index] >= minimum)
    || !WEAK_PARTIAL_GROUP_MAXIMUMS.every((maximum, index) => groupSizes[index] <= maximum)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_GROUP_SIZE_INVALID" });
  }
  if (!weakPartialMultisiteBoundariesProven(structure)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_BOUNDARY_UNPROVEN" });
  }
  if (!structure.groups.every(group => hasSequentialLabelsFromOne(group.rows))) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_LABEL_SEQUENCE_INVALID" });
  }
  const maximumRepairableRejects = Math.min(16, structure.rowCount);
  if (rejectedEvidence.unknownRejectedLineCount !== 0) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_UNKNOWN_REJECTION" });
  }
  if (rejectedEvidence.repairableRejectedLineCount < 1
    || rejectedEvidence.repairableRejectedLineCount > maximumRepairableRejects) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_REJECTION_COUNT_INVALID" });
  }
  const inputEvidence = weakPartialInputEvidence || createWeakPartialInputEvidence({
    isImageInput,
    structureText,
    explicitHandwrittenSignal,
    imageInputBuffer,
    rejectedEvidence
  });
  if (!weakPartialInputEvidenceValid(inputEvidence, structureText, rejectedEvidence, imageInputBuffer)) {
    return Object.freeze({ ...base, accepted: false, failClosed: true, reason: "DMS_GROUPED_WEAK_PARTIAL_INPUT_ATTESTATION_INVALID" });
  }
  return Object.freeze({
    ...base,
    qualification: createWeakPartialQualification(),
    inputEvidence,
    accepted: true,
    failClosed: false,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ELIGIBLE"
  });
}

function normalizedLongitudeLatitudePoint(line) {
  const match = String(line || "").trim().match(
    /^([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)(?:\s*,\s*[-+]?\d+(?:\.\d+)?)?$/
  );
  if (!match) return null;
  const longitude = Number(match[1]);
  const latitude = Number(match[2]);
  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)
    || Math.abs(longitude) > 180 || Math.abs(latitude) > 90) return null;
  return Object.freeze({ longitude, latitude });
}

function coordinateHemisphere(value, positive, negative) {
  if (!Number.isFinite(value) || value === 0 || Object.is(value, -0)) return null;
  return value > 0 ? positive : negative;
}

function hemisphereMatchesCoordinate(sourceHemisphere, value, positive, negative) {
  const normalizedHemisphere = coordinateHemisphere(value, positive, negative);
  return normalizedHemisphere === null
    ? sourceHemisphere === positive || sourceHemisphere === negative
    : sourceHemisphere === normalizedHemisphere;
}

export function evaluateDmsNormalizedPointwiseEquivalence({
  structureText = "",
  normalizedCoordinates = ""
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const allNormalizedRows = nonEmptyNormalizedCoordinateRows(normalizedCoordinates);
  const normalizedRows = normalizedCoordinateRows(normalizedCoordinates);
  const normalizedPoints = normalizedRows.map(normalizedLongitudeLatitudePoint);
  if (allNormalizedRows.length !== normalizedRows.length) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_MALFORMED_OR_EXTRA_ROW" });
  }
  if (structure.rowCount === 0 || normalizedPoints.length !== structure.rowCount
    || normalizedPoints.some(point => !point)) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_POINT_COVERAGE_MISMATCH" });
  }

  const headerAxisOrders = sourceLines(structureText)
    .map(explicitGenericDmsTableHeaderAxisOrder)
    .filter(Boolean);
  const expectedSourceAxisOrder = headerAxisOrders.length > 0
    ? headerAxisOrders[0]
    : "latitude_longitude";
  if ((headerAxisOrders.length > 0 && new Set(headerAxisOrders).size !== 1)
    || structure.rows.some(row => parseDmsSourceCoordinateRow(row)?.axisOrder !== expectedSourceAxisOrder)) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_AXIS_ORDER_MISMATCH" });
  }

  let offset = 0;
  for (const group of structure.groups) {
    for (let pointIndex = 0; pointIndex < group.rows.length; pointIndex += 1) {
      const sourcePoint = parseDmsSourceCoordinateRow(group.rows[pointIndex]);
      const normalizedPoint = normalizedPoints[offset + pointIndex];
      if (!sourcePoint || !normalizedPoint) {
        return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_POINT_UNPARSABLE" });
      }
      if (normalizeLabel(sourcePoint.label) !== String(pointIndex + 1)) {
        return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_LABEL_ORDER_MISMATCH" });
      }
      if (!hemisphereMatchesCoordinate(sourcePoint.latitudeHemisphere, normalizedPoint.latitude, "N", "S")
        || !hemisphereMatchesCoordinate(sourcePoint.longitudeHemisphere, normalizedPoint.longitude, "E", "W")) {
        return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_HEMISPHERE_MISMATCH" });
      }
      if (!closeEnough(sourcePoint.latitude, normalizedPoint.latitude, sourcePoint.latitudeTolerance)
        || !closeEnough(sourcePoint.longitude, normalizedPoint.longitude, sourcePoint.longitudeTolerance)) {
        return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_POINT_VALUE_MISMATCH" });
      }
    }
    offset += group.rows.length;
  }
  if (offset !== normalizedPoints.length) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_DMS_POINT_ORDER_MISMATCH" });
  }
  return Object.freeze({ accepted: true, reason: "NORMALIZED_DMS_POINTWISE_SEMANTIC_MATCH" });
}

export function reconstructDmsGroupsFromNormalizedCoordinates({ structureText = "", normalizedCoordinates = "" } = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const rows = normalizedCoordinateRows(normalizedCoordinates);
  if (structure.groupCount < 2 || !hasExplicitDmsMultiRegionEvidence(structureText)) {
    return Object.freeze({ accepted: false, reason: "STRONG_MULTI_REGION_EVIDENCE_REQUIRED", output: "" });
  }
  if (rows.length !== structure.rowCount) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_ROW_COUNT_MISMATCH", output: "" });
  }
  const groups = [];
  let offset = 0;
  for (const sourceGroup of structure.groups) {
    const size = sourceGroup.rows.length;
    const groupRows = rows.slice(offset, offset + size);
    if (groupRows.length !== size) {
      return Object.freeze({ accepted: false, reason: "NORMALIZED_GROUP_COVERAGE_MISMATCH", output: "" });
    }
    groups.push(groupRows);
    offset += size;
  }
  if (offset !== rows.length) {
    return Object.freeze({ accepted: false, reason: "NORMALIZED_GROUP_ORDER_MISMATCH", output: "" });
  }
  return Object.freeze({
    accepted: true,
    reason: "NORMALIZED_POINTS_GROUPED_BY_VERIFIED_STRUCTURE",
    output: groups.map(group => group.join("\n")).join("\n\n"),
    groupSizes: Object.freeze(groups.map(group => group.length)),
    groupIdentities: Object.freeze(structure.groups.map(group => normalizeDmsBoundaryIdentity(group.name)))
  });
}

export function resolveDmsEngineGroupNames({ structureText = "", groupSizes = [] } = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const expectedSizes = Array.isArray(groupSizes) ? groupSizes.map(Number) : [];
  if (!hasExplicitDmsMultiRegionEvidence(structureText)
    || structure.groups.length !== expectedSizes.length
    || structure.groups.some((group, index) => group.rows.length !== expectedSizes[index])) {
    return Object.freeze([]);
  }
  const identities = structure.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  if (identities.some(identity => !identity)) return Object.freeze([]);
  return Object.freeze(structure.groups.map(group => group.name));
}

export function evaluateDmsGroupedRetryEligibility({
  rawText = "",
  dmsGroupedInfo = {},
  isImageInput = false,
  groupedRowCount = 0,
  lonLatOrderLines = 0,
  familyRetryAllowed = false,
  typedRouteEligible = false,
  partialMultisiteRecoveryEligible = false,
  projectedTableSignal = false,
  explicitHandwrittenSignal = false,
  weakPartialMultisiteRecovery = {},
  imageInputBuffer = null
} = {}) {
  const structure = extractDmsSourceStructure(rawText);
  const explicitEvidence = hasExplicitDmsMultiRegionEvidence(rawText, dmsGroupedInfo);
  const typedVerificationRequired = Boolean(typedRouteEligible
    && structure.documentHasStrongMultiRegionEvidence
    && structure.rowCount === 13);
  const partialRecoveryRequired = Boolean(partialMultisiteRecoveryEligible
    && structure.rowCount === 8
    && structure.groupCount === 1);
  if (partialMultisiteRecoveryEligible === true && !partialRecoveryRequired) {
    return Object.freeze({
      allowed: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_STRUCTURE_UNPROVEN"
    });
  }
  const weakPartialRecoveryClaimed = weakPartialMultisiteRecovery?.claimed === true;
  const recomputedWeakPartialRecovery = weakPartialRecoveryClaimed
    ? evaluateDmsWeakPartialMultisiteRecovery({
        isImageInput,
        projectedTableSignal,
        explicitHandwrittenSignal,
        candidateSignal: true,
        structureText: rawText,
        weakPartialInputEvidence: weakPartialMultisiteRecovery?.inputEvidence,
        imageInputBuffer
      })
    : Object.freeze({ accepted: false });
  const weakGroupSizesBound = Array.isArray(weakPartialMultisiteRecovery?.groupSizes)
    && Array.isArray(recomputedWeakPartialRecovery?.groupSizes)
    && weakPartialMultisiteRecovery.groupSizes.length === recomputedWeakPartialRecovery.groupSizes.length
    && weakPartialMultisiteRecovery.groupSizes.every((size, index) => size === recomputedWeakPartialRecovery.groupSizes[index]);
  const weakRejectedEvidenceBound = weakPartialMultisiteRecovery?.rejectedEvidence
    && recomputedWeakPartialRecovery?.rejectedEvidence
    && [
      "sourceStage", "parserVersion", "candidateLineCount", "validRowCount",
      "rejectedLineCount", "repairableRejectedLineCount", "unknownRejectedLineCount"
    ].every(field => weakPartialMultisiteRecovery.rejectedEvidence[field]
      === recomputedWeakPartialRecovery.rejectedEvidence[field])
    && WEAK_PARTIAL_REPAIRABLE_REJECTIONS.every(reason => (
      weakPartialMultisiteRecovery.rejectedEvidence.rejectionReasonCounts?.[reason]
        === recomputedWeakPartialRecovery.rejectedEvidence.rejectionReasonCounts?.[reason]
    ));
  const weakPartialRecoveryRequired = weakPartialRecoveryClaimed
    && weakPartialMultisiteRecovery?.accepted === true
    && weakPartialMultisiteRecovery?.failClosed === false
    && recomputedWeakPartialRecovery.accepted === true
    && recomputedWeakPartialRecovery.failClosed === false
    && weakPartialMultisiteRecovery.rowCount === recomputedWeakPartialRecovery.rowCount
    && weakPartialMultisiteRecovery.groupCount === recomputedWeakPartialRecovery.groupCount
    && weakGroupSizesBound
    && weakRejectedEvidenceBound;
  if (weakPartialRecoveryClaimed && !weakPartialRecoveryRequired) {
    return Object.freeze({
      allowed: false,
      failClosed: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_STRUCTURE_UNPROVEN"
    });
  }
  const needsRepair = weakPartialRecoveryRequired || partialRecoveryRequired || typedVerificationRequired || (explicitEvidence && (
    !dmsGroupedInfo?.output
    || (Number(groupedRowCount) >= 4
      && Number(lonLatOrderLines) >= Math.max(2, Math.ceil(Number(groupedRowCount) * 0.5)))
  ));
  if (!isImageInput || structure.rowCount === 0 || !needsRepair) {
    return Object.freeze({ allowed: false, failClosed: false, reason: "DMS_GROUPED_RETRY_NOT_REQUIRED" });
  }
  if (!familyRetryAllowed) {
    return Object.freeze({ allowed: false, failClosed: true, reason: "DMS_GROUPED_RETRY_BUDGET_BLOCKED" });
  }
  return Object.freeze({
    allowed: true,
    failClosed: false,
    reason: weakPartialRecoveryRequired
      ? "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_AUTHORIZED"
      : partialRecoveryRequired
        ? "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_AUTHORIZED"
        : "DMS_GROUPED_RETRY_AUTHORIZED"
  });
}

function pointsEquivalent(left, right) {
  if (!left || !right || left.axisOrder !== right.axisOrder) return false;
  if (left.latitudeHemisphere !== right.latitudeHemisphere
    || left.longitudeHemisphere !== right.longitudeHemisphere) return false;
  if (left.label && normalizeLabel(left.label) !== normalizeLabel(right.label)) return false;
  return closeEnough(right.latitude, left.latitude, left.latitudeTolerance)
    && closeEnough(right.longitude, left.longitude, left.longitudeTolerance);
}

function weakPartialPointsEquivalent(left, right) {
  return pointsEquivalent(left, right)
    && normalizeLabel(left?.label) === normalizeLabel(right?.label);
}

function pointsShareCoordinateIdentity(left, right) {
  if (!left || !right || left.axisOrder !== right.axisOrder) return false;
  if (left.latitudeHemisphere !== right.latitudeHemisphere
    || left.longitudeHemisphere !== right.longitudeHemisphere) return false;
  return closeEnough(right.latitude, left.latitude, left.latitudeTolerance)
    && closeEnough(right.longitude, left.longitude, left.longitudeTolerance);
}

function groupsHaveContinuousLabels(structure) {
  return structure.groups.every(group => hasContinuousNumberedRows(group.rows));
}

export function normalizedCoordinatesFromDmsStructure(structureText = "") {
  const structure = extractDmsSourceStructure(structureText);
  if (!structure.rowCount || !structure.allBoundariesProven || !hasExplicitDmsMultiRegionEvidence(structureText)) {
    return Object.freeze({ accepted: false, reason: "VERIFIED_MULTI_REGION_STRUCTURE_REQUIRED", output: "" });
  }
  const outputGroups = [];
  for (const group of structure.groups) {
    const parsed = group.rows.map(parseDmsSourceCoordinateRow);
    if (parsed.some(point => !point)) {
      return Object.freeze({ accepted: false, reason: "UNPARSEABLE_DMS_ROW", output: "" });
    }
    // Production normalized coordinates are always longitude,latitude.
    outputGroups.push(parsed.map(point => `${point.longitude},${point.latitude}`));
  }
  return Object.freeze({
    accepted: true,
    reason: "VERIFIED_DMS_NORMALIZED",
    output: outputGroups.map(group => group.join("\n")).join("\n\n"),
    rowCount: structure.rowCount,
    groupCount: structure.groupCount,
    groupSizes: Object.freeze(structure.groups.map(group => group.rows.length)),
    groupIdentities: Object.freeze(structure.groups.map(group => normalizeDmsBoundaryIdentity(group.name)))
  });
}

export function evaluateDmsGroupedAcquisitionExpansion({
  baselineText = "",
  retryText = "",
  allowPartialMultisiteRecovery = false,
  allowWeakPartialMultisiteRecovery = false,
  isImageInput = false,
  projectedTableSignal = false,
  explicitHandwrittenSignal = false,
  weakPartialCandidateSignal = false,
  weakPartialQualification = null,
  weakPartialRejectedEvidence = null,
  weakPartialInputEvidence = null,
  imageInputBuffer = null
} = {}) {
  const baseline = extractDmsSourceStructure(baselineText);
  const retry = extractDmsSourceStructure(retryText);
  if (!baseline.rowCount) return Object.freeze({ accepted: false, reason: "NO_BASELINE_DMS_ROWS" });
  if (!retry.rowCount) return Object.freeze({ accepted: false, reason: "NO_RETRY_DMS_ROWS" });
  if (retry.rowCount <= baseline.rowCount) return Object.freeze({ accepted: false, reason: "RETRY_NOT_EXPANDED" });
  const partialMultisiteRecovery = allowPartialMultisiteRecovery === true
    && baseline.rowCount === 8
    && retry.rowCount === 16;
  const weakPartialEvaluation = allowWeakPartialMultisiteRecovery === true
    ? (hasRejectedDmsCandidateLine(baselineText)
      ? evaluateDmsWeakPartialMultisiteRecovery({
          isImageInput,
          projectedTableSignal,
          explicitHandwrittenSignal,
          candidateSignal: weakPartialCandidateSignal,
          structureText: baselineText,
          imageInputBuffer,
          weakPartialInputEvidence
        })
      : evaluateSanitizedWeakPartialMultisiteRecovery({
          structureText: baselineText,
          qualification: weakPartialQualification,
          rejectedEvidence: weakPartialRejectedEvidence,
          inputEvidence: weakPartialInputEvidence,
          imageInputBuffer
        }))
    : Object.freeze({ accepted: false });
  const weakPartialMultisiteRecovery = weakPartialEvaluation.accepted === true
    && weakPartialEvaluation.failClosed === false
    && retry.rowCount === 16;
  const establishedAcquisitionExpansion = baseline.rowCount === 13 && retry.rowCount === 16;
  if (!partialMultisiteRecovery && !weakPartialMultisiteRecovery && !establishedAcquisitionExpansion) {
    return Object.freeze({ accepted: false, reason: "UNSUPPORTED_ACQUISITION_DELTA_SIZE" });
  }
  if (!retry.documentHasStrongMultiRegionEvidence || !retry.allBoundariesProven
    || !hasExplicitDmsMultiRegionEvidence(retryText)) {
    return Object.freeze({ accepted: false, reason: "RETRY_BOUNDARY_EVIDENCE_UNPROVEN" });
  }
  if (!partialMultisiteRecovery && !weakPartialMultisiteRecovery && (baseline.groupCount !== 3
    || !baseline.documentHasStrongMultiRegionEvidence || !baseline.allBoundariesProven
    || !hasExplicitDmsMultiRegionEvidence(baselineText))) {
    return Object.freeze({ accepted: false, reason: "THREE_GROUP_BOUNDARY_CONTRACT_REQUIRED" });
  }
  const retryGroupSizes = retry.groups.map(group => group.rows.length);
  if (retry.groupCount !== 3 || retryGroupSizes.length !== 3
    || ![8, 4, 4].every((size, index) => retryGroupSizes[index] === size)) {
    return Object.freeze({ accepted: false, reason: "RETRY_ORDERED_8_4_4_GROUPING_REQUIRED" });
  }
  const identities = retry.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  const baselineIdentities = baseline.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  if (identities.some(identity => !identity)
    || new Set(identities).size !== identities.length) {
    return Object.freeze({ accepted: false, reason: "RETRY_GROUP_IDENTITY_INVALID" });
  }
  if (!partialMultisiteRecovery && !weakPartialMultisiteRecovery && (baselineIdentities.some(identity => !identity)
    || new Set(baselineIdentities).size !== baselineIdentities.length)) {
    return Object.freeze({ accepted: false, reason: "RETRY_GROUP_IDENTITY_INVALID" });
  }
  if (!partialMultisiteRecovery && !weakPartialMultisiteRecovery
    && baselineIdentities.some((identity, index) => identity !== identities[index])) {
    return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_IDENTITY_OR_ORDER_MISMATCH" });
  }
  if (!groupsHaveContinuousLabels(retry)) {
    return Object.freeze({ accepted: false, reason: "RETRY_LABEL_SEQUENCE_INVALID" });
  }
  if (hasRejectedDmsCandidateLine(retryText)) {
    return Object.freeze({ accepted: false, reason: "RETRY_REJECTED_DMS_ROW_PRESENT" });
  }
  if (weakPartialMultisiteRecovery) {
    const nonEmptyBaselineIdentities = baselineIdentities.filter(Boolean);
    if (nonEmptyBaselineIdentities.length !== 0
      && (nonEmptyBaselineIdentities.length !== baselineIdentities.length
        || new Set(baselineIdentities).size !== baselineIdentities.length
        || baselineIdentities.some((identity, index) => identity !== identities[index]))) {
      return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_IDENTITY_OR_ORDER_MISMATCH" });
    }
    const allRetryPoints = retry.groups.flatMap(group => group.rows.map(parseDmsSourceCoordinateRow));
    if (allRetryPoints.some((point, index) => allRetryPoints
      .slice(index + 1)
      .some(other => pointsShareCoordinateIdentity(point, other)))) {
      return Object.freeze({ accepted: false, reason: "WEAK_PARTIAL_RETRY_DUPLICATE_POINT" });
    }
    for (let groupIndex = 0; groupIndex < baseline.groups.length; groupIndex += 1) {
      const baselinePoints = baseline.groups[groupIndex].rows.map(parseDmsSourceCoordinateRow);
      const retryPoints = retry.groups[groupIndex].rows.map(parseDmsSourceCoordinateRow);
      if (baselinePoints.some(point => !point) || retryPoints.some(point => !point)) {
        return Object.freeze({ accepted: false, reason: "UNPARSEABLE_DMS_ROW" });
      }
      if (baselinePoints.length > retryPoints.length) {
        return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_ROW_REMOVED" });
      }
      for (let pointIndex = 0; pointIndex < baselinePoints.length; pointIndex += 1) {
        const baselinePoint = baselinePoints[pointIndex];
        const matches = retryPoints.filter(retryPoint => weakPartialPointsEquivalent(baselinePoint, retryPoint));
        if (matches.length !== 1 || !weakPartialPointsEquivalent(baselinePoint, retryPoints[pointIndex])) {
          return Object.freeze({
            accepted: false,
            reason: matches.length > 1
              ? "WEAK_PARTIAL_BASELINE_POINT_DUPLICATED"
              : "WEAK_PARTIAL_BASELINE_POINT_CHANGED_MISSING_REORDERED_OR_CROSS_GROUP"
          });
        }
      }
    }
    const normalized = normalizedCoordinatesFromDmsStructure(retryText);
    if (!normalized.accepted) return normalized;
    return Object.freeze({
      accepted: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_MULTISITE_RECOVERY_REVIEW_REQUIRED",
      recoveryMode: "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16",
      baselineRowCount: baseline.rowCount,
      retryRowCount: retry.rowCount,
      addedRowCount: retry.rowCount - baseline.rowCount,
      baselineRowsPreserved: true,
      groupLocalBaselineRowsPreserved: true,
      baselineGroupCount: baseline.groupCount,
      baselineGroupSizes: Object.freeze(baseline.groups.map(group => group.rows.length)),
      baselineGroupIdentities: Object.freeze([...baselineIdentities]),
      groupCount: retry.groupCount,
      groupSizes: normalized.groupSizes,
      groupIdentities: normalized.groupIdentities,
      weakPartialQualification: weakPartialEvaluation.qualification,
      weakPartialInputEvidence: weakPartialEvaluation.inputEvidence,
      stage1RejectedEvidence: weakPartialEvaluation.rejectedEvidence,
      normalizedCoordinates: normalized.output
    });
  }
  if (partialMultisiteRecovery) {
    const baselineRows = baseline.groups[0]?.rows || [];
    const baselineLabels = baselineRows.map(row => parseDmsSourceCoordinateRow(row)?.label || "");
    const baselineOrderProven = baselineLabels.every(label => !label)
      || hasContinuousNumberedRows(baselineRows);
    if (baseline.groupCount !== 1 || !baselineOrderProven) {
      return Object.freeze({ accepted: false, reason: "PARTIAL_BASELINE_SINGLE_ORDERED_GROUP_REQUIRED" });
    }
    const baselinePoints = baselineRows.map(parseDmsSourceCoordinateRow);
    const firstRetryGroupPoints = retry.groups[0].rows.map(parseDmsSourceCoordinateRow);
    if (baselinePoints.some(point => !point) || firstRetryGroupPoints.some(point => !point)) {
      return Object.freeze({ accepted: false, reason: "UNPARSEABLE_DMS_ROW" });
    }
    if (baselinePoints.length !== firstRetryGroupPoints.length
      || baselinePoints.some((point, index) => !pointsEquivalent(point, firstRetryGroupPoints[index]))) {
      return Object.freeze({ accepted: false, reason: "PARTIAL_BASELINE_POINT_CHANGED_MISSING_OR_REORDERED" });
    }
    const normalized = normalizedCoordinatesFromDmsStructure(retryText);
    if (!normalized.accepted) return normalized;
    return Object.freeze({
      accepted: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_REVIEW_REQUIRED",
      recoveryMode: "STAGE1_PARTIAL_MULTISITE_8_TO_16",
      baselineRowCount: baseline.rowCount,
      retryRowCount: retry.rowCount,
      addedRowCount: retry.rowCount - baseline.rowCount,
      baselineRowsPreserved: true,
      groupLocalBaselineRowsPreserved: true,
      baselineGroupCount: baseline.groupCount,
      baselineGroupSizes: Object.freeze(baseline.groups.map(group => group.rows.length)),
      baselineGroupIdentities: Object.freeze([]),
      groupCount: retry.groupCount,
      groupSizes: normalized.groupSizes,
      groupIdentities: normalized.groupIdentities,
      normalizedCoordinates: normalized.output
    });
  }
  for (let groupIndex = 0; groupIndex < baseline.groups.length; groupIndex += 1) {
    const baselinePoints = baseline.groups[groupIndex].rows.map(parseDmsSourceCoordinateRow);
    const retryPoints = retry.groups[groupIndex].rows.map(parseDmsSourceCoordinateRow);
    if (baselinePoints.some(point => !point) || retryPoints.some(point => !point)) {
      return Object.freeze({ accepted: false, reason: "UNPARSEABLE_DMS_ROW" });
    }
    if (baselinePoints.length > retryPoints.length) {
      return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_ROW_REMOVED" });
    }
    const used = new Set();
    for (const baselinePoint of baselinePoints) {
      const matches = retryPoints
        .map((retryPoint, index) => ({ retryPoint, index }))
        .filter(({ retryPoint, index }) => !used.has(index) && pointsEquivalent(baselinePoint, retryPoint));
      if (matches.length !== 1) {
        return Object.freeze({ accepted: false, reason: matches.length ? "BASELINE_GROUP_MATCH_AMBIGUOUS" : "BASELINE_GROUP_POINT_CHANGED_OR_MISSING" });
      }
      used.add(matches[0].index);
    }
  }
  const normalized = normalizedCoordinatesFromDmsStructure(retryText);
  if (!normalized.accepted) return normalized;
  return Object.freeze({
    accepted: true,
    reason: "DMS_GROUPED_ACQUISITION_EXPANSION_REVIEW_REQUIRED",
    baselineRowCount: baseline.rowCount,
    retryRowCount: retry.rowCount,
    addedRowCount: retry.rowCount - baseline.rowCount,
    baselineRowsPreserved: true,
    groupLocalBaselineRowsPreserved: true,
    baselineGroupCount: baseline.groupCount,
    baselineGroupSizes: Object.freeze(baseline.groups.map(group => group.rows.length)),
    baselineGroupIdentities: Object.freeze(baselineIdentities),
    groupCount: retry.groupCount,
    groupSizes: normalized.groupSizes,
    groupIdentities: normalized.groupIdentities,
    normalizedCoordinates: normalized.output
  });
}

export function buildDmsGroupedPartialMultisiteRecoveryCandidate({
  stage1RawText = "",
  stage1Coordinates = "",
  retryRawText = "",
  retryCoordinates = "",
  expansion = {},
  ownerFamily = "",
  weakPartialInputEvidence = null,
  allowUnsanitizedWeakPartialStage1 = false,
  imageInputBuffer = null
} = {}) {
  if (ownerFamily !== "dms_grouped"
    || ![stage1RawText, stage1Coordinates, retryRawText, retryCoordinates]
      .every(value => typeof value === "string" && value.trim())) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_CANDIDATE_INVALID"
    });
  }
  const declaredRecoveryMode = String(expansion?.recoveryMode || "");
  const weakPartialRecovery = declaredRecoveryMode === "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16";
  const boundWeakPartialInputEvidence = weakPartialInputEvidence
    ?? expansion?.weakPartialInputEvidence
    ?? null;
  if (weakPartialRecovery && hasRejectedDmsCandidateLine(stage1RawText)
    && allowUnsanitizedWeakPartialStage1 !== true) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_REJECTED_RAW_EVIDENCE_FORBIDDEN"
    });
  }
  if ((!weakPartialRecovery && hasRejectedDmsCandidateLine(stage1RawText))
    || hasRejectedDmsCandidateLine(retryRawText)) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_MALFORMED_SOURCE_ROW"
    });
  }
  const recomputedExpansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1RawText,
    retryText: retryRawText,
    allowPartialMultisiteRecovery: true,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: boundWeakPartialInputEvidence?.inputModality === "IMAGE"
      && boundWeakPartialInputEvidence?.imageInputEvidenceSource === "REQUEST_FILE_PRESENT",
    projectedTableSignal: getDmsDocumentEvidence(stage1RawText).projectedTableSignal,
    explicitHandwrittenSignal: boundWeakPartialInputEvidence?.independentHandwrittenSignal,
    weakPartialCandidateSignal: getDmsDocumentEvidence(stage1RawText).weakPartialMultisiteRecoveryCandidateSignal,
    weakPartialQualification: expansion?.weakPartialQualification,
    weakPartialRejectedEvidence: expansion?.stage1RejectedEvidence,
    weakPartialInputEvidence: boundWeakPartialInputEvidence,
    imageInputBuffer
  });
  if (recomputedExpansion.accepted !== true
    || ![
      "STAGE1_PARTIAL_MULTISITE_8_TO_16",
      "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16"
    ].includes(recomputedExpansion.recoveryMode)) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_CONTENT_UNPROVEN"
    });
  }
  const sameArray = (left, right) => Array.isArray(left)
    && Array.isArray(right)
    && left.length === right.length
    && left.every((value, index) => value === right[index]);
  const expansionBoundToContent = expansion?.accepted === true
    && expansion?.recoveryMode === recomputedExpansion.recoveryMode
    && expansion?.baselineRowCount === recomputedExpansion.baselineRowCount
    && expansion?.retryRowCount === recomputedExpansion.retryRowCount
    && expansion?.addedRowCount === recomputedExpansion.addedRowCount
    && expansion?.baselineRowsPreserved === recomputedExpansion.baselineRowsPreserved
    && expansion?.groupLocalBaselineRowsPreserved === recomputedExpansion.groupLocalBaselineRowsPreserved
    && expansion?.baselineGroupCount === recomputedExpansion.baselineGroupCount
    && expansion?.groupCount === recomputedExpansion.groupCount
    && sameArray(expansion?.baselineGroupSizes, recomputedExpansion.baselineGroupSizes)
    && sameArray(expansion?.baselineGroupIdentities, recomputedExpansion.baselineGroupIdentities)
    && sameArray(expansion?.groupSizes, recomputedExpansion.groupSizes)
    && sameArray(expansion?.groupIdentities, recomputedExpansion.groupIdentities)
    && (!weakPartialRecovery || (
      weakPartialQualificationMatches(
        expansion?.weakPartialQualification,
        recomputedExpansion.weakPartialQualification
      )
      && weakPartialRejectedEvidenceMatches(
        expansion?.stage1RejectedEvidence,
        recomputedExpansion.stage1RejectedEvidence,
        recomputedExpansion.baselineRowCount
      )
      && weakPartialInputEvidenceMatches(
        boundWeakPartialInputEvidence,
        recomputedExpansion.weakPartialInputEvidence,
        stage1RawText,
        recomputedExpansion.stage1RejectedEvidence,
        imageInputBuffer
      )
    ));
  if (!expansionBoundToContent) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_EXPANSION_BINDING_MISMATCH"
    });
  }
  const stage1Pointwise = evaluateDmsNormalizedPointwiseEquivalence({
    structureText: stage1RawText,
    normalizedCoordinates: stage1Coordinates
  });
  if (stage1Pointwise.accepted !== true) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_STAGE1_POINTWISE_MISMATCH"
    });
  }
  const retryPointwise = evaluateDmsNormalizedPointwiseEquivalence({
    structureText: retryRawText,
    normalizedCoordinates: retryCoordinates
  });
  if (retryPointwise.accepted !== true) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RETRY_POINTWISE_MISMATCH"
    });
  }
  const retainedStage1RawText = weakPartialRecovery
    ? sanitizeWeakPartialMultisiteStage1Text(stage1RawText)
    : stage1RawText;
  const stage1Candidate = Object.freeze({ rawText: retainedStage1RawText, coordinates: stage1Coordinates });
  const retryCandidate = Object.freeze({ rawText: retryRawText, coordinates: retryCoordinates });
  if (weakPartialRecovery && !bindOrValidateWeakPartialRuntimeAttestation({
    inputEvidence: boundWeakPartialInputEvidence,
    imageInputBuffer,
    stage1RawText,
    rejectedEvidence: recomputedExpansion.stage1RejectedEvidence,
    retryCandidate
  })) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_WEAK_PARTIAL_RUNTIME_ATTESTATION_REPLAY_OR_BINDING_INVALID"
    });
  }
  const provenance = Object.freeze({
    schemaVersion: "dms_grouped_partial_multisite_recovery_v1",
    ownerFamily,
    recoverySource: "dms_grouped",
    recoveryMode: recomputedExpansion.recoveryMode,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    baselineRowCount: recomputedExpansion.baselineRowCount,
    retryRowCount: recomputedExpansion.retryRowCount,
    addedRowCount: recomputedExpansion.addedRowCount,
    baselineRowsPreserved: true,
    groupLocalBaselineRowsPreserved: true,
    pointwiseBaselineEquivalenceProven: true,
    pointwiseAcquisitionDeltaProven: true,
    strongBoundariesProven: true,
    labelsContinuous: true,
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    baselineGroupCount: recomputedExpansion.baselineGroupCount,
    baselineGroupSizes: Object.freeze([...recomputedExpansion.baselineGroupSizes]),
    baselineGroupIdentities: Object.freeze([...(recomputedExpansion.baselineGroupIdentities || [])]),
    retryGroupCount: recomputedExpansion.groupCount,
    retryGroupSizes: Object.freeze([...recomputedExpansion.groupSizes]),
    retryGroupIdentities: Object.freeze([...recomputedExpansion.groupIdentities]),
    ...(recomputedExpansion.recoveryMode === "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16"
      ? {
        weakPartialQualification: Object.freeze({ ...recomputedExpansion.weakPartialQualification }),
        weakPartialInputEvidenceSha256: weakPartialInputEvidenceSha256(
          recomputedExpansion.weakPartialInputEvidence
        ),
        stage1RejectedEvidence: Object.freeze({
          ...recomputedExpansion.stage1RejectedEvidence,
          rejectionReasonCounts: Object.freeze(Object.fromEntries(
            WEAK_PARTIAL_REPAIRABLE_REJECTIONS.map(reason => [
              reason,
              recomputedExpansion.stage1RejectedEvidence.rejectionReasonCounts[reason]
            ])
          ))
        })
      }
      : {}),
    stage1CandidateSha256: createHash("sha256").update(JSON.stringify(stage1Candidate)).digest("hex"),
    retryCandidateSha256: createHash("sha256").update(JSON.stringify(retryCandidate)).digest("hex")
  });
  if (provenance.stage1CandidateSha256 === provenance.retryCandidateSha256) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_CANDIDATES_NOT_SEPARATE"
    });
  }
  return Object.freeze({
    accepted: true,
    failClosed: false,
    reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_CANDIDATE_RETAINED",
    stage1Candidate,
    rawText: retryCandidate.rawText,
    normalizedCoordinates: retryCandidate.coordinates,
    weakPartialInputEvidence: weakPartialRecovery
      ? Object.freeze({ ...recomputedExpansion.weakPartialInputEvidence })
      : null,
    provenance
  });
}

export function partialMultisiteRecoveryProvenanceMatches(declared = {}, recomputed = {}) {
  const scalarFields = [
    "schemaVersion", "ownerFamily", "recoverySource", "recoveryMode", "candidateRole",
    "baselineRowCount", "retryRowCount", "addedRowCount", "baselineRowsPreserved",
    "groupLocalBaselineRowsPreserved", "pointwiseBaselineEquivalenceProven",
    "pointwiseAcquisitionDeltaProven", "strongBoundariesProven", "labelsContinuous",
    "sourceCandidateSeparate", "directCanonicalPromotion", "baselineGroupCount",
    "retryGroupCount", "stage1CandidateSha256", "retryCandidateSha256",
    "weakPartialInputEvidenceSha256"
  ];
  const arrayFields = [
    "baselineGroupSizes", "baselineGroupIdentities", "retryGroupSizes", "retryGroupIdentities"
  ];
  const rejectedEvidenceMatches = (declared?.stage1RejectedEvidence == null
    && recomputed?.stage1RejectedEvidence == null)
    || weakPartialRejectedEvidenceMatches(
      declared?.stage1RejectedEvidence,
      recomputed?.stage1RejectedEvidence,
      recomputed?.baselineRowCount
    );
  const qualificationMatches = (declared?.weakPartialQualification == null
    && recomputed?.weakPartialQualification == null)
    || weakPartialQualificationMatches(
      declared?.weakPartialQualification,
      recomputed?.weakPartialQualification
    );
  return declared && typeof declared === "object" && !Array.isArray(declared)
    && scalarFields.every(field => declared[field] === recomputed[field])
    && arrayFields.every(field => Array.isArray(declared[field])
      && Array.isArray(recomputed[field])
      && declared[field].length === recomputed[field].length
      && declared[field].every((value, index) => value === recomputed[field][index]))
    && rejectedEvidenceMatches
    && qualificationMatches;
}

export function buildDmsGroupedRetryFailClosedPatch(reason, parserTrace = []) {
  const trace = Array.isArray(parserTrace) ? [...parserTrace] : [];
  if (!trace.includes("DMS_GROUPED:boundary_unresolved_review_required")) {
    trace.push("DMS_GROUPED:boundary_unresolved_review_required");
  }
  return Object.freeze({
    reason: String(reason || "DMS_GROUPED_RETRY_FAILED"),
    coordinates: "",
    dmsGroupedAccepted: false,
    dmsAccepted: false,
    resetChatCoordinates: true,
    resetWgs84TableCoordinates: true,
    parserTrace: Object.freeze(trace)
  });
}

export function createDmsGroupedRetryOrchestrator({ activeFamilyOwner = null, parserTrace = [] } = {}) {
  let owner = String(activeFamilyOwner || "").trim() || null;
  let reservedOwner = null;
  let failurePatch = null;
  const trace = Array.isArray(parserTrace) ? parserTrace : [];
  return Object.freeze({
    failClose(reason) {
      failurePatch = buildDmsGroupedRetryFailClosedPatch(reason, trace);
      return failurePatch;
    },
    reserve(targetOwner) {
      if (failurePatch) return false;
      const target = String(targetOwner || "").trim();
      if (!target || (reservedOwner && reservedOwner !== target)) return false;
      const authorization = authorizeFamilyRetryDispatch({ activeFamilyOwner: owner, targetOwner: target });
      if (!authorization.allowed) return false;
      reservedOwner = target;
      return true;
    },
    claim(targetOwner) {
      if (failurePatch) return false;
      const target = String(targetOwner || "").trim();
      if (reservedOwner && target !== reservedOwner) return false;
      const authorization = authorizeFamilyRetryDispatch({ activeFamilyOwner: owner, targetOwner: target });
      if (!authorization.allowed) return false;
      owner = target;
      return true;
    },
    snapshot() {
      return Object.freeze({
        activeFamilyOwner: owner,
        reservedFamilyOwner: reservedOwner,
        failed: Boolean(failurePatch),
        failureReason: failurePatch?.reason || null
      });
    }
  });
}

export function evaluateDmsGroupedRetryCoverage({ baselineText = "", retryText = "" } = {}) {
  const baselineStructure = extractDmsSourceStructure(baselineText);
  const retryStructure = extractDmsSourceStructure(retryText);

  if (baselineStructure.rowCount === 0) {
    return Object.freeze({ accepted: false, reason: "NO_BASELINE_DMS_ROWS" });
  }
  if (retryStructure.rowCount === 0) {
    return Object.freeze({ accepted: false, reason: "NO_RETRY_DMS_ROWS" });
  }
  if (retryStructure.rowCount !== baselineStructure.rowCount) {
    return Object.freeze({ accepted: false, reason: "ROW_COUNT_MISMATCH" });
  }
  if (baselineStructure.groupCount > 1 && retryStructure.groupCount !== baselineStructure.groupCount) {
    return Object.freeze({ accepted: false, reason: "GROUP_COUNT_MISMATCH" });
  }

  const baselineGroups = parsedGroupsFromStructure(baselineStructure);
  const retryGroups = parsedGroupsFromStructure(retryStructure);
  for (let groupIndex = 0; groupIndex < baselineGroups.length; groupIndex += 1) {
    const baselineGroup = baselineGroups[groupIndex];
    const retryGroup = retryGroups[groupIndex];
    if (!retryGroup) return Object.freeze({ accepted: false, reason: "MISSING_GROUP" });
    if (baselineGroup.points.length !== retryGroup.points.length) {
      return Object.freeze({ accepted: false, reason: "GROUP_SIZE_MISMATCH" });
    }
    if (baselineGroup.identity !== retryGroup.identity) {
      return Object.freeze({ accepted: false, reason: "GROUP_IDENTITY_MISMATCH" });
    }

    for (let pointIndex = 0; pointIndex < baselineGroup.points.length; pointIndex += 1) {
      const baselinePoint = baselineGroup.points[pointIndex];
      const retryPoint = retryGroup.points[pointIndex];
      if (!baselinePoint || !retryPoint) return Object.freeze({ accepted: false, reason: "UNPARSEABLE_DMS_ROW" });
      if (baselinePoint.axisOrder !== retryPoint.axisOrder) {
        return Object.freeze({ accepted: false, reason: "AXIS_ORDER_MISMATCH" });
      }
      if (baselinePoint.latitudeHemisphere !== retryPoint.latitudeHemisphere
        || baselinePoint.longitudeHemisphere !== retryPoint.longitudeHemisphere) {
        return Object.freeze({ accepted: false, reason: "HEMISPHERE_MISMATCH" });
      }
      if (baselinePoint.label && (!retryPoint.label || normalizeLabel(baselinePoint.label) !== normalizeLabel(retryPoint.label))) {
        return Object.freeze({ accepted: false, reason: "LABEL_ORDER_MISMATCH" });
      }

      if (!closeEnough(retryPoint.latitude, baselinePoint.latitude, baselinePoint.latitudeTolerance)
        || !closeEnough(retryPoint.longitude, baselinePoint.longitude, baselinePoint.longitudeTolerance)) {
        return Object.freeze({ accepted: false, reason: "POINT_VALUE_MISMATCH" });
      }
    }
  }

  return Object.freeze({
    accepted: true,
    reason: "EXACT_DMS_STRUCTURE_COVERAGE",
    rowCount: retryStructure.rowCount,
    groupCount: retryStructure.groupCount
  });
}
