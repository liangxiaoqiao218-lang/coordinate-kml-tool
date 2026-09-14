import { createHash } from "node:crypto";
import { authorizeFamilyRetryDispatch } from "./family-retry-policy.js";

const DMS_COMPONENT_PATTERN = /[-+]?\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?/gi;

export const DMS_RETRY_ROUTE_CLASSIFICATION = Object.freeze({
  DMS_GROUPED_ONLY: "DMS_GROUPED_ONLY",
  DMS_GROUPED_PARTIAL_RECOVERY_ONLY: "DMS_GROUPED_PARTIAL_RECOVERY_ONLY",
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
      && documentEvidence?.partialMultisiteRecoveryCandidateSignal === true
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
  partialMultisiteRecoveryCandidateSignal = false
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
  partialMultisiteRecoveryEligible = false
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
  const needsRepair = partialRecoveryRequired || typedVerificationRequired || (explicitEvidence && (
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
    reason: partialRecoveryRequired
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
  allowPartialMultisiteRecovery = false
} = {}) {
  const baseline = extractDmsSourceStructure(baselineText);
  const retry = extractDmsSourceStructure(retryText);
  if (!baseline.rowCount) return Object.freeze({ accepted: false, reason: "NO_BASELINE_DMS_ROWS" });
  if (!retry.rowCount) return Object.freeze({ accepted: false, reason: "NO_RETRY_DMS_ROWS" });
  if (retry.rowCount <= baseline.rowCount) return Object.freeze({ accepted: false, reason: "RETRY_NOT_EXPANDED" });
  const partialMultisiteRecovery = allowPartialMultisiteRecovery === true
    && baseline.rowCount === 8
    && retry.rowCount === 16;
  const establishedAcquisitionExpansion = baseline.rowCount === 13 && retry.rowCount === 16;
  if (!partialMultisiteRecovery && !establishedAcquisitionExpansion) {
    return Object.freeze({ accepted: false, reason: "UNSUPPORTED_ACQUISITION_DELTA_SIZE" });
  }
  if (!retry.documentHasStrongMultiRegionEvidence || !retry.allBoundariesProven
    || !hasExplicitDmsMultiRegionEvidence(retryText)) {
    return Object.freeze({ accepted: false, reason: "RETRY_BOUNDARY_EVIDENCE_UNPROVEN" });
  }
  if (!partialMultisiteRecovery && (baseline.groupCount !== 3
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
  if (!partialMultisiteRecovery && (baselineIdentities.some(identity => !identity)
    || new Set(baselineIdentities).size !== baselineIdentities.length)) {
    return Object.freeze({ accepted: false, reason: "RETRY_GROUP_IDENTITY_INVALID" });
  }
  if (!partialMultisiteRecovery && baselineIdentities.some((identity, index) => identity !== identities[index])) {
    return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_IDENTITY_OR_ORDER_MISMATCH" });
  }
  if (!groupsHaveContinuousLabels(retry)) {
    return Object.freeze({ accepted: false, reason: "RETRY_LABEL_SEQUENCE_INVALID" });
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
  ownerFamily = ""
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
  if (hasRejectedDmsCandidateLine(stage1RawText) || hasRejectedDmsCandidateLine(retryRawText)) {
    return Object.freeze({
      accepted: false,
      failClosed: true,
      reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_MALFORMED_SOURCE_ROW"
    });
  }
  const recomputedExpansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1RawText,
    retryText: retryRawText,
    allowPartialMultisiteRecovery: true
  });
  if (recomputedExpansion.accepted !== true
    || recomputedExpansion.recoveryMode !== "STAGE1_PARTIAL_MULTISITE_8_TO_16") {
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
    && sameArray(expansion?.groupIdentities, recomputedExpansion.groupIdentities);
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
  const stage1Candidate = Object.freeze({ rawText: stage1RawText, coordinates: stage1Coordinates });
  const retryCandidate = Object.freeze({ rawText: retryRawText, coordinates: retryCoordinates });
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
    "retryGroupCount", "stage1CandidateSha256", "retryCandidateSha256"
  ];
  const arrayFields = [
    "baselineGroupSizes", "baselineGroupIdentities", "retryGroupSizes", "retryGroupIdentities"
  ];
  return declared && typeof declared === "object" && !Array.isArray(declared)
    && scalarFields.every(field => declared[field] === recomputed[field])
    && arrayFields.every(field => Array.isArray(declared[field])
      && Array.isArray(recomputed[field])
      && declared[field].length === recomputed[field].length
      && declared[field].every((value, index) => value === recomputed[field][index]));
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
