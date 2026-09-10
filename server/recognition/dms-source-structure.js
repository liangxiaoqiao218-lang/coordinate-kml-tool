import { authorizeFamilyRetryDispatch } from "./family-retry-policy.js";

const DMS_COMPONENT_PATTERN = /[-+]?\d{1,3}\s*[°º]\s*\d{1,2}\s*['′’]\s*\d{1,2}(?:[.,]\d+)?\s*["″”]?\s*(?:N|S|E|W|O|NORD|NORTH|SUD|SOUTH|EST|EAST|OUEST|WEST)?/gi;

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
  const laterBoundaries = groups.slice(1).map(group => group.boundaryProvenance);
  const allBoundariesProven = groups.length > 1 && laterBoundaries.every(value => [
    "section_title",
    "repeated_table_header",
    "number_restart"
  ].includes(value));
  const hasProvenNumberRestart = groups.some((group, index) => index > 0
    && group.boundaryProvenance === "number_restart"
    && hasContinuousNumberedRows(group.rows));
  const documentHasStrongMultiRegionEvidence = hasDmsGroupBoundaryContext(text) || hasProvenNumberRestart;
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
  return Boolean(
    (laterBoundariesProven && hasDmsGroupBoundaryContext(text))
    || (laterBoundariesProven && namedIdentities.length >= 2)
    || strongNumberRestart
  );
}

export function evaluateDmsGroupedRoutePriority({
  isImageInput = false,
  printedTableSignal = false,
  explicitHandwrittenSignal = false,
  structureText = ""
} = {}) {
  const structure = extractDmsSourceStructure(structureText);
  const typedDmsGrouped = Boolean(isImageInput
    && printedTableSignal
    && !explicitHandwrittenSignal
    && structure.rowCount > 0
    && structure.documentHasStrongMultiRegionEvidence);
  return Object.freeze({
    typedDmsGrouped,
    suppressHandwrittenRetry: typedDmsGrouped,
    documentHasStrongMultiRegionEvidence: structure.documentHasStrongMultiRegionEvidence,
    allBoundariesProven: structure.allBoundariesProven,
    hasUnprovenBoundary: structure.hasUnprovenBoundary,
    reason: typedDmsGrouped ? "PRINTED_MULTI_SITE_DMS" : "DMS_GROUPED_PRIORITY_NOT_ESTABLISHED"
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
  typedRouteEligible = false
} = {}) {
  const structure = extractDmsSourceStructure(rawText);
  const explicitEvidence = hasExplicitDmsMultiRegionEvidence(rawText, dmsGroupedInfo);
  const typedVerificationRequired = Boolean(typedRouteEligible
    && structure.documentHasStrongMultiRegionEvidence
    && structure.rowCount === 13);
  const needsRepair = typedVerificationRequired || (explicitEvidence && (
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
  return Object.freeze({ allowed: true, failClosed: false, reason: "DMS_GROUPED_RETRY_AUTHORIZED" });
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
    outputGroups.push(parsed.map(point => `${point.latitude},${point.longitude}`));
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

export function evaluateDmsGroupedAcquisitionExpansion({ baselineText = "", retryText = "" } = {}) {
  const baseline = extractDmsSourceStructure(baselineText);
  const retry = extractDmsSourceStructure(retryText);
  if (!baseline.rowCount) return Object.freeze({ accepted: false, reason: "NO_BASELINE_DMS_ROWS" });
  if (!retry.rowCount) return Object.freeze({ accepted: false, reason: "NO_RETRY_DMS_ROWS" });
  if (retry.rowCount <= baseline.rowCount) return Object.freeze({ accepted: false, reason: "RETRY_NOT_EXPANDED" });
  if (baseline.rowCount !== 13 || retry.rowCount !== 16) {
    return Object.freeze({ accepted: false, reason: "UNSUPPORTED_ACQUISITION_DELTA_SIZE" });
  }
  if (!retry.documentHasStrongMultiRegionEvidence || !retry.allBoundariesProven
    || !hasExplicitDmsMultiRegionEvidence(retryText)) {
    return Object.freeze({ accepted: false, reason: "RETRY_BOUNDARY_EVIDENCE_UNPROVEN" });
  }
  if (baseline.groupCount !== 3 || retry.groupCount !== 3
    || !baseline.documentHasStrongMultiRegionEvidence || !baseline.allBoundariesProven
    || !hasExplicitDmsMultiRegionEvidence(baselineText)) {
    return Object.freeze({ accepted: false, reason: "THREE_GROUP_BOUNDARY_CONTRACT_REQUIRED" });
  }
  const baselineIdentities = baseline.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  const identities = retry.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  if (baselineIdentities.some(identity => !identity)
    || identities.some(identity => !identity)
    || new Set(baselineIdentities).size !== baselineIdentities.length
    || new Set(identities).size !== identities.length) {
    return Object.freeze({ accepted: false, reason: "RETRY_GROUP_IDENTITY_INVALID" });
  }
  if (baselineIdentities.some((identity, index) => identity !== identities[index])) {
    return Object.freeze({ accepted: false, reason: "BASELINE_GROUP_IDENTITY_OR_ORDER_MISMATCH" });
  }
  if (!groupsHaveContinuousLabels(retry)) {
    return Object.freeze({ accepted: false, reason: "RETRY_LABEL_SEQUENCE_INVALID" });
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
  let failurePatch = null;
  const trace = Array.isArray(parserTrace) ? parserTrace : [];
  return Object.freeze({
    failClose(reason) {
      failurePatch = buildDmsGroupedRetryFailClosedPatch(reason, trace);
      return failurePatch;
    },
    claim(targetOwner) {
      if (failurePatch) return false;
      const authorization = authorizeFamilyRetryDispatch({ activeFamilyOwner: owner, targetOwner });
      if (!authorization.allowed) return false;
      owner = String(targetOwner || "").trim();
      return true;
    },
    snapshot() {
      return Object.freeze({
        activeFamilyOwner: owner,
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
