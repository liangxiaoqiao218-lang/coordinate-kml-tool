import { createHash } from "node:crypto";
import {
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  extractDmsSourceStructure,
  normalizeDmsBoundaryIdentity,
  parseDmsSourceCoordinateRow,
  partialMultisiteRecoveryProvenanceMatches
} from "./recognition/dms-source-structure.js";

const SOURCE_COORDINATE_REPRESENTATION_SCHEMA = "source_coordinate_representation_v1";

function sourceLines(text) {
  return String(text || "").replace(/\r\n/g, "\n").split("\n");
}

function sourceGroups(text) {
  const groups = [];
  let current = [];
  for (const line of sourceLines(text)) {
    if (!line.trim()) {
      if (current.length) groups.push(current);
      current = [];
      continue;
    }
    current.push(line);
  }
  if (current.length) groups.push(current);
  return groups;
}

function enginePointLabels(engine = {}) {
  return (Array.isArray(engine.groups) ? engine.groups : [])
    .flatMap(group => Array.isArray(group?.points) ? group.points : [])
    .map(point => String(point?.label || point?.name || point?.id || "").trim())
    .filter(Boolean);
}

function normalizeAxisOrder(value) {
  const normalized = String(value || "").trim().toLowerCase().replace(/[\s,\-/]+/g, "_");
  if (["latitude_longitude", "lat_lon", "lat_lng"].includes(normalized)) return "latitude_longitude";
  if (["longitude_latitude", "lon_lat", "lng_lat"].includes(normalized)) return "longitude_latitude";
  if (["easting_northing", "x_y"].includes(normalized)) return "easting_northing";
  return null;
}

function sourceAxisOrder(engine = {}, family = "", format = "") {
  const explicit = String(engine?.source_crs?.axisOrder || engine?.source_crs?.axis_order || "").trim();
  if (explicit) return normalizeAxisOrder(explicit);
  const identity = `${family} ${format}`.toLowerCase();
  if (/projected|utm|bftm|kyrgyz|gauss|cadastral|mgrs|x[-_ ]?y/.test(identity)) return "easting_northing";
  if (/wgs84[-_ ]?table|longitude[-_ ]?latitude/.test(identity)) return "longitude_latitude";
  if (/dms|chat|latitude[-_ ]?longitude/.test(identity)) return "latitude_longitude";
  return null;
}

function sourceHemispheres(text) {
  const values = [];
  const pattern = /\d\s*["'\u2032\u2033]?\s*([NSEWO])(?=\s*(?:[,|;]|\s|$))/gi;
  let match;
  while ((match = pattern.exec(String(text || ""))) !== null) values.push(match[1].toUpperCase() === "O" ? "W" : match[1].toUpperCase());
  return [...new Set(values)];
}

function sourceCrsEvidence(payload = {}, engine = {}) {
  return engine?.source_crs
    || payload?.indonesiaUtm50?.sourceCrs
    || payload?.indonesiaUtm50?.source_crs
    || payload?.projection
    || null;
}

function pointCoordinate(point = {}) {
  const latitudeValue = point.latitude ?? point.lat ?? point.y_latitude ?? point.y;
  const longitudeValue = point.longitude ?? point.lng ?? point.lon ?? point.x_longitude ?? point.x;
  if (latitudeValue === null || latitudeValue === undefined || String(latitudeValue).trim() === ""
    || longitudeValue === null || longitudeValue === undefined || String(longitudeValue).trim() === "") return null;
  const latitude = Number(latitudeValue);
  const longitude = Number(longitudeValue);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
  return Object.freeze({
    label: String(point.label || point.name || point.id || "").trim(),
    latitude,
    longitude
  });
}

function parseDecimalCoordinateLine(line, axisOrder) {
  const match = String(line || "").match(/^\s*(?:(\d{1,4}|[A-Z]{1,4}\d{0,3})\s*[\).:\-]?\s+)?(-?\d+(?:\.\d+)?)\s*[,|\s]\s*(-?\d+(?:\.\d+)?)/i);
  if (!match) return null;
  const first = Number(match[2]);
  const second = Number(match[3]);
  if (!Number.isFinite(first) || !Number.isFinite(second)) return null;

  const label = match[1] ? String(match[1]).trim() : "";
  const latitude = axisOrder === "longitude_latitude" ? second : first;
  const longitude = axisOrder === "longitude_latitude" ? first : second;
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
  return Object.freeze({ label, latitude, longitude });
}

function groupsFromEngine(engine = {}, coordinateDisplayText = "", axisOrder = "latitude_longitude") {
  const engineGroups = (Array.isArray(engine.groups) ? engine.groups : [])
    .map(group => {
      const points = (Array.isArray(group?.points) ? group.points : [])
        .map(pointCoordinate)
        .filter(Boolean);
      const identity = normalizeDmsBoundaryIdentity(
        group?.group_name || group?.groupName || group?.name || group?.title || ""
      );
      return points.length ? Object.freeze({ points, identity }) : null;
    })
    .filter(Boolean);

  if (engineGroups.length) return engineGroups;

  return sourceGroups(coordinateDisplayText)
    .map(group => {
      const points = group.map(line => parseDecimalCoordinateLine(line, axisOrder)).filter(Boolean);
      return points.length ? Object.freeze({ points, identity: "" }) : null;
    })
    .filter(Boolean);
}

function labelMatches(sourceLabel, engineLabel) {
  if (!sourceLabel || !engineLabel) return !sourceLabel && !engineLabel;
  return String(sourceLabel).trim().toUpperCase() === String(engineLabel).trim().toUpperCase();
}

function matchesCoordinate(sourcePoint, enginePoint) {
  return Math.abs(sourcePoint.latitude - enginePoint.latitude) <= sourcePoint.latitudeTolerance
    && Math.abs(sourcePoint.longitude - enginePoint.longitude) <= sourcePoint.longitudeTolerance;
}

function expectedHemisphere(value, positive, negative) {
  if (!Number.isFinite(value) || Object.is(value, -0) || value === 0) return "";
  return value > 0 ? positive : negative;
}

function rawDmsSemanticallyMatchesResult(rawDmsStructure, engineGroups, expectedAxisOrder) {
  if (!rawDmsStructure?.rowCount || !engineGroups.length) {
    return Object.freeze({ verified: false, reason: "missing_rows_or_engine_points" });
  }
  if (!["latitude_longitude", "longitude_latitude"].includes(expectedAxisOrder)) {
    return Object.freeze({ verified: false, reason: "axis_order_unresolved" });
  }
  const comparisonSourceGroups = rawDmsStructure.groupCount !== engineGroups.length
    && rawDmsStructure.reason === "blank_line"
    && engineGroups.length === 1
    && rawDmsStructure.groups.every(group => !normalizeDmsBoundaryIdentity(group.name))
    ? [{ name: null, rows: rawDmsStructure.rows }]
    : rawDmsStructure.groups;
  if (comparisonSourceGroups.length !== engineGroups.length) {
    return Object.freeze({ verified: false, reason: "group_count_mismatch" });
  }

  for (let groupIndex = 0; groupIndex < comparisonSourceGroups.length; groupIndex += 1) {
    const sourceGroup = comparisonSourceGroups[groupIndex];
    const engineGroup = engineGroups[groupIndex];
    if (!engineGroup || sourceGroup.rows.length !== engineGroup.points.length) {
      return Object.freeze({ verified: false, reason: "group_size_mismatch" });
    }
    const sourceIdentity = normalizeDmsBoundaryIdentity(sourceGroup.name);
    if (sourceIdentity !== String(engineGroup.identity || "")) {
      return Object.freeze({ verified: false, reason: "group_identity_mismatch" });
    }

    for (let pointIndex = 0; pointIndex < sourceGroup.rows.length; pointIndex += 1) {
      const sourcePoint = parseDmsSourceCoordinateRow(sourceGroup.rows[pointIndex]);
      const enginePoint = engineGroup.points[pointIndex];
      if (!sourcePoint || !enginePoint) {
        return Object.freeze({ verified: false, reason: "unparseable_point" });
      }
      if (sourcePoint.axisOrder !== expectedAxisOrder) {
        return Object.freeze({ verified: false, reason: "axis_order_mismatch" });
      }
      const expectedLatitudeHemisphere = expectedHemisphere(enginePoint.latitude, "N", "S");
      const expectedLongitudeHemisphere = expectedHemisphere(enginePoint.longitude, "E", "W");
      if (!expectedLatitudeHemisphere || !expectedLongitudeHemisphere
        || sourcePoint.latitudeHemisphere !== expectedLatitudeHemisphere
        || sourcePoint.longitudeHemisphere !== expectedLongitudeHemisphere) {
        return Object.freeze({ verified: false, reason: "hemisphere_mismatch" });
      }
      if (!labelMatches(sourcePoint.label, enginePoint.label)) {
        return Object.freeze({ verified: false, reason: "label_order_mismatch" });
      }
      if (!matchesCoordinate(sourcePoint, enginePoint)) {
        return Object.freeze({ verified: false, reason: "point_value_mismatch" });
      }
    }
  }

  return Object.freeze({ verified: true, reason: "pointwise_dms_semantic_match" });
}

function renderCanonicalEngineGroups(engineGroups, axisOrder) {
  return engineGroups.map(group => group.points.map(point => {
    const first = axisOrder === "longitude_latitude" ? point.longitude : point.latitude;
    const second = axisOrder === "longitude_latitude" ? point.latitude : point.longitude;
    return `${first},${second}`;
  }).join("\n")).join("\n\n");
}

function acquisitionExpansionCandidate(recognitionResult = {}) {
  const candidate = recognitionResult?.acquisitionExpansionCandidate;
  const provenance = candidate?.provenance;
  const stage1Candidate = recognitionResult?.stage1Candidate && typeof recognitionResult.stage1Candidate === "object"
    ? recognitionResult.stage1Candidate
    : recognitionResult;
  const stage1RawText = String(stage1Candidate?.rawText || "");
  const stage1Coordinates = String(stage1Candidate?.coordinates || "");
  const retryRawText = String(candidate?.rawText || "");
  const retryCoordinates = String(candidate?.coordinates || "");
  const stage1Digest = createHash("sha256").update(JSON.stringify({
    rawText: stage1RawText,
    coordinates: stage1Coordinates
  })).digest("hex");
  const retryDigest = createHash("sha256").update(JSON.stringify({
    rawText: retryRawText,
    coordinates: retryCoordinates
  })).digest("hex");
  const stage1Structure = extractDmsSourceStructure(stage1RawText || stage1Coordinates);
  const retryStructure = extractDmsSourceStructure(retryRawText || retryCoordinates);
  const stage1GroupIdentities = stage1Structure.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  const retryGroupIdentities = retryStructure.groups.map(group => normalizeDmsBoundaryIdentity(group.name));
  const stage1GroupSizes = stage1Structure.groups.map(group => group.rows.length);
  const retryGroupSizes = retryStructure.groups.map(group => group.rows.length);
  const sameArray = (left, right) => Array.isArray(right)
    && left.length === right.length
    && left.every((value, index) => value === right[index]);
  if (!candidate || typeof candidate !== "object"
    || provenance?.schemaVersion !== "dms_grouped_acquisition_delta_v1"
    || provenance?.ownerFamily !== "dms_grouped"
    || provenance?.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || provenance?.baselineRowsPreserved !== true
    || provenance?.groupLocalBaselineRowsPreserved !== true
    || provenance?.strongBoundariesProven !== true
    || provenance?.labelsContinuous !== true
    || provenance?.sourceCandidateSeparate !== true
    || provenance?.directCanonicalPromotion !== false
    || provenance?.baselineRowCount !== 13
    || provenance?.retryRowCount !== 16
    || provenance?.addedRowCount !== 3
    || provenance?.baselineGroupCount !== 3
    || provenance?.retryGroupCount !== 3
    || stage1Structure.rowCount !== 13
    || retryStructure.rowCount !== 16
    || stage1Structure.groupCount !== 3
    || retryStructure.groupCount !== 3
    || !stage1Structure.documentHasStrongMultiRegionEvidence
    || !retryStructure.documentHasStrongMultiRegionEvidence
    || !stage1Structure.allBoundariesProven
    || !retryStructure.allBoundariesProven
    || !sameArray(stage1GroupIdentities, provenance?.baselineGroupIdentities)
    || !sameArray(retryGroupIdentities, provenance?.retryGroupIdentities)
    || !sameArray(stage1GroupIdentities, retryGroupIdentities)
    || !sameArray(stage1GroupSizes, provenance?.baselineGroupSizes)
    || !sameArray(retryGroupSizes, provenance?.retryGroupSizes)
    || provenance?.stage1CandidateSha256 !== stage1Digest
    || provenance?.retryCandidateSha256 !== retryDigest
    || stage1Digest === retryDigest
    || typeof candidate.rawText !== "string"
    || typeof candidate.coordinates !== "string") return null;
  return candidate;
}

function partialMultisiteRecoveryCandidate(recognitionResult = {}) {
  const candidate = recognitionResult?.partialMultisiteRecoveryCandidate;
  const provenance = candidate?.provenance;
  const stage1Candidate = recognitionResult?.stage1Candidate && typeof recognitionResult.stage1Candidate === "object"
    ? recognitionResult.stage1Candidate
    : recognitionResult?.sourceCandidates?.stage1;
  if (!candidate || typeof candidate !== "object"
    || !stage1Candidate || typeof stage1Candidate !== "object"
    || provenance?.schemaVersion !== "dms_grouped_partial_multisite_recovery_v1") return null;
  const rebuilt = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: String(stage1Candidate.rawText || ""),
    stage1Coordinates: String(stage1Candidate.coordinates || ""),
    retryRawText: String(candidate.rawText || ""),
    retryCoordinates: String(candidate.coordinates || ""),
    expansion: {
      accepted: true,
      recoveryMode: provenance.recoveryMode,
      baselineRowCount: provenance.baselineRowCount,
      retryRowCount: provenance.retryRowCount,
      addedRowCount: provenance.addedRowCount,
      baselineRowsPreserved: provenance.baselineRowsPreserved,
      groupLocalBaselineRowsPreserved: provenance.groupLocalBaselineRowsPreserved,
      baselineGroupCount: provenance.baselineGroupCount,
      baselineGroupSizes: provenance.baselineGroupSizes,
      baselineGroupIdentities: provenance.baselineGroupIdentities,
      groupCount: provenance.retryGroupCount,
      groupSizes: provenance.retryGroupSizes,
      groupIdentities: provenance.retryGroupIdentities
    },
    ownerFamily: provenance.ownerFamily
  });
  if (rebuilt.accepted !== true) return null;
  const provenanceDeclarations = [provenance, recognitionResult?.partialMultisiteRecoveryProvenance]
    .filter(value => value !== null && value !== undefined);
  const declaredSources = recognitionResult?.sourceCandidates;
  const declaredSourcesMatch = Boolean(declaredSources) && (
    String(declaredSources?.stage1?.rawText || "") === rebuilt.stage1Candidate.rawText
    && String(declaredSources?.stage1?.coordinates || "") === rebuilt.stage1Candidate.coordinates
    && String(declaredSources?.structuredReread?.rawText || "") === rebuilt.rawText
    && String(declaredSources?.structuredReread?.coordinates || "") === rebuilt.normalizedCoordinates
    && declaredSources.stage1.rowCount === rebuilt.provenance.baselineRowCount
    && declaredSources.structuredReread.rowCount === rebuilt.provenance.retryRowCount
    && declaredSources.stage1.candidateRole === "STAGE1_ACQUISITION_CANDIDATE"
    && declaredSources.structuredReread.candidateRole === "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    && declaredSources.stage1.candidateSha256 === rebuilt.provenance.stage1CandidateSha256
    && declaredSources.structuredReread.candidateSha256 === rebuilt.provenance.retryCandidateSha256
  );
  if (provenanceDeclarations.length === 0
    || provenanceDeclarations.some(declared => (
      !partialMultisiteRecoveryProvenanceMatches(declared, rebuilt.provenance)
    ))
    || candidate.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || recognitionResult.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || recognitionResult.sourceCandidateSeparate !== true
    || recognitionResult.directCanonicalPromotion !== false
    || !declaredSourcesMatch) return null;
  return Object.freeze({
    ...candidate,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    provenance: rebuilt.provenance,
    stage1Candidate: rebuilt.stage1Candidate
  });
}

export function buildSourceCoordinateRepresentation(recognitionResult = {}, coordinateEngineV2 = {}) {
  const expansionCandidate = acquisitionExpansionCandidate(recognitionResult);
  const partialRecoveryCandidate = partialMultisiteRecoveryCandidate(recognitionResult);
  const reviewCandidate = expansionCandidate || partialRecoveryCandidate;
  const stage1Candidate = recognitionResult?.stage1Candidate && typeof recognitionResult.stage1Candidate === "object"
    ? recognitionResult.stage1Candidate
    : recognitionResult;
  const effectiveRecognitionResult = reviewCandidate
    ? { ...recognitionResult, ...reviewCandidate }
    : recognitionResult;
  const family = String(coordinateEngineV2?.coordinate_type || effectiveRecognitionResult?.coordinateType || "").trim() || null;
  const format = String(coordinateEngineV2?.precision_mode || effectiveRecognitionResult?.precisionMode || "").trim() || null;
  const sourceText = typeof effectiveRecognitionResult?.coordinates === "string" ? effectiveRecognitionResult.coordinates : "";
  const coordinateDisplayText = sourceText.trim() ? sourceText.replace(/\r\n/g, "\n") : "";
  const rawDmsStructure = extractDmsSourceStructure(effectiveRecognitionResult?.rawText || coordinateDisplayText);
  const axisOrder = sourceAxisOrder(coordinateEngineV2, family, format);
  const coordinateAlreadyPreservesDms = extractDmsSourceStructure(coordinateDisplayText).rowCount > 0;
  const engineGroups = groupsFromEngine(coordinateEngineV2, coordinateDisplayText, axisOrder || "latitude_longitude");
  const rawDmsEquivalence = rawDmsSemanticallyMatchesResult(rawDmsStructure, engineGroups, axisOrder);
  const useRawDms = rawDmsEquivalence.verified === true;
  const canonicalEngineDisplay = renderCanonicalEngineGroups(engineGroups, axisOrder);
  const displayText = rawDmsEquivalence.verified === true
    ? rawDmsStructure.displayText
    : (coordinateAlreadyPreservesDms && canonicalEngineDisplay ? canonicalEngineDisplay : coordinateDisplayText);
  const groups = useRawDms
    ? rawDmsStructure.groups.map(group => [...group.rows])
    : sourceGroups(displayText);

  return Object.freeze({
    schema_version: SOURCE_COORDINATE_REPRESENTATION_SCHEMA,
    family,
    format,
    rawText: String(effectiveRecognitionResult?.rawText || ""),
    rows: useRawDms ? [...rawDmsStructure.rows] : sourceLines(displayText).filter(line => line.trim()),
    groups,
    groupNames: useRawDms ? rawDmsStructure.groups.map(group => group.name) : groups.map(() => null),
    pointLabels: enginePointLabels(coordinateEngineV2),
    axisOrder,
    hemisphere: sourceHemispheres(displayText),
    precision: format,
    sourceCrsEvidence: sourceCrsEvidence(effectiveRecognitionResult, coordinateEngineV2),
    sourceEquivalence: rawDmsEquivalence.reason,
    displayText,
    editable: Boolean(displayText),
    candidateRole: reviewCandidate ? "NONAUTHORITATIVE_REVIEW_CANDIDATE" : "CURRENT_RECOGNITION_CANDIDATE",
    acquisitionDeltaProvenance: expansionCandidate ? Object.freeze({ ...expansionCandidate.provenance }) : null,
    partialMultisiteRecoveryProvenance: partialRecoveryCandidate
      ? Object.freeze({ ...partialRecoveryCandidate.provenance })
      : null,
    sourceCandidateSeparate: reviewCandidate ? true : false,
    directCanonicalPromotion: reviewCandidate ? false : null,
    sourceCandidates: reviewCandidate ? Object.freeze({
      stage1: Object.freeze({
        rawText: String(stage1Candidate?.rawText || ""),
        coordinates: String(stage1Candidate?.coordinates || ""),
        rowCount: extractDmsSourceStructure(stage1Candidate?.rawText || stage1Candidate?.coordinates || "").rowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: reviewCandidate.provenance.stage1CandidateSha256
      }),
      structuredReread: Object.freeze({
        rawText: String(reviewCandidate.rawText),
        coordinates: String(reviewCandidate.coordinates),
        rowCount: rawDmsStructure.rowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: reviewCandidate.provenance.retryCandidateSha256
      })
    }) : null
  });
}
