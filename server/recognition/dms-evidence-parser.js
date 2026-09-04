export const DMS_PARSE_STATUS = Object.freeze({
  VALID_COMPACT: "VALID_COMPACT",
  VALID_STANDARD: "VALID_STANDARD",
  AMBIGUOUS: "AMBIGUOUS",
  OUT_OF_RANGE: "OUT_OF_RANGE",
  MALFORMED: "MALFORMED",
  MISSING: "MISSING"
});

export const DMS_SOURCE_NOTATION = Object.freeze({
  COMPACT: "COMPACT_DMS",
  STANDARD: "STANDARD_DMS",
  SPACE: "SPACE_DMS",
  DOT: "DOT_DMS",
  UNRESOLVED: "UNRESOLVED"
});

const normalizeMarks = value => String(value || "")
  .normalize("NFKC")
  .replace(/OUEST|WEST/gi, "W")
  .replace(/[º˚]/g, "°")
  .replace(/[‘’´`′]/g, "'")
  .replace(/[“”″]/g, '"');

function stripLeadingCoordinateLabel(line) {
  return String(line || "")
    .replace(/^\s*(?:point|pt|ponto|sommet|vertex)\s*[-#:]?\s*\d{1,3}\s*[\).:：-]?\s*/i, "")
    .replace(/^\s*[A-Z]\s*[\).:：-]\s*/i, "")
    .replace(/^\s*\d{1,3}\s*[\).:：-]\s*/, "");
}

export function normalizeDmsLineForParsing(line) {
  return stripLeadingCoordinateLabel(normalizeMarks(line))
    .replace(/(\d{1,3}\s*°\s*\d{1,2})\s*"\s*(?=\d{1,2}(?:\.\d+)?\s*["']?\s*[NSEWO])/gi, "$1'");
}

export function stripOcrBboxPrefix(line) {
  return String(line || "").replace(/^\s*(?:\d+(?:\.\d+)?\s*,\s*){4,6}(?=\d{1,2}\s*[\).:\-])/i, "");
}

function normalizeHemisphere(value) {
  const hemisphere = String(value || "").toUpperCase();
  return hemisphere === "O" ? "W" : hemisphere;
}

function axisForHemisphere(hemisphere) {
  if (["N", "S"].includes(hemisphere)) return "latitude";
  if (["E", "W"].includes(hemisphere)) return "longitude";
  return null;
}

function canonicalDecimalString(value) {
  const text = String(value ?? "").trim();
  const match = text.match(/^([+-]?)(\d+)(?:\.(\d+))?$/);
  if (!match) return null;
  const [, sign, integerPart, fractionPart = ""] = match;
  const integer = integerPart.replace(/^0+(?=\d)/, "") || "0";
  const fraction = fractionPart.replace(/0+$/, "");
  const magnitude = fraction ? `${integer}.${fraction}` : integer;
  return sign === "-" && magnitude !== "0" ? `-${magnitude}` : magnitude;
}

function canonicalIntegerString(value) {
  const text = String(value ?? "").trim();
  const match = text.match(/^([+-]?)(\d+)$/);
  if (!match) return null;
  const integer = match[2].replace(/^0+(?=\d)/, "") || "0";
  return match[1] === "-" && integer !== "0" ? `-${integer}` : integer;
}

function exactDecimalIsZero(canonical) {
  return canonical === "0";
}

function exactDecimalIsNegative(canonical) {
  return String(canonical).startsWith("-");
}

function exactNonnegativeDecimalAtLeastInteger(canonical, limit) {
  if (exactDecimalIsNegative(canonical)) return false;
  const integerPart = String(canonical).split(".")[0];
  return BigInt(integerPart) >= BigInt(limit);
}

function validateParts(degreesInput, minutesInput, secondsInput, hemisphereInput) {
  const exactDegrees = canonicalIntegerString(degreesInput);
  const exactMinutes = canonicalIntegerString(minutesInput);
  const exactSeconds = canonicalDecimalString(secondsInput);
  const degrees = Number(exactDegrees);
  const minutes = Number(exactMinutes);
  const seconds = Number(exactSeconds);
  const hemisphere = normalizeHemisphere(hemisphereInput);
  const axis = axisForHemisphere(hemisphere);
  if (exactDegrees === null || exactMinutes === null || exactSeconds === null || ![degrees, minutes, seconds].every(Number.isFinite)) return { ok: false, parseStatus: DMS_PARSE_STATUS.MALFORMED, reason: "nonnumeric_component" };
  if (!axis) return { ok: false, parseStatus: DMS_PARSE_STATUS.AMBIGUOUS, reason: "axis_not_unique" };
  if (exactMinutes.startsWith("-") || BigInt(exactMinutes) >= 60n) return { ok: false, parseStatus: DMS_PARSE_STATUS.OUT_OF_RANGE, reason: "minutes_out_of_range", axis };
  if (exactDecimalIsNegative(exactSeconds) || exactNonnegativeDecimalAtLeastInteger(exactSeconds, 60)) return { ok: false, parseStatus: DMS_PARSE_STATUS.OUT_OF_RANGE, reason: "seconds_out_of_range", axis };
  const maxDegrees = BigInt(axis === "latitude" ? 90 : 180);
  const absoluteDegrees = BigInt(exactDegrees.startsWith("-") ? exactDegrees.slice(1) : exactDegrees);
  if (absoluteDegrees > maxDegrees || (absoluteDegrees === maxDegrees && (exactMinutes !== "0" || !exactDecimalIsZero(exactSeconds)))) {
    return { ok: false, parseStatus: DMS_PARSE_STATUS.OUT_OF_RANGE, reason: "degrees_out_of_range", axis };
  }
  const signHemisphere = ["S", "W"].includes(hemisphere) ? -1 : 1;
  if (exactDegrees.startsWith("-") && signHemisphere > 0) return { ok: false, parseStatus: DMS_PARSE_STATUS.AMBIGUOUS, reason: "sign_hemisphere_conflict", axis };
  const normalizedNumericValue = signHemisphere * (Math.abs(degrees) + minutes / 60 + seconds / 3600);
  const canonicalDegrees = absoluteDegrees.toString();
  const canonicalMinutes = exactMinutes;
  const normalizedSemanticKey = `${axis}|${hemisphere}|${canonicalDegrees}|${canonicalMinutes}|${exactSeconds}`;
  return { ok: true, degrees: Math.abs(degrees), minutes, seconds, exactSeconds, hemisphere, axis, normalizedNumericValue, normalizedSemanticKey, value: normalizedSemanticKey };
}

export function decimalFromDms(degrees, minutes, seconds, direction) {
  const validated = validateParts(degrees, minutes, seconds, direction || (Number(degrees) < 0 ? "W" : "E"));
  return validated.ok ? String(validated.normalizedNumericValue) : null;
}

function validResult(parts, sourceText, sourceNotation, parseStatus) {
  return Object.freeze({ ...parts, sourceText, sourceNotation, parseStatus, reason: null, direction: parts.hemisphere });
}

function unresolved(sourceText, parseStatus, reason, axis = null, sourceNotation = DMS_SOURCE_NOTATION.UNRESOLVED) {
  return Object.freeze({ sourceText, sourceNotation, parseStatus, reason, axis });
}

export function parseDmsField(source, { ownerFamily } = {}) {
  const sourceText = String(source ?? "");
  const text = normalizeMarks(sourceText).trim();
  if (!text) return unresolved(sourceText, DMS_PARSE_STATUS.MISSING, "empty_field");
  const hemisphereMatches = text.match(/[NSEWO]/gi) || [];
  const hemispheres = [...new Set(hemisphereMatches.map(normalizeHemisphere))];
  if (hemispheres.length > 1) return unresolved(sourceText, DMS_PARSE_STATUS.AMBIGUOUS, "conflicting_hemisphere");
  const hemisphere = hemispheres[0] || "";
  const knownAxis = axisForHemisphere(hemisphere);
  if (!hemisphere) return unresolved(sourceText, DMS_PARSE_STATUS.AMBIGUOUS, "missing_hemisphere");

  if (ownerFamily === "handwritten_dms") {
    const compact = text.replace(/\s+/g, "").match(/^([-+]?\d{1,3})°(\d{2})\.(\d{2})(\d{1,3})['"]?([NSEWO])$/i);
    if (compact) {
      const parts = validateParts(compact[1], compact[2], `${compact[3]}.${compact[4]}`, compact[5]);
      return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.COMPACT, DMS_PARSE_STATUS.VALID_COMPACT) : unresolved(sourceText, parts.parseStatus, parts.reason, parts.axis || knownAxis, DMS_SOURCE_NOTATION.COMPACT);
    }
  }

  const standard = text.replace(/\s+/g, "").match(/^([-+]?\d{1,3})°(\d{1,2})['"]([-+]?\d{1,2}(?:\.\d+)?)["']?([NSEWO])$/i);
  if (standard) {
    const parts = validateParts(standard[1], standard[2], standard[3], standard[4]);
    return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.STANDARD, DMS_PARSE_STATUS.VALID_STANDARD) : unresolved(sourceText, parts.parseStatus, parts.reason, parts.axis || knownAxis, DMS_SOURCE_NOTATION.STANDARD);
  }

  const splitFractionSpace = text.match(/^\s*([-+]?\d{1,3})\s*°\s*(\d{1,2})\s+(\d{1,2})\s+(\d+)\s*([NSEWO])\s*$/i);
  if (splitFractionSpace) {
    const parts = validateParts(splitFractionSpace[1], splitFractionSpace[2], `${splitFractionSpace[3]}.${splitFractionSpace[4]}`, splitFractionSpace[5]);
    return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.SPACE, DMS_PARSE_STATUS.VALID_STANDARD) : unresolved(sourceText, parts.parseStatus, parts.reason, parts.axis || knownAxis, DMS_SOURCE_NOTATION.SPACE);
  }

  const space = text.match(/^\s*([-+]?\d{1,3})\s+(\d{1,2})\s+(\d{1,2}(?:\.\d+)?)\s*([NSEWO])\s*$/i);
  if (space) {
    const parts = validateParts(space[1], space[2], space[3], space[4]);
    return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.SPACE, DMS_PARSE_STATUS.VALID_STANDARD) : unresolved(sourceText, parts.parseStatus, parts.reason, parts.axis || knownAxis, DMS_SOURCE_NOTATION.SPACE);
  }

  const dot = text.match(/^\s*([-+]?\d{1,3})\.(\d{1,2})\.(\d{1,2})(?:\.(\d+))?\s*([NSEWO])\s*$/i);
  if (dot) {
    const seconds = dot[4] ? `${dot[3]}.${dot[4]}` : dot[3];
    const parts = validateParts(dot[1], dot[2], seconds, dot[5]);
    return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.DOT, DMS_PARSE_STATUS.VALID_STANDARD) : unresolved(sourceText, parts.parseStatus, parts.reason, parts.axis || knownAxis, DMS_SOURCE_NOTATION.DOT);
  }

  if (/°.*°/.test(text) || /[^\d+\-°.'"NSEWO\s]/i.test(text)) return unresolved(sourceText, DMS_PARSE_STATUS.MALFORMED, "malformed_separator", knownAxis);
  if (ownerFamily === "handwritten_dms" && /^[-+]?\d{1,3}°\d{2}\.\d{1,2}['"]?[NSEWO]$/i.test(text.replace(/\s+/g, ""))) {
    return unresolved(sourceText, DMS_PARSE_STATUS.MALFORMED, "compact_fraction_insufficient", knownAxis, DMS_SOURCE_NOTATION.COMPACT);
  }
  return unresolved(sourceText, DMS_PARSE_STATUS.MALFORMED, "unrecognized_dms", knownAxis);
}

export function parseCompactDmsToken(token, fallbackDirection = "", options = {}) {
  const sourceText = String(token || "");
  const withFallback = /[NSEWO]\s*$/i.test(sourceText) ? sourceText : `${sourceText}${fallbackDirection || ""}`;
  const canonical = parseDmsField(withFallback, options);
  if ([DMS_PARSE_STATUS.VALID_COMPACT, DMS_PARSE_STATUS.VALID_STANDARD].includes(canonical.parseStatus)) return canonical;

  const cleaned = normalizeMarks(sourceText).replace(/\s+/g, "").replace(/[|[\]_=]/g, "").replace(/LONGITUDE|LATITUDE|POINT|N°|NO\.?/gi, "");
  const directionMatch = cleaned.match(/[NSEWO]$/i);
  const direction = normalizeHemisphere(directionMatch ? directionMatch[0] : fallbackDirection || "");
  const body = cleaned.replace(/[NSEWO]$/i, "").replace(/['"]$/, "");
  const patterns = [
    /^([-+]?\d{1,3})°(\d{1,2})'(\d{1,4}(?:\.\d+)?)"?$/,
    /^([-+]?\d{1,3})°(\d{2})(\d{2})(\d{2,3})$/,
    /^([-+]?\d{1,3})°(\d{2})(\d{1,2})\.(\d+)$/
  ];
  for (let index = 0; index < patterns.length; index += 1) {
    const match = body.match(patterns[index]);
    if (!match) continue;
    const seconds = index === 0 ? (!match[3].includes(".") && match[3].length === 4 ? `${match[3].slice(0, 2)}.${match[3].slice(2)}` : match[3]) : `${match[3]}.${match[4]}`;
    const parts = validateParts(match[1], match[2], seconds, direction);
    return parts.ok ? validResult(parts, sourceText, DMS_SOURCE_NOTATION.STANDARD, DMS_PARSE_STATUS.VALID_STANDARD) : null;
  }
  return null;
}

export function parseLooseDmsPart(part, fallbackDirection = "", options = {}) {
  const sourceText = String(part || "");
  const withFallback = /[NSEWO]/i.test(sourceText) ? sourceText : `${sourceText}${fallbackDirection || ""}`;
  const result = parseDmsField(withFallback, options);
  if (![DMS_PARSE_STATUS.VALID_COMPACT, DMS_PARSE_STATUS.VALID_STANDARD].includes(result.parseStatus)) return null;
  return { ...result, direction: result.hemisphere, axis: result.axis === "latitude" ? "lat" : "lon" };
}

export function parseLooseDmsLine(line, options = {}) {
  const text = normalizeDmsLineForParsing(stripOcrBboxPrefix(line)).trim();
  const commaParts = text.split(/\s*[,;|]\s*/).filter(part => part.length > 0);
  const parts = commaParts.length >= 2 ? commaParts : (text.match(/[-+]?\d{1,3}(?:°[^NSEWO]*|(?:\.\d+){2,3}|\s+\d{1,2}\s+\d{1,2}(?:\.\d+)?)\s*[NSEWO]/gi) || []);
  const parsed = parts.map(part => parseDmsField(part, options)).filter(item => [DMS_PARSE_STATUS.VALID_COMPACT, DMS_PARSE_STATUS.VALID_STANDARD].includes(item.parseStatus));
  const latitude = parsed.find(item => item.axis === "latitude");
  const longitude = parsed.find(item => item.axis === "longitude");
  if (!latitude || !longitude) return null;
  return { latitude: latitude.normalizedNumericValue, longitude: longitude.normalizedNumericValue };
}

const HANDWRITTEN_DMS_TOKEN_PATTERN = /[-+]?\d{1,3}\s*°\s*\d{2}\.\d{3,5}\s*['"]?\s*[NSEWO]|[-+]?\d{1,3}\s*°\s*\d{1,2}\s*['"]\s*\d{1,2}(?:\.\d+)?\s*["']?\s*[NSEWO]|[-+]?\d{1,3}\s*°\s*\d{1,2}\s+\d{1,2}\s+\d+\s*[NSEWO]|[-+]?\d{1,3}\.\d{1,2}\.\d{1,2}(?:\.\d+)?\s*[NSEWO]|[-+]?\d{1,3}\s+\d{1,2}\s+\d{1,2}(?:\.\d+)?\s*[NSEWO]/gi;

function unresolvedResidue(sourceText, reason, axis = null, parseStatus = DMS_PARSE_STATUS.AMBIGUOUS) {
  return unresolved(sourceText, parseStatus, reason, axis);
}

export function tokenizeHandwrittenDmsRow(row, { ownerFamily } = {}) {
  if (ownerFamily !== "handwritten_dms") throw new TypeError("handwritten_tokenizer_owner_invalid");
  const source = String(row ?? "");
  if (!source.trim()) return Object.freeze({ fields: Object.freeze([]), unresolved: Object.freeze([unresolvedResidue(source, "empty_row", null, DMS_PARSE_STATUS.MISSING)]), pairAmbiguous: false });
  const matches = [...source.matchAll(HANDWRITTEN_DMS_TOKEN_PATTERN)];
  const occupied = new Array(source.length).fill(false);
  const parsedMatches = [];
  let overlapDetected = false;
  for (const match of matches) {
    const start = match.index;
    const end = start + match[0].length;
    if (occupied.slice(start, end).some(Boolean)) { overlapDetected = true; continue; }
    for (let index = start; index < end; index += 1) occupied[index] = true;
    const parsed = parseDmsField(match[0], { ownerFamily });
    if ([DMS_PARSE_STATUS.VALID_COMPACT, DMS_PARSE_STATUS.VALID_STANDARD].includes(parsed.parseStatus)) {
      parsedMatches.push(Object.freeze({
        ...parsed,
        sourceSpan: Object.freeze({ start, end })
      }));
    }
  }
  const residue = [...source].map((character, index) => occupied[index] ? " " : character).join("");
  const meaningfulResidue = residue.replace(/^[\s,;|:.)\-]+|[\s,;|:.)\-]+$/g, "").trim();
  const unresolvedItems = [];
  const invalidAxes = new Set();
  let pairAmbiguous = overlapDetected;
  if (overlapDetected) unresolvedItems.push(unresolvedResidue(source, "overlapping_token_spans"));
  if (meaningfulResidue) {
    const coordinateLikeResidue = /[\d°.'"]/.test(meaningfulResidue);
    const hemisphereOnlyResidue = /^[NSEWO]+$/i.test(meaningfulResidue);
    const residueHemispheres = coordinateLikeResidue || hemisphereOnlyResidue ? [...new Set((meaningfulResidue.match(/[NSEWO]/gi) || []).map(normalizeHemisphere))] : [];
    const residueAxes = [...new Set(residueHemispheres.map(axisForHemisphere).filter(Boolean))];
    if (residueAxes.length === 1) {
      invalidAxes.add(residueAxes[0]);
      unresolvedItems.push(unresolvedResidue(meaningfulResidue, residueHemispheres.length > 1 ? "conflicting_hemisphere" : "malformed_axis_token", residueAxes[0], residueHemispheres.length > 1 ? DMS_PARSE_STATUS.AMBIGUOUS : DMS_PARSE_STATUS.MALFORMED));
    } else {
      pairAmbiguous = coordinateLikeResidue;
      unresolvedItems.push(unresolvedResidue(meaningfulResidue, coordinateLikeResidue ? "unconsumed_coordinate_like_residue" : "malformed_axis_token", null, coordinateLikeResidue ? DMS_PARSE_STATUS.AMBIGUOUS : DMS_PARSE_STATUS.MALFORMED));
    }
  }
  const byAxis = new Map();
  for (const parsed of parsedMatches) {
    const list = byAxis.get(parsed.axis) || [];
    list.push(parsed);
    byAxis.set(parsed.axis, list);
  }
  for (const [axis, list] of byAxis.entries()) {
    if (list.length > 1) {
      invalidAxes.add(axis);
      unresolvedItems.push(unresolvedResidue(list.map(item => item.sourceText).join(" | "), `multiple_${axis}_tokens`, axis));
    }
  }
  const fields = pairAmbiguous ? [] : parsedMatches.filter(parsed => !invalidAxes.has(parsed.axis) && byAxis.get(parsed.axis)?.length === 1);
  return Object.freeze({ fields: Object.freeze(fields), unresolved: Object.freeze(unresolvedItems), pairAmbiguous });
}
