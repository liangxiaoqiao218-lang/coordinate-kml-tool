import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(__filename), "..");
const fixtureRoot = path.join(root, "fixtures", "mining-block-nomenclature");
const schemaPath = path.join(fixtureRoot, "mining-block-nomenclature-grammar-fixture-v1.schema.json");
const casesPath = path.join(fixtureRoot, "mining-block-nomenclature-grammar-fixture-v1.cases.json");

const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
const cases = JSON.parse(fs.readFileSync(casesPath, "utf8"));

const SCHEMA_VERSION = "mining_block_nomenclature_grammar_fixture_v1";
const DATA_CLASS = "SYNTHETIC_ONLY";
const RULESET_STATUS = "UNKNOWN_RULESET";
const AUTHORITY_FAILURE_REASON = "UNKNOWN_RULESET_NO_SPATIAL_AUTHORITY";
const PARTIAL_FAILURE_REASON = "PARTIAL_WITHOUT_CLIP_OUTLINE";
const QUALIFIER_TOKENS = new Set([
  "QUALIFIER:PARTIAL",
  "QUALIFIER:ЧАСТИЧНО",
  "QUALIFIER:ТОЛЫҚ ЕМЕС"
]);
const ZERO_SPATIAL_KEYS = [
  "geometry_expected",
  "crs_expected",
  "bounds_expected",
  "adjacency_expected",
  "area_expected",
  "legal_boundary_expected",
  "resolver_success_expected",
  "finalizer_auto_export_expected",
  "map_authority_expected",
  "kml_export_expected",
  "kmz_export_expected"
];
const REQUIRED_FIELDS = [
  "schema_version",
  "data_class",
  "case_id",
  "case_class",
  "raw_synthetic_string",
  "normalized_tokens",
  "lexical_safety_status",
  "lexical_reason_code",
  "normalization_status",
  "normalization_reason_code",
  "grammar_status",
  "grammar_reason_code",
  "interpretation_status",
  "interpretation_reason_code",
  "ruleset_status",
  "authority_failure_reason",
  "partial_assertions_required",
  "partial_assertions",
  "spatial_expectations",
  "spatial_authority_expected"
];

const GRAMMAR_V1 = Object.freeze({
  document: "sequence [qualifier]",
  sequence: "term (',' term)*",
  term: "primary [qualifier]",
  primary: "atom | range | group"
});

function normalizeSource(raw) {
  return raw
    .trim()
    .replace(/[–—]/gu, "-")
    .replace(/\s*-\s*/gu, "-")
    .replace(/\s*~\s*/gu, "~")
    .toUpperCase();
}

function atomTokenAt(source, offset) {
  const match = source.slice(offset).match(/^ZZSYN-[A-ZА-ЯЁӘҒҚҢӨҰҮҺІ]{2}-[0-9]{2}/u);
  if (!match) return null;
  return { value: `ATOM:${match[0]}`, length: match[0].length };
}

function tokenize(source) {
  const tokens = [];
  let offset = 0;
  while (offset < source.length) {
    const atom = atomTokenAt(source, offset);
    if (atom) {
      tokens.push(atom.value);
      offset += atom.length;
      continue;
    }

    const qualifier = ["~ТОЛЫҚ ЕМЕС", "~ЧАСТИЧНО", "~PARTIAL"]
      .find((candidate) => source.startsWith(candidate, offset));
    if (qualifier) {
      tokens.push(`QUALIFIER:${qualifier.slice(1)}`);
      offset += qualifier.length;
      continue;
    }

    if (source.startsWith("..", offset)) {
      tokens.push("RANGE_OPERATOR:..");
      offset += 2;
      continue;
    }

    const punctuation = {
      ",": "LIST_SEPARATOR:,",
      "(": "GROUP_OPEN:(",
      ")": "GROUP_CLOSE:)"
    }[source[offset]];
    if (punctuation) {
      tokens.push(punctuation);
      offset += 1;
      continue;
    }

    let end = offset + 1;
    while (end < source.length && ![",", "(", ")"].includes(source[end]) && !source.startsWith("..", end)) {
      end += 1;
    }
    tokens.push(`UNKNOWN:${source.slice(offset, end)}`);
    offset = end;
  }
  return tokens;
}

function containsMixedScriptBand(tokens) {
  return tokens.some((token) => {
    if (!token.startsWith("ATOM:ZZSYN-")) return false;
    const band = token.slice("ATOM:ZZSYN-".length).split("-")[0];
    return /[A-Z]/u.test(band) && /[А-ЯЁӘҒҚҢӨҰҮҺІ]/u.test(band);
  });
}

function grammarReason(tokens) {
  const unknown = tokens.find((token) => token.startsWith("UNKNOWN:"));
  if (unknown) {
    if (/^UNKNOWN:ZZSYN-(?:-[0-9]{2}|[A-ZА-ЯЁӘҒҚҢӨҰҮҺІ]{2})$/u.test(unknown)) return "MISSING_LEVEL";
    if (unknown.startsWith("UNKNOWN:~")) return "UNKNOWN_QUALIFIER";
    return "UNKNOWN_TOKEN";
  }
  if (tokens.some((token, index) => token === "LIST_SEPARATOR:," && tokens[index + 1] === "LIST_SEPARATOR:,")) {
    return "EMPTY_LIST_MEMBER";
  }
  if (tokens.at(-1) === "RANGE_OPERATOR:..") return "INCOMPLETE_RANGE";

  let cursor = 0;
  const peek = () => tokens[cursor];
  const consume = (value) => {
    if (peek() !== value) return false;
    cursor += 1;
    return true;
  };
  const consumeAtom = () => {
    if (!peek()?.startsWith("ATOM:")) return false;
    cursor += 1;
    return true;
  };
  const parsePrimary = () => {
    if (consume("GROUP_OPEN:(")) {
      if (!parseSequence() || !consume("GROUP_CLOSE:)")) return false;
      return true;
    }
    if (!consumeAtom()) return false;
    if (consume("RANGE_OPERATOR:..") && !consumeAtom()) return false;
    return true;
  };
  const parseTerm = () => {
    if (!parsePrimary()) return false;
    if (QUALIFIER_TOKENS.has(peek())) cursor += 1;
    return true;
  };
  const parseSequence = () => {
    if (!parseTerm()) return false;
    while (consume("LIST_SEPARATOR:,")) {
      if (!parseTerm()) return false;
    }
    return true;
  };

  if (!parseSequence()) return tokens.includes("GROUP_OPEN:(") || tokens.includes("GROUP_CLOSE:)")
    ? "MALFORMED_GROUP"
    : "UNKNOWN_TOKEN";
  if (QUALIFIER_TOKENS.has(peek())) cursor += 1;
  if (cursor !== tokens.length) return tokens.includes("GROUP_OPEN:(") || tokens.includes("GROUP_CLOSE:)")
    ? "MALFORMED_GROUP"
    : "UNKNOWN_TOKEN";
  return "GRAMMAR_OK";
}

function evaluateSyntheticGrammar(raw) {
  const normalizedSource = normalizeSource(raw);
  const normalizedTokens = tokenize(normalizedSource);
  const lexicalUnsafe = containsMixedScriptBand(normalizedTokens);
  const atomValues = normalizedTokens.filter((token) => token.startsWith("ATOM:"));
  const rawAtomSpellings = raw.match(/zzsyn-[A-ZА-ЯЁӘҒҚҢӨҰҮҺІ]{2}-[0-9]{2}/giu) ?? [];
  const normalizationCollision = new Set(rawAtomSpellings).size > new Set(atomValues).size;
  const normalizationApplied = normalizedSource !== raw;
  const parsedReason = grammarReason(normalizedTokens);
  const grammarStatus = parsedReason === "GRAMMAR_OK" ? "PARSED" : "REJECTED";

  let interpretationStatus = "UNRESOLVED";
  let interpretationReason = "RULESET_UNKNOWN";
  if (grammarStatus === "REJECTED") {
    interpretationStatus = "BLOCKED";
    interpretationReason = "GRAMMAR_REJECTED";
  } else if (lexicalUnsafe) {
    interpretationStatus = "BLOCKED";
    interpretationReason = "LEXICAL_SAFETY_BLOCK";
  } else if (normalizationCollision) {
    interpretationStatus = "BLOCKED";
    interpretationReason = "NORMALIZATION_COLLISION_BLOCK";
  } else if (normalizedTokens.some((token) => QUALIFIER_TOKENS.has(token))) {
    const qualifierIndex = normalizedTokens.findIndex((token) => QUALIFIER_TOKENS.has(token));
    interpretationReason = normalizedTokens.slice(qualifierIndex + 1).includes("LIST_SEPARATOR:,")
      ? "PARTIAL_SCOPE_AMBIGUOUS"
      : "GRAMMAR_OK_PARTIAL_UNRESOLVED";
  } else if (normalizedTokens.includes("GROUP_OPEN:(")) {
    interpretationReason = "GROUP_SEMANTICS_UNKNOWN";
  } else if (normalizedTokens.includes("RANGE_OPERATOR:..")) {
    interpretationReason = "RANGE_SEMANTICS_UNKNOWN";
  }

  return {
    normalizedTokens,
    lexical_safety_status: lexicalUnsafe ? "UNSAFE" : "SAFE",
    lexical_reason_code: lexicalUnsafe ? "MIXED_SCRIPT_CONFUSABLE" : "LEXICAL_OK",
    normalization_status: normalizationCollision ? "COLLISION" : normalizationApplied ? "NORMALIZED" : "UNCHANGED",
    normalization_reason_code: normalizationCollision
      ? "NORMALIZATION_COLLISION"
      : normalizationApplied
        ? "NORMALIZATION_APPLIED"
        : "NORMALIZATION_NOT_REQUIRED",
    grammar_status: grammarStatus,
    grammar_reason_code: parsedReason,
    interpretation_status: interpretationStatus,
    interpretation_reason_code: interpretationReason,
    partial_assertions_required: normalizedTokens.some((token) => QUALIFIER_TOKENS.has(token))
  };
}

function allowedToken(token) {
  return /^ATOM:ZZSYN-[A-ZА-ЯЁӘҒҚҢӨҰҮҺІ]{2}-[0-9]{2}$/u.test(token)
    || ["LIST_SEPARATOR:,", "RANGE_OPERATOR:..", "GROUP_OPEN:(", "GROUP_CLOSE:)", ...QUALIFIER_TOKENS].includes(token)
    || /^UNKNOWN:[^\r\n]{1,96}$/u.test(token);
}

function resolveLocalRef(rootSchema, reference) {
  assert.match(reference, /^#\//u);
  return reference.slice(2).split("/").reduce((value, segment) => {
    const key = segment.replace(/~1/gu, "/").replace(/~0/gu, "~");
    return value[key];
  }, rootSchema);
}

function matchesSchema(value, definition, rootSchema = schema) {
  if (definition === true) return true;
  if (definition === false) return false;
  if (!definition || typeof definition !== "object" || Array.isArray(definition)) return false;
  if (definition.$ref && !matchesSchema(value, resolveLocalRef(rootSchema, definition.$ref), rootSchema)) return false;
  if (definition.const !== undefined && !Object.is(value, definition.const)) return false;
  if (definition.enum && !definition.enum.some((candidate) => Object.is(value, candidate))) return false;
  if (definition.type) {
    const typeMatches = definition.type === "null"
      ? value === null
      : definition.type === "array"
        ? Array.isArray(value)
        : definition.type === "object"
          ? value !== null && typeof value === "object" && !Array.isArray(value)
          : typeof value === definition.type;
    if (!typeMatches) return false;
  }
  if (typeof value === "string") {
    if (definition.minLength !== undefined && value.length < definition.minLength) return false;
    if (definition.maxLength !== undefined && value.length > definition.maxLength) return false;
    if (definition.pattern && !new RegExp(definition.pattern, "u").test(value)) return false;
  }
  if (Array.isArray(value)) {
    if (definition.minItems !== undefined && value.length < definition.minItems) return false;
    if (definition.items && !value.every((item) => matchesSchema(item, definition.items, rootSchema))) return false;
    if (definition.contains && !value.some((item) => matchesSchema(item, definition.contains, rootSchema))) return false;
  }
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    if (definition.required && !definition.required.every((key) => Object.hasOwn(value, key))) return false;
    if (definition.properties) {
      for (const [key, childSchema] of Object.entries(definition.properties)) {
        if (Object.hasOwn(value, key) && !matchesSchema(value[key], childSchema, rootSchema)) return false;
      }
    }
    if (definition.additionalProperties === false) {
      const declared = new Set(Object.keys(definition.properties ?? {}));
      if (Object.keys(value).some((key) => !declared.has(key))) return false;
    }
  }
  if (definition.oneOf && definition.oneOf.filter((candidate) => matchesSchema(value, candidate, rootSchema)).length !== 1) return false;
  if (definition.not && matchesSchema(value, definition.not, rootSchema)) return false;
  if (definition.allOf && !definition.allOf.every((candidate) => matchesSchema(value, candidate, rootSchema))) return false;
  if (definition.if) {
    const branch = matchesSchema(value, definition.if, rootSchema) ? definition.then : definition.else;
    if (branch && !matchesSchema(value, branch, rootSchema)) return false;
  }
  return true;
}

function validateCaseContract(candidate) {
  assert.equal(matchesSchema(candidate, schema), true, `${candidate.case_id ?? "UNIDENTIFIED"} must satisfy the frozen schema`);
  assert.deepEqual(Object.keys(candidate).sort(), [...REQUIRED_FIELDS].sort());
  assert.equal(candidate.schema_version, SCHEMA_VERSION);
  assert.equal(candidate.data_class, DATA_CLASS);
  assert.match(candidate.case_id, /^(P(0[1-9]|1[01])|N0[1-8]|A0[1-6])$/u);
  assert.ok(["POSITIVE", "NEGATIVE", "AMBIGUOUS"].includes(candidate.case_class));
  assert.equal(typeof candidate.raw_synthetic_string, "string");
  assert.ok(candidate.raw_synthetic_string.includes("ZZSYN") || candidate.raw_synthetic_string.includes("zzsyn"));
  assert.doesNotMatch(candidate.raw_synthetic_string, /[-+]?\d{1,3}\.\d+\s*[,; ]\s*[-+]?\d{1,3}\.\d+/u);
  assert.ok(Array.isArray(candidate.normalized_tokens) && candidate.normalized_tokens.length > 0);
  assert.ok(candidate.normalized_tokens.every(allowedToken));
  assert.ok(["SAFE", "UNSAFE"].includes(candidate.lexical_safety_status));
  assert.ok(["LEXICAL_OK", "MIXED_SCRIPT_CONFUSABLE"].includes(candidate.lexical_reason_code));
  assert.ok(["UNCHANGED", "NORMALIZED", "COLLISION"].includes(candidate.normalization_status));
  assert.ok(["NORMALIZATION_NOT_REQUIRED", "NORMALIZATION_APPLIED", "NORMALIZATION_COLLISION"].includes(candidate.normalization_reason_code));
  assert.ok(["PARSED", "REJECTED"].includes(candidate.grammar_status));
  assert.ok(["GRAMMAR_OK", "MISSING_LEVEL", "UNKNOWN_TOKEN", "MALFORMED_GROUP", "EMPTY_LIST_MEMBER", "INCOMPLETE_RANGE", "UNKNOWN_QUALIFIER"].includes(candidate.grammar_reason_code));
  assert.ok(["UNRESOLVED", "BLOCKED"].includes(candidate.interpretation_status));
  assert.ok(["RULESET_UNKNOWN", "RANGE_SEMANTICS_UNKNOWN", "GROUP_SEMANTICS_UNKNOWN", "GRAMMAR_OK_PARTIAL_UNRESOLVED", "PARTIAL_SCOPE_AMBIGUOUS", "GRAMMAR_REJECTED", "LEXICAL_SAFETY_BLOCK", "NORMALIZATION_COLLISION_BLOCK"].includes(candidate.interpretation_reason_code));
  assert.equal(candidate.ruleset_status, RULESET_STATUS);
  assert.equal(candidate.authority_failure_reason, AUTHORITY_FAILURE_REASON);
  assert.equal(typeof candidate.partial_assertions_required, "boolean");
  assert.equal(candidate.spatial_authority_expected, false);
  assert.deepEqual(Object.keys(candidate.spatial_expectations).sort(), [...ZERO_SPATIAL_KEYS].sort());
  for (const key of ZERO_SPATIAL_KEYS) assert.equal(candidate.spatial_expectations[key], false);

  const partialReason = ["GRAMMAR_OK_PARTIAL_UNRESOLVED", "PARTIAL_SCOPE_AMBIGUOUS"].includes(candidate.interpretation_reason_code);
  assert.equal(candidate.partial_assertions_required, partialReason);
  if (partialReason) {
    assert.deepEqual(candidate.partial_assertions, {
      clip_outline_present: false,
      legal_boundary_generation_allowed: false,
      geometry_export_allowed: false,
      failure_reason: PARTIAL_FAILURE_REASON
    });
    assert.ok(candidate.normalized_tokens.some((token) => QUALIFIER_TOKENS.has(token)));
  } else {
    assert.equal(candidate.partial_assertions, null);
    assert.ok(!candidate.normalized_tokens.some((token) => QUALIFIER_TOKENS.has(token)));
  }

  if (candidate.case_class === "POSITIVE") {
    assert.match(candidate.case_id, /^P/u);
    assert.equal(candidate.lexical_safety_status, "SAFE");
    assert.equal(candidate.grammar_status, "PARSED");
    assert.equal(candidate.interpretation_status, "UNRESOLVED");
  } else if (candidate.case_class === "NEGATIVE") {
    assert.match(candidate.case_id, /^N/u);
    assert.equal(candidate.grammar_status, "REJECTED");
    assert.equal(candidate.interpretation_status, "BLOCKED");
    assert.equal(candidate.interpretation_reason_code, "GRAMMAR_REJECTED");
  } else {
    assert.match(candidate.case_id, /^A/u);
  }

  const derived = evaluateSyntheticGrammar(candidate.raw_synthetic_string);
  assert.deepEqual(candidate.normalized_tokens, derived.normalizedTokens);
  for (const field of [
    "lexical_safety_status",
    "lexical_reason_code",
    "normalization_status",
    "normalization_reason_code",
    "grammar_status",
    "grammar_reason_code",
    "interpretation_status",
    "interpretation_reason_code",
    "partial_assertions_required"
  ]) {
    assert.equal(candidate[field], derived[field], `${candidate.case_id}.${field}`);
  }
}

test("schema identity and source-safe canonical URI are frozen", () => {
  const source = fs.readFileSync(schemaPath, "utf8");
  assert.match(source, /"\$schema": "https:\\u002f\\u002fjson-schema\.org\\u002fdraft\\u002f2020-12\\u002fschema"/u);
  assert.equal(schema.$schema, "https://json-schema.org/draft/2020-12/schema");
  assert.equal(schema.$id, "urn:geokit-lab:g2-r2:mining-block-nomenclature-grammar-fixture-v1");
  assert.equal(schema.type, "object");
  assert.equal(schema.additionalProperties, false);
  assert.deepEqual(schema.required, REQUIRED_FIELDS);
});

test("corrected A06 grammar contract is frozen without spatial semantics", () => {
  assert.deepEqual(GRAMMAR_V1, {
    document: "sequence [qualifier]",
    sequence: "term (',' term)*",
    term: "primary [qualifier]",
    primary: "atom | range | group"
  });
  const a06 = cases.find((candidate) => candidate.case_id === "A06");
  assert.equal(a06.grammar_status, "PARSED");
  assert.equal(a06.interpretation_status, "UNRESOLVED");
  assert.equal(a06.interpretation_reason_code, "PARTIAL_SCOPE_AMBIGUOUS");
  assert.equal(a06.partial_assertions_required, true);
});

test("fixture contains the exact 25-case class distribution", () => {
  assert.equal(cases.length, 25);
  assert.equal(new Set(cases.map(({ case_id }) => case_id)).size, 25);
  assert.deepEqual(
    Object.fromEntries(["POSITIVE", "NEGATIVE", "AMBIGUOUS"].map((kind) => [kind, cases.filter(({ case_class }) => case_class === kind).length])),
    { POSITIVE: 11, NEGATIVE: 8, AMBIGUOUS: 6 }
  );
  assert.deepEqual(cases.map(({ case_id }) => case_id), [
    "P01", "P02", "P03", "P04", "P05", "P06", "P07", "P08", "P09", "P10", "P11",
    "N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08",
    "A01", "A02", "A03", "A04", "A05", "A06"
  ]);
});

test("schema freezes the token and status vocabularies used by the fixture", () => {
  assert.deepEqual(schema.properties.case_class.enum, ["POSITIVE", "NEGATIVE", "AMBIGUOUS"]);
  assert.deepEqual(schema.properties.lexical_safety_status.enum, ["SAFE", "UNSAFE"]);
  assert.deepEqual(schema.properties.normalization_status.enum, ["UNCHANGED", "NORMALIZED", "COLLISION"]);
  assert.deepEqual(schema.properties.grammar_status.enum, ["PARSED", "REJECTED"]);
  assert.deepEqual(schema.properties.interpretation_status.enum, ["UNRESOLVED", "BLOCKED"]);
  assert.equal(schema.properties.ruleset_status.const, RULESET_STATUS);
  assert.equal(schema.properties.authority_failure_reason.const, AUTHORITY_FAILURE_REASON);
  assert.equal(schema.properties.spatial_authority_expected.const, false);
});

test("schema freezes all three non-partial reverse constraints", () => {
  const expectedDenial = {
    not: { contains: { enum: [...QUALIFIER_TOKENS] } }
  };
  const byReason = schema.allOf.find((branch) =>
    branch.if?.properties?.interpretation_reason_code?.enum?.includes("PARTIAL_SCOPE_AMBIGUOUS")
    && branch.then?.properties?.partial_assertions_required?.const === true
  );
  const byRequiredFlag = schema.allOf.find((branch) => branch.if?.properties?.partial_assertions_required?.const === true);
  const byNullAssertions = schema.allOf.find((branch) => branch.if?.properties?.partial_assertions?.type === "null");
  assert.deepEqual(byReason.else.properties.normalized_tokens, expectedDenial);
  assert.deepEqual(byRequiredFlag.else.properties.normalized_tokens, expectedDenial);
  assert.deepEqual(byNullAssertions.then.properties.normalized_tokens, expectedDenial);
});

for (const candidate of cases) {
  test(`${candidate.case_id} ${candidate.case_class} synthetic grammar contract`, () => {
    validateCaseContract(candidate);
  });
}

test("contract validation rejects missing, extra, and authority-widening mutations", () => {
  const baseline = structuredClone(cases[0]);
  const mutations = [
    (() => { const value = structuredClone(baseline); delete value.grammar_status; return value; })(),
    { ...structuredClone(baseline), unexpected: true },
    { ...structuredClone(baseline), spatial_authority_expected: true },
    { ...structuredClone(baseline), spatial_expectations: { ...baseline.spatial_expectations, map_authority_expected: true } },
    { ...structuredClone(baseline), ruleset_status: "KNOWN_RULESET" }
  ];
  for (const mutation of mutations) assert.throws(() => validateCaseContract(mutation));
});

test("schema rejects qualifier tokens in every non-partial state", () => {
  const baseline = structuredClone(cases.find(({ case_id }) => case_id === "P01"));
  for (const qualifier of QUALIFIER_TOKENS) {
    const mutation = structuredClone(baseline);
    mutation.normalized_tokens.push(qualifier);
    assert.equal(matchesSchema(mutation, schema), false, `${qualifier} must be rejected by the schema`);
    assert.throws(() => validateCaseContract(mutation));
  }
});

test("fixture and regression stay grammar-only and Provider-free", () => {
  const scriptSource = fs.readFileSync(__filename, "utf8");
  const fixturePayload = cases.flatMap(({ raw_synthetic_string, normalized_tokens }) => [
    raw_synthetic_string,
    ...normalized_tokens
  ]).join("\n");
  assert.doesNotMatch(scriptSource, /from\s+["']\.\.\/server\//u);
  assert.doesNotMatch(scriptSource, /\b(fetch|axios|http\.request|https\.request)\s*\(/u);
  assert.doesNotMatch(fixturePayload, /(?:latitude|longitude|easting|northing|EPSG|CRS)/iu);
  assert.ok(cases.every(({ data_class }) => data_class === DATA_CLASS));
  assert.ok(cases.every(({ spatial_authority_expected }) => spatial_authority_expected === false));
});
