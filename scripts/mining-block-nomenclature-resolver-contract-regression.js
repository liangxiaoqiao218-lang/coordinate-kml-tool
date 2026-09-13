import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(__filename), "..");
const fixtureRoot = path.join(root, "fixtures", "mining-block-nomenclature");
const schemaPath = path.join(fixtureRoot, "mining-block-nomenclature-ruleset-provenance-v1.schema.json");
const sourcesPath = path.join(fixtureRoot, "mining-block-nomenclature-ruleset-provenance-v1.sources.json");
const casesPath = path.join(fixtureRoot, "mining-block-nomenclature-resolver-contract-v1.cases.json");
const contractPath = path.join(root, "docs", "MINING_BLOCK_NOMENCLATURE_RESOLVER_CONTRACT_V1.md");

const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
const registry = JSON.parse(fs.readFileSync(sourcesPath, "utf8"));
const cases = JSON.parse(fs.readFileSync(casesPath, "utf8"));
const contract = fs.readFileSync(contractPath, "utf8");

const SOURCE_STATUSES = Object.freeze(["VERIFIED_CURRENT", "HISTORICAL_OR_SUPERSEDED", "UNKNOWN"]);
const REQUIREMENT_KEYS = Object.freeze([
  "base_sheet_semantics",
  "fine_grid_numbering_direction",
  "cell_angular_dimensions",
  "crs_and_datum",
  "axis_order",
  "boundary_and_edge_rules"
]);
const CASE_KEYS = Object.freeze([
  "case_id",
  "description",
  "synthetic_identifier",
  "observation_source",
  "source_status",
  "all_required_semantics_proven",
  "sources_consistent",
  "partial",
  "authoritative_clip_outline_proven",
  "expected_decision",
  "expected_reason"
]);
const EXPECTED_CASE_SNAPSHOT = Object.freeze([
  ["RF01", "current sources are incomplete", "ZZSYN-RA-01", "DOCUMENT_OBSERVATION", "VERIFIED_CURRENT", false, true, false, false, "BLOCKED", "RULESET_REQUIREMENTS_INCOMPLETE"],
  ["RF02", "historical source cannot grant authority", "ZZSYN-RA-02", "DOCUMENT_OBSERVATION", "HISTORICAL_OR_SUPERSEDED", true, true, false, false, "BLOCKED", "RULESET_SOURCE_SUPERSEDED"],
  ["RF03", "unknown source fails closed", "ZZSYN-RA-03", "DOCUMENT_OBSERVATION", "UNKNOWN", false, true, false, false, "BLOCKED", "RULESET_SOURCE_UNKNOWN"],
  ["RF04", "conflicting current sources fail closed", "ZZSYN-RA-04", "DOCUMENT_OBSERVATION", "VERIFIED_CURRENT", true, false, false, false, "BLOCKED", "RULESET_SOURCES_CONFLICT"],
  ["RF05", "vision observation cannot grant authority", "ZZSYN-RB-01", "VISION_PROVIDER_OBSERVATION", "VERIFIED_CURRENT", false, true, false, false, "BLOCKED", "VISION_OBSERVATION_NONAUTHORITATIVE"],
  ["RF06", "partial identifier without legal clip outline", "ZZSYN-RB-02~PARTIAL", "DOCUMENT_OBSERVATION", "VERIFIED_CURRENT", true, true, true, false, "BLOCKED", "PARTIAL_BOUNDARY_UNKNOWN"],
  ["RF07", "non-authoritative annotation is not a legal clip outline", "ZZSYN-RB-03~PARTIAL", "VISION_PROVIDER_OBSERVATION", "VERIFIED_CURRENT", true, true, true, false, "BLOCKED", "PARTIAL_BOUNDARY_UNKNOWN"],
  ["RF08", "complete synthetic provenance is only eligible for a future resolver", "ZZSYN-RC-01", "DOCUMENT_OBSERVATION", "VERIFIED_CURRENT", true, true, false, false, "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION", "PROVENANCE_REQUIREMENTS_SATISFIED"],
  ["RF09", "complete partial provenance remains contract-only even with a legal outline", "ZZSYN-RC-02~PARTIAL", "DOCUMENT_OBSERVATION", "VERIFIED_CURRENT", true, true, true, true, "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION", "PROVENANCE_AND_PARTIAL_CLIP_REQUIREMENTS_SATISFIED"],
  ["RF10", "malformed provenance fails closed", "ZZSYN-RC-03", "DOCUMENT_OBSERVATION", "MALFORMED", false, false, false, false, "BLOCKED", "PROVENANCE_MALFORMED"]
]);
const FORBIDDEN_CASE_KEYS = new Set([
  "area",
  "bounds",
  "coordinates",
  "crs",
  "customer",
  "geometry",
  "kml",
  "kmz",
  "latitude",
  "license",
  "location",
  "longitude",
  "map"
]);
const EXPECTED_SOURCE_SNAPSHOT = Object.freeze([
  {
    source_id: "KZ_CARTOGRAPHIC_PRODUCT_INSTRUCTION_2023",
    source_status: "VERIFIED_CURRENT",
    title: "Instruction for the creation of cartographic products using budget funds",
    official_uri: "https://www.adilet.zan.kz/rus/docs/V2300032161",
    effective_status: "CURRENT",
    supported_claim_classes: ["TOPOGRAPHIC_SHEET_FRAMEWORK"],
    requirements_supported: {
      base_sheet_semantics: true,
      fine_grid_numbering_direction: false,
      cell_angular_dimensions: false,
      crs_and_datum: false,
      axis_order: false,
      boundary_and_edge_rules: false
    }
  },
  {
    source_id: "KZ_UNIFIED_SUBSOIL_PLATFORM_RULES_2026",
    source_status: "VERIFIED_CURRENT",
    title: "Rules for operation of the unified subsoil-use platform",
    official_uri: "https://adilet.zan.kz/rus/docs/V2600038204",
    effective_status: "CURRENT",
    supported_claim_classes: ["PUBLIC_SUBSOIL_MAP_AVAILABILITY"],
    requirements_supported: {
      base_sheet_semantics: false,
      fine_grid_numbering_direction: false,
      cell_angular_dimensions: false,
      crs_and_datum: false,
      axis_order: false,
      boundary_and_edge_rules: false
    }
  },
  {
    source_id: "KZ_PUBLIC_SUBSOIL_INFORMATION_ORDER_2023",
    source_status: "HISTORICAL_OR_SUPERSEDED",
    title: "Superseded public subsoil information order with identifier-format example",
    official_uri: "https://adilet.zan.kz/rus/docs/V2300032366",
    effective_status: "SUPERSEDED",
    supported_claim_classes: ["IDENTIFIER_FORMAT_EXAMPLE_ONLY"],
    requirements_supported: {
      base_sheet_semantics: false,
      fine_grid_numbering_direction: false,
      cell_angular_dimensions: false,
      crs_and_datum: false,
      axis_order: false,
      boundary_and_edge_rules: false
    }
  },
  {
    source_id: "KZ_FINE_GRID_AND_PARTIAL_BOUNDARY_RULESET",
    source_status: "UNKNOWN",
    title: "Current fine-grid numbering and partial legal-boundary rules not yet established",
    official_uri: null,
    effective_status: "NOT_ESTABLISHED",
    supported_claim_classes: [],
    requirements_supported: {
      base_sheet_semantics: false,
      fine_grid_numbering_direction: false,
      cell_angular_dimensions: false,
      crs_and_datum: false,
      axis_order: false,
      boundary_and_edge_rules: false
    }
  }
]);
const ZERO_AUTHORITY = Object.freeze({
  resolver_success: false,
  geometry: false,
  crs: false,
  bounds: false,
  area: false,
  legal_boundary: false,
  finalizer_auto_export: false,
  map: false,
  kml: false,
  kmz: false
});

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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
  if (!isPlainObject(definition)) return false;
  if (definition.$ref && !matchesSchema(value, resolveLocalRef(rootSchema, definition.$ref), rootSchema)) return false;
  if (definition.const !== undefined && !Object.is(value, definition.const)) return false;
  if (definition.enum && !definition.enum.some((candidate) => Object.is(value, candidate))) return false;
  if (definition.type) {
    const typeMatches = definition.type === "null"
      ? value === null
      : definition.type === "array"
        ? Array.isArray(value)
        : definition.type === "object"
          ? isPlainObject(value)
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
    if (definition.maxItems !== undefined && value.length > definition.maxItems) return false;
    if (definition.items && !value.every((item) => matchesSchema(item, definition.items, rootSchema))) return false;
  }
  if (isPlainObject(value)) {
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
  if (definition.allOf && !definition.allOf.every((candidate) => matchesSchema(value, candidate, rootSchema))) return false;
  if (definition.if) {
    const branch = matchesSchema(value, definition.if, rootSchema) ? definition.then : definition.else;
    if (branch && !matchesSchema(value, branch, rootSchema)) return false;
  }
  return true;
}

function assertNoForbiddenCaseFields(value, pathParts = []) {
  if (Array.isArray(value)) {
    value.forEach((child, index) => assertNoForbiddenCaseFields(child, [...pathParts, String(index)]));
    return;
  }
  if (!isPlainObject(value)) return;
  for (const [key, child] of Object.entries(value)) {
    assert.equal(FORBIDDEN_CASE_KEYS.has(key.toLowerCase()), false, `forbidden case field ${[...pathParts, key].join(".")}`);
    assertNoForbiddenCaseFields(child, [...pathParts, key]);
  }
}

function caseSnapshot(candidate) {
  return [
    candidate.case_id,
    candidate.description,
    candidate.synthetic_identifier,
    candidate.observation_source,
    candidate.source_status,
    candidate.all_required_semantics_proven,
    candidate.sources_consistent,
    candidate.partial,
    candidate.authoritative_clip_outline_proven,
    candidate.expected_decision,
    candidate.expected_reason
  ];
}

function validateCase(candidate) {
  assert.equal(isPlainObject(candidate), true);
  assert.deepEqual(Object.keys(candidate).sort(), [...CASE_KEYS].sort());
  assertNoForbiddenCaseFields(candidate);
  assert.match(candidate.case_id, /^RF(?:0[1-9]|10)$/u);
  assert.equal(typeof candidate.description, "string");
  assert.match(candidate.synthetic_identifier, /^ZZSYN-[A-Z]{2}-[0-9]{2}(?:~PARTIAL)?$/u);
  assert.doesNotMatch(candidate.synthetic_identifier, /\d{1,3}[°′'"]|[-+]?\d+\.\d+/u);
  assert.ok(["DOCUMENT_OBSERVATION", "VISION_PROVIDER_OBSERVATION"].includes(candidate.observation_source));
  assert.ok([...SOURCE_STATUSES, "MALFORMED"].includes(candidate.source_status));
  for (const key of ["all_required_semantics_proven", "sources_consistent", "partial", "authoritative_clip_outline_proven"]) {
    assert.equal(typeof candidate[key], "boolean");
  }
  assert.ok(["BLOCKED", "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION"].includes(candidate.expected_decision));
  assert.match(candidate.expected_reason, /^[A-Z0-9_]+$/u);
  const expected = EXPECTED_CASE_SNAPSHOT.find(([caseId]) => caseId === candidate.case_id);
  assert.ok(expected, `unlisted resolver foundation case ${candidate.case_id}`);
  assert.deepEqual(caseSnapshot(candidate), expected, `${candidate.case_id} semantic snapshot drift`);
}

function evaluateFoundation(input) {
  const zero = { ...ZERO_AUTHORITY };
  if (!isPlainObject(input)
    || !["DOCUMENT_OBSERVATION", "VISION_PROVIDER_OBSERVATION"].includes(input.observation_source)
    || typeof input.all_required_semantics_proven !== "boolean"
    || typeof input.sources_consistent !== "boolean"
    || typeof input.partial !== "boolean"
    || typeof input.authoritative_clip_outline_proven !== "boolean") {
    return { decision: "BLOCKED", reason: "PROVENANCE_MALFORMED", authority: zero };
  }
  if (input.source_status === "HISTORICAL_OR_SUPERSEDED") {
    return { decision: "BLOCKED", reason: "RULESET_SOURCE_SUPERSEDED", authority: zero };
  }
  if (input.source_status === "UNKNOWN") {
    return { decision: "BLOCKED", reason: "RULESET_SOURCE_UNKNOWN", authority: zero };
  }
  if (input.source_status !== "VERIFIED_CURRENT") {
    return { decision: "BLOCKED", reason: "PROVENANCE_MALFORMED", authority: zero };
  }
  if (input.observation_source === "VISION_PROVIDER_OBSERVATION" && !input.all_required_semantics_proven) {
    return { decision: "BLOCKED", reason: "VISION_OBSERVATION_NONAUTHORITATIVE", authority: zero };
  }
  if (!input.sources_consistent) {
    return { decision: "BLOCKED", reason: "RULESET_SOURCES_CONFLICT", authority: zero };
  }
  if (!input.all_required_semantics_proven) {
    return { decision: "BLOCKED", reason: "RULESET_REQUIREMENTS_INCOMPLETE", authority: zero };
  }
  if (input.partial && !input.authoritative_clip_outline_proven) {
    return { decision: "BLOCKED", reason: "PARTIAL_BOUNDARY_UNKNOWN", authority: zero };
  }
  return {
    decision: "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION",
    reason: input.partial
      ? "PROVENANCE_AND_PARTIAL_CLIP_REQUIREMENTS_SATISFIED"
      : "PROVENANCE_REQUIREMENTS_SATISFIED",
    authority: zero
  };
}

function validateSource(source) {
  assert.equal(matchesSchema(source, schema.$defs.source), true, `${source?.source_id ?? "UNIDENTIFIED"} must satisfy source schema`);
  assert.equal(isPlainObject(source), true);
  assert.match(source.source_id, /^[A-Z0-9_]{3,96}$/u);
  assert.ok(SOURCE_STATUSES.includes(source.source_status));
  assert.equal(source.jurisdiction, "KAZAKHSTAN");
  assert.equal(isPlainObject(source.requirements_supported), true);
  assert.deepEqual(Object.keys(source.requirements_supported).sort(), [...REQUIREMENT_KEYS].sort());
  for (const key of REQUIREMENT_KEYS) assert.equal(typeof source.requirements_supported[key], "boolean");
  assert.equal(source.all_required_semantics_proven, false);
  assert.equal(source.spatial_authority_eligible, false);

  if (source.source_status === "VERIFIED_CURRENT") {
    assert.equal(source.official_source, true);
    assert.equal(source.effective_status, "CURRENT");
    assert.equal(source.status_checked_on, "2026-09-13");
    assert.match(source.official_uri, /^https:\/\/(?:www\.)?adilet\.zan\.kz\//u);
  } else if (source.source_status === "HISTORICAL_OR_SUPERSEDED") {
    assert.equal(source.official_source, true);
    assert.equal(source.effective_status, "SUPERSEDED");
    assert.match(source.official_uri, /^https:\/\/(?:www\.)?adilet\.zan\.kz\//u);
  } else {
    assert.equal(source.official_source, false);
    assert.equal(source.effective_status, "NOT_ESTABLISHED");
    assert.equal(source.official_uri, null);
    assert.equal(source.status_checked_on, null);
    assert.deepEqual(source.supported_claim_classes, []);
  }
}

test("provenance schema freezes identity, status vocabulary, and zero authority", () => {
  assert.equal(schema.$schema, "https://json-schema.org/draft/2020-12/schema");
  assert.equal(schema.$id, "urn:geokit-lab:mining-block-nomenclature-ruleset-provenance-v1");
  assert.equal(schema.additionalProperties, false);
  assert.deepEqual(schema.$defs.source.properties.source_status.enum, SOURCE_STATUSES);
  assert.equal(schema.$defs.source.properties.all_required_semantics_proven.const, false);
  assert.equal(schema.$defs.source.properties.spatial_authority_eligible.const, false);
});

test("source registry remains an UNKNOWN_RULESET foundation snapshot", () => {
  assert.equal(matchesSchema(registry, schema), true, "registry must satisfy the complete provenance schema");
  assert.equal(registry.schema_version, "mining_block_nomenclature_ruleset_provenance_v1");
  assert.equal(registry.data_class, "PUBLIC_OFFICIAL_METADATA_ONLY");
  assert.equal(registry.as_of_date, "2026-09-13");
  assert.equal(registry.aggregate_ruleset_status, "UNKNOWN_RULESET");
  assert.equal(registry.authority_failure_reason, "UNKNOWN_RULESET_NO_SPATIAL_AUTHORITY");
  assert.equal(registry.sources.length, 4);
  assert.deepEqual(
    Object.fromEntries(SOURCE_STATUSES.map((status) => [status, registry.sources.filter((source) => source.source_status === status).length])),
    { VERIFIED_CURRENT: 2, HISTORICAL_OR_SUPERSEDED: 1, UNKNOWN: 1 }
  );
});

test("source registry exact identities and requirement matrices are frozen", () => {
  const actual = registry.sources.map((source) => ({
    source_id: source.source_id,
    source_status: source.source_status,
    title: source.title,
    official_uri: source.official_uri,
    effective_status: source.effective_status,
    supported_claim_classes: source.supported_claim_classes,
    requirements_supported: source.requirements_supported
  }));
  assert.deepEqual(actual, EXPECTED_SOURCE_SNAPSHOT);
});

test("schema rejects extra, missing, mistyped, unlisted, and contradictory source metadata", () => {
  const clone = () => structuredClone(registry);
  const mutations = [];
  const rootExtra = clone();
  rootExtra.coordinates = [1, 2];
  mutations.push(rootExtra);
  const sourceExtra = clone();
  sourceExtra.sources[0].coordinates = [1, 2];
  mutations.push(sourceExtra);
  const requirementExtra = clone();
  requirementExtra.sources[0].requirements_supported.grid_origin = true;
  mutations.push(requirementExtra);
  const missingTitle = clone();
  delete missingTitle.sources[0].title;
  mutations.push(missingTitle);
  const wrongType = clone();
  wrongType.sources[0].official_source = "true";
  mutations.push(wrongType);
  const unlistedStatus = clone();
  unlistedStatus.sources[0].source_status = "ASSUMED_CURRENT";
  mutations.push(unlistedStatus);
  const currentContradiction = clone();
  currentContradiction.sources[0].official_source = false;
  mutations.push(currentContradiction);
  const historicalContradiction = clone();
  historicalContradiction.sources[2].effective_status = "CURRENT";
  mutations.push(historicalContradiction);
  const unknownContradiction = clone();
  unknownContradiction.sources[3].official_uri = "https://adilet.zan.kz/rus/docs/UNKNOWN";
  mutations.push(unknownContradiction);
  for (const mutation of mutations) assert.equal(matchesSchema(mutation, schema), false);
});

for (const source of registry.sources) {
  test(`${source.source_id} provenance contract`, () => validateSource(source));
}

test("current official sources do not collectively prove the resolver ruleset", () => {
  const current = registry.sources.filter((source) => source.source_status === "VERIFIED_CURRENT");
  const combined = Object.fromEntries(REQUIREMENT_KEYS.map((key) => [key, current.some((source) => source.requirements_supported[key])]));
  assert.deepEqual(combined, {
    base_sheet_semantics: true,
    fine_grid_numbering_direction: false,
    cell_angular_dimensions: false,
    crs_and_datum: false,
    axis_order: false,
    boundary_and_edge_rules: false
  });
  assert.equal(REQUIREMENT_KEYS.every((key) => combined[key]), false);
});

test("superseded identifier example is metadata only", () => {
  const historical = registry.sources.find((source) => source.source_status === "HISTORICAL_OR_SUPERSEDED");
  assert.deepEqual(historical.supported_claim_classes, ["IDENTIFIER_FORMAT_EXAMPLE_ONLY"]);
  assert.equal(Object.values(historical.requirements_supported).some(Boolean), false);
  assert.equal(historical.spatial_authority_eligible, false);
});

test("contract freezes the authority chain and explicit non-implementation boundary", () => {
  assert.match(contract, /Vision \/ Document Observation[\s\S]*Grammar and lexical safety[\s\S]*Ruleset provenance verification[\s\S]*Deterministic Mining Block Resolver[\s\S]*Coordinate Finalizer[\s\S]*normalized_geometry_v1[\s\S]*Map \/ KML \/ KMZ/u);
  assert.match(contract, /does not implement a Resolver/u);
  assert.match(contract, /must not decide the final CRS, axis order, bounds, geometry, legal boundary, confirmation state, or export permission/u);
  assert.match(contract, /PARTIAL_BOUNDARY_UNKNOWN/u);
});

test("fixture contains only synthetic identifiers and no coordinate-like values", () => {
  assert.equal(cases.length, 10);
  assert.equal(new Set(cases.map((candidate) => candidate.case_id)).size, cases.length);
  for (const candidate of cases) {
    validateCase(candidate);
  }
  assert.deepEqual(cases.map(caseSnapshot), EXPECTED_CASE_SNAPSHOT);
});

test("resolver fixture rejects extra spatial, customer, and real-location fields recursively", () => {
  const base = cases.find((candidate) => candidate.case_id === "RF08");
  for (const mutation of [
    { ...base, geometry: true },
    { ...base, coordinates: [1, 2] },
    { ...base, evidence: { customer: "synthetic-but-forbidden" } },
    { ...base, evidence: { location: "synthetic-but-forbidden" } },
    Object.fromEntries(Object.entries(base).filter(([key]) => key !== "expected_reason"))
  ]) {
    assert.throws(() => validateCase(mutation));
  }
});

test("resolver fixture rejects coordinated input and oracle drift", () => {
  const mutate = (caseId, changes) => ({ ...cases.find((candidate) => candidate.case_id === caseId), ...changes });
  for (const mutation of [
    mutate("RF02", {
      source_status: "VERIFIED_CURRENT",
      all_required_semantics_proven: false,
      expected_reason: "RULESET_REQUIREMENTS_INCOMPLETE"
    }),
    mutate("RF03", {
      source_status: "VERIFIED_CURRENT",
      expected_reason: "RULESET_REQUIREMENTS_INCOMPLETE"
    }),
    mutate("RF04", {
      sources_consistent: true,
      expected_decision: "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION",
      expected_reason: "PROVENANCE_REQUIREMENTS_SATISFIED"
    })
  ]) {
    assert.throws(() => validateCase(mutation), /semantic snapshot drift/u);
  }
});

for (const candidate of cases) {
  test(`${candidate.case_id} resolver foundation decision`, () => {
    const actual = evaluateFoundation(candidate);
    assert.equal(actual.decision, candidate.expected_decision);
    assert.equal(actual.reason, candidate.expected_reason);
    assert.deepEqual(actual.authority, ZERO_AUTHORITY);
  });
}

test("malformed and missing provenance types fail closed", () => {
  const base = cases.find((candidate) => candidate.case_id === "RF08");
  for (const mutation of [
    null,
    [],
    { ...base, all_required_semantics_proven: "true" },
    { ...base, sources_consistent: null },
    { ...base, partial: 0 },
    { ...base, authoritative_clip_outline_proven: "false" },
    { ...base, observation_source: "UNLISTED_OBSERVATION" },
    { ...base, source_status: "UNLISTED_STATUS" }
  ]) {
    const result = evaluateFoundation(mutation);
    assert.equal(result.decision, "BLOCKED");
    assert.equal(result.reason, "PROVENANCE_MALFORMED");
    assert.deepEqual(result.authority, ZERO_AUTHORITY);
  }
});

test("Vision claims cannot cure incomplete provenance", () => {
  const result = evaluateFoundation({
    ...cases.find((candidate) => candidate.case_id === "RF08"),
    observation_source: "VISION_PROVIDER_OBSERVATION",
    all_required_semantics_proven: false
  });
  assert.equal(result.decision, "BLOCKED");
  assert.equal(result.reason, "VISION_OBSERVATION_NONAUTHORITATIVE");
  assert.deepEqual(result.authority, ZERO_AUTHORITY);
});

test("partial blocks require a positively proven authoritative clip outline", () => {
  const unresolved = evaluateFoundation(cases.find((candidate) => candidate.case_id === "RF06"));
  const contractEligible = evaluateFoundation(cases.find((candidate) => candidate.case_id === "RF09"));
  assert.equal(unresolved.reason, "PARTIAL_BOUNDARY_UNKNOWN");
  assert.equal(unresolved.decision, "BLOCKED");
  assert.equal(contractEligible.decision, "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION");
  assert.deepEqual(contractEligible.authority, ZERO_AUTHORITY);
});

test("even a future-eligible provenance case emits no Resolver or downstream authority", () => {
  for (const candidate of cases.filter(({ expected_decision }) => expected_decision === "ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION")) {
    assert.deepEqual(evaluateFoundation(candidate).authority, ZERO_AUTHORITY);
  }
});
