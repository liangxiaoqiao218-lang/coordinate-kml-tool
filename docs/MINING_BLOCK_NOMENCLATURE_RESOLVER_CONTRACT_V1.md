# Mining Block Nomenclature Resolver Contract v1

Document status: `FOUNDATION_ONLY_NOT_A_SPATIAL_RULESET`

Contract version: `mining_block_nomenclature_resolver_contract_v1`

Authority status at this revision: `UNKNOWN_RULESET`

Data boundary: `SYNTHETIC_ONLY`

## 1. Purpose

This contract separates observed mining-block nomenclature from deterministic spatial resolution. It does not implement a Resolver and does not grant CRS, geometry, Finalizer, Map, KML, or KMZ authority.

The required future flow is:

```text
Vision / Document Observation
-> Grammar and lexical safety
-> Ruleset provenance verification
-> Deterministic Mining Block Resolver
-> Canonical CRS and axis validation
-> Coordinate Finalizer
-> normalized_geometry_v1
-> Map / KML / KMZ
```

Vision or document extraction may report what text appears to be present. It must not decide the final CRS, axis order, bounds, geometry, legal boundary, confirmation state, or export permission.

## 2. Ruleset provenance states

Every source is classified as exactly one of:

- `VERIFIED_CURRENT`: an official source whose current status was checked on the recorded date. This classification alone does not make it sufficient for spatial resolution.
- `HISTORICAL_OR_SUPERSEDED`: an official or otherwise traceable source that is historical, repealed, superseded, or not safe to treat as current authority.
- `UNKNOWN`: a required rule or source that has not been positively established.

Only a `VERIFIED_CURRENT` source may contribute to a future authoritative ruleset. Before deterministic resolution is eligible, current official evidence must positively and consistently establish all of:

1. base sheet semantics;
2. fine-grid subdivision and numbering direction;
3. cell angular dimensions;
4. CRS and datum;
5. axis order;
6. boundary inclusion and edge rules.

Missing, malformed, historical, superseded, or conflicting evidence fails closed. The current registry does not satisfy all six requirements, so its aggregate status remains `UNKNOWN_RULESET`.

## 3. Source registry scope

The registry records source identity, current-status checks, supported claim classes, and a six-part requirement matrix. It does not copy protected source text and does not contain customer data, coordinates, licences, or real mining-block identifiers.

The registry includes:

- current official cartographic instructions that establish general topographic sheet concepts;
- current official rules for the unified subsoil-use platform and its public map;
- a clearly marked superseded official order retained only as historical evidence of an identifier format;
- explicit `UNKNOWN` entries for the unresolved fine-grid and partial-boundary rules.

An official example is not an algorithm. A historical example must never be promoted into a current coordinate rule.

## 4. Resolver foundation decision

The foundation evaluator may return only:

- `ELIGIBLE_FOR_FUTURE_DETERMINISTIC_RESOLUTION`: all provenance requirements are positively established by mutually consistent current sources, but this repository phase still emits no geometry;
- `BLOCKED`: the candidate cannot proceed to deterministic resolution.

The current source registry always resolves to `BLOCKED / UNKNOWN_RULESET_NO_SPATIAL_AUTHORITY`.

Allowed failure reasons are frozen in the accompanying schema and fixture. In all cases in this phase:

```text
resolver_success=false
geometry=false
crs=false
bounds=false
area=false
legal_boundary=false
finalizer_auto_export=false
map=false
kml=false
kmz=false
```

## 5. Partial blocks

A partial qualifier identifies a review requirement, not a clipping polygon. Without a positively proven authoritative legal clipping outline, the only permitted classification is:

```text
BLOCK_IDENTIFIED
PARTIAL_BOUNDARY_UNKNOWN
```

The system must not substitute the complete standard block, infer an area, close a guessed polygon, or grant geometry/export authority. A Vision observation, user-drawn outline, screenshot boundary, or non-authoritative annotation cannot satisfy the legal clipping requirement.

## 6. Authority separation

This contract cannot modify or bypass:

- `finalized_coordinate_result_v1`;
- Coordinate quality, CRS, geometry, identity, revision, or confirmation gates;
- `normalized_geometry_v1`;
- Map Preview eligibility;
- KML or KMZ permission.

Future Resolver output must enter those contracts through a separately reviewed adapter. No adapter is authorized or implemented here.

## 7. Versioning and next gate

Changing any grid direction, subdivision, cell dimension, CRS, axis, edge, or partial-boundary rule is authority-significant and requires a new reviewed ruleset version with current official provenance.

The next implementation gate is blocked until the complete current ruleset is independently verified. This foundation may be merged without enabling production behavior because it adds documentation, synthetic fixtures, a schema, and an offline regression only.
