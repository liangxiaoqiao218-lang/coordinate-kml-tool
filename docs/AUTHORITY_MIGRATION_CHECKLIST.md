# GeoKit Lab Authority Migration Checklist

Document status: `APPROVED_GOVERNANCE_BASELINE`  
Checklist owner: `TRACK-G`  
Migration target: future Coordinate Authority transition from `LEGACY_V2` to an approved V3 path  
Gate count: `13`

## 1. Non-negotiable state

```text
CURRENT_PRODUCTION_AUTHORITY=LEGACY_V2
V3_MODE=TRUE_SHADOW_ONLY
V3_PRODUCTION_AUTHORITY=false
V3_CANARY=BLOCKED
V3_MIGRATION=BLOCKED
```

No branch merge, deployment, shadow success, or private V3 status changes authority by itself. Each gate requires evidence, owning-track completion, TRACK-G review, and the stated human approval.

Long-lived TRACK-L, TRACK-E, and TRACK-S feature branches do not merge directly into one another or directly become production authority. Cross-track gate evidence is assembled on a temporary integration branch in a clean, dedicated worktree. A V3, Spatial, or Satellite branch cannot bypass M0-M12 by merging into production.

M12 rollback must be designed and approved before M10 or M11 even though it is numbered after the authority-swap gate.

## 2. Gate checklist

| Gate | Name | Minimum exit evidence | Owner(s) | Current G-02 status |
| --- | --- | --- | --- | --- |
| M0 | V3 Module Qualification | Family-scoped module tests; explicit supported/unsupported scope; deterministic failure behavior; no hidden production takeover | TRACK-E | OPEN; shadow development allowed, migration not cleared |
| M1 | V3 Canonical Adapter | An explicit adapter from V3 private result to `finalized_coordinate_result_v1`; no direct private-schema exposure to KML/Spatial; identity generation defined | TRACK-E, reviewed by TRACK-G | BLOCKED; adapter not implemented |
| M2 | Canonical Contract Compatibility | Field-by-field compatibility including WGS84 geometry, `crs.axisOrder`, identity, revision, hash, authority states, and versioning judgment | TRACK-E + TRACK-G | BLOCKED_BY_M1; current baseline gap recorded |
| M3 | KML Consumer Compatibility | Legacy/server KML behavior comparison including technical readiness, review, confirmation, safety/account gates, reason codes, and P08F semantics | TRACK-L + TRACK-E | BLOCKED_BY_M2 |
| M4 | Spatial Consumer Compatibility | Map Preview identity/CRS/geometry/hash compatibility; Spatial Facts compatibility; proof that map permission remains independent of KML | TRACK-S + TRACK-E | BLOCKED_BY_M2 |
| M5 | Review / Confirmation Compatibility | Accepted, pending, rejected, not-required, idempotent, stale revision, and geometry-hash mismatch cases; no stale confirmation reuse | TRACK-L + TRACK-E | BLOCKED_BY_M2 |
| M6 | Result Identity / Revision Compatibility | Stable `resultId`; monotonic safe-integer revision; deterministic `geometryHash`; stale response and revision rejection across consumers | TRACK-L + TRACK-E + TRACK-S | BLOCKED_BY_M2 |
| M7 | Golden Comparison | Approved golden corpus comparing Legacy and V3 canonical outputs, with differences classified and no unsupported PASS | TRACK-E + TRACK-L | BLOCKED_BY_M3_M6 |
| M8 | Real Sample Comparison | Authorized representative real samples; provenance recorded; provider/fixture limitations reported as blockers rather than PASS | TRACK-E + TRACK-L | BLOCKED_BY_M7 |
| M9 | Shadow Stability | Sustained `TRUE_SHADOW_ONLY` observation window; error, timeout, disagreement, and regression thresholds approved; zero authority side effects | TRACK-E + TRACK-G | BLOCKED_BY_M7_M8 |
| M10 | Canary | Family-scoped, default-OFF canary; allowlist and rollback controls; runtime branch/commit identity verified; M12 rollback approved before enablement | TRACK-E + release owner + TRACK-G | BLOCKED_BY_M9_AND_M12 |
| M11 | Production Authority Swap | Explicit human approval; verified runtime identity; KML and Spatial compatibility closed; canary success; rollback ready; authority flag change performed by owning release track | TRACK-L/approved successor + TRACK-G | BLOCKED_BY_M10; `V3_PRODUCTION_AUTHORITY=false` |
| M12 | Rollback | Tested authority reversion to Legacy/V2; data/identity compatibility; trigger thresholds, operator, timing, and post-rollback verification defined | TRACK-L + TRACK-E + release owner | OPEN_DESIGN_REQUIRED; must be approved before M10/M11 |

## 3. Gate evidence rules

For every gate, record:

- exact branch, commit, runtime identity where applicable, and date;
- test commands, totals, failures, skips, missing fixtures, and provider limitations;
- canonical contract version and field-level differences;
- whether application, runtime, configuration, flags, allowlists, or data were changed;
- owner sign-off, TRACK-G integration judgment, and human approval when required.

Targeted PASS does not convert fixture or credential blockers into full PASS. Runtime identity is stronger release evidence than a pushed Git commit.

## 4. Required compatibility invariants

Every gate from M1 onward must preserve:

```text
ONE_WAY_DEPENDENCY_RULE=Recognition Producer -> Coordinate Authority -> Canonical Result -> Consumers
KML_SPATIAL_SIBLING_RULE=PRESERVED
MAP_KML_PERMISSION_SEPARATION=PRESERVED
AUTHORITATIVE_GEOMETRY=EPSG:4326 longitude,latitude
V3_PRIVATE_SCHEMA_EXPOSED_TO_CONSUMERS=false
```

`FinalizedResultSpatialGeometryAdapter` is export-grade / `AUTO_EXPORT`-oriented and cannot be used as the general Map or Satellite gate.

## 5. Authority-swap lock

Until M11 is explicitly approved and completed:

```text
V3_PRODUCTION_AUTHORITY=false
CURRENT_PRODUCTION_AUTHORITY=LEGACY_V2
KML_AUTHORITY=LEGACY_SERVER_AUTHORITATIVE_KML_ELIGIBILITY
```

TRACK-G can issue a readiness recommendation but cannot perform the authority mutation.

`coordinateAuthorityEngine` is not currently exposed as runtime metadata. It remains a `GOVERNANCE_ENHANCEMENT_CANDIDATE` / `PROPOSED` field and is not evidence that M11 has occurred.

## 6. Current release coordination

This migration checklist does not block TRACK-E internal `TRUE_SHADOW_ONLY` module development. It blocks V3 canary, migration, and production authority.

P08G is a TRACK-L-only hotfix deployment activity, not an authority migration. G-02 records `P08G_GOVERNANCE_AUTHORIZATION=AUTHORIZED` for TRACK-L to execute `P08G_CONTROLLED_PRODUCTION_REDEPLOYMENT`; TRACK-G does not execute it.

`P09_STATUS=HOLD` remains unchanged and requires at least P08G production regression closure before a later resume decision.
