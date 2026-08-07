# UTM Controlled Migration Decision Specification

## 1. Status and Scope

Phase 4D is a design-only decision contract for a future controlled UTM
migration. It defines the evidence required to decide whether legacy UTM30 may
be replaced by the canonical typed UTM result. It does not perform that
migration and does not authorize a production routing, API, Export, parser,
`precisionMode`, registry, baseline, or user-data change.

The Phase 4C Migration Gate remains shadow-only. A `V2_ALLOWED` decision is an
input to Phase 4D review, not permission for code to switch production output.

## 2. Migration Goal

The controlled migration goal is:

```text
Legacy UTM30
utm30n-projected-x-y
        |
        | verified compatibility and alias resolution
        v
Canonical UTM Typed Result
utm-projected-x-y
+ utm_projected_xy
+ explicit datum / zone / hemisphere / EPSG
```

For the initial migration, the canonical result must represent exactly:

```yaml
projection: utm
datum: WGS84
zone: 30
hemisphere: north
epsg: EPSG:32630
```

The migration changes the internal canonical representation only after all
gates pass. It must not silently rewrite historical records, change frozen KML,
or use the legacy alias as CRS evidence for a new image.

UTM50S and other future zones demonstrate canonical capability, but they do not
by themselves prove that the legacy UTM30 path is safe to replace.

## 3. Shadow Observation Requirements

The migration decision must be based on retained, reviewable Shadow
Observation records using the fixed Phase 3 statuses:

- `MATCH`: legacy and V2 CRS plus transformed coordinates agree within the
  approved tolerance. This is the required positive status for UTM30 migration.
- `V2_ONLY`: V2 has confirmed typed capability with no comparable legacy UTM
  result. It is reviewed as new capability and does not count as UTM30
  compatibility evidence.
- `LEGACY_ONLY`: legacy has a result but V2 lacks a complete, confirmed CRS or
  otherwise cannot produce an eligible typed result. Any real unexplained UTM30
  occurrence blocks migration.
- `CRS_CONFLICT`: legacy and V2 CRS disagree or explicit evidence conflicts.
  Any real unresolved occurrence blocks migration.
- `TRANSFORMATION_MISMATCH`: the CRS agrees but point counts or transformed
  coordinates disagree. Any real unresolved occurrence blocks migration.

Observation summaries must distinguish intentional regression fixtures from
real observations. Deliberately constructed conflict and mismatch tests prove
that blocking works; they are not production conflict incidents.

Before a migration decision, the observation plan must define:

- the UTM30 sample set and locked fixtures;
- the real-observation duration and minimum reviewed sample count;
- status counts and the list of all non-`MATCH` UTM30 observations;
- redaction, retention, and access rules;
- the comparison tolerance and transformation implementation version;
- evidence that observation itself did not change the authoritative legacy
  result, KML, or user flow.

## 4. Migration Preconditions

Every precondition is mandatory. Missing evidence is a decision to remain on
the legacy-authoritative path.

### 4.1 UTM30 legacy comparison

- All locked UTM30 comparison fixtures remain `MATCH`.
- Datum, zone, hemisphere, and EPSG match exactly.
- Projected and transformed point counts match.
- Independently generated WGS84 coordinates remain within the approved
  comparison tolerance.
- The Phase 4C data-integrity checks reject invalid X/Y, stale transformed
  results, and point-count mismatches.

### 4.2 Real migration observation

- No real UTM30 `CRS_CONFLICT` remains unresolved.
- No real UTM30 `TRANSFORMATION_MISMATCH` remains unresolved.
- Every real UTM30 `LEGACY_ONLY` result is explained and either fixed through a
  separately reviewed change or explicitly excluded from migration scope.
- `V2_ONLY` cases remain separately reviewed new capabilities.
- BFTM, MGRS, Kyrgyz GK, unknown projected X/Y, and other non-UTM protections
  remain intact.

### 4.3 Export comparison

- Legacy and canonical Export consume the same accepted point order and
  geometry.
- Longitude/latitude order, precision, rounding, altitude, names, and metadata
  are compared explicitly.
- Locked UTM30 KML is byte-identical where byte stability is required, or passes
  an approved semantic comparison where serialization differences are allowed.
- Export consumes the accepted typed result and never reparses OCR text or
  guesses CRS.
- A failed Export comparison blocks migration even when coordinate comparison
  is `MATCH`.

### 4.4 Regression and operational readiness

- UTM Intent, CRS Evidence, Typed Result, Migration Observation, Migration Gate,
  confirmation flow, structured projected priority, and Export comparison
  suites all pass.
- Locked BFTM, UTM30, MGRS, Kyrgyz GK, DMS, and related baselines remain
  unchanged.
- The rollback procedure is executable, reviewed, and tested before enabling
  canonical authority.
- The migration change is isolated from unrelated UI, parser, OCR, and Export
  changes.
- An explicit human approval records the reviewed commit, observation report,
  regression report, and rollback owner.

## 5. Controlled Decision States

Phase 4D uses decision states separate from the Phase 4C per-result Gate:

```text
HOLD_LEGACY
READY_FOR_CONTROLLED_MIGRATION
MIGRATION_BLOCKED
```

- `HOLD_LEGACY`: evidence is incomplete, observation is still running, or a
  required precondition has not yet been demonstrated.
- `READY_FOR_CONTROLLED_MIGRATION`: every precondition passes and the reviewed
  migration may proceed as a separate, reversible implementation change.
- `MIGRATION_BLOCKED`: a real CRS conflict, transformation mismatch, Export
  mismatch, regression failure, or unavailable rollback prevents migration.

No software component may infer `READY_FOR_CONTROLLED_MIGRATION` from one
`MATCH`, one `V2_ALLOWED`, or a successful UTM50S result. The decision is an
audited release decision over the complete evidence set.

## 6. Alias Strategy

The target registry relationship is:

```yaml
canonical:
  typeId: utm-projected-x-y
  coordinateType: utm_projected_xy

legacyAliases:
  - typeId: utm30n-projected-x-y
    crs:
      projection: utm
      datum: WGS84
      zone: 30
      hemisphere: north
      epsg: EPSG:32630
```

Alias rules:

1. Existing `utm30n-projected-x-y` records remain readable and retain their
   stored identifier.
2. Exact alias resolution may create an internal canonical view; it must not
   rewrite historical data or frozen baselines.
3. New confirmed UTM results use the canonical type plus explicit CRS fields.
4. The alias is compatibility metadata, not evidence for newly uploaded images.
5. Explicit CRS evidence and conflicts always take precedence over alias lookup.
6. Export must receive an already accepted result and must not resolve or infer
   CRS independently.
7. The alias cannot be removed until the final freeze conditions and the
   documented compatibility-retention period are satisfied.

## 7. Controlled Migration Sequence

An approved implementation must be staged and independently reversible:

1. Keep legacy authoritative and verify the final Shadow observation report.
2. Register the canonical type and exact legacy alias without changing output.
3. Enable canonical evaluation only for confirmed UTM30 results that receive
   `V2_ALLOWED`.
4. Compare canonical Export with legacy Export before returning any canonical
   output.
5. Enable canonical authority for a bounded cohort behind a kill switch.
6. Monitor decision status, Export comparison, errors, and rollback indicators.
7. Expand only when the bounded cohort satisfies the same migration gates.
8. Preserve legacy read and rollback capability throughout the observation and
   compatibility-retention period.

Phase 4D does not implement any of these steps.

## 8. Rollback Plan

Rollback must restore the legacy-authoritative UTM30 path without deleting data
or reconstructing previous results.

### 8.1 Rollback triggers

Rollback is required for:

- a real `CRS_CONFLICT` or `TRANSFORMATION_MISMATCH` after canonical enablement;
- canonical/legacy Export disagreement beyond the approved rules;
- a locked regression failure;
- invalid projected coordinates or stale transformed data reaching an allowed
  path;
- unexpected BFTM, MGRS, Kyrgyz GK, DMS, or unknown-X/Y routing changes;
- loss of required telemetry, comparison, or auditability.

### 8.2 Rollback procedure

1. Disable the canonical-authority kill switch.
2. Route UTM30 recognition, transformation, and Export back to the unchanged
   legacy `utm30n-projected-x-y` path.
3. Keep the canonical type and alias readable, but prevent canonical results
   from becoming authoritative.
4. Preserve affected inputs, decision records, comparison output, and error
   details for diagnosis without exposing sensitive source content.
5. Re-run the locked legacy UTM30 and non-UTM regression suites.
6. Confirm historical KML and user data were not rewritten.
7. Record the rollback commit, trigger, scope, and recovery verification.

Rollback must not require a database migration, baseline rewrite, or deletion
of canonical observations. If rollback cannot be performed by routing control
alone, migration is not ready.

## 9. Freeze Conditions

The controlled migration is complete and eligible for freeze only when all of
the following are true:

1. The registry has one canonical `utm-projected-x-y` type with explicit typed
   CRS parameters and the exact `utm30n-projected-x-y` compatibility alias.
2. All locked UTM30 comparisons remain `MATCH` and all approved Export
   comparisons pass.
3. The completed real-observation window contains no unexplained UTM30
   `LEGACY_ONLY`, unresolved `CRS_CONFLICT`, or unresolved
   `TRANSFORMATION_MISMATCH` result.
4. Migration Gate data-integrity, Quality Gate, and explicit CRS confirmation
   remain mandatory for canonical authority.
5. All UTM and protected non-UTM regression suites pass against the final
   migration commit.
6. Historical user data, legacy identifiers, baselines, and frozen KML remain
   readable and unchanged.
7. The controlled cohort completes its defined monitoring window without a
   rollback trigger.
8. The rollback procedure has been successfully exercised against the final
   architecture and remains available for the compatibility-retention period.
9. No Export component reparses text or infers datum, zone, or hemisphere.
10. Migration evidence, approvals, final commit, and rollback ownership are
    recorded in an auditable freeze report.

Until every condition is met, the status remains `HOLD_LEGACY` or
`MIGRATION_BLOCKED`; Phase 4D documentation alone never changes production
authority.

## 10. Prohibited Changes in Phase 4D Design

This design phase must not:

- modify `server.js`, `index.html`, Export, parser routing, or `precisionMode`;
- enable the canonical type in production;
- remove or rewrite the legacy UTM30 type;
- change locked baselines or historical user data;
- treat country, coordinate ranges, northing, or a legacy alias as new CRS
  evidence;
- treat `V2_ALLOWED`, `MATCH`, or `V2_ONLY` as automatic release approval;
- submit or push an implementation change as part of this design task.
