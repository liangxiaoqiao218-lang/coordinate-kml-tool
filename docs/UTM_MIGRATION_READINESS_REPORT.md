# UTM Migration Readiness Report

Report date: 2026-08-07

Branch: `v2/utm-intent-router`

Phase: 5-0 Migration Readiness Review

## 1. Current State

### Legacy UTM30

- The production-authoritative UTM30 path remains
  `utm30n-projected-x-y`.
- Existing `precisionMode`, parser routing, KML behavior, historical records,
  and locked baselines remain unchanged.
- No Phase 5 production migration, canonical default routing, or cohort rollout
  is active.

### V2 Typed UTM

- V2 represents UTM as `utm_projected_xy` with explicit `projection`, `datum`,
  `zone`, `hemisphere`, and `epsg` fields.
- WGS84 UTM30N resolves to `EPSG:32630`.
- WGS84 UTM50S resolves to `EPSG:32750`.
- The Typed Result validates projected coordinates and independently transforms
  them to WGS84 in the shadow path.

### CRS Evidence

- The independent CRS Vision pass collects document evidence for projection,
  datum, zone, hemisphere, EPSG, and CRS labels.
- The Evidence Collector and Shadow Resolver do not infer CRS from country,
  coordinate ranges, or northing values.
- Missing or conflicting evidence remains candidate, unknown, or blocked.
- The committed real-image evidence report records Indonesia UTM50S positive
  coverage and BFTM, MGRS, and Kyrgyz GK negative protection.

### Migration Gate

- The Phase 4C Gate remains shadow-only and returns `V2_ALLOWED`,
  `LEGACY_ONLY`, or `BLOCKED`.
- `V2_ALLOWED` requires confirmed conflict-free CRS, a valid Typed Result,
  independently verified transformation provenance, Quality Gate `PASS`,
  explicit user confirmation, and an allowed migration observation status.
- Invalid projected coordinates, mismatched point counts, stale transformations,
  CRS conflicts, and transformation mismatches are blocked.
- A Gate result does not change the production path or authorize Export.

## 2. Compatibility Evidence

### UTM30 MATCH

The current Typed UTM regression compares the legacy UTM30 result with the V2
typed representation for eight projected points:

```yaml
legacy: utm30n-projected-x-y
v2:
  coordinateType: utm_projected_xy
  datum: WGS84
  zone: 30
  hemisphere: north
  epsg: EPSG:32630
comparison:
  status: MATCH
  pointCount: 8
  maximumDifference: 0
```

The Phase 3 observation regression also classifies the UTM30 fixture as
`MATCH`.

### UTM50S V2_ONLY

The Indonesia UTM50S typed fixture resolves to:

```yaml
migrationStatus: V2_ONLY
coordinateType: utm_projected_xy
datum: WGS84
zone: 50
hemisphere: south
epsg: EPSG:32750
```

This proves that the typed engine can represent a confirmed UTM zone that the
legacy dedicated type does not support. It is new-capability evidence, not
UTM30 production-migration authorization.

### Transformation results

- UTM30 legacy/V2 comparison: eight points, maximum difference `0`.
- A deliberately shifted legacy coordinate above tolerance produces
  `TRANSFORMATION_MISMATCH`.
- The Migration Gate independently recomputes WGS84 from X/Y and Typed CRS with
  a `1e-8` degree provenance tolerance.
- Changed X/Y with an unchanged WGS84 result produces
  `STALE_TRANSFORMED_RESULT` and is blocked.
- Invalid, non-finite, out-of-range, or point-count-mismatched projected data is
  blocked.

No canonical-versus-legacy Export comparison suite or approved Export report
is present. Coordinate transformation compatibility is demonstrated; complete
KML Export compatibility is not yet demonstrated.

## 3. Safety Evidence

### MGRS

- MGRS evidence blocks `utm_projected_xy`.
- The Shadow Resolver does not create a typed UTM projected-X/Y result.
- Migration Observation records MGRS as `BLOCKED`.
- Migration Gate returns `BLOCKED` for the MGRS fixture.

### BFTM

- Explicit BFTM evidence excludes UTM intent.
- BFTM remains on its dedicated legacy path.
- Migration Observation records BFTM as `NOT_UTM`.
- Migration Gate preserves BFTM as `LEGACY_ONLY` rather than migrating it to
  typed UTM.

### Kyrgyz GK

- Explicit Kyrgyz Gauss-Kruger evidence excludes UTM intent.
- Migration Observation records Kyrgyz GK as `NOT_UTM`.
- Migration Gate preserves the legacy result as `LEGACY_ONLY`.

### Unknown projected X/Y

- X/Y values without explicit CRS evidence remain `unknown`.
- Country, coordinate range, and northing are not accepted as CRS evidence.
- Unknown projected X/Y cannot generate a confirmed typed result.
- Migration Gate returns `BLOCKED` when no safe legacy result exists.
- The confirmation flow requires explicit user-supplied CRS parameters and does
  not automatically guess a zone or hemisphere.

## 4. Regression Evidence

The following deterministic suites were executed from the current working tree
on 2026-08-07:

| Area | Script | Result |
|---|---|---:|
| Shadow Intent | `scripts/utm-intent-shadow-regression.js` | PASS, 13 cases and all unchanged-output invariants |
| CRS Evidence | `scripts/crs-evidence-acquisition-regression.js` | 7/7 PASS |
| Typed UTM | `scripts/utm-typed-result-shadow-regression.js` | 6/6 PASS |
| Migration Observation | `scripts/utm-migration-observation-regression.js` | 8/8 PASS |
| Migration Gate | `scripts/utm-migration-gate-regression.js` | 17/17 PASS |
| CRS Confirmation | `scripts/crs-confirmation-ui-regression.js` | 4/4 PASS |

The committed real-image asset report records:

- CRS Evidence real-image regression: 6/6 PASS for three Indonesia UTM50S
  images and BFTM, MGRS, and Kyrgyz GK negative samples.

Phase 4B validation previously reported Structured Projected Priority 3/3 PASS
for the three Indonesia UTM50S images. The executable regression script is
present, but no standalone machine-readable result artifact is retained in the
repository.

The real-image suites were not rerun during this Phase 5-0 review because the
current process has no configured Aliyun/DashScope Vision credentials or
external environment file. The recorded results remain useful evidence, but
they do not replace a repeatable release-candidate run with pinned provider and
model configuration.

The Phase 3 summary contains deliberate negative fixtures for `LEGACY_ONLY`,
`CRS_CONFLICT`, and `TRANSFORMATION_MISMATCH`. Those counts prove status
classification and blocking behavior; they are not real production incidents.
No completed real production observation window or controlled-cohort report is
available.

## 5. Rollback Readiness

### Legacy alias

The documented target relationship preserves:

```yaml
canonical: utm-projected-x-y
legacyAlias: utm30n-projected-x-y
```

Historical legacy identifiers remain readable and must not be rewritten. The
alias is compatibility metadata and cannot be used as evidence for a new image.

### Rollback strategy

The controlled-migration specification defines rollback to the unchanged
legacy-authoritative UTM30 route, preservation of diagnostic evidence, rerun of
locked regressions, and verification that historical data and KML were not
rewritten.

This strategy is documented but has not been exercised against an implemented
canonical-authority production path. There is no executed rollback report.

### Kill switch

The specification requires a canonical-authority kill switch before a bounded
cohort can be enabled. No production kill switch or controlled-cohort routing
implementation is present in this phase, and no kill-switch exercise has been
recorded.

Rollback readiness is therefore incomplete. The unchanged legacy path provides
a safe current authority, but it does not prove that a future canonical rollout
can be reversed operationally.

## 6. Final Decision

```text
HOLD_LEGACY
```

Reasons:

1. UTM30 typed coordinate compatibility is demonstrated, but canonical versus
   legacy Export comparison has not been completed.
2. There is no completed real Shadow Observation window or controlled-cohort
   readiness report.
3. The production kill switch is not implemented or exercised.
4. The documented rollback procedure has not been tested against a real
   canonical-authority path.
5. Real-image evidence is recorded, but the release-candidate real-image suites
   were not repeatable in this review environment because Vision provider
   configuration was unavailable.

The legacy UTM30 path remains authoritative. No default-path migration is
authorized by this report.
