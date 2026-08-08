# Coordinate Engine V2 Release Candidate Notes

## Release Candidate Identity

- Candidate: `Coordinate Engine V2 RC1`
- Candidate branch: `v2/utm-intent-router`
- Candidate commit: `095168fc44b697ab11f8e4af2277704e9bd240cc`
- Candidate scope: Coordinate Type Arbitration and the existing UTM V2 shadow and migration-governance stack
- Production status: `HOLD`
- Primary Production deployment: not approved
- UTM migration status: `HOLD_LEGACY`

This release candidate is a testable V2 package. It is not authorization to
merge to `main`, deploy to `geokitlab.com`, or make the canonical UTM path
authoritative in production.

## New Capabilities

### Coordinate Type Arbitration

Coordinate interpretations now pass through a final arbitration layer with the
following authority order:

1. explicit CRS evidence;
2. typed projection;
3. structured projected X/Y;
4. validated WGS84;
5. DMS;
6. chat coordinates.

Explicit CRS conflicts are blocked instead of being resolved by numeric or
geographic guessing.

### Typed UTM handling

Explicit WGS84 UTM evidence can produce a canonical typed result containing:

- projection;
- datum;
- zone;
- hemisphere;
- EPSG identifier.

The Indonesia UTM Zone 50 South fixtures resolve to `EPSG:32750` without being
renamed as legacy UTM30 or ordinary DMS.

### Response finalization

Coordinate API success paths pass through a common response finalizer. The
finalizer preserves existing payload fields and adds a consistent arbitration
contract:

- `coordinateType`;
- `coordinateArbitration`;
- `arbitrationEligible`;
- `confirmationStatus`;
- `qualityGateStatus`;
- `kml_ready`.

### Confirmation and KML safety

CRS recognition, user confirmation, Quality Gate status, and KML readiness are
separate states. Canonical UTM results remain `kml_ready=false` until the
required CRS confirmation is complete. Explicit CRS conflicts and possible
latitude/longitude order swaps block KML generation.

## Intentional Behavior Changes

### Explicit UTM no longer falls back

Inputs containing explicit UTM CRS evidence no longer fall back to ordinary
DMS or legacy UTM30 merely because those parsers also find plausible values.
Validated structured projected coordinates receive:

```text
coordinateType: utm_projected_xy
precisionMode: utm-projected-x-y
```

### Incomplete UTM evidence is not guessed

The browser no longer maps a generic UTM label to UTM30, and it no longer uses
country or place names such as Guinea, Kankan, or Siguiri to infer UTM29.
Explicit `Zone 28N`, `Zone 29N`, and `Zone 30N` compatibility selections remain
available. Missing zone or hemisphere evidence requires confirmation or remains
blocked.

### Latitude/longitude references do not take parser authority

When a projected X/Y table has explicit UTM CRS evidence, accompanying
latitude/longitude values are transformation-verification sources. They do not
replace the projected table as the primary coordinate interpretation.

## Compatibility Notes

The response schema change is additive for legacy clients. Existing fields such
as `rawText`, `coordinates`, `quota`, `parserTrace`, and
`coordinateEngineV2` remain present.

Regression coverage confirms the established precision modes for:

- BFTM;
- MGRS;
- Kyrgyz Gauss-Kruger;
- ordinary and handwritten DMS;
- WGS84 tables;
- WGS84 chat coordinates;
- Mozambique geographic tables;
- legacy UTM30.

The semantic result for explicit UTM inputs is intentionally different because
the typed CRS result now has authority over legacy fallback behavior.

## Validation Summary

- Live complete API image gate: `5/5 PASS`
- Coordinate Type Arbitration real-asset regression: `5/5 PASS`
- Dedicated type protection: `4/4 PASS`
- Response Finalizer regression: `7/7 PASS`
- Coordinate Verification regression: `9/9 PASS`
- CRS Confirmation UI regression: `4/4 PASS`
- Typed UTM shadow regression: `6/6 PASS`
- Migration Observation regression: `8/8 PASS`
- Migration Gate regression: `17/17 PASS`
- Migration Infrastructure regression: `12/12 PASS`
- Explicit CRS conflict and swapped latitude/longitude safety gates: `PASS`

## Known Limitations

- This candidate has not been deployed to Primary Production.
- A Secondary test deployment has not yet been executed or observed.
- Synthetic and development-fixture regressions do not replace a production
  cohort observation window.
- Production Export Compare and production rollback evidence are still absent.
- Canonical UTM is not authorized to replace legacy UTM30 in production.
- Unknown projected X/Y remains blocked until CRS parameters are explicitly
  supplied and confirmed.
- Commercial-flow review found no changed quota, VIP, payment, entitlement, or
  Supabase logic, but this does not replace a real account and billing test in
  the target environment.

## Release Status

```text
V2 Release Candidate: ALLOWED
Secondary test deployment: NOT YET EXECUTED
Primary Production deployment: NOT ALLOWED
UTM migration: HOLD_LEGACY
```
