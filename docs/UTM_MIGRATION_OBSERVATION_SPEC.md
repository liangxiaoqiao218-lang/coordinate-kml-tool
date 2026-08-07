# UTM Migration Observation Specification

## Purpose

Phase 3 observes legacy and V2 UTM results in parallel. It answers whether V2 reproduces legacy UTM30 behavior and records where V2 adds capability or where migration must stop.

This phase remains shadow-only. The observer is not imported by `server.js` or `index.html` and does not alter API responses, KML, Export, parser routing, `precisionMode`, or user-visible behavior.

## Observation Record

```json
{
  "sample": "UTM30_burkina",
  "legacy": {
    "type": "utm30n-projected-x-y",
    "pointCount": 4
  },
  "v2": {
    "type": "utm_projected_xy",
    "datum": "WGS84",
    "zone": 30,
    "hemisphere": "north",
    "epsg": "EPSG:32630",
    "pointCount": 4
  },
  "comparison": {
    "status": "MATCH",
    "maxDifference": 0
  },
  "migrationStatus": "MATCH",
  "disposition": "OBSERVE"
}
```

## Migration Status

Only UTM migration candidates receive one of these fixed statuses:

- `MATCH`: legacy and V2 CRS plus transformed coordinates agree within tolerance.
- `V2_ONLY`: V2 has a confirmed typed UTM result and legacy has no supported UTM result.
- `LEGACY_ONLY`: legacy has supported UTM output but V2 lacks confirmed CRS evidence.
- `CRS_CONFLICT`: legacy and V2 CRS fields disagree or Shadow Intent contains an explicit CRS conflict.
- `TRANSFORMATION_MISMATCH`: CRS agrees but transformed point count or coordinates do not.

## Non-UTM Disposition

BFTM and Kyrgyz GK are outside UTM migration scope and use `migrationStatus: null` with `disposition: NOT_UTM`. MGRS is also outside projected-X/Y UTM migration and uses `migrationStatus: null` with `disposition: BLOCKED` when it blocks `utm_projected_xy`.

Non-UTM cases must never be reported as `V2_ONLY` or `LEGACY_ONLY`.

## Report Summary

The report aggregates all five migration statuses plus these dispositions:

- `OBSERVE`: a migration candidate requiring observation;
- `NOT_UTM`: explicitly outside UTM migration;
- `BLOCKED`: projected-X/Y UTM fallback is explicitly blocked.

The report is diagnostic data only. A `MATCH` does not authorize production migration, and `V2_ONLY` does not authorize export.

## Regression Contract

- UTM30 must be `MATCH` with maximum coordinate difference zero for the frozen comparison fixture.
- UTM50S must be `V2_ONLY` with EPSG:32750.
- Missing confirmed V2 CRS evidence must produce `LEGACY_ONLY` when legacy UTM exists.
- Zone or EPSG disagreement must produce `CRS_CONFLICT`.
- Coordinate difference above tolerance must produce `TRANSFORMATION_MISMATCH`.
- BFTM and Kyrgyz GK must remain `NOT_UTM`.
- MGRS must remain `BLOCKED` and produce no typed UTM result.

Phase 4 alias migration is a separate decision and cannot be inferred from a single Phase 3 report.
