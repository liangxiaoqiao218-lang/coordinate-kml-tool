# UTM Shadow Migration Gate Specification

## Status and Boundary

Phase 4C-1 adds a shadow-only decision gate. It consumes existing CRS Intent,
Typed UTM Result, Migration Observation, Quality Gate, and explicit user
confirmation. It is not imported by production code and does not alter API,
KML, Export, parser routing, `precisionMode`, locked baselines, or user flow.

The gate is diagnostic authorization data only. `V2_ALLOWED` does not switch
production routing or authorize canonical Export.

## Output Schema

```json
{
  "migrationDecision": "V2_ALLOWED|LEGACY_ONLY|BLOCKED",
  "reason": ["STABLE_REASON_CODE"],
  "legacyAvailable": true,
  "v2Available": true
}
```

No coordinates, OCR text, image data, or user identifiers are included.

## Decision Rules

### V2_ALLOWED

All conditions are required:

- Shadow CRS Intent is `confirmed`, WGS84 UTM, internally EPSG-consistent, and conflict-free.
- Typed Result is present, matches the confirmed intent, and contains valid transformed WGS84 points.
- Every projected point contains finite UTM easting/northing values inside the accepted UTM bounds.
- Projected and transformed point counts match.
- The gate independently recomputes WGS84 from the projected points and Typed CRS; supplied transformed points must match within `1e-8` degrees.
- Migration Observation is `MATCH` or `V2_ONLY`.
- Quality Gate status is `PASS`.
- The user confirmation status is explicitly `CONFIRMED`.

`MATCH` proves compatibility for an existing legacy UTM type. `V2_ONLY`
represents new typed capability such as Indonesia UTM50S. Both remain shadow
decisions in Phase 4C-1.

### LEGACY_ONLY

Legacy remains authoritative when it is available and V2 is not eligible,
including incomplete CRS evidence, missing Typed Result, Quality Gate not
passing, or missing user confirmation. BFTM returns `LEGACY_ONLY` with reason
`NOT_UTM` and remains on its dedicated pipeline.

### BLOCKED

The gate always blocks:

- explicit CRS conflict;
- transformation mismatch;
- MGRS or another explicit `utm_projected_xy` block;
- unknown projected X/Y with no safe legacy result;
- a V2-only candidate that has not passed Quality Gate or user confirmation.
- invalid, non-finite, or out-of-range projected coordinates (`INVALID_PROJECTED_COORDINATES`);
- projected/transformed point-count mismatch (`POINT_COUNT_MISMATCH`);
- transformed WGS84 points that do not match an independent recomputation (`STALE_TRANSFORMED_RESULT`).

Hard safety failures never degrade to `LEGACY_ONLY` merely because a legacy
object exists.

## Rollback

Rollback is deletion or non-use of the isolated gate module. Production is
already legacy-authoritative, so no data, API, KML, or parser rollback is
required.
