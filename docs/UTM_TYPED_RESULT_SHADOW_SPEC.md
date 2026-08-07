# Typed UTM Result Shadow Migration Specification

## Scope

Phase 2 creates a typed UTM shadow result and compares it with frozen legacy UTM30 output. It does not replace, mutate, or route production behavior.

The Phase 2 modules are not imported by `server.js` or `index.html`. They do not alter API responses, `precisionMode`, `parserTrace`, coordinate parsers, KML, or Export.

## Typed Result

Only a conflict-free, confirmed WGS84 UTM Shadow Intent may produce:

```json
{
  "coordinateType": "utm_projected_xy",
  "projection": "utm",
  "datum": "WGS84",
  "zone": 30,
  "hemisphere": "north",
  "epsg": "EPSG:32630",
  "source": "shadow"
}
```

The builder rejects candidates, unknown intents, conflicts, non-WGS84 datums, invalid zones or hemispheres, and inconsistent EPSG values. BFTM, MGRS, Kyrgyz GK, and unknown projected X/Y therefore cannot produce a typed UTM result.

## Transformation

The shadow transformer performs WGS84 UTM inverse Transverse Mercator conversion using dynamic zone and hemisphere parameters. It never infers either parameter from coordinate values.

Transformed points are diagnostic shadow data. They are not KML-ready production output and are not consumed by Export.

## Legacy Comparison

The comparator recognizes the frozen legacy alias `utm30n-projected-x-y` as WGS84 UTM Zone 30 North / EPSG:32630. It compares:

- projection;
- datum;
- zone;
- hemisphere;
- EPSG;
- transformed longitude and latitude for every point.

The accepted tolerance is configurable from `1e-8` through `1e-6` degrees. A CRS mismatch, row-count mismatch, non-finite result, or coordinate difference above tolerance returns:

```json
{
  "status": "migration_compare_failed"
}
```

An unsupported legacy type, including the current UTM50S legacy outcome, returns `not_comparable`; this does not authorize migration.

## Regression Contract

- UTM30: legacy `utm30n-projected-x-y` and V2 EPSG:32630 must match point-for-point.
- Tolerance breach: must return `migration_compare_failed`.
- UTM50S: V2 must produce EPSG:32750 while legacy remains unsupported.
- BFTM, MGRS, and Kyrgyz GK: must not produce a typed UTM result.

Phase 2 remains shadow-only after these checks pass. Production migration requires a separate reviewed phase and full locked-baseline approval.
