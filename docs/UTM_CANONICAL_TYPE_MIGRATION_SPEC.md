# UTM Canonical Type Migration Specification

## 1. Status and Scope

Phase 4A is a design-only migration contract. It defines the future canonical UTM type and the compatibility relationship with the frozen legacy UTM30 type. It does not modify a registry, parser, API response, `precisionMode`, database record, KML, Export, or user flow.

Phase 4A remains shadow-only. No production component may treat this document as authorization to switch routing.

## 2. Current State

The production-compatible legacy type is:

```yaml
legacyTypeId: utm30n-projected-x-y
legacyPrecisionMode: utm30n-projected-x-y
implicitCrs:
  projection: utm
  datum: WGS84
  zone: 30
  hemisphere: north
  epsg: EPSG:32630
```

The zone and hemisphere are encoded in the legacy type name. Existing recognition, parser routing, historical data, regression expectations, and KML behavior depend on that identifier and must remain readable.

## 3. Target State

The future registry canonical type is dynamic:

```yaml
canonicalTypeId: utm-projected-x-y
engineCoordinateType: utm_projected_xy
projection: utm
datum: dynamic
zone: dynamic
hemisphere: dynamic
epsg: dynamic
```

Naming has two deliberately separate layers:

- `utm-projected-x-y` is the registry `typeId` and public type-registration identifier.
- `utm_projected_xy` is the typed engine `coordinateType` value.

Neither identifier embeds a country, zone, or hemisphere.

## 4. Registry Relationship

The target registry relationship is:

```yaml
typeId: utm-projected-x-y
coordinateType: utm_projected_xy
aliases:
  - utm30n-projected-x-y
```

Conceptually:

```text
utm30n-projected-x-y
        |
        | legacy alias lookup
        v
utm-projected-x-y
        |
        v
utm_projected_xy + typed CRS parameters
```

Alias lookup identifies compatible semantics; it does not rewrite the source record. Reading a legacy alias may construct an internal typed CRS view, but serialization of the unchanged legacy object must preserve its original identifier.

### 4.1 Alias resolution rules

1. Exact legacy alias matching is required; partial or numeric matching is forbidden.
2. `utm30n-projected-x-y` resolves only to WGS84 / UTM Zone 30 North / EPSG:32630.
3. An alias cannot supply evidence for a new image or new OCR result.
4. An alias cannot override explicit CRS evidence or a CRS conflict.
5. Alias resolution must not run during Export as a second CRS inference pass.
6. Unknown identifiers remain unknown; they must not fall back to UTM by numeric range.
7. Future zones are represented by the canonical type plus typed CRS parameters, not by adding zone-specific aliases.

## 5. Typed Result Contract

The canonical typed result contract is:

```json
{
  "coordinateType": "utm_projected_xy",
  "projection": "utm",
  "datum": "WGS84",
  "zone": 30,
  "hemisphere": "north",
  "epsg": "EPSG:32630"
}
```

The required invariants are:

- `coordinateType` equals `utm_projected_xy`.
- `projection` equals `utm`.
- `datum`, `zone`, and `hemisphere` originate from accepted typed CRS evidence or an explicitly resolved historical alias.
- `zone` is an integer from 1 through 60.
- `hemisphere` is `north` or `south`.
- WGS84 UTM north EPSG is `32600 + zone`.
- WGS84 UTM south EPSG is `32700 + zone`.
- Explicit EPSG, zone, hemisphere, and datum must agree.
- Any conflict prevents an accepted canonical result.

The canonical result must carry CRS as data. Consumers must not reconstruct CRS by parsing `coordinateType`, filenames, country names, coordinate ranges, or user-visible labels.

## 6. Legacy Compatibility Contract

### 6.1 Historical baseline

- Existing baseline entries containing `utm30n-projected-x-y` remain unchanged.
- Existing expected `precisionMode`, parser trace, point count, coordinate hash, and KML hash remain unchanged.
- A future canonical baseline is additive and cannot silently replace the legacy baseline.

### 6.2 Historical user data

- No bulk database migration is required or authorized.
- Stored `utm30n-projected-x-y` values remain valid and readable.
- Reading old data may expose an internal canonical view, but saving unrelated edits must not rewrite the legacy identifier.
- Any optional future data migration requires its own reversible plan, audit count, backup, and approval.

### 6.3 Production output

- Existing requests continue returning the legacy `precisionMode` until a later migration phase is explicitly approved.
- Phase 4A does not add canonical fields to API responses.
- Phase 4A does not change parser selection, warnings, `parserTrace`, or user-visible labels.
- Legacy and canonical objects may coexist internally during observation, but only the legacy result remains authoritative in production.

### 6.4 KML and Export

- Historical KML coordinates, order, precision, geometry, and metadata remain byte-compatible where currently frozen.
- Export continues using the current accepted legacy result until a separate Export migration gate passes.
- Export must not reparse OCR text, infer zone from X/Y, infer hemisphere from northing, or use country as CRS evidence.
- Canonical Export may consume only a Quality-Gate-accepted typed result in a later phase.

## 7. Migration Gate

Canonical routing cannot replace legacy UTM30 until every required gate passes.

### 7.1 Intent and CRS gate

- UTM30 confirmed evidence resolves to WGS84 / Zone 30 / North / EPSG:32630.
- UTM50S confirmed evidence resolves to WGS84 / Zone 50 / South / EPSG:32750.
- Missing datum, zone, or hemisphere cannot become confirmed.
- BFTM, MGRS, Kyrgyz GK, and generic X/Y protections pass.
- No unresolved `CRS_CONFLICT` enters transformation or Export.

### 7.2 Legacy comparison gate

- All locked UTM30 points compare as `MATCH`.
- CRS fields match exactly.
- Transformed WGS84 point counts match.
- Maximum coordinate difference is within the approved `1e-8` through `1e-6` tolerance.
- Any `TRANSFORMATION_MISMATCH` blocks migration.

### 7.3 Migration observation gate

- Real UTM30 observations have no unexplained `LEGACY_ONLY`, `CRS_CONFLICT`, or `TRANSFORMATION_MISMATCH` result.
- `V2_ONLY` results are reviewed as new capability and do not automatically authorize Export.
- Observation volume and duration are defined before production sampling begins.
- Observation reports contain no secrets or unnecessary source-image content.

### 7.4 Export comparison gate

- Legacy and canonical KML use the same accepted point sequence and geometry.
- Longitude/latitude order is identical.
- Coordinate precision and rounding rules are explicitly compared.
- KML hashes or semantic coordinate comparisons pass for locked UTM30 fixtures.
- UTM50S Export remains disabled until its typed Quality Gate and real-image KML validation pass.

### 7.5 Regression gate

- UTM Intent, CRS Evidence, Typed Result, Migration Observation, and compatibility suites pass.
- Locked BFTM, UTM30, MGRS, Kyrgyz GK, DMS, and related baselines remain unchanged.
- No protected production file changes are hidden inside the migration commit.

## 8. Phase 4B Design Boundary

Phase 4B may add shadow production observation only after this specification is reviewed. Its purpose is to measure:

- `MATCH` rate;
- `V2_ONLY` rate;
- `LEGACY_ONLY` rate;
- `CRS_CONFLICT` rate;
- `TRANSFORMATION_MISMATCH` rate;
- unknown and blocked counts.

Phase 4B must still leave the legacy result authoritative. It must define sampling, redaction, retention, performance budget, failure isolation, and a kill switch before integration.

## 9. Freeze Conditions

The canonical type may be frozen only when:

1. The registry contains one canonical UTM type and the documented UTM30 legacy alias.
2. Legacy records remain readable without mutation.
3. UTM30 comparison and Export comparison pass for all locked fixtures.
4. Real observation shows no unexplained CRS conflict or transformation mismatch.
5. Negative-type protection remains intact.
6. UTM50S real-image intent and transformation regression remains stable.
7. Quality Gate owns the final authorization to transform and export.
8. Rollback to the legacy-authoritative path is documented and tested.
9. The migration is approved as a separate change from this design document.

## 10. Prohibited Changes in Phase 4A

Phase 4A must not:

- modify `server.js` or `index.html`;
- register the canonical type in production;
- change `precisionMode`;
- replace or delete the legacy UTM30 parser;
- rewrite historical baseline or user data;
- change KML or Export;
- add country- or zone-specific UTM parsers;
- infer CRS from coordinate values;
- expose shadow results through the production API;
- treat a Phase 3 `MATCH` as automatic migration approval.
