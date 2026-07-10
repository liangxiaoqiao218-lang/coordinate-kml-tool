# Coordinate Regression Test Samples Plan

This document plans the long-term regression sample library for GeoKit Lab coordinate recognition.
It does not include real images. Real samples should be stored separately and reviewed before use.

## Planned Directory Structure

```text
test-data/coordinate/
├── kyrgyz/
├── mozambique/
├── madagascar/
├── mgrs/
├── dms/
└── expected/
```

## Sample Metadata Template

Each future sample should record:

- Sample file name:
- Coordinate type:
- Expected precisionMode:
- Expected point count:
- Expected first point:
- Expected last point:
- KML generation allowed:
- Projection conversion required:
- Must not be captured by fallback:

## Initial Coverage Targets

### Kyrgyzstan Gauss-Kruger

- Expected precisionMode: `kyrgyz-gk-point-x-y`
- Must preserve point numbers and ordering.
- Must not be captured by WGS84 chat coordinates or ordinary projected fallback.

### Mozambique Geographic Table

- Expected precisionMode: `mozambique-geographic-table`
- Must return the full Latitude / Longitude table.
- Must not be captured by WGS84 Chat Coordinates.

### Cote d'Ivoire Geographic DMS Table

- V2 coordinate_type: `cote_divoire_geographic_dms_table`
- Expected precisionMode: `cote-divoire-geographic-dms-table`
- Normalization adapter: `normalizeCoteDIvoireGeographicDmsTable()`
- Must preserve one image with one mine area and one image with multiple mine areas.
- Must read N / Nord as positive latitude and W / Ouest as negative longitude.
- Must support degree symbols, space-separated DMS, and decimal comma seconds.
- Must calculate `declared_area_ha`, `calculated_area_ha`, area error, self-intersection, and `kml_ready`.
- Must not be captured by WGS84 Chat Coordinates, decimal lat/lon, local Tesseract fallback, BFTM, Madagascar cadastral, Mozambique geographic table, Kyrgyzstan GK, or MGRS.

Sample assertions:

- `科特迪瓦01.png`: one group, four points, declared area 89.93 ha, area error under 2 percent, not self-intersecting, `kml_ready = true`.
- `科特迪瓦02.png`: one group, four points, area error under 2 percent, not self-intersecting, `kml_ready = true`.
- `科特迪瓦03.png`: one group, four points, area error under 2 percent, not self-intersecting, `kml_ready = true`.
- `科特迪瓦04.png`: one group, four points, supports `LATITUDE NORD` / `LONGITUDE OUEST` and decimal comma seconds, area error under 2 percent, `kml_ready = true`.
- `科特迪瓦4个矿区坐标.jpg`: four groups; first three groups `kml_ready = true`; `SION RESSOURCE_矿区2` must return `requires_review = true`, `kml_ready = false`, and the self-intersection warning.

### Madagascar Cadastral Grid

- Expected precisionMode: `cadastral-grid-num-xv-yv`
- Must preserve `num | XV | YV`.
- Must not be captured by DMS or ordinary decimal coordinates.

### MGRS / UTM Grid Reference

- Expected precisionMode: `mgrs-utm-grid-reference`
- Must parse zone, latitude band, grid square, easting, and northing.
- Must not be captured by decimal coordinates, BFTM, or UTM numeric fallback.

### DMS

- Expected precisionMode: depends on source format.
- Must preserve handwritten DMS and standard DMS table display paths.
- Must not be overwritten by new display or chat-coordinate layers.

## Maintenance Rule

Every new coordinate type that passes real-image testing should add at least one regression sample plan entry here before the type is treated as stable.
