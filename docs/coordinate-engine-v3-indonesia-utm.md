# Coordinate Engine V3 Indonesia UTM Recognizer

`indonesia_utm` is an isolated deterministic recognizer for Indonesia-style
structured WGS84 UTM tables. It does not perform image acquisition, OCR, Vision,
provider calls, selective reread, arbitration, migration, or confirmation
gating.

## Ownership signature

The recognizer requires explicit UTM/WGS84 context plus projected X/Y table
structure. Typical accepted text:

```text
SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y | Latitude | Longitude
```

Plain projected numeric pairs are not sufficient and must remain `NO_MATCH`.
This phase intentionally keeps the recognizer Indonesia-specific rather than
creating a generic projected-coordinate router.

## Source semantics

- `X` is Easting.
- `Y` is Northing.
- Decimal comma is a decimal separator: `778807,293 -> 778807.293`.
- Point labels are preserved in source order.
- One point becomes `Point`, two points become `LineString`, and three or more
  points become `Polygon`.

## CRS

The CRS must come from explicit source text such as `UTM WGS 1984 ZONA 50S`,
`UTM WGS84 Zone 50S`, or `WGS 84 / UTM Zone 50 South`.

For the frozen Indonesia fixtures:

- Zone: `50`
- Hemisphere: `south`
- Source CRS: `EPSG:32750`
- Output coordinates: WGS84 latitude/longitude for KML

The recognizer must not infer CRS by reverse-transforming coordinate values.

## DMS reference

Latitude/Longitude DMS table columns are optional verification references.
They are not the primary coordinate source and must not replace projected X/Y.

Verification compares:

```text
X/Y + EPSG:32750
↓
WGS84 transform
↓
optional DMS decimal reference
```

Tolerance: `1e-6` degrees.

If DMS is missing, the result remains valid when X/Y and CRS are valid. If DMS
mismatches, the recognizer returns warnings and `suspectedPoints`; it does not
retry, reread, reject X/Y, or block technically constructible KML.

## KML

KML coordinates use transformed WGS84 in `longitude,latitude,0` order. Source
projected X/Y values are retained only as sanitized source metadata.

## Out of scope

- Image acquisition
- OCR / Vision / provider calls
- X/Y selective reread
- DMS reference selective reread
- Evidence arbitration
- Shadow winner
- Review / confirmation gates
- Migration lifecycle

These belong to future layers outside the deterministic recognizer.
