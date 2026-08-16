# Coordinate Engine V3 Kyrgyzstan Gauss-Kruger Recognizer

Status: `IMPLEMENTED`

This recognizer ports the historical Kyrgyzstan / Soviet Gauss-Kruger
deterministic table parser into the isolated V3 recognizer architecture. It is
not connected to `server.js`, `index.html`, public API routes, Vision, OCR,
provider acquisition, V2 evidence, arbitration, migration, or confirmation
flows.

## Historical source

The historical stable path is documented in `regression-samples/Kyrgyz_GK/` and
the stable-path notes:

- source table shape: `№ points | X | Y` or `point | X | Y`
- precision mode: `kyrgyz-gk-point-x-y`
- source CRS: `EPSG:28413`
- point count fixture: 65
- first point: `1 | 13261341 | 4607777`
- last point: `65 | 13261317 | 4607721`

## Table signature

The recognizer requires strong Kyrgyz / Russian / Soviet Gauss-Kruger context,
a `point|X|Y`-style header, or a dense historical numeric pattern. Plain
two-column projected numbers are not sufficient.

## Point numbering

Original point numbers are preserved. Output rows are sorted by point number,
not by input order or coordinate value.

## X/Y semantics

Historical semantics are frozen:

- `X` is the full EPSG:28413 easting with the zone prefix, such as `13261341`
- `Y` is northing, such as `4607777`
- do not swap axes
- do not strip the leading `13` zone prefix

## CRS decision

The isolated recognizer resolves the historical Kyrgyzstan GK table to:

```text
EPSG:28413
Pulkovo 1942 / Gauss-Kruger zone 13
```

The projected-to-WGS84 transform uses the historical proj4 definition:

```text
+proj=tmerc +lat_0=0 +lon_0=75 +k=1 +x_0=13500000 +y_0=0 +ellps=krass +towgs84=25,-141,-78.5,0,-0.35,-0.736,0 +units=m +no_defs +type=crs
```

## Conversion

The recognizer performs deterministic conversion:

```text
EPSG:28413 X/Y
↓
inverse transverse Mercator on Krassowsky ellipsoid
↓
Pulkovo 1942 to WGS84 datum shift
↓
WGS84 EPSG:4326 latitude / longitude
```

Converted coordinates must land inside Kyrgyzstan bounds.

## Validation

The recognizer rejects:

- missing X
- missing Y
- NaN / Infinity
- duplicate point numbers
- abnormal point numbers
- unsupported numeric ranges
- ambiguous numeric tables without GK evidence
- Indonesia UTM
- WGS84 decimal
- MGRS
- DMS
- Madagascar-like cadastral rows

## Geometry

The recognizer uses the shared V3 normalized result contract:

- 1 point -> `Point`
- 2 points -> `LineString`
- 3+ points -> `Polygon`

## KML order

Normalized coordinates store:

```text
latitude
longitude
```

KML coordinates use:

```text
longitude,latitude,0
```

## Isolation boundary

Kyrgyzstan GK only handles already-extracted text or structured rows for the
historical GK table shape. It does not own image acquisition, OCR, provider
retry, generic projected routing, UTM recognition, Madagascar recognition,
evidence arbitration, shadow decisions, migration, or KML export policy.

Provider calls, Vision calls, and OCR calls are always `0`. If the V3 latency
budget is already expired before parsing, the recognizer returns
`deadline_exceeded` without starting work.
