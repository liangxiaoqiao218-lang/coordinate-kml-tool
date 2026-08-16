# Coordinate Engine V3 WGS84 Decimal Recognizer

Status: `IMPLEMENTED`

This recognizer is deterministic and isolated. It is not connected to
`server.js`, `index.html`, or the Primary recognition runtime.

## Supported

- Explicit WGS84 decimal latitude/longitude pairs.
- One point, two points, or three-or-more points.
- Optional labels, such as `A: 12.319572, -11.178174`.
- Comma or whitespace separators.
- Negative latitude and negative longitude.
- Boundary latitude `-90..90`.
- Boundary longitude `-180..180`.

## Not supported

- DMS.
- MGRS.
- UTM.
- Gauss-Kruger.
- Madagascar cadastral.
- Indonesia UTM.
- Côte d'Ivoire DMS.
- WGS84 header tables.
- OCR image acquisition.

Unsupported types return `canHandle=false` or a rejected deterministic result.
The recognizer never falls back to another recognizer.

## Input semantics

Input order is frozen as:

```text
latitude, longitude
```

The recognizer does not silently swap values.

## Output semantics

The normalized result uses:

- `coordinateType=wgs84_decimal`
- `recognizerId=wgs84_decimal`
- `crs=EPSG:4326`
- `precisionMode=wgs84-decimal`

## Geometry inference

- `1` point -> `point`
- `2` points -> `line`
- `3+` points -> `polygon`

The normalized coordinates do not duplicate the first polygon point.

## KML order

KML order is always:

```text
longitude,latitude,altitude
```

Example:

```text
Input: 12.319572, -11.178174
Normalized: latitude=12.319572 longitude=-11.178174
KML: -11.178174,12.319572,0
```

## Warnings

If the first value is outside latitude range and the second value could be a
latitude, the recognizer emits:

```text
POSSIBLE_LAT_LON_SWAP
```

It may include a suspected interpretation, but it does not change the user's
input semantics.

## Isolation boundary

This recognizer does not import or call:

- UTM recognizers.
- Madagascar recognizers.
- DMS recognizers.
- MGRS recognizers.
- V2 evidence.
- V2 arbitration.
- V2 migration.
- Provider / Vision / OCR acquisition.

