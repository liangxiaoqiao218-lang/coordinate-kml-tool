# Coordinate Engine V3 WGS84 Table Recognizer

Status: `IMPLEMENTED`

This recognizer ports the WGS84 decimal table semantics into the isolated V3
recognizer architecture. It is table-only: it consumes already extracted text
tables or structured rows and does not perform image acquisition, Vision, OCR,
provider calls, generic table routing, V2 evidence, arbitration, migration, or
confirmation flow.

## Supported headers

The recognizer accepts explicit geographic table headers:

- `Longitude`, `Latitude`
- `Lon`, `Lat`
- `LONGITUDE`, `LATITUDE`
- `经度`, `纬度`
- `东经`, `西经`, `北纬`, `南纬`
- mixed Chinese / English geographic headers

Headers decide column roles. Numeric order never decides roles by itself.

## Header role mapping

Examples:

```text
Longitude | Latitude
16.0320 | 3.7638

=> longitude=16.0320 latitude=3.7638
```

```text
Latitude | Longitude
3.7638 | 16.0320

=> latitude=3.7638 longitude=16.0320
```

Plain decimal pairs without headers remain owned by `wgs84_decimal`.

## Hemisphere signs

Hemisphere headers are deterministic:

- `东经` / East / E keeps longitude positive
- `西经` / West / W makes longitude negative
- `北纬` / North / N keeps latitude positive
- `南纬` / South / S makes latitude negative

Already-negative west or south values are not double-negated.

If an east or north header receives a negative value, the row is rejected with a
warning instead of silently corrected.

## Labels

Optional label columns are preserved:

- `Point`
- `ID`
- `No.`
- `Name`
- Chinese point / id labels

Without a label column, V3 shared row numbering is used.

## Duplicates

Exact duplicate coordinate rows are removed deterministically while preserving
the first occurrence order. No fuzzy dedupe is performed.

## Validation

The recognizer rejects:

- missing latitude or longitude fields
- invalid numeric tokens
- NaN / Infinity
- latitude outside `[-90, 90]`
- longitude outside `[-180, 180]`
- DMS cells
- `X | Y`
- `Coordinate1 | Coordinate2`
- projected / UTM / GK / Madagascar cadastral / MGRS inputs

## Geometry

The recognizer uses the shared V3 geometry policy:

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

Frozen table example:

```text
经度东 | 北纬
16.0320 | 3.7638

=> normalized lat=3.7638 lon=16.0320
=> KML 16.032,3.7638,0
```

## Isolation boundary

Provider calls, Vision calls, and OCR calls are always `0`.

The recognizer does not import WGS84 Decimal, Generic DMS, MGRS, Kyrgyzstan GK,
Madagascar cadastral, future Indonesia UTM, V2 evidence, V2 arbitration, V2
migration, `server.js`, or `index.html`.

Image acquisition is explicitly out of scope for this phase.
