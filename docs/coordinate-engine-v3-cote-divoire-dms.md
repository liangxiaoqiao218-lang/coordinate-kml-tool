# Coordinate Engine V3 Côte d'Ivoire DMS Recognizer

Status: `IMPLEMENTED`

This recognizer ports the Côte d'Ivoire geographic DMS table interpretation
into the isolated V3 recognizer architecture. It handles already extracted text
tables or structured rows only. Image acquisition, Vision, OCR, provider retry,
V2 evidence, arbitration, migration, confirmation, and Primary runtime
integration are out of scope.

## Responsibility boundary

`generic_dms` owns plain DMS coordinate pair text.

`cote_divoire_dms` owns structured Côte d'Ivoire-style geographic DMS tables:

```text
Point | Nord | Ouest
A | 10°52'15" | 08°16'00"
```

The table header supplies column roles and may supply hemisphere semantics.

## Table signature

Supported point columns:

- `Point`
- `No.`
- `N°`
- `Num`
- `ID`
- numeric labels

Supported latitude headers:

- `Nord`
- `Latitude Nord`
- `Latitude`
- `N`
- `Sud`
- `Latitude Sud`
- `S`

Supported longitude headers:

- `Est`
- `Longitude Est`
- `Longitude`
- `E`
- `Ouest`
- `Longitude Ouest`
- `W`
- `O`

Plain DMS text without a table header remains a `generic_dms` responsibility.

## Header authority

Header semantic authority is higher than column order.

Example:

```text
Point | Longitude Ouest | Latitude Nord
A | 08°16'00" | 10°52'15"
```

Normalizes to:

```text
label=A
latitude=10.870833333333334
longitude=-8.266666666666667
```

## Cell and header hemisphere merge

The recognizer supports:

1. Hemisphere in cells, such as `08°16'00"W`.
2. Hemisphere only in headers, such as `Longitude Ouest`.
3. Header and cell carrying the same hemisphere.

Conflicts are rejected deterministically:

```text
Longitude Ouest + 08°16'00"E -> HEMISPHERE_CONFLICT
Latitude Nord + 10°52'15"S -> HEMISPHERE_CONFLICT
```

West / Ouest / O is negative longitude. North / Nord is positive latitude.
Already-negative west or south values are not double-negated.

## Source row preservation

Rows are preserved in source order. Point labels such as `A-Z` or numeric point
IDs are retained. Duplicate coordinate rows are preserved when they represent
separate source rows; the recognizer does not dedupe or reorder them.

## DMS conversion

Conversion is deterministic:

```text
decimal = degrees + minutes / 60 + seconds / 3600
```

Valid ranges:

- minutes: `0-59`
- seconds: `0 <= seconds < 60`
- latitude degree: `<= 90`
- longitude degree: `<= 180`

## Geometry

The recognizer uses the shared V3 geometry policy:

- 1 point -> `Point`
- 2 points -> `LineString`
- 3+ points -> `Polygon`

Polygon closure belongs to the downstream KML / geometry construction layer.
Source coordinates are not modified by duplicating the first point.

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

Frozen point:

```text
10°52'15"N
08°16'00"W

=> -8.266666666666667,10.870833333333334,0
```

## Rejected table types

The recognizer rejects:

- plain DMS text without table header
- decimal WGS84 table
- MGRS
- Kyrgyzstan Gauss-Kruger
- Madagascar cadastral
- Indonesia UTM X/Y

## Isolation boundary

Provider calls, Vision calls, and OCR calls are always `0`.

The recognizer does not import Generic DMS, WGS84 Table, WGS84 Decimal, MGRS,
Kyrgyzstan GK, Madagascar cadastral, future Indonesia UTM, V2 evidence, V2
arbitration, V2 migration, `server.js`, or `index.html`.
