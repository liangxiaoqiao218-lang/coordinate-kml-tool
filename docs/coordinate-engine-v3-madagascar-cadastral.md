# Coordinate Engine V3 Madagascar Cadastral Recognizer

Status: `IMPLEMENTED`

This recognizer ports the historical Madagascar cadastral grid stable path into
the isolated V3 recognizer architecture. It is table-only: it consumes already
extracted text or structured rows and does not perform image acquisition,
Vision, OCR, provider retry, generic projected routing, V2 evidence,
arbitration, migration, confirmation, or KML export policy.

## Historical stable source

The historical stable path is the Madagascar cadastral table route:

```text
Liste_Carrés / Liste_Carres
NC | XV | YV | CM_NOMFIR | num
↓
num | XV | YV
```

The Primary runtime success contract used:

```text
model: qwen-vl-plus+cadastral-grid-priority
precisionMode: cadastral-grid-num-xv-yv
cadastralGrid.rowCount: 32
```

V3 only ports the deterministic table semantics, not provider acquisition.

## Table signature

The recognizer requires a strong Madagascar cadastral signature:

- `Liste_Carrés` or `Liste_Carres` with `XV` and `YV`
- or Madagascar / Ilakaka / Andriandampy cadastral context with `XV` and `YV`

Plain projected numbers are rejected.

## Row shape

Supported rows:

```text
NC | XV | YV | CM_NOMFIR | num
1 | 292812,5 | 360937,5 | Ilakaka | 280
```

The final cadastral identifier is `num`, not `NC`.

## Decimal comma

Decimal comma is frozen:

```text
292812,5 -> 292812.5
360937,5 -> 360937.5
```

It is not treated as a thousands separator.

## XV / YV semantics

`XV` and `YV` are cadastral cell centers.

Cell geometry is constructed in source projected coordinates:

```text
center +/- half inferred spacing
```

For the historical 32-row fixture, inferred spacing is:

```text
dx = 625m
dy = 625m
```

Each cell polygon is closed in projected space before conversion to WGS84.

## CRS

Source CRS:

```text
EPSG:29702
Tananarive (Paris) / Laborde Grid approximation
```

Output CRS:

```text
EPSG:4326
```

The isolated recognizer uses a deterministic local Laborde approximation for
the Ilakaka cadastral fixture and bounds-checks converted coordinates inside
Madagascar. It does not enter UTM, EPSG:32750, generic projected CRS guessing,
or global CRS arbitration.

## Map tick rejection

Map frame ticks such as:

```text
290625
295625
300625
535625
540625
```

are not cadastral rows. The recognizer requires `.5` cadastral cell-center
values in the Madagascar grid range and a strong table signature.

## KML semantics

Normalized coordinates store WGS84:

```text
latitude
longitude
```

KML coordinates use:

```text
longitude,latitude,0
```

For cadastral cells, polygons are built in EPSG:29702 source space first and
then converted to WGS84. The recognizer does not build front-end KML files.

## Isolation boundary

Provider calls, Vision calls, and OCR calls are always `0`.

The recognizer does not import WGS84 Decimal, MGRS, Generic DMS, Kyrgyzstan GK,
future Indonesia UTM, V2 evidence, V2 arbitration, V2 migration, `server.js`, or
`index.html`.

Real Madagascar image recognition remains out of scope until a separate V3
image acquisition phase.
