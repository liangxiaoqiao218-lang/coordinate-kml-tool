# Coordinate Engine V3 MGRS Recognizer

Status: `IMPLEMENTED`

This recognizer ports the historical deterministic MGRS parser/converter into
the isolated V3 recognizer architecture. It is not connected to `server.js`,
`index.html`, public API routes, Vision, OCR, or provider acquisition.

## Supported formats

- `47RLH 24469 42832`
- `47R LH 24469 42832`
- `47RLH2446942832`
- `47RLH,24469,42832`
- `A: 47RLH 24469 42832`
- `1. 47RLH 24469 42832`
- Multiple rows with labels.

## Validation

The recognizer validates:

- zone `1..60`
- latitude band `C..X`, excluding `I` and `O`
- 100km square letters excluding `I` and `O`
- easting/northing precision `1..5` digits
- equal easting/northing precision
- even compact numeric payload

Invalid MGRS, decimal WGS84, UTM-scale X/Y, DMS, Madagascar cadastral rows, and
other coordinate types return `canHandle=false` or deterministic rejection.

## Conversion

MGRS is converted deterministically:

```text
MGRS
↓
UTM grid interpretation
↓
WGS84 EPSG:4326
```

No provider, Vision, OCR, evidence, shadow, arbitration, migration, or
confirmation layer is used.

## Precision

Supported easting/northing digit precision:

- `1` digit
- `2` digits
- `3` digits
- `4` digits
- `5` digits

The two sides must have equal precision.

## Labels and order

Input order is preserved. If no label is present, labels are assigned
sequentially as `1`, `2`, `3`, ...

## Geometry

The recognizer uses the V3 normalized result contract:

- `1` point -> `point`
- `2` points -> `line`
- `3+` points -> `polygon`

## KML order

Normalized coordinates store:

```text
latitude, longitude
```

KML semantic order is:

```text
longitude,latitude,altitude
```

## Historical ground truth

Historical registered MGRS sample:

```text
A | 47RLH 24469 42832
B | 47RLH 24257 42938
C | 47RLH 24123 42905
D | 47RLH 24124 43163
E | 47RLH 24386 43228
F | 47RLH 24673 43099
G | 47RLH 24620 42882
```

Frozen expected points:

- First KML: `97.2636250946,24.7901938391,0`
- Last KML: `97.2651119873,24.7906625174,0`

Regression tolerance: `<1e-5°`.

## Isolation boundary

The recognizer does not import or call:

- WGS84 decimal recognizer.
- UTM recognizers.
- Madagascar recognizers.
- DMS recognizers.
- V2 evidence.
- V2 arbitration.
- V2 migration.
- Provider / Vision / OCR acquisition.

