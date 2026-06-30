# MGRS Regression Samples

## Purpose

This directory stores real MGRS / UTM Grid Reference coordinate samples.

## Sample Storage Rules

Each sample should include:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected KML or key expected coordinates.

## Regression Rule

After modifying the corresponding parser or Vision Retry path, all samples in this directory and the full Coordinate Engine regression suite must pass before commit.

## Registered Sample: Myanmar MGRS Table

Sample file:

- Local path: `D:/关于西非的业务/测试素材/缅甸坐标.jpg`
- Do not commit the image file unless explicitly requested; preserve the path and expected results here.

Expected result:

- Expected `precisionMode`: `mgrs-utm-grid-reference`
- Expected `parserTrace`: `OCR -> MGRS:retry_vision -> BFTM:rejected -> MGRS:accepted`
- Expected labels: `A, B, C, D, E, F, G`
- Expected C point: `47RLH 24123 42905`
- Expected row count in formatted output: 8 lines including header (`label | MGRS | WGS84 | KML`) plus 7 labeled points.

Sample expected rows:

```text
A | 47RLH 24469 42832
B | 47RLH 24257 42938
C | 47RLH 24123 42905
D | 47RLH 24124 43163
E | 47RLH 24386 43228
F | 47RLH 24673 43099
G | 47RLH 24620 42882
```

Guardrail:

- Do not use Kyrgyz GK samples as MGRS regression substitutes.
- MGRS must not be captured by WGS84 Chat Coordinates, BFTM, or ordinary decimal coordinate fallback.
