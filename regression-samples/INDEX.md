# Coordinate Engine V1 Stable Regression Suite

Baseline Commit:

`66dc438f95fe3d26dc14e8214f1f257e23826c2c`

This directory is the fixed regression sample library for GeoKit Lab Coordinate Engine V1.

## Supported Coordinate Types

Current stable coordinate types:

1. `DMS_GROUPED`
2. `french_perimeter_dms_prose`
3. `DMS`
4. `BFTM / X-Y`
5. `MGRS`
6. `Kyrgyzstan GK`
7. `Madagascar cadastral`
8. `Mozambique Geographic Table`
9. `WGS84 Table` with longitude/latitude headers
10. `WGS84 Chat Coordinates`
11. `Fallback`

## Directory Guide

| Directory | Purpose | Initial samples |
| --- | --- | ---: |
| `BFTM/` | Burkina Faso BFTM / X-Y long table samples | 0 |
| `RC2/` | Longitude-east / north-latitude table samples | 0 |
| `DMS_GROUPED/` | Mining Area grouped DMS samples | 0 |
| `FRENCH_PERIMETER_DMS/` | French prose perimeter DMS samples | 1 |
| `DMS/` | Standard and single-point DMS samples | 0 |
| `MGRS/` | MGRS / UTM Grid Reference samples | 0 |
| `CHAT/` | Plain WGS84 chat coordinate samples | 0 |
| `Kyrgyz_GK/` | Kyrgyzstan Gauss-Kruger table samples | 0 |
| `Madagascar/` | Madagascar cadastral grid samples | 0 |
| `Mozambique/` | Portuguese Mozambique geographic table samples | 0 |

## Sample Requirements

Each real sample should preserve:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected geometry.
- `expected.kml` or key expected coordinates.
- Notes about which fallback must not capture the sample.

## Regression Rule

Any coordinate recognition fix must add the related real-world failing sample here before changing parser code. After the fix, the full regression suite must pass before commit.
