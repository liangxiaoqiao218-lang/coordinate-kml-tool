# Mozambique Regression Samples

## Purpose

This directory stores real Portuguese Mozambique geographic coordinate table samples.

## Sample Storage Rules

Each sample should include:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected KML or key expected coordinates.

## Regression Rule

After modifying the corresponding parser or Vision Retry path, all samples in this directory and the full Coordinate Engine regression suite must pass before commit.

## Stable Path

Mozambique geographic tables use their own parser path:

1. Detect Portuguese table context such as `COORDENADAS GEOGRAFICAS`, `Datum: Tete`, `Latitude`, `Longitude`, `Ordem`, `INAMI`, or `MIREME`.
2. Run the Mozambique decimal prompt.
3. Apply the Mozambique quality gate.
4. If the decimal result is weak, run the Mozambique DMS transcription prompt.
5. Parse `Order | LatDeg | LatMin | LatSec | LonDeg | LonMin | LonSec`.
6. Output formatted rows with `precisionMode = mozambique-geographic-table`.

## Quality Gate

- The current Tete regression sample expects 22 rows.
- Duplicated coordinate pairs make the result weak even when the row count is 22.
- A weak decimal result must trigger the Mozambique-specific DMS transcription retry.
- The retry must not change generic OCR, WGS84 Chat, BFTM, DMS, MGRS, or other parser behavior.

## Expected Key Rows

- First row: `1 | -14.600000 | 32.955556 | 32.955556,-14.600000,0`
- Second row: `2 | -14.600000 | 33.100000 | 33.100000,-14.600000,0`
- Third row: `3 | -14.655556 | 33.100000 | 33.100000,-14.655556,0`
- Last row: `22 | -14.633333 | 32.955556 | 32.955556,-14.633333,0`
