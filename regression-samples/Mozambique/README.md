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
- Current production-safe behavior must prefer no KML over wrong KML: `rows=0`,
  non-22-row results, duplicated tail coordinates, or suspected unstable reads must
  not be exported as final KML.

## 2026-07-01 Stability Note

Twenty consecutive real uploads of `莫桑比克矿地.jpg` showed that the current
stabilization attempt can prevent the two dangerous outcomes already observed:

- WGS84 Chat takeover: 0/20 in the latest protected runs.
- Pseudo 36-row output: 0/20.

However, the sample still had an intermittent `rows=0` result after multiple
Mozambique-specific vision retries. This means the proposed hotfix did not meet
the hard stability target of `20/20` runs returning
`precisionMode = mozambique-geographic-table` with exactly 22 rows.

Do not keep increasing generic visual retry counts for this case. The next
direction should be Coordinate Engine V2 / intent-layer work and deterministic
table reading for Portuguese geographic DMS tables. Until then, unstable
Mozambique reads must return a clear retry/manual-check message and must not be
handed to WGS84 Chat or exported as a malformed KML.

## Expected Key Rows

- First row: `1 | -14.600000 | 32.955556 | 32.955556,-14.600000,0`
- Second row: `2 | -14.600000 | 33.100000 | 33.100000,-14.600000,0`
- Third row: `3 | -14.655556 | 33.100000 | 33.100000,-14.655556,0`
- Last row: `22 | -14.633333 | 32.955556 | 32.955556,-14.633333,0`
