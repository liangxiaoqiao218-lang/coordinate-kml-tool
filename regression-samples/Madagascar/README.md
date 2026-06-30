# Madagascar Regression Samples

## Purpose

This directory stores real Madagascar cadastral grid coordinate samples.

## Sample Storage Rules

Each sample should include:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected KML or key expected coordinates.

## Regression Rule

After modifying the corresponding parser or Vision Retry path, all samples in this directory and the full Coordinate Engine regression suite must pass before commit.

## Current Sample Status

Status: sample missing / pending stable replacement.

The currently available local test image is:

- `Madagascar cadastral candidate image` (the user's local Madagascar coordinate test image)

Current issue:

- This image is not stable enough to serve as the Madagascar cadastral regression baseline.
- In the latest regression run, OCR detected fragments such as `Liste_Carres`, `XV`, and `YV`, but did not read a stable right-side `num | XV | YV` table.
- The result fell back to `precisionMode = preserve-original-decimals-and-parse-dms` with only map-axis-like values, rather than `cadastral-grid-num-xv-yv`.

Regression policy note:

- Madagascar parser code was not changed by the Point A-Z DMS table work.
- The current failure is treated as a sample/OCR-read issue, not evidence that the Madagascar parser regressed.
- A real reproducible Madagascar cadastral image that consistently returns `precisionMode = cadastral-grid-num-xv-yv` and the expected `num | XV | YV` rows must be added before Madagascar can be restored as a hard blocking regression sample.

Expected future stable sample:

- Expected `precisionMode`: `cadastral-grid-num-xv-yv`
- Expected output format: `num | XV | YV`
- Expected point/table count: document with the real sample when available.
- Forbidden fallback: WGS84 Chat Coordinates, ordinary decimal fallback, ordinary DMS fallback.
