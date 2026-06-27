# FRENCH_PERIMETER_DMS Regression Samples

## Purpose

This directory tracks French prose perimeter DMS samples for `french_perimeter_dms_prose`.

The current real-world failing sample is:

- `模糊坐标.jpg`

Do not store private or large source images here unless they are explicitly approved for regression storage. Preserve the expected OCR text and key KML coordinates instead.

## Sample Storage Standard

Each future sample should include:

- Original image, when approved.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected geometry.
- `expected.kml` or key expected coordinates.
- Notes about which fallback must not capture the sample.

## Expected Parser Behavior

Expected parser trace:

```text
OCR -> FRENCH_PERIMETER_DMS:retry_vision -> FRENCH_PERIMETER_DMS:accepted
```

Expected precision mode:

```text
french-perimeter-dms-prose
```

Expected key KML coordinates for `模糊坐标.jpg`:

```text
Point A: -8.833333333333334,12.066666666666666,0
Point B: -8.75,12.066666666666666,0
Point C: -8.75,12.036666666666667,0
Point D: -8.833333333333334,12.036666666666667,0
```

## Regression Rule

Any change to the French perimeter parser or its vision retry must run the full Coordinate Engine regression suite. This sample must not be captured by WGS84 Chat Coordinates, ordinary DMS fallback, or generic decimal extraction.
