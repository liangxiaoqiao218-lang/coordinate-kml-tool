# POINT_AZ_DMS_TABLE Regression Samples

## Purpose

This directory stores regression samples for Point A-Z long DMS boundary tables with `Point / Nord / Est` or equivalent `Point / North / East` headers.

## Expected Parser Behavior

- Type id: `point-az-dms-table`
- Expected `precisionMode`: `point-az-dms-table`
- Expected `parserTrace`: `OCR -> POINT_AZ_DMS_TABLE:accepted`
- Expected point count: 26 points, preserving A-Z row order.
- Expected first coordinate: `-8.266666666666667,10.870833333333334`
- Expected last coordinate: `-8.254444444444445,10.870833333333334`

## Sample Preservation Rules

Each real sample should preserve:

- Original image.
- OCR raw text.
- Parser retry raw text, when available.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected geometry.
- Expected KML or key expected coordinates.

## Regression Rule

Any change to Point A-Z DMS table recognition must run the full Coordinate Engine regression suite before commit. Ordinary DMS must not capture this long-table type before the dedicated Point A-Z retry has a chance to correct row alignment.
