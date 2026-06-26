# CHAT Regression Samples

## Purpose

This directory stores real plain WGS84 chat coordinate samples.

## Sample Storage Rules

Each sample should include:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected KML or key expected coordinates.

## Regression Rule

After modifying the corresponding parser or Vision Retry path, all samples in this directory and the full Coordinate Engine regression suite must pass before commit.
