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

Status: confirmed coordinate truth / stable specialized parser restored by the P0 recovery.

The confirmed local source image is:

- `Madagascar cadastral candidate image` (the user's local Madagascar coordinate test image)

Current qualification:

- SR-08H.3B human approval confirmed all 32 `num | XV | YV` rows, row order, column binding, and decimal-comma literal semantics on 2026-08-26.
- Truth maturity is `CONFIRMED_TRUTH`; approval source is `SR-08H.3B_HUMAN_APPROVAL`.
- The stable `Liste_Carrés + XV/YV` parser path is available again. This does not authorize a Provider call in regression and does not change Production recognition authority.
- KML remains geometry-dependent: export is available only after the current EPSG:29702 grid-cell geometry converts to finite EPSG:4326 coordinates; review warnings remain visible.

Regression policy note:

- Madagascar parser code was not changed by the Point A-Z DMS table work.
- Provider reliability remains separate from the confirmed coordinate truth.
- `projected_xy` takeover remains prohibited.

Confirmed truth contract:

- Expected `precisionMode`: `cadastral-grid-num-xv-yv`
- Expected output format: `num | XV | YV`
- Expected point/table count: 32.
- Forbidden fallback: WGS84 Chat Coordinates, ordinary decimal fallback, ordinary DMS fallback.
