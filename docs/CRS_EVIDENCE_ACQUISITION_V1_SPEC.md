# CRS Evidence Acquisition V1 Specification

## Purpose

Phase 1.5 adds a shadow-only image pass that transcribes visible coordinate reference system metadata. It closes the evidence gap between the original image and the UTM Shadow Intent Resolver without changing the legacy coordinate-recognition path.

The coordinate pass continues to extract coordinate rows. The CRS pass independently inspects the original image for projection, datum, zone, hemisphere, EPSG, and grid-reference labels. Its output is diagnostic input only and is not returned by the production API or used by parsers and exporters.

## Architecture

```text
Original Image
  |-- Legacy Coordinate Vision -> legacy parser -> legacy export
  `-- Shadow CRS Vision -> Evidence Collector -> Shadow UTM Intent Resolver
```

There is no routing edge from the shadow branch back into the legacy branch.

## CRS Vision Responsibility

CRS Vision performs literal transcription only. It scans the whole original image, especially footers, title blocks, legends, map frames, and coordinate-table captions.

It may observe:

- projection or CRS labels such as `UTM` and `BFTM`;
- datum labels such as `WGS 1984` and `ITRF 2008`;
- explicit zone and hemisphere labels such as `ZONA 50S`;
- printed EPSG identifiers;
- grid-reference labels such as `MGRS` and `Map Ref`.

It must not:

- return coordinate point rows;
- infer CRS from country, filename, language, or map location;
- infer zone from easting or northing ranges;
- infer hemisphere from northing values;
- derive evidence from legacy `precisionMode` or parser selection;
- invent, normalize, or complete text that is not visible.

## Acquisition Schema

```json
{
  "status": "observed",
  "observations": [
    {
      "field": "crs_label",
      "rawText": "UTM WGS 1984 ZONA 50S",
      "source": "crs_vision",
      "region": "bottom_footer"
    }
  ]
}
```

`status` is `observed`, `none`, or `invalid`. A normalized observation has `field`, literal `rawText`, fixed source `crs_vision`, and a coarse image `region`.

## Evidence Collector

The collector deterministically parses literal observations. For `UTM WGS 1984 ZONA 50S`, it emits:

```json
{
  "projection": "utm",
  "datum": "WGS84",
  "zone": 50,
  "hemisphere": "south"
}
```

The collector does not derive EPSG. The unchanged Shadow Resolver owns the existing confirmed-intent rule and derives `EPSG:32750` only when explicit UTM, WGS84, zone, and hemisphere evidence are all present.

Explicit BFTM, MGRS, and Gauss-Kruger observations are exclusions or conflicts, not fallback hints.

## Shadow Integration

The shadow pipeline passes only literal CRS observations to the existing Shadow Resolver. It returns diagnostic values to regression tooling:

- normalized CRS Vision output;
- collected evidence;
- typed shadow intent.

It is not integrated into `server.js`, API responses, parser routing, `precisionMode`, `parserTrace`, KML transformation, or export.

## Regression

Deterministic regression validates schema normalization, evidence collection, exclusions, conflicts, and resolver integration.

Real-image regression uses the original images and the dedicated CRS prompt:

- Indonesia 01, 02, and 03: must expose literal UTM, WGS84, and 50S evidence and resolve to confirmed `EPSG:32750`;
- BFTM: must not become confirmed UTM;
- MGRS: must not become projected-XY UTM intent;
- Kyrgyz GK: must not become confirmed UTM.

An image that exposes only `UTM`, without explicit datum, zone, and hemisphere, remains candidate or unknown and cannot be auto-converted.

## Freeze Boundary

Phase 1.5 may be considered ready only when all three Indonesia images pass the real-image acquisition chain and the negative protection cases remain safe. Failure to acquire visible CRS text is an Evidence Acquisition failure; it must never be repaired by numeric, geographic, or country inference in the resolver.
