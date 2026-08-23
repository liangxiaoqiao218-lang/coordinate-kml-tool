# Coordinate Engine V3 DMS Grouped Coordinates Recognizer

Status: `IMPLEMENTED`

`dms_grouped_coordinates` is an isolated deterministic recognizer for already
acquired grouped DMS coordinate text, such as Mining Area documents that contain
multiple separate coordinate groups in the same source.

## Ownership signature

The recognizer owns inputs that contain:

- explicit grouped coordinate context such as `Mining Area`, `Area Two`, or
  `The coordinates are as follows`;
- at least two coordinate groups;
- at least three DMS coordinate rows per accepted group;
- ordered point labels or source order that keeps each group separate.

It does not own plain single-group DMS text. That remains `generic_dms`.

## Input contract

Inputs must come from the acquisition candidate layer as sanitized text,
structured rows, document cues, or coordinate blocks.

The recognizer never reads:

- image bytes
- base64 payloads
- raw provider responses
- local filesystem paths
- provider credentials

## Conversion

DMS conversion is deterministic and uses explicit hemisphere tokens:

```text
N/E -> positive
S/W/O/Ouest -> negative
```

Token order does not determine role; hemisphere does.

## Geometry

The V3 normalized coordinate contract currently stores a flat coordinate list.
This recognizer therefore emits a technically constructible `multipolygon`
normalized result while preserving source trace and point order. The production
KML grouping contract remains a later runtime/product integration concern.

## Evidence

Initial evidence is `STRUCT_REAL_007`:

```text
Acquisition: PASS
Rows: 8/8
Groups: 2
Expected first KML: -8.88705278,11.87381111,0
Expected last KML: -8.89821111,11.86879167,0
```

This phase ports the deterministic recognizer only. It does not mark the family
as production-supported because the Phase 12B runtime contract has not been
implemented.

## Isolation boundary

Provider calls, Vision calls, OCR calls, acquisition retries, structural router
changes, model selection, production routing, KML export UI, and Primary runtime
are all out of scope.
