# Recognition inspection output contract

Date: 2026-10-01
Contract: `recognition_output_capability_v1`

## Product rule

Map preview and unverified KML are inspection and correction tools. They are
available whenever the current recognition result contains a finite, valid
EPSG:4326 geometry bound to the current result identity, revision and geometry
hash. Recognition confidence, source authority, evidence completeness,
coordinate-family classification, review status and user confirmation do not
decide this technical capability.

Showing a map or downloading an unverified KML does not state that the
coordinates are correct and does not grant formal export authority.

## One generic decision

`evaluateRecognitionOutputCapability` is the shared capability decision for
all coordinate representations and future families. It checks only:

- finalized result schema and identity;
- current result revision;
- explicit target CRS `EPSG:4326` and longitude/latitude axis order;
- finite, structurally valid geometry;
- geometry hash consistency.

If these checks pass, both `mapReady` and `kmlReady` are true. All acquisition,
parsing, row, label, direction, CRS-source and cross-representation concerns
remain visible as warnings and keep the result unverified, but they do not hide
inspectable output that already exists.

If no valid WGS84 geometry can be produced, output remains blocked. Examples
include missing or non-finite coordinates, an unresolved source CRS that cannot
be converted, invalid geometry, a stale result revision, and an identity or
geometry-hash mismatch.

## State separation

| State | Meaning | May affect Map/KML capability |
|---|---|---|
| `technicallyGeneratable` | Current finite WGS84 geometry can be rendered and serialized | Yes |
| warning/review reasons | Accuracy or evidence requires user inspection | No |
| `unverified` | Output is not a formal correctness claim | Label only |
| formal authorization | Result may be used by an authoritative downstream workflow | No |
| usage settlement | Recognition request charging outcome | No |

Edits must create or bind a new result revision and geometry hash. An old map
or KML request must not be accepted against a changed result.

## Non-goals

This contract does not add rules for a country, filename, image, point count,
coordinate value, coordinate family or Provider response style. It does not
change parsing priority, coordinate values, CRS conversion, formal
authorization or billing.

## Regression boundary

The permanent regression suite covers DMS, decimal, projected and a synthetic
future family through the same evaluator. Positive cases include review,
confidence and evidence-conflict warnings. Negative cases include invalid or
non-finite geometry, non-WGS84 CRS, stale revision and geometry-hash mismatch.
