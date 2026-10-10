# Context-bound Coordinate Evidence IR + Shadow Canonical Adapter

Baseline commit: `45778224c0b0f1279477a3ee781d212a02d2c43e`
Baseline tree: `2da28a76aefc9d370538b0432e727234602c3668`

## Boundary

This phase adds an offline/shadow evidence contract. It does not mount a production route, select a Provider, call a model, change the production recognition authority, authorize Map/KML, or replace `finalized_coordinate_result_v1`.

The IR binds every normalized coordinate field to retained document context:

- document SHA-256;
- page, region, table, row, and field source nodes;
- original row text and source order;
- group identity and dual geographic/projected representations;
- CRS, datum, zone, hemisphere, unit, and axis order;
- geometry intent;
- assertion state: `explicit`, `inferred`, `human_confirmed`, or `unknown`.

An inferred assertion requires both source evidence and a rationale. It remains `needs_review`; it cannot become confirmed truth or grant output authority. Unknown geometry remains `Unknown`. The adapter preserves the current agentic result shape only as a shadow preview for the existing finalizer contract.

## Minimal batch and truth eligibility

| Case | Role | Independent truth | Accuracy denominator |
|---|---|---:|---:|
| `indonesia-dms-real-001` | confirmed dual UTM/DMS baseline | yes | yes |
| `ocr_dms_civ_001` | confirmed DMS baseline | yes | yes |
| `dms_grouped_two_areas_001` | grouped contract baseline | not recorded independently | no |
| `low_clarity_blurry_dms_001` | confirmed review-required case | yes | yes |
| `wgs84_table_rc2_congo_001` | unresolved two-area geometry | baseline only | no |
| `long_coordinate_table_001` | missing truth diagnostic | no | no |
| `ocr_dms_artisanat_001` | confirmed holdout | yes | yes |
| `cote_divoire_single_03` | confirmed holdout pending main-Golden reconciliation | yes | yes |

Reference KML is not truth. Cases without independent truth may exercise evidence and safety contracts but must not raise the accuracy numerator or denominator.

## Acceptance meaning

The targeted regression verifies fixture identity, exact retained source text, coordinate values and order for independently confirmed records, dual representation binding, review preservation, assertion-state isolation, and zero production mounting. It is an offline contract test, not a real-model accuracy, latency, availability, or cost result.

Any later real-model qualification requires a new V-C authorization with exact sample hashes, model snapshot, call/retry/timeout/cost ceilings, retention/cache restrictions, and end-to-end timing from submission to usable result. Previous V-C authorization is not reusable.

## Approved acquisition baseline drift

`coordinate-result-shadow-regression` originally required every current
acquisition object to equal an older `86ea4ac` module byte-for-byte while that
module resolves its imported parsers from the current checkout. The reported
drift exposed an obsolete `NO_COORDINATE_EVIDENCE / NOT_ESTABLISHED`
expectation. The merged acquisition contract now intentionally retains all 16
source candidates as `COMPLETED / REVIEW_REQUIRED`; replaying the historical
module against current imports is not a stable product contract.

The regression therefore checks the approved semantic boundary instead of the
obsolete exact object equality: both historical and current evidence remain
`EVIDENCE_ONLY`; the current candidates remain review-required; acquisition
Map and KML states remain `CLOSED`; and an evidence-only input without a
finalized geometry cannot gain formal authority, Map capability or KML
capability. This is a test expectation correction only and does not alter the
candidate parser, finalizer, output capability, usage settlement or production
route.

The same boundary preserves the emitting stage: JSON table rows are attributed
to `adaptRecognitionTableCandidates`, while plain-text rows remain attributed
to `extractRecognitionCandidateEvidence`. Field values and order are still
compared exactly; the regression does not relabel one entry path as the other.
Both compact and explanatory-wrapper JSON fixtures must retain all 16 rows;
the former zero-row parser-coverage expectation described the pre-D1 path and
is no longer the approved acquisition contract.

The inspectable-output example is finalized through the existing Finalizer so
that its schema, result ID, revision and geometry hash are real contract
values. A review result with valid current WGS84 geometry may expose
provisional Map and unverified KML capability while formal authorization stays
false; missing or conflicting identity continues to block both capabilities.
