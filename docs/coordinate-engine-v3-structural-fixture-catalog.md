# Coordinate Engine V3 Structural Fixture Catalog

Phase 11A establishes a structural fixture catalog for the experimental V3 acquisition strategy router.

This catalog is not a production router evaluation and does not change thresholds. It records real image/document structure, provenance, dry-run observations, and ground-truth availability for future generalization testing.

## Scope

- Router threshold changes: none
- Router logic changes: none
- Provider calls: `0`
- Primary integration: none
- Production routing: none
- Image storage policy: real images under `artifacts/` remain excluded from Git

## Ground Truth Levels

| Level | Meaning |
|---|---|
| `LEVEL_0` | image only |
| `LEVEL_1` | structural ground truth |
| `LEVEL_2` | expected acquisition structure |
| `LEVEL_3` | expected recognizer owner |
| `LEVEL_4` | coordinate ground truth |

Router evaluation only requires structural truth, but provider/model validation should prefer `LEVEL_3` or `LEVEL_4`.

## Target Structural Categories

| Category | Real fixture available? | File | Ground truth | Router dry-run possible? | Notes |
|---|---:|---|---|---:|---|
| `small_table_map` | yes | `artifacts/fixtures/indonesia-utm50s-real-001.jpg` | `LEVEL_4` | yes | Core validated sample |
| `medium_table_map` | yes | `artifacts/fixtures/indonesia-utm50s-real-002.jpg` | `LEVEL_4` | yes | Core validated near-boundary sample |
| `large_dense_table_map` | yes | `artifacts/fixtures/indonesia-utm50s-real-003.jpg` | `LEVEL_4` | yes | Core validated complex sample |
| `full_frame_compact_table` | yes | `artifacts/fixtures/cote-divoire-dms-real-001.jpeg` | `LEVEL_4` | yes | Core validated full-frame table |
| `mixed_text_table_map` | no | none | unavailable | no | Needs user-provided real image |
| `large_non_coordinate_table` | no | none | unavailable | no | Needs user-provided real image |
| `plain_coordinate_screenshot` | no | none | unavailable | no | Needs user-provided real image |
| `handwritten_or_low_structure` | no | historical path unavailable | unavailable | no | `D:/关于西非的业务/测试素材/手写坐标.jpg` not present locally |
| `multi_table_document` | no | historical path unavailable | unavailable | no | `D:/关于西非的业务/测试素材/两块矿地.jpg` not present locally |
| `table_plus_heavy_annotations` | no | none | unavailable | no | Needs user-provided real image |

## Core Validated Fixtures

### STRUCT_REAL_001

| Field | Value |
|---|---|
| Source name | Côte d’Ivoire |
| Source file | `artifacts/fixtures/cote-divoire-dms-real-001.jpeg` |
| Artifact policy | `EXCLUDED_REAL_FIXTURE` |
| Structural category | `full_frame_compact_table` |
| Quality | `HIGH` |
| File size | `102211` bytes |
| Original dimensions | `964×229` |
| Table detected | `true` |
| Table region | `x=0 y=0 width=964 height=229` |
| Table region percentage | `100` |
| Table height ratio | `1` |
| Composite dimensions | `1000×334` |
| Composite height ratio | `1.4585` |
| Background/table ratio | `0` |
| Horizontal / vertical signals | `2 / 19` |
| Current experimental path | `PATH_A` |
| Known validated best path | `PATH_A` |
| Provider model validated | `qwen-vl-plus` |
| Ground truth level | `LEVEL_4` |
| Catalog status | `CORE_VALIDATED` |

Predicate distances:

- `largeDocumentImage`: `width=-36`, `height=-571`, pass `false`
- `substantialEmbeddedTable`: min `+88`, max `-60`, pass `false`
- `largeVerticalTableExtent`: `+0.7`, pass `true`
- `largeCompositeRepresentation`: `+1.0085`, pass `true`

### STRUCT_REAL_002

| Field | Value |
|---|---|
| Source name | Indonesia #001 |
| Source file | `artifacts/fixtures/indonesia-utm50s-real-001.jpg` |
| Artifact policy | `EXCLUDED_REAL_FIXTURE` |
| Structural category | `small_table_map` |
| Quality | `HIGH` |
| File size | `277506` bytes |
| Original dimensions | `1600×1132` |
| Table detected | `true` |
| Table region | `x=919 y=831 width=650 height=198` |
| Table region percentage | `7.1058` |
| Table height ratio | `0.1749` |
| Composite dimensions | `1636×362` |
| Composite height ratio | `0.3198` |
| Background/table ratio | `13.073` |
| Horizontal / vertical signals | `2 / 2` |
| Current experimental path | `PATH_A` |
| Known validated best path | `PATH_A` |
| Provider model validated | `qwen-vl-plus` |
| Ground truth level | `LEVEL_4` |
| Catalog status | `CORE_VALIDATED` |

Predicate distances:

- `largeDocumentImage`: `width=+600`, `height=+332`, pass `true`
- `substantialEmbeddedTable`: min `-4.8942`, max `+32.8942`, pass `false`
- `largeVerticalTableExtent`: `-0.1251`, pass `false`
- `largeCompositeRepresentation`: `-0.1302`, pass `false`

### STRUCT_REAL_003

| Field | Value |
|---|---|
| Source name | Indonesia #002 |
| Source file | `artifacts/fixtures/indonesia-utm50s-real-002.jpg` |
| Artifact policy | `EXCLUDED_REAL_FIXTURE` |
| Structural category | `medium_table_map` |
| Quality | `HIGH` |
| File size | `288226` bytes |
| Original dimensions | `1600×1132` |
| Table detected | `true` |
| Table region | `x=791 y=751 width=774 height=274` |
| Table region percentage | `11.7091` |
| Table height ratio | `0.242` |
| Composite dimensions | `1636×438` |
| Composite height ratio | `0.3869` |
| Background/table ratio | `7.5403` |
| Horizontal / vertical signals | `2 / 2` |
| Current experimental path | `PATH_A` |
| Known validated best path | `PATH_A` |
| Provider model validated | `qwen-vl-plus` |
| Ground truth level | `LEVEL_4` |
| Catalog status | `CORE_VALIDATED` |

Predicate distances:

- `largeDocumentImage`: `width=+600`, `height=+332`, pass `true`
- `substantialEmbeddedTable`: min `-0.2909`, max `+28.2909`, pass `false`
- `largeVerticalTableExtent`: `-0.058`, pass `false`
- `largeCompositeRepresentation`: `-0.0631`, pass `false`

### STRUCT_REAL_004

| Field | Value |
|---|---|
| Source name | Indonesia #003 |
| Source file | `artifacts/fixtures/indonesia-utm50s-real-003.jpg` |
| Artifact policy | `EXCLUDED_REAL_FIXTURE` |
| Structural category | `large_dense_table_map` |
| Quality | `HIGH` |
| File size | `302795` bytes |
| Original dimensions | `1600×1132` |
| Table detected | `true` |
| Table region | `x=19 y=599 width=626 height=438` |
| Table region percentage | `15.1385` |
| Table height ratio | `0.3869` |
| Composite dimensions | `1636×602` |
| Composite height ratio | `0.5318` |
| Background/table ratio | `5.6057` |
| Horizontal / vertical signals | `2 / 2` |
| Current experimental path | `PATH_B` |
| Known validated best path | `PATH_B` |
| Provider model validated | `qwen-vl-ocr-latest` |
| Ground truth level | `LEVEL_4` |
| Catalog status | `CORE_VALIDATED` |

Predicate distances:

- `largeDocumentImage`: `width=+600`, `height=+332`, pass `true`
- `substantialEmbeddedTable`: min `+3.1385`, max `+24.8615`, pass `true`
- `largeVerticalTableExtent`: `+0.0869`, pass `true`
- `largeCompositeRepresentation`: `+0.0818`, pass `true`

## Coverage Summary

- Existing core real fixtures: `4`
- Raw new image files found locally: `17`
- Exact duplicates excluded: `1`
- Related variants retained: `1`
- Additional independent real fixture candidates: `16`
- Total independent real fixture candidates: `20`
- Target categories: `10`
- Categories covered: `9/10`
- Real fixture target: `>=10`
- Target met: `YES`
- Structural diversity target: `>=6`
- Structural diversity target met: `YES`
- Expected path known: `4`
- Expected path unresolved: `16`

Covered categories:

- `small_table_map`
- `medium_table_map`
- `large_dense_table_map`
- `full_frame_compact_table`
- `mixed_text_table_map`
- `handwritten_or_low_structure`
- `multi_table_document`
- `table_plus_heavy_annotations`
- `OTHER_STRUCTURAL`

Missing categories:

- `large_non_coordinate_table`

## Phase 11A.1 New Fixture Intake

User-provided real images were added under `artifacts/fixtures/structural-expansion/`. The images remain excluded from Git and were not moved, renamed, copied, or used for provider calls.

### Intake Summary

| Metric | Result |
|---|---:|
| Raw new image files | `17` |
| Exact duplicates | `1` |
| Related variants | `1` |
| Independent new fixture candidates | `16` |
| Existing core fixtures | `4` |
| Total independent real fixture candidates | `20` |
| Categories covered | `9/10` |
| Provider calls | `0` |
| Router threshold changes | `0` |
| Router logic changes | `0` |

### Duplicate Handling

| File | Status | Handling |
|---|---|---|
| `artifacts/fixtures/structural-expansion/印尼矿地03.jpg` | Exact duplicate of `artifacts/fixtures/indonesia-utm50s-real-003.jpg` | Excluded from new fixture count |
| `artifacts/fixtures/structural-expansion/微信图片_20260503091216_182_19.jpg` | Related variant of `STRUCT_REAL_008` | Retained as independent candidate, flagged `RELATED_VARIANT` |

### New Fixture Catalog

| Fixture | File | Category | Quality | Ground truth | Observed dry-run path | Notes |
|---|---|---|---|---|---|---|
| `STRUCT_REAL_005` | `6d2cb08956f2a1e6511fbf18d8922200.png` | `full_frame_compact_table` | `HIGH` | `LEVEL_1` | `PATH_A` | Compact table |
| `STRUCT_REAL_006` | `df55cd6c829b1d62375626aca75225c4.jpg` | `OTHER_STRUCTURAL` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Full-page document with embedded coordinate table |
| `STRUCT_REAL_007` | `两块矿地.jpg` | `multi_table_document` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Multiple coordinate blocks in one document |
| `STRUCT_REAL_008` | `刚果，两个坐标在同一张图.jpg` | `OTHER_STRUCTURAL` | `MEDIUM` | `LEVEL_4` | `PATH_A` | RC2 / WGS84 table frozen from historical baseline; expectedPath remains `UNRESOLVED` |
| `STRUCT_REAL_009` | `吉尔吉斯斯坦矿地坐标.png` | `full_frame_compact_table` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Photographed/scanned coordinate table |
| `STRUCT_REAL_010` | `布基纳法索01.jpg` | `OTHER_STRUCTURAL` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Full-page photographed document |
| `STRUCT_REAL_011` | `微信图片_20260427122118_114_19.jpg` | `mixed_text_table_map` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Mobile screenshot with table and map context |
| `STRUCT_REAL_012` | `微信图片_20260503091216_182_19.jpg` | `OTHER_STRUCTURAL` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Related variant of `STRUCT_REAL_008` |
| `STRUCT_REAL_013` | `手写坐标.jpg` | `handwritten_or_low_structure` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Handwritten coordinate sample |
| `STRUCT_REAL_014` | `模糊坐标.jpg` | `handwritten_or_low_structure` | `LOW` | `LEVEL_4` | `PATH_A` | French perimeter DMS Point A-D coordinate truth frozen from historical sources; expectedPath remains `UNRESOLVED` |
| `STRUCT_REAL_015` | `科特迪瓦03.png` | `full_frame_compact_table` | `HIGH` | `LEVEL_1` | `PATH_A` | Compact Côte d’Ivoire table |
| `STRUCT_REAL_016` | `科特迪瓦04.png` | `OTHER_STRUCTURAL` | `MEDIUM` | `LEVEL_3` | `PATH_A` | Côte d’Ivoire DMS owner/row truth frozen; complete coordinates intentionally not claimed; expectedPath remains `UNRESOLVED` |
| `STRUCT_REAL_017` | `缅甸坐标.jpg` | `mixed_text_table_map` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Map/image plus coordinate text |
| `STRUCT_REAL_018` | `莫桑比克矿地.jpg` | `full_frame_compact_table` | `MEDIUM` | `LEVEL_1` | `PATH_A` | Full-page coordinate table/document |
| `STRUCT_REAL_019` | `邓巴坐标01.jpg` | `full_frame_compact_table` | `HIGH` | `LEVEL_1` | `PATH_A` | Compact coordinate table |
| `STRUCT_REAL_020` | `马达加斯加坐标.png` | `table_plus_heavy_annotations` | `HIGH` | `LEVEL_1` | `PATH_A` | Map with side coordinate table and annotations |

### Observed Dry-Run Distribution

| Path | Count |
|---|---:|
| `PATH_A` | `19` |
| `PATH_B` | `1` |
| `UNRESOLVED expectedPath` | `16` |

Only the four core fixtures have known expected path from previous real-provider validation. The 16 new fixtures are structural-catalog entries only; their expected acquisition path remains `UNRESOLVED` until future evidence exists.

### Near-Boundary Fixtures

The closest known threshold-boundary fixtures remain the core Indonesia samples:

- `STRUCT_REAL_003` (`Indonesia #002`): observed `PATH_A`, table percentage distance `-0.2909`
- `STRUCT_REAL_004` (`Indonesia #003`): observed `PATH_B`, table percentage distance `+3.1385`

No new fixture is treated as a validated path boundary because no provider/model ground truth was collected in this phase.

## Phase 11C Ground Truth Recovery

Phase 11C searches existing project history, frozen regression metadata, docs, and stable baselines for fixture-specific ground truth. It does not use current recognizer output, current provider output, or Router output as truth.

Expected acquisition path remains `UNRESOLVED` for all newly recovered fixtures. Ground truth recovery answers what the correct coordinate family / row count / structure is; provider A/B evidence is still required before assigning `expectedPath`.

### Priority Fixture Recovery

| Fixture | File | Historical match | Recovered level | Expected owner | Expected rows | Coordinate truth | Provenance |
|---|---|---:|---|---|---:|---|---|
| `STRUCT_REAL_020` | `马达加斯加坐标.png` | yes, but blocked | `LEVEL_1` | unavailable | unavailable | unavailable | Madagascar README says stable original sample is missing and current candidate image is not stable enough |
| `STRUCT_REAL_007` | `两块矿地.jpg` | yes | `LEVEL_3` | `dms_grouped_coordinates_historical_not_ported` | `8` | partial | `regression-samples/DMS_GROUPED/expected.json` and locked baseline |
| `STRUCT_REAL_011` | `微信图片_20260427122118_114_19.jpg` | yes | `LEVEL_3` | `point_az_dms_table_historical_not_ported` | `26` | partial | `regression-samples/POINT_AZ_DMS_TABLE/expected.json` |
| `STRUCT_REAL_013` | `手写坐标.jpg` | yes | `LEVEL_3` | `handwritten_dms_historical_not_ported` | `16` | partial | `regression-samples/HANDWRITTEN_DMS/expected.json` |
| `STRUCT_REAL_009` | `吉尔吉斯斯坦矿地坐标.png` | yes | `LEVEL_3` | `kyrgyzstan_gauss_kruger` | `65` | partial | `regression-samples/Kyrgyz_GK/expected.json` and locked baseline |
| `STRUCT_REAL_017` | `缅甸坐标.jpg` | yes | `LEVEL_3` | `mgrs` | `7` | partial | `regression-samples/MGRS/expected.json`, README, and locked baseline |

### Recovered Evidence Set

| Metric | Result |
|---|---:|
| Recovered `LEVEL_4` | `0` |
| Recovered `LEVEL_3` | `5` |
| Still `LEVEL_1/2` among priority fixtures | `1` |
| Independent ground-truth-ready priority fixtures | `5` |
| Structural categories represented by recovered `LEVEL_3` | `4` |

Recovered structural categories:

- `multi_table_document`
- `mixed_text_table_map`
- `handwritten_or_low_structure`
- `full_frame_compact_table`

Integrity constraints:

- current recognizer output used as truth: `NO`
- current provider output used as truth: `NO`
- Router output used as truth: `NO`
- expectedPath changes: `0`
- provider calls: `0`
- runtime changes: `0`

Important limitation:

Three recovered fixtures have historical owners that are not currently V3 isolated recognizers:

- `dms_grouped_coordinates_historical_not_ported`
- `point_az_dms_table_historical_not_ported`
- `handwritten_dms_historical_not_ported`

These are still valid ground-truth recovery records, but Phase 11D must separate provider/acquisition evidence from deterministic recognizer coverage gaps.

## Phase 11H.1 Ground Truth Freeze

Phase 11H.1 freezes only the correct-answer / expected-recognition metadata for three independently sourced fixtures. It does not resolve acquisition strategy, and it does not use current provider output, current recognizer output, or Router output as truth.

Expected acquisition path remains `UNRESOLVED` for all three fixtures in this phase.

### Frozen Fixtures

| Fixture | File | Frozen level | Expected owner | Expected rows | Coordinate truth | Expected path | Provenance |
|---|---|---:|---|---:|---|---|---|
| `STRUCT_REAL_014` | `模糊坐标.jpg` | `LEVEL_4` | `french_perimeter_dms_historical_not_ported` | `4` | complete Point A-D KML coordinates | `UNRESOLVED` | `regression-samples/FRENCH_PERIMETER_DMS/expected.json`; `regression-samples/FRENCH_PERIMETER_DMS/README.md`; `COORDINATE_ENGINE_AUDIT_2026-07-01.md`; `COORDINATE_TYPE_REGISTRY.md` |
| `STRUCT_REAL_008` | `刚果，两个坐标在同一张图.jpg` | `LEVEL_4` | `wgs84_table` | `11` | frozen RC2 baseline hash, point count, first/last KML | `UNRESOLVED` | `regression-samples/RC2/expected.json`; `regression-samples/RC2/baseline.json`; `COORDINATE_RECOGNITION_GOLDEN_BASELINE.json`; `COORDINATE_ENGINE_AUDIT_2026-07-01.md` |
| `STRUCT_REAL_016` | `科特迪瓦04.png` | `LEVEL_3` | `cote_divoire_dms` | `4` | partial; full coordinate truth not claimed | `UNRESOLVED` | `COORDINATE_RECOGNITION_GOLDEN_BASELINE.json`; `REGRESSION_TEST_SAMPLES.md`; `COORDINATE_RECOGNITION_STABLE_PATHS.md` |

### Freeze Counts

| Metric | Result |
|---|---:|
| New `LEVEL_4` added | `2` |
| New `LEVEL_3` added | `1` |
| Independent ground-truth-ready fixtures added | `3` |
| ExpectedPath changes | `0` |
| Provider calls | `0` |
| Runtime changes | `0` |

Future Full-Image OCR evidence priority:

1. `STRUCT_REAL_014` — `HIGH`
2. `STRUCT_REAL_008` — `HIGH`
3. `STRUCT_REAL_016` — `MEDIUM`

## Phase 11D.2 Evidence Reclassification

Phase 11D.2 repairs only the experimental provider evidence classifier. It reclassifies the existing Phase 11D artifact without calling the provider again.

Reclassified evidence:

| Fixture | PATH A | PATH B | Comparison | Expected path decision |
|---|---|---|---|---|
| `STRUCT_REAL_007` | `GROUPED_ACQUISITION_COMPLETE`, safe rows `8/8` | `PROVIDER_TIMEOUT` | `PATH_A_BETTER` | `PATH_A` |
| `STRUCT_REAL_009` | `PROVIDER_TIMEOUT` | `PATH_NOT_APPLICABLE` | `UNRESOLVED` | `UNRESOLVED` |
| `STRUCT_REAL_011` | `PROVIDER_TIMEOUT` | `ACQUISITION_INCOMPLETE`, safe rows `25/26` | `NEITHER_VALID` | `UNRESOLVED` |
| `STRUCT_REAL_013` | `ACQUISITION_INCOMPLETE`, safe rows `15/16` | `ACQUISITION_INCOMPLETE_EMPTY`, safe rows `0/16` | `NEITHER_VALID` | `UNRESOLVED` |
| `STRUCT_REAL_017` | `PROVIDER_TIMEOUT` | `PATH_NOT_APPLICABLE` | `UNRESOLVED` | `UNRESOLVED` |

Only `STRUCT_REAL_007` is promoted:

```text
expectedPath: PATH_A
expectedPathSource: PHASE_11D_EXISTING_PROVIDER_EVIDENCE_RECLASSIFIED
```

The promotion is acquisition-path evidence only. V3 end-to-end remains a coverage gap because the historical grouped DMS recognizer has not been ported.

Post-reclassification gate:

- total validated expected paths: `5`
- independent validation fixtures: `1`
- generalization gate: `FAIL`
- Router threshold changes: `0`
- Router logic changes: `0`
- provider rerun: `NO`

## Existing Repository Search

Repository image search now includes the four core `artifacts/fixtures` images and the user-provided structural expansion images under `artifacts/fixtures/structural-expansion/`.

UI/share assets and QR codes are still excluded because they are not real coordinate/document structural fixtures.

## Router Dry-Run Status

- Router dry-run performed: `YES`
- Provider calls: `0`
- Thresholds changed: `NO`
- Experimental fit recalculated: `NO`
- Production generalization: `UNPROVEN`

## Data Integrity

Expected path is counted only for the four `CORE_VALIDATED` fixtures with prior real provider evidence. No unresolved fixture is treated as PASS/FAIL.

No recognizer output was used to create new coordinate ground truth.

## Missing Fixture Plan

The minimum Phase 11A intake target is now satisfied:

- `>=10 independent real fixtures`: `PASS`
- `>=6 structural categories`: `PASS`
- core fixture protection: `PASS`
- provider calls: `0`
- router threshold changes: `0`

Remaining useful expansion target:

- `large_non_coordinate_table`
- more independent full-page document samples with manually assigned structural labels
- later LEVEL_3 / LEVEL_4 ground truth for provider/model validation

## Decision

Structural fixture diversity is sufficient to start Phase 11B generalization evaluation.

Decision:

```text
STRUCTURAL_FIXTURE_EXPANSION_READY
```

Recommended next step:

```text
Phase 11B — Router Generalization Evaluation
```
