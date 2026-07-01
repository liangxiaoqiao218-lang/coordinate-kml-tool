# Coordinate Engine Audit 2026-07-01

Status: Audit Only  
Scope: Coordinate Engine V1 current production baseline and long-term governance plan  
Audit date: 2026-07-01  
Current branch: main  
Current HEAD: 8ddaeb18ad4900c4d8e91ebe8cc010f51f4837b6  
Current origin/main: 8ddaeb18ad4900c4d8e91ebe8cc010f51f4837b6  
Baseline note: this audit does not change parser code, frontend code, OCR prompts, or export logic.

## Executive Summary

Coordinate Engine V1 can recognize many real-world coordinate layouts, but recent fixes exposed a structural risk: adding or adjusting one parser can affect another parser, frontend display, or export path when the system does not enforce a strict Intent -> Parser -> Quality Gate -> Export boundary.

The current production baseline includes the frozen Coordinate Engine development specification and should be treated as the last V1 baseline before further architecture work. New functionality should pause until the regression matrix is complete and a V1.5/V2 implementation plan is accepted.

The immediate governance rule is:

- Do not patch global OCR prompts for one sample.
- Do not let WGS84 Chat compete with structured coordinate types.
- Do not let frontend code reinterpret backend accepted coordinates.
- Do not accept a coordinate result unless its dedicated Quality Gate passes.
- Prefer unstable/no KML over wrong KML.

## Regression Matrix

This matrix records the latest known state from historical real-image validations and current issue reports. Items marked "needs formal sample" require a stable image fixture and automated replay before they can be considered release-grade.

| # | Coordinate type | Sample file | Sample path | Expected precisionMode | Expected parserTrace | Expected points | First KML | Last KML | 20-run required | Current actual | Status | Failure reason / note |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| 1 | BFTM Projected XY | 布基纳法索02.jpg | D:/关于西非的业务/测试素材/布基纳法索02.jpg | bftm-projected-x-y | OCR -> BFTM:accepted | 20 | BFTM XY 655000,1333600 | BFTM XY 655000,1356200 | Yes | Corrected OCR digit duplication; backend and workspace use corrected rows | PASS | Must preserve repairs such as 13335600 -> 1335600 only inside BFTM context |
| 2 | BFTM Projected XY long table | 长坐标.png | sample path to confirm | bftm-projected-x-y | OCR -> BFTM:accepted | 20 | BFTM XY 655000,1333600 | BFTM XY 655000,1356200 | Yes | Fallback OCR promoted to BFTM projected mode | PASS | Needs formal sample path in regression-samples/BFTM |
| 3 | Burkina UTM30 / X-Y | 布基纳法索03.png | D:/关于西非的业务/测试素材/布基纳法索03.png | UTM30 / projected XY, not BFTM | OCR -> UTM_XY:accepted or projected XY accepted | 8 | UTM XY 727250,1219700 | UTM XY 729200,1219500 | Yes | No longer classified as BFTM | PASS | Export path must keep UTM30 separate from BFTM |
| 4 | RC2 / WGS84 Table | 刚果，两个坐标在同一张图.jpg | D:/关于西非的业务/测试素材/刚果，两个坐标在同一张图.jpg | wgs84-table-coordinates | OCR -> WGS84_TABLE:retry_vision -> BFTM:rejected -> WGS84_TABLE:accepted | 11 | 16.0320,3.7638,0 | 16.0348,3.7351,0 | Yes | 11 rows; F/G duplicate and second group B retained | PASS | Frontend export must use KML column, not WGS84 display column |
| 5 | RC2 / WGS84 Table timeout path | 微信图片_20260503091216_182_19.jpg | D:/关于西非的业务/测试素材/微信图片_20260503091216_182_19.jpg | wgs84-table-coordinates | OCR -> WGS84_TABLE:timeout_rescue -> BFTM:rejected -> WGS84_TABLE:accepted | 11 | 16.0320,3.7638,0 | 16.0348,3.7351,0 | Yes | Timeout rescue can recover RC2 table | PASS | Timeout rescue must not run when another structured intent is accepted |
| 6 | Mozambique Geographic Table | 莫桑比克矿地.jpg | sample path to confirm | mozambique-geographic-table | OCR -> BFTM:rejected -> MOZAMBIQUE_GEOGRAPHIC:accepted | 22 | 32.955556,-14.600000,0 | 32.955556,-14.633333,0 | Yes | 20-run test did not reach 20/20; rows=0 occurred intermittently | UNSTABLE | Vision DMS transcription can return rows=0; Chat/36-row false output should remain blocked |
| 7 | MGRS / UTM Grid Reference | 缅甸坐标.jpg | sample path to confirm | mgrs-utm-grid-reference | OCR -> MGRS:retry_vision -> BFTM:rejected -> MGRS:accepted | 7 | 97.2636250946,24.7901938391,0 | 97.2651119873,24.7906625174,0 | Yes | Parser passes when rawText includes A-G | PASS WITH VISION RISK | One visual retry previously missed F; parser was not at fault |
| 8 | Kyrgyz GK | 吉尔吉斯斯坦矿地坐标.png | sample path to confirm | kyrgyz-gk-point-x-y | OCR -> KYRGYZ_GK:accepted | 65 | EPSG:28413 converted point 1 inside Kyrgyzstan | EPSG:28413 converted point 65 inside Kyrgyzstan | Yes | Historical regression recovered 1-65, excluded 513/520 | PASS | Needs formal fixture and expected KML snapshot |
| 9 | Madagascar Cadastral | Madagascar cadastral stable sample | stable sample missing | cadastral-grid-num-xv-yv | OCR -> MADAGASCAR_CADASTRAL:accepted | 32 | expected first grid center pending sample | expected last grid center pending sample | Yes | Current available sample did not read right-side table | FAIL - SAMPLE MISSING | Cannot prove parser regression; stable original image must be restored |
| 10 | French Perimeter DMS | 模糊坐标.jpg | D:/关于西非的业务/测试素材/模糊坐标.jpg | french-perimeter-dms-prose | OCR -> FRENCH_PERIMETER_DMS:retry_vision -> FRENCH_PERIMETER_DMS:accepted | 4 | -8.833333333333334,12.066666666666666,0 | -8.833333333333334,12.036666666666667,0 | Yes | Backend and download path fixed to use formatted KML rows | PASS | Must not capture Point A-Z tables |
| 11 | Point A-Z DMS Table | 微信图片_20260427122118_114_19.jpg | D:/关于西非的业务/测试素材/微信图片_20260427122118_114_19.jpg | point-az-dms-table | OCR -> POINT_AZ_DMS_TABLE:accepted | 26 | -8.266666666666667,10.870833333333334,0 | -8.254444444444445,10.870833333333334,0 | Yes | French parser exclusion fixed; Point A-Z now owns this type | PASS | Requires dedicated intent before French prose parser |
| 12 | DMS_GROUPED | 两块矿地.jpg | D:/关于西非的业务/测试素材/两块矿地.jpg | dms-grouped-coordinates | OCR -> DMS_GROUPED:retry_vision -> DMS_GROUPED(blank_line):accepted | 8 in 2 groups | -8.88705278,11.87381111,0 | -8.89821111,11.86879167,0 | Yes | 2 Polygon groups, 4 points each | PASS | Frontend must preserve grouped state and not flatten into one polygon |
| 13 | Ordinary DMS | single DMS point text/image | formal image sample needed | DMS accepted mode | OCR -> DMS:accepted | 1 | -10.22406111111111,13.022647222222224,0 | same | Yes | Single-point DMS regression has passed historically | PASS - NEEDS FIXTURE | Needs a permanent sample file and expected JSON/KML |
| 14 | WGS84 Chat | text: 12.319572, -11.178174 | text fixture | wgs84-chat-coordinates | OCR/TEXT -> WGS84_CHAT:accepted | 1 | -11.178174,12.319572,0 | same | Yes | Chat works for unstructured lat,lon text | PASS | Chat must remain final fallback only |

## Current Stable Items

The following items are currently considered stable enough to remain in V1, assuming their dedicated regression samples are preserved and rerun before any future release:

- BFTM Projected XY
- Burkina UTM30 / X-Y projected table
- RC2 / WGS84 Table
- MGRS parser path
- Kyrgyz GK
- French Perimeter DMS
- Point A-Z DMS Table
- DMS_GROUPED
- Ordinary DMS
- WGS84 Chat

## Current Failed Items

### Madagascar Cadastral

Current status: failed as a regression fixture, not proven parser regression.

The current available sample does not reliably read the right-side cadastral table. The expected stable behavior remains:

- precisionMode = cadastral-grid-num-xv-yv
- 32 rows
- XV/YV parsed as Madagascar grid values
- KML generated after EPSG:29702 conversion

Required action:

- Recover the original stable Madagascar image that previously produced `cadastral-grid-num-xv-yv`.
- Add it to the regression evidence set or record its external path.
- Do not modify the Madagascar parser until the stable sample is available and current failure is reproducible on that sample.

## Current Unstable Items

### Mozambique Geographic Table

Current status: unstable.

Known current behavior:

- Chat takeover has been blocked in the tested path.
- 36-row false output has been blocked in the tested path.
- Intermittent rows=0 remains in repeated real uploads.
- The target result is 22 rows, order 1-22.

Expected stable result:

- precisionMode = mozambique-geographic-table
- 22 rows
- row 1: 32.955556,-14.600000,0
- row 2: 33.100000,-14.600000,0
- row 3: 33.100000,-14.655556,0
- row 22: 32.955556,-14.633333,0

Required action:

- Do not keep adding more blind visual retries.
- Treat rows=0 or non-22 rows as unstable and do not generate KML.
- Move Mozambique to V1.5/V2 deterministic table handling.

## OCR / Vision Intermittency, Not Parser Bugs

The following cases should not be treated as parser defects until rawText proves the parser received correct input and still failed:

- Mozambique Geographic Table: DMS transcription can intermittently return rows=0.
- MGRS: one run missed F in vision rawText; parser passed when F was present.
- Madagascar: current sample likely does not expose the required right-side table to OCR.

Governance implication: each structured type needs intent-level visual routing and a Quality Gate before export.

## Why Temporary Patching Must Stop

Recent history shows the same failure pattern:

1. A new parser or fallback is added to solve one real sample.
2. The new logic broadens an existing detection path.
3. Another coordinate type becomes misclassified, flattened, deduplicated, or exported from the wrong field.
4. A frontend workaround or parser blocker is added.
5. The cycle repeats.

This is not sustainable because Coordinate Engine now has more than ten active recognition paths. Continuing to patch individual symptoms inside shared OCR, fallback, Chat, or frontend extraction code will keep creating cross-type regressions.

## Coordinate Engine V1 Problems

### Parser Competition

Several parsers currently infer type from overlapping keywords or numeric structures. Examples include:

- French Perimeter DMS vs Point A-Z DMS Table
- BFTM vs UTM30 / X-Y projected tables
- WGS84 Table vs WGS84 Chat
- DMS_GROUPED vs ordinary DMS

### RawText Variability

The same image can produce different rawText across runs:

- Full structured table
- Decimal-only coordinates
- OCR garbage after timeout
- Missing rows
- Duplicated rows

Parsers should not be forced to compensate for every rawText variant globally.

### Frontend Reinterpretation

Several failures were caused after the backend had already accepted correct coordinates:

- Frontend badge displayed the wrong type.
- Workspace content was overwritten by a fallback extractor.
- KML export reparsed the human-readable WGS84 display column instead of using the KML column.
- Grouped polygons were flattened.

Frontend must consume the backend accepted result, not rediscover coordinate type.

### Missing Automated Regression

Regression is currently mostly manual and screenshot-driven. This makes it hard to prove that a new patch did not break older types.

### Missing 20-Run Stability Gate

For OCR/vision-dependent types, a single pass is not enough. Mozambique shows why: 16/20 or 19/20 is still not release-grade when wrong KML or missing KML can be produced.

## Long-Term Solution: Coordinate Engine V1.5 / V2

Target architecture:

```text
Image
-> Intent Router
-> Dedicated Vision
-> Dedicated Parser
-> Quality Gate
-> Export
```

### Intent Router

The Intent Router decides which coordinate type an image likely contains before any parser runs.

It should use:

- filename hints
- visible headers
- language and table keywords
- geometry of the document area
- coordinate token shapes
- strong negative blockers for competing types

The router returns one of:

- a specific structured type intent
- multiple candidate intents with priority
- Unknown

### Dedicated Vision

Each structured type owns its own visual extraction prompt.

Examples:

- BFTM Projected XY prompt
- WGS84 Table prompt
- Mozambique Geographic Table prompt
- MGRS prompt
- Point A-Z DMS Table prompt
- French Perimeter DMS prose prompt

Do not modify the universal OCR prompt to fix one type.

### Dedicated Parser

Parsers only parse the dedicated format they own. They do not guess document type.

Examples:

- BFTM parser parses BFTM X/Y rows.
- WGS84 Table parser parses lon/lat table rows.
- Mozambique parser parses Portuguese Latitude/Longitude DMS tables.
- Chat parser parses unstructured user text only when Intent=Unknown.

### Quality Gate

Quality Gate validates accepted output before export.

It checks:

- expected row count
- label/order continuity
- coordinate range
- duplicate pattern validity
- projection bounds
- expected geometry type
- no cross-type takeover

If Quality Gate fails, the only legal outcomes are:

- retry the same type-specific vision/parser path
- return unstable/no KML

It must not fall through to a competing parser.

### Export

Export consumes only Quality Gate accepted data.

Frontend rules:

- `data.coordinates` is the single source of truth after backend acceptance.
- Do not reparse `rawText` when `precisionMode` is already accepted.
- Do not use WGS84 display columns to build KML when a KML column exists.
- Preserve groups and MultiPolygon structures.
- Preserve accepted duplicate boundary points when the type requires them.

### Chat Fallback

WGS84 Chat Coordinates is allowed only when Intent=Unknown.

Chat must not handle:

- BFTM
- UTM projected tables
- MGRS
- DMS_GROUPED
- French Perimeter DMS
- Point A-Z DMS Table
- WGS84 Table
- Mozambique Geographic Table
- Kyrgyz GK
- Madagascar cadastral grids

## Coordinate Type Governance Summary

| Type | Intent features | Dedicated Vision | Dedicated Parser | Quality Gate | Export |
|---|---|---|---|---|---|
| BFTM Projected XY | BFTM, X/Y, SOMMETS, Burkina projected values | BFTM XY table extraction | BFTM row parser | X/Y ranges, row count, projection bounds | Projected polygon KML |
| Burkina UTM30 / X-Y | UTM, X/Y, Burkina coordinates, no BFTM reference | UTM XY table extraction | UTM projected parser | UTM zone/range and no BFTM intent | UTM30 converted KML |
| RC2 / WGS84 Table | longitude/latitude headers, 经度东/北纬 | WGS84 lon/lat table extraction | WGS84 table parser | header order, duplicate boundary preservation, row count | KML column direct export |
| Mozambique Geographic Table | COORDENADAS GEOGRAFICAS, Datum Tete, Latitude/Longitude DMS columns | Mozambique DMS table extraction | Mozambique DMS table parser | exactly 22 rows for known sample, order continuity, duplicate gate | Polygon KML |
| MGRS | zone/band/grid square, labels A-G | MGRS visual retry | MGRS parser | zone 1-60, valid band/grid, label count | Point/Line/Polygon KML |
| Kyrgyz GK | Russian table, point/X/Y, 13xxxxxx/46xxxxx | Kyrgyz GK prompt | EPSG:28413 parser | point continuity and Kyrgyz bounds | Polygon KML |
| Madagascar Cadastral | Liste_Carres, num/XV/YV | Madagascar table prompt | cadastral grid parser | 32 rows, XV/YV ranges, EPSG:29702 conversion bounds | Polygon KML |
| French Perimeter DMS | Coordonnees du perimetre, meridien, parallele, Ouest/Nord prose | French prose DMS retry | French perimeter parser | Point A-D and DMS direction consistency | Polygon KML |
| Point A-Z DMS Table | Point A-Z, Nord, Est/Ouest table | Point A-Z table prompt | Point A-Z table parser | A-Z continuity, 26 rows for known sample | Polygon KML |
| DMS_GROUPED | Mining Area sections, repeated DMS groups, blank-line groups | Grouped DMS retry | grouped DMS parser | group count, per-group point count | MultiPolygon KML |
| Ordinary DMS | plain DMS lines or single DMS point | ordinary OCR or direct text | DMS parser | DMS range/direction | Point/Line/Polygon KML |
| WGS84 Chat | unstructured decimal user text, no structured intent | none or text normalization | Chat decimal parser | WGS84 range, swapped warning | Point/Line/Polygon KML |

## Required Development Rules

Any future coordinate recognition change must include:

1. Intent definition
2. Dedicated Vision prompt or explicit no-vision reason
3. Dedicated Parser
4. Dedicated Quality Gate
5. Regression sample
6. Stable Paths update
7. Full regression matrix run
8. 20-run stability verification for vision-dependent types

Forbidden:

- Modifying common OCR prompt to fix one sample.
- Letting Chat parse structured coordinate documents.
- Letting one parser own multiple coordinate systems.
- Falling through from a failed structured parser to Chat.
- Exporting KML from unaccepted or weak results.
- Submitting a parser change without regression evidence.

## Release Checklist

Before any coordinate release:

- [ ] Regression Matrix is fully run.
- [ ] All PASS items remain PASS.
- [ ] All unstable items remain blocked from wrong KML export.
- [ ] No structured type falls into WGS84 Chat.
- [ ] Frontend uses backend accepted coordinates.
- [ ] KML coordinates are verified as longitude,latitude,0.
- [ ] 20-run stability is complete for affected vision-dependent types.
- [ ] Stable Paths and regression sample docs are updated.
- [ ] No unrelated files are included.

## Change Impact Checklist

Every future change must answer:

- Does this modify Intent?
- Does this modify Vision?
- Does this modify Parser?
- Does this modify Quality Gate?
- Does this modify Export?
- Which regression samples are affected?
- Can Chat be affected?
- Can any structured type be reclassified?
- Can any accepted backend result be overwritten by frontend code?
- What is the rollback plan?

## Next Steps

1. Keep code changes paused until this audit is reviewed.
2. Complete formal regression sample inventory.
3. Recover or replace missing Madagascar stable sample.
4. Record MGRS and ordinary DMS sample paths in regression-samples.
5. Build an automated regression runner around current matrix.
6. Design Coordinate Engine V1.5 Intent Router without touching current parser code.
7. Move Mozambique to V1.5/V2 deterministic table handling.
8. Only resume feature work after the matrix can be run before every release.

