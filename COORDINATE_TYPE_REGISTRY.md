# Coordinate Type Registry

Status: Coordinate Engine type registry
Purpose: This is the single registry for Coordinate Engine coordinate types.

This registry records the stable contract for every coordinate type. It does not
define parser implementation. Parser routing, development rules, and release
gates are governed by:

- `COORDINATE_ENGINE_ARCHITECTURE.md`
- `COORDINATE_RECOGNITION_STABLE_PATHS.md`
- `COORDINATE_ENGINE_REGRESSION_SPEC.md`
- `COORDINATE_ENGINE_AUDIT_2026-07-01.md`

## Global Rules

1. Every coordinate type must have one Type ID.
2. Every structured coordinate type must have a dedicated Intent, Vision path,
   Parser, Quality Gate, Export rule, and Regression Sample.
3. Parsers do not guess coordinate type. Intent decides ownership first.
4. Quality Gate failure must not fall through to another parser. It can only
   retry the same type or return unstable/no KML.
5. `wgs84-chat-coordinates` is fallback only. It can run only when
   Intent=Unknown and no structured type is detected.
6. Structured coordinate types must not be handled by Chat.
7. Frontend export must use backend accepted `data.coordinates` as the single
   source of truth after a structured type is accepted.
8. Do not change a stable type to fix a new type unless the full regression
   matrix passes.
9. Every new type must add or update regression samples before release.
10. Vision-dependent types require 20-run stability validation before being
    considered Frozen.

## Required Registry Fields

Each type is recorded with:

- Type ID
- Intent
- Dedicated Vision
- Dedicated Parser
- Quality Gate
- Export
- Regression Sample
- Stable Since commit
- Status
- Known Issues

Status values:

- `Frozen`: stable path, must not be changed without full regression.
- `Beta`: usable but still needs stronger regression or stability evidence.
- `Pending`: known type, but sample or stability evidence is incomplete.
- `Unstable`: known current production risk; do not extend by patching shared
  parser paths.

---

## bftm-projected-x-y

- Type ID: `bftm-projected-x-y`
- Legacy aliases: `bftm_xy`, `bftm`
- Intent:
  - Burkina Faso projected coordinate table.
  - Keywords: `BFTM`, `Projection BFTM`, `Coordonnees en BFTM (XY)`,
    `SOMMETS`, `X(m)`, `Y(m)`, `ITRF 2008`.
  - Numeric pattern: projected X around Burkina easting range and Y around
    Burkina northing range.
- Dedicated Vision:
  - BFTM / X-Y table extraction.
  - Preserve row relationship between summit/label, X, and Y.
- Dedicated Parser:
  - BFTM projected X/Y parser.
  - Must not treat values as WGS84 decimal degrees.
- Quality Gate:
  - Requires valid projected X/Y rows.
  - Rejects bbox pollution, X/X or Y/Y duplication, and non-BFTM UTM tables.
  - OCR digit repair is allowed only inside confirmed BFTM long-table context
    and only when the repaired Y enters legal range and is consistent with
    neighboring rows.
- Export:
  - Convert from BFTM projected coordinates to WGS84 KML when export is
    requested.
  - KML order must be `longitude,latitude,0`.
  - Frontend must use backend accepted `data.coordinates`.
- Regression Sample:
  - `regression-samples/BFTM/`
  - Known samples:
    - `D:/about-west-africa-business/test-materials/布基纳法索02.jpg`
    - `长坐标.png` path pending formal record.
- Stable Since commit:
  - `5fcc3eb` promoted BFTM fallback OCR to projected mode.
  - `9f77985` repaired OCR digit duplication in BFTM projected tables.
  - `9d7a514` preserved backend BFTM coordinates in frontend workspace.
- Status: Frozen
- Known Issues:
  - Must not capture ordinary UTM30 X/Y tables.
  - Must not expose repaired backend coordinates and raw OCR coordinates as two
    competing frontend data sources.

---

## utm30n-projected-x-y

- Type ID: `utm30n-projected-x-y`
- Legacy aliases: UTM numeric X/Y, projected X/Y.
- Intent:
  - Burkina Faso or West Africa UTM Zone 30N projected X/Y table.
  - Keywords: `UTM`, `X`, `Y`, `Sommet(s)`, projected coordinate values.
  - No explicit BFTM keyword or BFTM projection reference.
- Dedicated Vision:
  - UTM / X-Y table extraction.
- Dedicated Parser:
  - UTM Zone 30N projected coordinate parser.
- Quality Gate:
  - Must be recognized as UTM30N or user-selected/system-detected UTM30N.
  - Must not be promoted to BFTM without BFTM intent evidence.
- Export:
  - Convert UTM30N projected X/Y to WGS84 KML.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - Sample: `D:/about-west-africa-business/test-materials/布基纳法索03.png`
  - Expected first row: `727250,1219700`
  - Expected last row: `729200,1219500`
- Stable Since commit:
  - `9042a88` avoided classifying UTM30 projected tables as BFTM.
- Status: Frozen
- Known Issues:
  - Must remain separate from `bftm-projected-x-y`.

---

## wgs84-table-coordinates

- Type ID: `wgs84-table-coordinates`
- Legacy aliases: WGS84 Longitude/Latitude table, lon/lat table, RC2.
- Intent:
  - Structured WGS84 table with explicit longitude/latitude headers.
  - Keywords: `longitude`, `latitude`, `lon`, `lat`, `经度`, `纬度`,
    `经度东`, `北纬`, `Longitude Latitude Table`.
  - Coordinate order is determined by table headers.
- Dedicated Vision:
  - WGS84 table vision retry / timeout rescue for clear lon/lat tables.
  - Preserve labels and duplicate boundary points when table represents mining
    polygon boundaries.
- Dedicated Parser:
  - WGS84 table parser.
  - Parses `label | longitude | latitude` or formatted
    `label | WGS84 | KML`.
- Quality Gate:
  - Requires valid WGS84 ranges.
  - Header order must be respected.
  - Duplicate boundary points must be preserved when enabled by table path.
  - Chat must not accept this type.
- Export:
  - Use backend KML column directly when available.
  - Do not rebuild KML from display-only `WGS84` column.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/RC2/`
  - Known samples:
    - `D:/about-west-africa-business/test-materials/刚果，两个坐标在同一张图.jpg`
    - `D:/about-west-africa-business/test-materials/微信图片_20260503091216_182_19.jpg`
  - Expected rows: 11.
  - Expected first KML: `16.0320,3.7638,0`.
  - Must preserve F/G duplicate and second group B.
- Stable Since commit:
  - `6d0b30b` preserved WGS84 table duplicate boundary points.
  - `d478523` rescued WGS84 table extraction after vision timeout.
- Status: Frozen
- Known Issues:
  - Frontend must not reparse this as Chat or use the human-readable WGS84
    column for export.

---

## wgs84-chat-coordinates

- Type ID: `wgs84-chat-coordinates`
- Legacy aliases: `wgs84_chat_coordinates`, `decimal_latlon`, chat coordinates.
- Intent:
  - Unstructured user text or chat paste with decimal WGS84 coordinates.
  - Examples: `12.319572, -11.178174`, `A 12.319572, -11.178174`.
  - Intent must be Unknown; no structured coordinate type may be detected.
- Dedicated Vision:
  - None by default.
  - Text normalization only.
- Dedicated Parser:
  - Chat decimal coordinate parser.
  - Default input order is `lat,lon` when no table header says otherwise.
- Quality Gate:
  - Latitude must be within `[-90, 90]`.
  - Longitude must be within `[-180, 180]`.
  - Rejects UTM-scale numbers and structured table contexts.
  - Emits swapped-coordinate warning when needed, but does not auto-project.
- Export:
  - Automatic geometry inference:
    - 1 point -> Point
    - 2 points -> LineString
    - 3+ points -> Polygon with closure
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/CHAT/`
  - Text sample: `12.319572, -11.178174`
  - Expected KML: `-11.178174,12.319572,0`
- Stable Since commit:
  - `f67181c` added WGS84 Chat parser v1.
  - Later parser-priority fixes restrict it to fallback-only behavior.
- Status: Frozen
- Known Issues:
  - Fallback only. Must never process structured coordinate documents.

---

## cote-divoire-geographic-dms-table

- Type ID: `cote-divoire-geographic-dms-table`
- V2 coordinate_type: `cote_divoire_geographic_dms_table`
- Intent:
  - Côte d'Ivoire / Cote d'Ivoire / 科特迪瓦 French geographic DMS tables.
  - Keywords: `POINTS`, `Latitude N`, `LATITUDE NORD`, `longitude W`,
    `LONGITUDE OUEST`, `Superficies (ha)`, `Superficie`, `hectares`.
  - Supports one image with one mining area and one image with multiple
    company + mining-area groups.
- Dedicated Vision:
  - V2 shadow/normalization path only at this stage.
  - Preserve group titles such as `CONNEXION RESSOURCES` + `矿区1`.
- Dedicated Parser:
  - `normalizeCoteDIvoireGeographicDmsTable()`.
  - Parses degree-minute-second rows with symbols or spaces, including
    `05°35'08,00"N` and `6 45 20N`.
- Quality Gate:
  - `N` / `Nord` is positive latitude.
  - `W` / `Ouest` is negative longitude.
  - Do not swap latitude and longitude.
  - Calculate area per group and compare with declared `Superficie(s)` when
    available. Area error above 2% requires review.
  - Self-intersecting polygons require review and are not KML-ready.
  - Do not auto-reorder points to hide a self-intersection.
- Export:
  - V2 groups expose `kml_ready` and optional per-group KML preview only.
  - Existing frontend KML download remains unchanged in Phase 2.
  - KML coordinate order is `longitude,latitude,0`.
- Regression Sample:
  - `科特迪瓦01.png`, `科特迪瓦02.png`, `科特迪瓦03.png`,
    `科特迪瓦04.png`, `科特迪瓦4个矿区坐标.jpg`.
  - `科特迪瓦4个矿区坐标.jpg` must produce four groups; the original
    `SION RESSOURCE_矿区2` point order is self-intersecting and must return
    `requires_review=true`, `kml_ready=false`.
- Stable Since commit:
  - Pending.
- Status: Beta
- Known Issues:
  - This is a V2 type. Do not extend V1 fallback paths for this type.
  - Must not be captured by WGS84 Chat, decimal, local OCR, BFTM, Madagascar,
    Mozambique, Kyrgyz GK, or MGRS fallback paths once V2 detects it.

---

## dms-coordinates

- Type ID: `dms-coordinates`
- Legacy aliases: `handwritten_dms`, `standard_dms_table`, ordinary DMS.
- Intent:
  - Ordinary DMS point/line/polygon text or image.
  - Keywords and symbols: degrees/minutes/seconds, `N`, `S`, `E`, `W`,
    `O`, `Ouest`, `Nord`.
- Dedicated Vision:
  - Ordinary DMS extraction.
  - Preserve original DMS format where possible.
- Dedicated Parser:
  - DMS parser.
  - Supports label stripping before matching:
    - `1.`
    - `2)`
    - `3:`
    - `A.`
    - `Point 1:`
  - Supports quote tolerance:
    - `11°52"11.93"N`
    - `11°52'11.93"N`
    - `11°52′11.93″N`
    - `11°52 11.93 N`
- Quality Gate:
  - DMS values must be within valid range.
  - Direction must be applied correctly.
  - Must not flatten grouped DMS documents.
- Export:
  - Convert DMS to WGS84 decimal KML.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/DMS/`
  - Known text sample: `13°01'21.53"N 10°13'26.62"W`
  - Expected KML: `-10.22406111111111,13.022647222222224,0`
- Stable Since commit:
  - DMS path predates current freeze.
  - Label stripping and quote tolerance stabilized during DMS_GROUPED work.
- Status: Frozen
- Known Issues:
  - Needs formal image fixture for ordinary DMS single-point regression.

---

## dms-grouped-coordinates

- Type ID: `dms-grouped-coordinates`
- Legacy aliases: DMS_GROUPED, Mining Area grouped DMS.
- Intent:
  - Multiple DMS polygons in one image or OCR text.
  - Keywords: `Mining Area`, `Mining Area Two`, `The coordinates are as follows`.
  - Also supports headerless grouping when numbered DMS rows restart or blank
    lines separate DMS blocks.
- Dedicated Vision:
  - DMS_GROUPED visual retry.
  - Must preserve original `N,W` order and grouped sections.
- Dedicated Parser:
  - Grouped DMS parser.
  - Must remove line labels before DMS matching.
  - Must preserve groups.
- Quality Gate:
  - Requires group-level valid DMS coordinates.
  - Headerless grouping must not split a single ordinary DMS polygon.
  - Must not enter WGS84 Chat.
- Export:
  - MultiPolygon / multiple Placemark Polygons.
  - Frontend must not flatten to one 8-point polygon.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/DMS_GROUPED/`
  - Sample: `D:/about-west-africa-business/test-materials/两块矿地.jpg`
  - Expected: 2 groups x 4 points.
- Stable Since commit:
  - `a43ef7a` preserved grouped DMS parsing.
  - `5d988f3` normalized grouped DMS parsing.
  - `c21e6f6` supported headerless grouped DMS detection.
  - `c3356aa` preserved grouped DMS state in frontend export.
  - `396fc1f` retried grouped DMS vision extraction.
- Status: Frozen
- Known Issues:
  - Frontend must not show `local-ocr-dms-fallback` after grouped backend
    acceptance.

---

## mgrs-utm-grid-reference

- Type ID: `mgrs-utm-grid-reference`
- Legacy aliases: `mgrs_utm_grid_reference`, `mgrs`.
- Intent:
  - MGRS / UTM Grid Reference coordinates.
  - Examples:
    - `47RLH 24469 42832`
    - `47R LH 24469 42832`
    - `47RLH2446942832`
- Dedicated Vision:
  - MGRS visual retry.
- Dedicated Parser:
  - MGRS parser.
  - Validates:
    - zone 1-60
    - latitude band C-X excluding I/O
    - 100km grid square excluding I/O
    - easting/northing 1-5 digits and equal precision
    - compact digits even length
- Quality Gate:
  - Converted latitude must match latitude band range.
  - Invalid MGRS must not fall into ordinary numeric or Chat parser.
- Export:
  - Convert MGRS -> UTM -> WGS84.
  - Point / LineString / Polygon supported.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/MGRS/`
  - Known sample: `缅甸坐标.jpg` path pending formal record.
  - Expected C point: `47RLH 24123 42905`.
  - Expected A-G count: 7.
- Stable Since commit:
  - `1ec741e` added MGRS UTM grid reference coordinate parsing.
- Status: Frozen
- Known Issues:
  - One visual run previously missed F point, but parser passed when rawText
    contained F. This is a vision intermittency risk, not a parser defect.

---

## kyrgyz-gk-point-x-y

- Type ID: `kyrgyz-gk-point-x-y`
- Legacy aliases: `kyrgyzstan_gk`, Kyrgyz GK.
- Intent:
  - Kyrgyzstan / Soviet Gauss-Kruger mining coordinate table.
  - Russian/Kyrgyz table with point number, X, Y.
  - Numeric pattern: easting like `13261341`, northing like `4607777`.
- Dedicated Vision:
  - Kyrgyz GK visual prompt.
  - Fallback table-row reconstruction only when dual-column table structure is
    preserved.
- Dedicated Parser:
  - Kyrgyz GK point/X/Y parser.
  - Keeps full easting including zone prefix.
  - Does not swap X/Y.
- Quality Gate:
  - Point numbers must be continuous.
  - Reject abnormal labels such as `513` / `520`.
  - EPSG:28413 conversion must land in Kyrgyzstan bounds.
- Export:
  - EPSG:28413 -> WGS84.
  - Polygon auto-closed.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/Kyrgyz_GK/`
  - Known sample: `吉尔吉斯斯坦矿地坐标.png` path pending formal record.
  - Expected points: 1-65.
- Stable Since commit:
  - Kyrgyz GK fallback recovery and row-order repair were stabilized before the
    Coordinate Engine freeze.
- Status: Frozen
- Known Issues:
  - Needs formal sample path and expected full KML snapshot.

---

## madagascar-cadastral-grid

- Type ID: `madagascar-cadastral-grid`
- Legacy aliases: `madagascar_cadastral_grid`, cadastral grid.
- Intent:
  - Madagascar cadastral grid image/table.
  - Keywords: `Liste_Carres`, `Liste_Carrés`, `num`, `XV`, `YV`, cadastral
    grid table.
- Dedicated Vision:
  - Madagascar cadastral table extraction.
  - Must focus on the table and ignore central map DMS labels.
- Dedicated Parser:
  - `num | XV | YV` parser.
  - XV/YV are grid cell center values.
- Quality Gate:
  - Expected stable sample has 32 rows.
  - EPSG:29702 conversion must land in Madagascar.
  - Reject direct WGS84 interpretation of XV/YV.
- Export:
  - EPSG:29702 -> WGS84.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/Madagascar/`
  - Current status: stable original sample missing.
- Stable Since commit:
  - Stable path predates current freeze.
- Status: Pending
- Known Issues:
  - Marked Pending stable sample.
  - Current available sample does not reliably read the right-side table.
  - Do not modify parser until a stable original sample is recovered.

---

## mozambique-geographic-table

- Type ID: `mozambique-geographic-table`
- Legacy aliases: `mozambique_geographic_table`, Portuguese geographic DMS table.
- Intent:
  - Mozambique Portuguese geographic DMS coordinate table.
  - Keywords: `COORDENADAS GEOGRAFICAS`, `COORDENADAS GEOGRÁFICAS`,
    `Datum: Tete`, `Latitude`, `Longitude`, `Ordem`, `Order`, `INAMI`,
    `MIREME`, `Provincia`, `Província`.
- Dedicated Vision:
  - Mozambique-specific table reading.
  - Reads:
    - `Order`
    - Latitude degree/minute/second
    - Longitude degree/minute/second
  - Must not invent rows outside the visible table.
- Dedicated Parser:
  - Mozambique geographic DMS table parser.
  - `LatDeg` negative means south latitude.
  - Longitude in Mozambique / Tete defaults east and positive.
  - Decimal comma seconds are supported.
- Quality Gate:
  - Known sample must output exactly 22 rows, order 1-22.
  - Reject non-22 rows, rows=0, massive duplicate rows, and out-of-Mozambique
    coordinates.
  - If unstable, return no KML and do not fall through to Chat.
- Export:
  - WGS84 table KML.
  - KML order must be `longitude,latitude,0`.
  - Frontend must use backend accepted `data.coordinates`.
- Regression Sample:
  - `regression-samples/Mozambique/`
  - Known sample: `莫桑比克矿地.jpg` path pending formal record.
  - Expected:
    - row 1: `32.955556,-14.600000,0`
    - row 2: `33.100000,-14.600000,0`
    - row 3: `33.100000,-14.655556,0`
    - row 22: `32.955556,-14.633333,0`
- Stable Since commit:
  - Not frozen.
  - `c30c600` improved retry quality gate, but 20-run stability was not
    achieved.
- Status: Unstable
- Known Issues:
  - Needs V1.5 Intent Router or deterministic table reading.
  - Current visual transcription may return rows=0 intermittently.
  - Chat takeover and 36-row false acceptance must remain blocked.

---

## french-perimeter-dms-prose

- Type ID: `french-perimeter-dms-prose`
- Legacy aliases: French perimeter DMS.
- Intent:
  - French prose-style perimeter boundary descriptions.
  - Keywords: `Coordonnees du perimetre`, `Coordonnées du périmètre`,
    `meridien`, `méridien`, `parallele`, `parallèle`, `Ouest`, `Nord`,
    `Point A`, `Point B`.
  - Prose description, not A-Z tabular Point/Nord/Est table.
- Dedicated Vision:
  - French perimeter DMS prose retry.
- Dedicated Parser:
  - French perimeter prose parser.
  - Converts Ouest to negative longitude and Nord to positive latitude.
- Quality Gate:
  - Requires coherent Point A-D style perimeter.
  - Must reject Point A-Z table intent.
- Export:
  - Polygon KML from backend formatted coordinates.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/FRENCH_PERIMETER_DMS/`
  - Sample: `D:/about-west-africa-business/test-materials/模糊坐标.jpg`
  - Expected:
    - `-8.833333333333334,12.066666666666666,0`
    - `-8.75,12.066666666666666,0`
    - `-8.75,12.036666666666667,0`
    - `-8.833333333333334,12.036666666666667,0`
- Stable Since commit:
  - `42497ec` added French perimeter DMS prose parser.
  - `7e7f92f` exported French perimeter DMS as polygon KML.
  - `53faa63` used formatted coordinates for French DMS frontend.
- Status: Frozen
- Known Issues:
  - Must not capture `point-az-dms-table`.

---

## point-az-dms-table

- Type ID: `point-az-dms-table`
- Legacy aliases: Point A-Z DMS Table.
- Intent:
  - Structured table with Point A-Z and columns such as `Nord` and `Est` /
    `Ouest`.
  - Continuous Point A, Point B, Point C... labels.
  - Table form has priority over French prose DMS.
- Dedicated Vision:
  - Point A-Z DMS table prompt.
- Dedicated Parser:
  - Point A-Z DMS table parser.
  - Preserves A-Z order.
- Quality Gate:
  - Known stable sample expects 26 points A-Z.
  - Reject if French prose parser captured the sample.
- Export:
  - Polygon KML.
  - KML order must be `longitude,latitude,0`.
- Regression Sample:
  - `regression-samples/POINT_AZ_DMS_TABLE/`
  - Sample:
    `D:/about-west-africa-business/test-materials/微信图片_20260427122118_114_19.jpg`
  - Expected first KML:
    `-8.266666666666667,10.870833333333334,0`
  - Expected last KML:
    `-8.254444444444445,10.870833333333334,0`
- Stable Since commit:
  - `d1787a5` added Point AZ DMS table parser.
  - `1780aea` prevented French DMS parser from capturing Point AZ tables.
- Status: Frozen
- Known Issues:
  - Requires French parser exclusion to remain active.

---

## Legacy Type Mapping

These older names remain documented for historical compatibility. They should
map to the canonical Type IDs above.

| Legacy name | Canonical Type ID |
|---|---|
| `handwritten_dms` | `dms-coordinates` |
| `standard_dms_table` | `dms-coordinates` |
| `decimal_latlon` | `wgs84-chat-coordinates` when Intent=Unknown |
| `wgs84_chat_coordinates` | `wgs84-chat-coordinates` |
| `bftm_xy` | `bftm-projected-x-y` |
| `mgrs_utm_grid_reference` | `mgrs-utm-grid-reference` |
| `mgrs` | `mgrs-utm-grid-reference` |
| `kyrgyzstan_gk` | `kyrgyz-gk-point-x-y` |
| `madagascar_cadastral_grid` | `madagascar-cadastral-grid` |
| `mozambique_geographic_table` | `mozambique-geographic-table` |

## Registry Maintenance Checklist

Before changing this registry:

- Confirm whether the change is a new type, alias, status change, or known
  issue update.
- Confirm the matching regression sample directory exists.
- Confirm Stable Paths and Architecture documents do not conflict.
- Do not mark a type `Frozen` unless regression and stability evidence exists.
- Do not remove legacy aliases without a migration note.
