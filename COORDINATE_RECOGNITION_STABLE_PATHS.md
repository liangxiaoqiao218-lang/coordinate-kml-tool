# 坐标识别稳定方案清单

本文档记录 GeoKit Lab 当前已验证稳定的坐标识别主路径。后续任何坐标识别修改，都必须先判断坐标类型，只改对应类型，不允许整体重构或让不同路径互相覆盖。

## 1. 手写 DMS

稳定方案：

- `recognizedLines` 优先。
- `recognizedLines` 来自 `rawText`。
- 使用 `groupEveryFourLinesWhenLikely()`。
- 手写 DMS 每 4 行自动分组。
- 工作区优先显示原图 DMS 格式。
- KML 内部再转十进制度。

维护规则：

- 不要再整体改 `chooseDisplayCoordinateResult`。
- 不要新增显示重建层覆盖原始 DMS 显示。

## 2. 标准 DMS 表

稳定方案：

- 视觉模型优先。
- 按表格结构读取。
- 支持 `Latitude nord` / `Longitude ouest`。
- DMS 原格式显示。
- `W` / `O` / `Ouest` 表示负经度。

维护规则：

- 不要强行每 4 行分组。
- 不要让 OCR bbox 优先于视觉表格结构。

## 3. BFTM / X-Y

稳定方案：

- 视觉模型读取表格版面。
- 不走 OCR bbox 主路径。
- 输出 `SOMMETS | X | Y` 对应行数据。
- `X,Y` 保持平面坐标。
- 拦截 `X,X`、`Y,Y`、bbox 污染。

维护规则：

- 不要让 `extractDecimalCoordinateLines` 优先覆盖 BFTM。
- 不要把 OCR bbox 当作主结果。

## 4. Madagascar Cadastral Grid

稳定方案：

- 检测关键词：
  - `Liste_Carrés`
  - `cadastral grid`
  - `grille cadastrale`
  - `num | XV | YV`
- 优先识别右侧表格区域。
- 忽略地图中央的大号 DMS 标注。
- 只提取 `num | XV | YV`。
- 根据唯一 `XV` / `YV` 推断 `dx` / `dy`。
- `XV/YV` 默认按单格中心点处理。
- KML 生成时按中心点生成单格四角，再用 EPSG:29702 转 WGS84。
- KML 最终写入 `longitude,latitude,0`。

风险说明：

- `XV/YV` 当前默认按中心点解释。
- EPSG:29702 转换结果需结合原图人工核对。
- 不自动识别其它国家或其它矿籍系统的投影参数。

## 5. Kyrgyzstan Gauss-Kruger

适用场景：

- 吉尔吉斯斯坦 / 苏联高斯克吕格矿权坐标表。
- 俄文矿权坐标表，常见标题包括：
  - `Координаты угловых точек`
  - `лицензионной площади`
  - `прямоугольной системе координат`
  - `№ точек`
- 表格结构为 `№ points | X | Y` 或 `point | X | Y`。

稳定方案：

1. 识别并输出 `point | X | Y`。
2. 必须保留点号，不允许只输出 `X,Y`。
3. 按 `point` 数字升序排序，避免左右分栏或下方补充表造成 polygon 顺序混乱。
4. `X` 使用完整 easting，例如 `13261341`。
5. `Y` 使用 northing，例如 `4607777`。
6. 不交换 `X/Y`。
7. 不去掉 `13` 区号前缀。
8. 使用 EPSG:28413（Pulkovo 1942 / Gauss-Kruger zone 13）转换为 WGS84。
9. KML 最终写入 `longitude,latitude,0`。
10. polygon 按 point 升序生成，并自动闭合回第一个点。

KML 逻辑：

- 识别阶段只负责稳定输出 `point | X | Y`。
- KML 导出阶段才执行 EPSG:28413 -> WGS84。
- 转换失败时不生成 KML，也不扣次数。
- 经纬度结果必须提示用户结合原图人工核对。

禁止改动点：

- 不允许把 Kyrgyzstan GK 的 `X/Y` 当普通经纬度。
- 不允许把点号丢弃。
- 不允许按视觉读取顺序直接生成 polygon。
- 不允许交换 `X/Y`。
- 不允许去掉 `13` 区号前缀。
- 不允许让普通 projected X/Y 路径覆盖 `point | X | Y`。

回归测试样本：

- `吉尔吉斯斯坦矿地坐标.png`
- 应识别 `1-65` 点。
- 前几行应包含：
  - `1 | 13261341 | 4607777`
  - `2 | 13261396 | 4607769`
  - `3 | 13261377 | 4607682`
- 最后几行应包含：
  - `63 | 13261142 | 4607568`
  - `64 | 13261253 | 4607628`
  - `65 | 13261317 | 4607721`
- 转换后应落在 Kyrgyzstan 范围内，大致 `72E`、`41N` 附近。

## 6. 普通小数经纬度

稳定方案：

- 普通 decimal coordinate 走普通 polygon 路径。
- 不进入矿权网格模式。
- 不覆盖 BFTM / X-Y 或 Madagascar cadastral grid 的表格路径。

## 7. WGS84 Chat Coordinates

稳定方案：

- 适用于聊天、复制文本、图片 OCR 中的 WGS84 小数经纬度列表。
- 默认按 `lat, lon` 解释。
- 支持纯坐标行、A/B/C 标签、换行、逗号和空格混合格式。
- KML 必须写为 `longitude,latitude,0`。
- 按点数自动推断 geometry：1 点为 Point，2 点为 LineString，3 点及以上为 Polygon，Polygon 自动闭合。
- 优先级低于 DMS / MGRS / UTM / BFTM / Madagascar / Kyrgyzstan GK，且高于普通文本兜底。
- 不做 UTM 猜测、不做 MGRS 解析、不做投影转换。
- 如果存在经纬度反转风险，只输出 `possible swapped lat/lon` warning，不阻断结果。

## 总维护原则

1. 不再整体重构识别系统。
2. 后续修复必须先判断坐标类型，只改对应类型，不影响已通过主路径。
3. 表格类优先视觉模型。
4. OCR 只作为 fallback。
5. 后端规则负责校验、分组、提取、bbox / `X,X` / `Y,Y` 拦截。
6. 不允许普通 decimal 覆盖 BFTM。
7. 不允许 DMS 覆盖 cadastral grid。
8. 不允许新的显示层覆盖 `recognizedLines`。
9. 不允许把 `num | XV | YV` 当普通 polygon 点直接写入 KML。
10. 不允许在识别阶段把 Madagascar cadastral grid 强行转成经纬度；转换只发生在 KML 导出阶段。
11. 不允许把 Kyrgyzstan GK 的 `point | X | Y` 降级为普通 `X,Y`。
12. 不允许在 Kyrgyzstan GK 中交换 `X/Y` 或去掉 `13` 区号前缀。
13. 所有新增坐标类型一旦真实测试成功，必须写入本稳定方案文档，记录适用场景、识别关键词、输出格式、转换坐标系、KML 逻辑、禁止改动点和回归测试样本。

## Coordinate Parser Priority Freeze

The coordinate parser priority is frozen in this order:

1. DMS_GROUPED / Mining Area grouped DMS
2. Point A-Z DMS table
3. DMS
4. BFTM / X-Y long tables
5. MGRS / UTM Grid Reference
6. Kyrgyzstan GK
7. Madagascar cadastral
8. Mozambique Geographic Table
9. WGS84 table coordinates with longitude/latitude headers
10. WGS84 Chat Coordinates
11. fallback

Maintenance rules:

- New parsers must not be inserted before existing stable parsers unless explicit regression tests prove no existing path is affected.
- WGS84 Chat Coordinates must remain a low-priority fallback parser.
- The Chat parser must not capture BFTM, MGRS, DMS, longitude/latitude table coordinates, or Mozambique Geographic Table input.
- Every new parser must write a clear `parserTrace` entry.
- Before commit, run the coordinate parser conflict test set, including BFTM long table, longitude/latitude table, plain chat coordinates, DMS single point, and MGRS.

## DMS_GROUPED / Mining Area grouped DMS

Applies when an image or OCR text contains Mining Area / Mining Area Two / The coordinates are as follows / N W / degree-minute-second symbols, and the source contains multiple DMS coordinate groups.

Priority:

- DMS_GROUPED must run before WGS84 Chat Coordinates.

Headerless grouping:

- If OCR drops `Mining Area` titles, DMS_GROUPED must still detect grouped DMS when row numbers restart, such as `1,2,3,4,1,2,3,4`.
- If OCR drops titles but preserves a blank line between DMS blocks, split groups at the blank line.
- Repeated boundary points may be used only as a low-priority helper; number restart and blank-line grouping are preferred.
- Accepted headerless grouped DMS traces should identify the reason, such as `OCR -> DMS_GROUPED(number_restart):accepted` or `OCR -> DMS_GROUPED(blank_line):accepted`.

Forbidden behavior:

- Do not convert DMS_GROUPED coordinates to decimal first and then hand them to the WGS84 Chat parser.
- Do not flatten multiple Mining Area sections into one Polygon.

Output rules:

- Each Mining Area must generate its own Polygon.
- Preserve grouping such as Mining Area 1 / Mining Area 2.
- KML coordinates must be written as `lon,lat,0`.

parserTrace:

- Success must show `OCR -> DMS_GROUPED:accepted`.
- `WGS84_CHAT:accepted` must not appear for this path.

### DMS_LABEL_STRIP

- DMS parsers must clean leading row labels before matching DMS tokens.
- Supported labels include `1.`, `2)`, `3:`, `3：`, `A.`, and `Point 1:`.
- Only the leading label may be removed; digits inside the coordinate body must never be changed or deleted.

### DMS_QUOTE_TOLERANCE

- DMS parsing must tolerate OCR quote variants including:
  - `11°52"11.93"N`
  - `11°52'11.93"N`
  - `11°52′11.93″N`
  - `11°52 11.93 N`
- When the separator after minutes is mistakenly recognized as `"`, treat it as the minute separator `'` if a decimal seconds value and a direction follow.

### DMS_GROUPED_OUTPUT_LOCK

- Backend accepted mode must be `precisionMode=dms-grouped-coordinates`.
- Frontend must not re-run fallback extraction over accepted grouped DMS output.
- Frontend must not overwrite, reorder, or flatten grouped coordinates.
- Multiple Mining Area groups must remain separate polygons, not one flattened Polygon.

## French Perimeter DMS Prose

Type id:

- `french_perimeter_dms_prose`

Applicable scene:

- French prose-style perimeter descriptions, especially permit text headed by `Coordonnees du perimetre` / `Coordonnees du périmètre`.
- Boundary text describing intersections of `meridien` / `méridien` and `parallele` / `parallèle`.
- Point labels such as `Point A`, `Point B`, `Point C`, `Point D`.
- Direction words `Ouest` and `Nord`.
- Degree/minute/second wording or symbols in prose, such as `8°50'00" Ouest` and `12°04'00" Nord`.

Parser priority:

- Runs after `DMS_GROUPED`.
- Runs before ordinary `DMS` and before `WGS84 Chat Coordinates`.

Recognition and conversion rules:

- Extract one point per `Point X` block.
- Read the longitude from the `Ouest` / west DMS value and make it negative.
- Read the latitude from the `Nord` / north DMS value and make it positive.
- KML output must be `longitude,latitude,0`.
- Geometry is inferred from point count: 1 point = Point, 2 points = LineString, 3+ points = Polygon with automatic closure.

Vision retry:

- If generic OCR returns no useful coordinates for a French perimeter image, use the parser-specific `FRENCH_PERIMETER_DMS Retry`.
- Do not broaden the generic OCR prompt for this type.
- Retry output must preserve `Point X | longitude Ouest | latitude Nord`, not decimal coordinates.

Forbidden behavior:

- Do not let `WGS84 Chat Coordinates` capture French perimeter prose.
- Do not parse unrelated payment amounts, article numbers, dates, or document text as coordinates.
- Do not change BFTM, MGRS, DMS_GROUPED, WGS84_TABLE, Madagascar, Kyrgyz GK, or Mozambique parser behavior for this type.

Parser trace:

- Success must include `OCR -> FRENCH_PERIMETER_DMS:accepted`.
- Vision retry success should include `OCR -> FRENCH_PERIMETER_DMS:retry_vision -> FRENCH_PERIMETER_DMS:accepted`.

Regression requirement:

- The failing sample `模糊坐标.jpg` is tracked under `regression-samples/FRENCH_PERIMETER_DMS/`.
- Expected precision mode: `french-perimeter-dms-prose`.
- Expected key KML points:
  - `Point A`: `-8.833333333333334,12.066666666666666,0`
  - `Point B`: `-8.75,12.066666666666666,0`
  - `Point C`: `-8.75,12.036666666666667,0`
  - `Point D`: `-8.833333333333334,12.036666666666667,0`

## Coordinate Engine V1 Stable

Baseline commit:

- `66dc438`

Coordinate Engine V1 is the frozen baseline for production coordinate recognition. This section is the operating policy for all future coordinate parser work.

### Parser Priority Chain

The parser priority chain is frozen in this exact order:

1. `DMS_GROUPED`
2. `french_perimeter_dms_prose`
3. `point-az-dms-table`
4. `DMS`
5. `BFTM / X-Y`
6. `MGRS`
7. `Kyrgyzstan GK`
8. `Madagascar cadastral`
9. `Mozambique Geographic Table`
10. `WGS84 Table` with longitude/latitude headers
11. `WGS84 Chat Coordinates`
12. `Fallback`

Freeze rules:

- Existing parser behavior must not be changed casually.
- New coordinate types must not modify or weaken any existing stable parser.
- New coordinate types must be added as independent parsers or independent vision retry paths.
- A new parser may not be inserted before an existing parser unless the full regression suite proves that no stable path is affected.
- `WGS84 Chat Coordinates` must remain a low-priority fallback for unstructured coordinate text only.

### Vision Retry Framework

Vision Retry is a permanent Coordinate Engine architecture layer.

Current stable retry paths:

- `DMS_GROUPED Retry`
- `FRENCH_PERIMETER_DMS Retry`
- `POINT_AZ_DMS_TABLE Retry`
- `WGS84_TABLE Retry`
- `MGRS Retry`

Maintenance rules:

- If a new parser needs better image understanding, add a parser-specific Vision Retry.
- Do not broaden the generic OCR prompt to fix a specific coordinate type.
- Vision Retry output must preserve the source coordinate structure expected by that parser.
- Vision Retry must write explicit `parserTrace` entries, such as `OCR -> MGRS:retry_vision -> MGRS:accepted`.

### Regression Policy

Regression samples are stored under:

```text
regression-samples/
├── BFTM/
├── RC2/
├── DMS_GROUPED/
├── FRENCH_PERIMETER_DMS/
├── DMS/
├── MGRS/
├── CHAT/
├── Kyrgyz_GK/
├── Madagascar/
└── Mozambique/
```

Each sample directory should preserve:

- Original image.
- OCR raw text.
- Expected `parserTrace`.
- Expected `precisionMode`.
- Expected geometry.
- `expected.kml` or key expected coordinates.
- Notes about which fallback must not capture the sample.

Before any coordinate parser commit, the regression suite must verify:

- BFTM long table remains `BFTM:accepted`.
- RC2 longitude/latitude table remains `WGS84_TABLE:accepted`.
- DMS grouped samples remain grouped and are not flattened.
- French perimeter DMS prose remains `FRENCH_PERIMETER_DMS:accepted` and is never captured by WGS84 Chat.
- Point A-Z DMS long tables remain `POINT_AZ_DMS_TABLE:accepted` and are not captured by ordinary DMS.
- DMS single point remains DMS.
- MGRS remains MGRS and is not captured by chat coordinates.
- Plain chat coordinates still enter `WGS84_CHAT:accepted`.
- Kyrgyzstan GK, Madagascar cadastral, and Mozambique Geographic Table keep their registered precision modes and KML behavior.

Any regression failure blocks the commit.

### Coordinate Engine Freeze Policy

For any future coordinate recognition change, all three items are mandatory:

1. Code change for the specific parser or retry path.
2. Matching regression sample or expected-result update.
3. Documentation update in this stable-path document and, when applicable, `COORDINATE_TYPE_REGISTRY.md`.

Real-world failure handling rule:

- Any real business recognition failure must be added to `regression-samples/` before code is changed.
- The fix must target only the corresponding parser or Vision Retry path.
- After the fix, the full Coordinate Engine regression suite must pass before commit.
- If any existing coordinate type regresses, the commit is blocked.

Git policy for coordinate recognition changes:

- Every coordinate recognition commit must include the parser or Vision Retry change, the matching regression sample, and the stable documentation update.
- These three items are mandatory; missing any one of them blocks the commit.

Forbidden changes:

- Do not let a fallback parser override a high-confidence parser.
- Do not let `WGS84 Chat Coordinates` capture structured tables.
- Do not flatten grouped polygon outputs.
- Do not change projection assumptions for registered projected systems without a dedicated regression proof.
- Do not change parser priority without documenting the reason and passing the full regression suite.

Coordinate Engine V1 is frozen and may now enter real sample accumulation.

## Point A-Z DMS Table

Type id:

- `point-az-dms-table`

Applicable scene:

- Point A-Z long boundary tables with `Point / Nord / Est` columns.
- French or English table headers such as `Point`, `Nord`, `Est`, `North`, or `East`.
- One point per row, normally preserving labels from `Point A` through `Point Z`.

Parser priority:

- The dedicated Point A-Z DMS table retry runs before ordinary DMS is allowed to finalize this long-table case.
- Ordinary DMS must not capture this type when the source is a clear Point / Nord / Est table.

Recognition and conversion rules:

- Use the parser-specific visual retry to reread the table row by row.
- Preserve A-Z row order.
- Keep every table row as one boundary point.
- Interpret the `Nord` column as latitude.
- Interpret the longitude column according to its direction marker; west values must remain negative.
- KML coordinates must be written as `longitude,latitude,0`.

Output contract:

- `precisionMode = point-az-dms-table`
- `parserTrace = OCR -> POINT_AZ_DMS_TABLE:accepted`

Regression requirement:

- Sample directory: `regression-samples/POINT_AZ_DMS_TABLE/`
- Expected point count: 26 points, A-Z.
- Expected first coordinate: `-8.266666666666667,10.870833333333334`
- Expected last coordinate: `-8.254444444444445,10.870833333333334`

Forbidden behavior:

- Do not let ordinary DMS finalize this long-table case before the Point A-Z retry has a chance to correct row alignment.
- Do not change WGS84 Table, WGS84 Chat, BFTM, MGRS, Kyrgyz GK, Madagascar, Mozambique, French perimeter DMS, or DMS_GROUPED behavior for this type.
