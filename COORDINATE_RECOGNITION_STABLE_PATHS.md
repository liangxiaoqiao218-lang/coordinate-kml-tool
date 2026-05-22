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
