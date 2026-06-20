# Coordinate Type Registry

本文件是 GeoKit Lab 坐标类型规则库，用于长期固化已经真实验证成功的坐标识别路径。

## 维护原则

1. 后续任何新增坐标类型，真实测试成功后，必须加入本规则库。
2. 修某一类型时，只允许改该类型分支。
3. 不允许为了修新类型，破坏已通过类型。
4. 每次修改坐标识别，必须至少检查相关稳定路径是否受影响。
5. 表格类坐标优先视觉模型，不优先 OCR bbox。
6. fallback 只能兜底，不能覆盖高可信主识别结果。
7. fallback 残缺结果不能直接生成 KML。

---

## handwritten_dms

1. 类型 ID：`handwritten_dms`
2. 适用场景：手写或截图形式的 DMS 坐标。
3. 典型样本文件名：手写 DMS 坐标图、野外手写坐标截图。
4. 触发关键词 / 版面特征：DMS 符号、N/S/E/W/O 方向、多行手写坐标、每 4 行构成一个点。
5. 识别主流程：`recognizedLines` 优先；`recognizedLines` 来自 `rawText`；`groupEveryFourLinesWhenLikely()` 每 4 行自动分组；工作区优先显示原图 DMS 格式；KML 内部再转十进制度。
6. fallback 逻辑：OCR 仅作为 fallback，不得覆盖高可信 `recognizedLines`。
7. 坐标转换规则：显示层保留 DMS，KML 生成时内部转换为十进制度。
8. KML 生成规则：由 DMS 解析结果生成普通 WGS84 KML。
9. 失败保护规则：无法形成有效点时不生成 KML；不允许用残缺 OCR 行硬拼 polygon。
10. 禁止修改项：不允许新显示层覆盖 `recognizedLines`；不允许把手写 DMS 重建为不同显示格式后再写入工作区。
11. 回归测试要求：工作区仍显示原始 DMS 风格；每 4 行分组正常；KML 输出 WGS84 经纬度。

---

## standard_dms_table

1. 类型 ID：`standard_dms_table`
2. 适用场景：标准 DMS 表格，尤其包含法语字段的坐标表。
3. 典型样本文件名：标准 DMS 表、Latitude nord / Longitude ouest 坐标表。
4. 触发关键词 / 版面特征：`Latitude nord`、`Longitude ouest`、`W` / `O` / `Ouest`、表格行列结构。
5. 识别主流程：视觉模型优先读取表格；保留表格行关系；显示原始 DMS；`W` / `O` / `Ouest` = 负经度。
6. fallback 逻辑：OCR 只作为低置信兜底，不得破坏表格行关系。
7. 坐标转换规则：显示层保留 DMS，KML 内部将西经转换为负经度。
8. KML 生成规则：按表格行顺序生成普通 WGS84 KML。
9. 失败保护规则：表格行关系不完整时，不强行生成 polygon。
10. 禁止修改项：不允许按每 4 行硬拆标准表格；不允许 OCR bbox 优先于视觉表格识别。
11. 回归测试要求：`Latitude nord / Longitude ouest` 表格仍显示原始 DMS；`O / Ouest` 仍按西经处理。

---

## bftm_xy

1. 类型 ID：`bftm_xy`
2. 适用场景：Burkina Faso BFTM / X-Y 平面坐标表。
3. 典型样本文件名：BFTM 坐标表、SOMMETS X Y 表。
4. 触发关键词 / 版面特征：`BFTM`、`SOMMETS`、`X`、`Y`、`ITRF 2008`、`Projection BFTM`。
5. 识别主流程：视觉模型优先读取 `SOMMETS | X | Y`；保持 X/Y 的表格行关系；保留平面坐标文本。
6. fallback 逻辑：OCR 只作为 fallback，且必须通过 X/Y 合理性校验。
7. 坐标转换规则：当前显示层保留 X/Y 平面坐标，不允许当作普通经纬度。
8. KML 生成规则：未明确转换坐标系前，不直接写入 Google Earth 经纬度 KML。
9. 失败保护规则：拦截 `X,X` / `Y,Y` / bbox 污染；行关系异常时不生成 KML。
10. 禁止修改项：不允许普通 decimal 分支抢先覆盖；不允许 OCR bbox 当主结果。
11. 回归测试要求：`SOMMETS | X | Y` 行关系保持；`X,X` / `Y,Y` 污染仍被拦截。

---

## madagascar_cadastral_grid

1. 类型 ID：`madagascar_cadastral_grid`
2. 适用场景：Madagascar 矿权网格 / Liste_Carrés / cadastral grid 图。
3. 典型样本文件名：`马达加斯加坐标.png`
4. 触发关键词 / 版面特征：`Liste_Carrés`、`cadastral grid`、`grille cadastrale`、`carreau`、`num`、`XV`、`YV`、右侧矿权网格表。
5. 识别主流程：优先识别 `Liste_Carrés` 表格区域；忽略地图中央大号 DMS 标注；输出 `num | XV | YV`；推断 dx/dy；XV/YV 按单格中心点处理。
6. fallback 逻辑：fallback 不能把中央 DMS 覆盖为主结果，不能把 XV/YV 当普通 polygon 点。
7. 坐标转换规则：EPSG:29702 转 WGS84；XV/YV 为中心点，单格四角为中心点 +/- dx/2、dy/2。
8. KML 生成规则：每个 num 生成一个 polygon；KML 输出 `longitude,latitude,0`。
9. 失败保护规则：投影转换失败时不生成 KML；转换结果必须落在 Madagascar 合理范围内。
10. 禁止修改项：不允许把 XV/YV 直接当 WGS84；不允许改成左下角假设；不允许 DMS 覆盖 cadastral grid。
11. 回归测试要求：`num | XV | YV` 保持；Grid 280 四角按中心点模式生成；KML 经纬度落在 Madagascar 图示区域附近。

---

## kyrgyzstan_gk

1. 类型 ID：`kyrgyzstan_gk`
2. 适用场景：Kyrgyzstan / Soviet Gauss-Kruger 俄文矿权角点表。
3. 典型样本文件名：`吉尔吉斯斯坦矿地坐标.png`
4. 触发关键词 / 版面特征：`№ точек`、`Координаты угловых точек`、`лицензионной площади`、`прямоугольной системе координат`、`Kyrgyzstan`、`Киргиз`、`Кыргыз`、俄文双列表格、`13xxxxxx` + `46xxxxx` 坐标组合。
5. 识别主流程：命中 Kyrgyz GK 预判后直接走专用视觉 prompt；输出 `point | X | Y`；保留点号；按 point 升序排序；X 使用完整 easting，例如 `13261341`；Y 使用 northing，例如 `4607777`；不交换 X/Y；不去掉 13 区号。
6. fallback 逻辑：主视觉超时时，先 visual retry；fallback OCR 只有在保留左右双列表格结构时，才允许按行序恢复点号；左列恢复 `1-33`；右列恢复 `34-65`；常见 OCR 错误 `607447` 可修复为 `4607447`；恢复后必须通过连续性检查；恢复后必须落在 Kyrgyzstan 范围。
7. 坐标转换规则：EPSG:28413 转 WGS84；X 是完整 easting，保留 13 区号；Y 是 northing。
8. KML 生成规则：按 point 升序生成 polygon；polygon 自动闭合；KML 输出 `longitude,latitude,0`。
9. 失败保护规则：fallback 残缺点号不能直接生成 KML；点号不连续不能生成 KML；异常大点号不能生成 KML；转换后不在 Kyrgyzstan 范围内不能生成 KML。
10. 禁止修改项：不允许 fallback 残缺点号直接生成 KML；不允许 `513` / `520` 这类异常点号进入最终结果；不允许交换 X/Y；不允许去掉 13 区号。
11. 回归测试要求：原图 direct prompt 成功时识别 `1-65`；fallback OCR 保留双列表格时可恢复 `1-65`；`513` / `520` 不进入最终结果；点 60 可修复为 `13260521,4607447`；EPSG:28413 转换结果落在 Kyrgyzstan `72E-78E / 39N-43N` 范围。

---

## decimal_latlon

1. 类型 ID：`decimal_latlon`
2. 适用场景：普通小数经纬度 polygon、点、线。
3. 典型样本文件名：普通经纬度坐标文本或截图。
4. 触发关键词 / 版面特征：十进制度经纬度；数值范围符合 WGS84 经纬度；不包含特殊投影/网格表关键词。
5. 识别主流程：普通 polygon 路径；不进入矿权网格模式；不进入 GK / BFTM 特殊分支。
6. fallback 逻辑：OCR fallback 只用于提取普通经纬度文本，不允许覆盖更高优先级的特殊坐标类型。
7. 坐标转换规则：已是 WGS84 经纬度时不再投影转换。
8. KML 生成规则：按普通 KML polygon / line / point 逻辑输出。
9. 失败保护规则：坐标范围异常时不生成 KML；点数不足时不生成 polygon。
10. 禁止修改项：不允许进入 Madagascar cadastral grid；不允许进入 Kyrgyz GK；不允许进入 BFTM 特殊分支。
11. 回归测试要求：普通小数经纬度仍走原普通 KML 路径；特殊表格坐标不会被 decimal 分支抢先覆盖。


---

## wgs84_chat_coordinates

1. 类型 ID：`wgs84_chat_coordinates`
2. 适用场景：聊天记录、复制文本、图片 OCR 后得到的 WGS84 小数经纬度列表，例如 `12.319572, -11.178174`、`A 12.319572, -11.178174`。
3. 典型样本文件名：聊天坐标文本、复制粘贴坐标列表、OCR 小数经纬度结果。
4. 触发关键词 / 版面特征：每行或每段包含一组小数坐标；可带 A/B/C 标签；数值符合 WGS84 范围；不包含 DMS、MGRS、UTM、BFTM 或特殊投影关键词。
5. 识别主流程：按 `lat, lon` 解释；支持逗号、空格、换行、A/B/C 标签；输出 `label | WGS84 | KML`。
6. fallback 逻辑：备用 OCR 可识别该类型，但不得覆盖 MGRS、DMS、BFTM、Madagascar、Kyrgyzstan GK 等更高优先级稳定路径。
7. 坐标转换规则：不做投影转换；输入为 WGS84 decimal degrees；KML 写入时必须转换为 `longitude,latitude,0`。
8. KML 生成规则：每个点生成 Point；选择 LineString 时按输入顺序连线；选择 Polygon 且点数 >= 3 时按输入顺序成面并自动闭合。
9. 失败保护规则：纬度超出 `[-90,90]`、经度超出 `[-180,180]`、百万级 UTM 数字、明显特殊坐标格式均拒绝；如果存在经纬度反转风险，只返回 `possible swapped lat/lon` warning，不阻断生成。
10. 禁止修改项：不允许自动猜 UTM；不允许引入 MGRS 解析；不允许投影转换；不允许把 `lat,lon` 直接按 `lon,lat` 显示；不允许影响 MGRS / BFTM / UTM 数字坐标优先级。
11. 回归测试要求：三点样本 `12.319572,-11.178174`、`12.318957,-11.178055`、`12.318693,-11.177711` 应识别 3 点；Polygon KML 应按输入顺序闭合；KML 坐标必须为 `-11.178174,12.319572,0` 形式。

---

## mgrs_utm_grid_reference

1. 类型 ID：`mgrs_utm_grid_reference`
2. 适用场景：MGRS / UTM Grid Reference 坐标文本、截图或批量列表，例如 `47RLH 24469 42832`、`47R LH 24469 42832`、`47RLH2446942832`。
3. 典型样本文件名：MGRS 坐标表、UTM Grid Reference 坐标截图、缅甸/东南亚矿区 MGRS 点位列表。
4. 触发关键词 / 版面特征：zone 1-60 + latitude band C-X（排除 I/O）+ 100km grid square 两字母（排除 I/O）+ 等长 easting/northing 数字；可带 A/B/C 点号标签。
5. 识别主流程：在普通数字坐标、UTM 数字坐标、BFTM/X-Y 之前优先检测 MGRS；row type 固定为 `MGRS`；输出 `label | MGRS | WGS84 | KML`。
6. fallback 逻辑：fallback OCR 也可检测 MGRS，但只能作为人工核对结果；不得覆盖更高可信的专用表格识别分支。
7. 坐标转换规则：MGRS 先解析为 UTM easting/northing，再按 zone 与纬度带转换为 WGS84；最终 KML 坐标为 `longitude,latitude,0`。
8. KML 生成规则：Point / LineString / Polygon 均使用转换后的 WGS84 坐标；Polygon 自动闭合。
9. 失败保护规则：无效 zone、无效 band、I/O 字母、easting/northing 位数不等、紧凑数字奇数位、超过 5 位、转换纬度不在 band 范围内时必须拒绝。
10. 禁止修改项：不允许把 MGRS 当普通小数坐标；不允许让普通 decimal / projected X-Y 分支抢先覆盖；不允许输出未知坐标类型。
11. 回归测试要求：`47RLH 24469 42832` 应转换到约 `97.2636250946,24.7901938391`；完整 A-G 样本应识别 7 点并按输入顺序生成闭合 Polygon KML。
