# 阶段 D1 统一表格输入适配 — 部分完成，HTTP 门禁阻塞

## 结论与边界

2026-09-30，本轮仅在隔离工作树实施 `buildRecognitionAcquisitionEvidence` 输入适配。分支为 `codex/recognition-architecture-audit`，HEAD 保持 `86ea4ac44a6d44a2332d7aaaffb1926504a73dca`。

新增入口级回归 39/39 通过；普通表格和 Markdown 的真实本地 HTTP 基线/新入口/诊断开启三路对照通过。随后，紧凑 JSON 的**旧基线请求**返回 HTTP 422，测试却错误要求 HTTP 200，发生首次断言失败。本轮立即停止，未运行该场景的新入口 HTTP 请求，未重试，也未继续任何后续测试或代码修改。

因此阶段 D1 状态是 **PARTIAL / HTTP 验收阻塞**，不是已完成、已具备发布资格或已解决生产 03 图片。以下报告是失败后的证据整理，不是继续修复。历史生产原始 Provider/OCR 响应仍缺失，最早数字偏差阶段继续 UNKNOWN。

没有提交、暂存、推送、PR、合并、部署、生产访问、生产图片上传或生产识别任务。真实 Provider 调用 0；没有使用真实 Supabase 凭证。离线 runner 清空真实 Provider/Supabase/usage 相关环境变量，禁用 dotenv，并向子进程传播非 loopback 网络拒绝保护。本地 HTTP 沿用测试专用假凭证以通过配置入口，fetch 被固定到合成响应；假凭证不是生产密钥。

## 本轮实现

新增 `coordinate_table_input_v1`，只负责输入序列化与字段证据，不负责转换、几何、选址、输出权限或计费。

路径为：原有选源优先级 → 输入适配 → 原有候选行解析 → 原有采集结果。优先级仍是已绑定投影表头、已绑定多表示 DMS、原始 Provider 文本。没有按最多行选择解析结果。

JSON 语法由原生 JSON.parse 验证，语法遍历只保存原始标量字面值、重复键和 UTF-16 字符区间，随后交给既有坐标解析器。不能复用旧 JSON 对象展平作为安全证据：它会丢失重复键和数字末尾零，且可能提取内层数组而丢掉外层信息。新模块不实现坐标转换或另一套识别模型；DMS 数值校验复用既有 normalizeProviderDmsReviewResult。

普通表格/Markdown 不改原解析输入。字段模型统一记录 rowOrdinal、pointLabel、representation、group、rawRow、fields、sourceCells 和 selectedCandidate。JSON 字段包含 rawLiteral、decodedValue、normalizedValue、JSON Pointer 与原文字符区间。普通表格现有解析器没有提供的字段字面值或精确字段区间不推断，保留整行/原始单元格并标 UNKNOWN。物理表身份和图片身份也不由 JSON 容器或组号冒充。

来源的字符指针指向 ADAPTER_INPUT。若此前选源生成了绑定文本，诊断同时保留真正 rawResponse、adapterInputText 和 selectedInputSource，避免把生成文本的位置冒充原始 Provider 或图片位置。字段证据只进入显式私有诊断；公开业务返回结构没有增加该诊断模型。默认诊断捕获对新增原始文本字段执行 WITHHELD。

`buildPreD1RecognitionAcquisitionEvidence` 保留迁移前候选输入路径，供差分和回退。它与新入口复用结果装配，不复制或修改最终决策逻辑。没有删除任何旧实现。

## 已验证改善与全部受测差异

完整合成表的六种 JSON 变体（DMS、X/Y、混合表示，各紧凑/冗长）在旧入口均为 0 个候选，新入口均为完整 16 个候选。3、6、9、23 行的合成用例也通过，不存在按 16 行解锁的产品规则。

| 输入 | 逐字段结果 | 权限结论 |
| --- | --- | --- |
| DMS、X/Y、混合的普通表格及 Markdown，共 6 组 | 新旧采集结果完整深比较相同 | 未变 |
| DMS JSON 紧凑/冗长 | 0→16 候选；候选行/组、完整性计数及 DMS 方向证据随完整行恢复 | 采集证据为 REVIEW_REQUIRED，不是正式授权 |
| X/Y JSON 紧凑/冗长 | 0→16 候选，原始数字不改写；触发原有投影证据提取调用 | 原始 JSON 尚未迁移到其他投影提取入口；不推断 CRS、不开放最终地图/KML |
| 混合 JSON 紧凑/冗长 | 按原解析器规则保留 DMS 选择；X/Y 原字段另留证据，不覆盖 DMS | 不新增两种表示一致性证明 |

差分回执 `input-differences.json` 保存上述 12 组所有差异位置及前后值：6 组无差异；JSON DMS/混合每组 49 处、X/Y 每组 42 处，共 280 处对象比较差异。变化字段为 acquisitionStatus、status、authorizationStatus、normalizationStatus、candidateCoordinateLines、candidateCoordinates、candidateCoordinateGroups、diagnostics、geographicCrsEvidence、reviewReasons，以及 X/Y 的 projectedCoordinateEvidence。authorizationStatus 从 NOT_ESTABLISHED 到 REVIEW_REQUIRED 是“获得候选证据”，没有提升为正式授权。

rawProviderText、providerResponseId、providerCompletionState、imageEvidence 及 EVIDENCE_ONLY 权属保持；点号、输入行序和选中的逐行坐标与相同内容的旧普通表格解析结果一致。JSON 里的数值近似值保持不同，没有以源值替换；数字末尾零保存在 rawLiteral/normalizedText，Number 表示只存在 normalizedValue。

## 已验证保留的阻断

入口回归保留缺字段、额外字段、重复 JSON 键、同义字段重复/冲突、方向冲突、无效 DMS、多个未绑定表、字段注入、列顺序冲突和 JSON 外额外坐标。结构性 JSON 错误不裁成“成功子集”。缺行、重复点号、逆序保留原点号和原顺序，并由原校验阻止继续几何验证。

已有同图绑定、CRS 冲突、轴序冲突、图片冲突结果及源选择优先级新旧深比较相同。缺失 CRS 不补全。所有入口用例在没有最终几何的情况下均未获得最终地图/KML或正式授权。

这些是入口及少量 HTTP 的证据，不是完整 HTTP 负例已经通过。紧凑/冗长 JSON、缺行、额外字段和方向冲突的 HTTP 完整验证尚未完成。

## 本轮测试结果与首次失败

| 项目 | 本轮结果 |
| --- | --- |
| 4 个受改动入口/测试脚本的语法检查 | PASS，exit 0 |
| recognition-table-input-regression | 39/39 PASS，exit 0，275ms |
| 新入口阶段 C 适配/重放（上述 39 项之一） | MATCHED_OBSERVATIONS；3 个实际产品阶段重放 MATCH；未解释安全差异 0 |
| HTTP 普通表格 | 基线、新入口、诊断开启业务结果等价；身份/版本与本次响应一致 |
| HTTP Markdown | 同上 |
| HTTP 紧凑 JSON 基线 | FAIL：HTTP 422，测试要求 200；exit 1，整项 3303ms，非超时 |
| HTTP 紧凑 JSON 新入口及之后的 HTTP 场景 | NOT RUN |
| 完整阶段 C 36 项影子对照 | NOT RUN |
| 阶段 B 诊断及 HTTP回归、10 个受影响专项 | NOT RUN |
| 134/134 完整离线验收 | NOT RUN |
| 42/42 核心离线回归 | NOT RUN |

失败位置：`scripts/recognition-diagnostics-http-regression.js:145`，由 D1 循环的旧基线请求触发。该用例刻意不提供 OCR 绑定文本，以进入原始 JSON 入口；但测试仍套用“旧入口必须 HTTP 200，且新旧业务状态完全相同”的默认预期。这与此次允许修复旧格式丢行的目标不一致，是我本轮新增 HTTP 差分测试的设计错误。

不能据此推断新入口 HTTP 通过或失败。失败回执保存了状态码和调用栈，没有保存失败响应体，因此旧请求具体业务 reason/code 仍未知；不能只凭 422 认定是哪个安全检查。本次没有补跑请求来追取响应。

下一步需在不改产品的前提下，把“原已成功的输入严格等价”和“旧入口已失败、允许恢复候选的输入”分开。后者仍需记录旧响应、逐字段验证新候选及原有安全条件，不可把任意 4xx 视为成功，不可放松几何/身份/版本/权限/计费断言。

## 回执与保护基线

全部路径相对于当前隔离工作树，回执保存在已忽略的 Temp 下，没有覆盖旧回执。

| 回执 | SHA-256 |
| --- | --- |
| Temp/recognition-table-phase-d1/results.json | 7c54d9d00ed7dba2703da10d0860d64d770b750a1e34809a232b0b5b9f642f34 |
| Temp/recognition-table-phase-d1/input-differences.json | 1e43b1b915b97062ad205342b77240fd06444ae679aa112064a87cc14ffcebf8 |
| Temp/recognition-table-phase-d1/http-results.json | 0ea6db371593a451ab30d51fa78060b67180d69a545601f8614c85be423cb018 |

旧 A、B 初始失败、B 恢复、C 总回执及 C 比较回执哈希均与此前记录一致，保留完整。server.js、index.html、package.json、package-lock.json 与 D1 开始前内容哈希相同；server.js 的现有未提交修改属于此前阶段，并非本轮修改。

## 本轮修改文件

| 文件 | 本轮用途 |
| --- | --- |
| server/recognition/recognition-table-input.js | 新增序列化适配及版本化字段行证据 |
| server/recognition/recognition-first-acquisition.js | 接入单一入口，保留 pre-D1 可调用旧路径；不改选源或决策 |
| server/recognition/recognition-diagnostics.js | 新增原始字段名称的默认文本隐藏保护 |
| scripts/recognition-table-input-regression.js | 新增 39 项入口行为、差分、负例及影子验证 |
| scripts/recognition-diagnostics-http-regression.js | 复用真实 HTTP 测试基架，增加 D1 模式；此处基线预期有误，待授权修正 |
| scripts/recognition-diagnostic-replay.js | 允许重放实际新增输入适配函数 |
| scripts/coordinate-result-shadow.js | 观察新入口的候选结果，保留旧入口支持 |
| scripts/coordinate-result-shadow-regression.js | 保留完整 C 历史断言，新增显式 pre-D1 历史模式与独立输出目录 |
| scripts/recognition-diagnostics-regression.js | 原诊断等价测试可显式使用保留旧入口，不伪称新旧 JSON 无差异 |
| scripts/recognition-architecture-audit-runner.js | 新增 D1 独立回执及失败即停止的执行计划 |
| docs/recognition-table-input-phase-d1-2026-09-30.md | 本报告 |

阶段 C 原测试保留 0 个 JSON 候选的历史断言，D1 新入口由独立 39 项验证，不能用旧入口通过代替新入口通过。本轮完整 C 测试还没有执行。

Git：加上本报告共 18 个未提交文件（2 个已跟踪修改、16 个未跟踪文件），暂存区为空。既有 15 个文件全部保留，本轮新增适配模块、专项测试和报告 3 个文件。没有删除或退役代码。

## 尚未解决与不能提前退役的内容

1. 未完成 JSON 的真实 HTTP 路径验证及完整回归，因此不满足 D1 退出标准。
2. 物理表、区域、图片字符位置不由此次文本适配提供，继续 UNKNOWN。同图同表协调应在 D2 中处理。
3. 原有投影提取器仍接收原始响应；新 JSON X/Y 候选恢复不等于投影 CRS、轴序及几何已经获准。不得以此声称生产投影问题已修复。
4. 混合表示里的未选字段、独立数值冲突校验、源绑定优先级仍由旧模块负责。适配器不做自动选优、坐标替换或容差放行。
5. 当前保守支持一个明确 JSON 表容器；多表、异构列顺序、未知额外字段仍拒绝，不能宣称支持任意 Provider JSON。
6. 本地合成测试不测真实 OCR/Provider 数字准确率、延迟、生产结算或浏览器交互。历史 03 原始证据缺失，仍不能确定最早错数阶段，也不要求用户重新上传试错。

潜在退役对象只有“采集入口对序列化的重复适配”这一职责，不是整段解析器。目前 buildPreD1RecognitionAcquisitionEvidence 必须保留用于差分/回退；normalizeProviderDmsReviewResult、extractRecognitionCandidateEvidence、投影提取、表示绑定、最终权限及 UI 路径仍有实际调用，均不可删除。需等 D1 门禁、D2 字段绑定和后续消费者迁移完成，再复核动态调用和行为等价证据。

下一阶段首先是 D1 HTTP 验收恢复，而不是 D2、PR 或生产发布。需要有界授权修正测试基线分类和失败响应回执，随后只重跑失败 HTTP 项一次，再继续未执行门禁；若发现必须修改产品或权限政策则另行停止说明。

## D1 全量差异收口（2026-10-01）

### 最终根因与统一路径

D1 最终确认的根因不是单一 OCR 失败，而是序列化适配、候选消费和最终能力判断之间缺少同一份字段级证据契约：JSON、普通表格与 Markdown 曾由不同入口重复解释；完整性阻断可能在点位降级路径丢失；地图能力曾被 KML 状态连带关闭。统一后的路径为：原始 Provider 字面值 → `recognition-table-input` 的版本化字段行 → `recognition-candidate-evidence` 的完整消费与冲突证据 → `recognition-first-acquisition` 的同一完整性结果 → 现有几何、身份及版本校验 → 最终地图与 KML 分别决策。原始值、规范化值和转换值保持分离。

### 模块分类与处理结论

| 分类 | 保留/处理 | 依据 |
| --- | --- | --- |
| 产品实现 | 保留 `server.js`、本地 OCR 投影证据、候选证据、采集、诊断、表格输入和源坐标表示模块 | 均有生产调用方或为统一证据链提供必要字段；动态 HTTP、finalizer 和测试沙箱调用已复核 |
| 长期行为级回归 | 保留表格输入、候选下游、诊断、影子对照、完整性输出、投影、地图/KML能力等回归 | 覆盖真实产品函数/本地 HTTP、严格负例及身份版本边界，不是源码字符串检查 |
| 离线诊断及重放工具 | 保留诊断重放、架构 probe、shadow adapter 和离线网络保护 | 用于 UNKNOWN 证据、差分定位与 Provider=0 的可重复验收；不接入生产响应 |
| runner 和回执基础设施 | 保留统一架构审计 runner；退役一次性地图/KML恢复 runner | 统一 runner 提供独立回执和失败即停；一次性 runner 已由永久能力回归及统一 runner 取代 |
| 架构及阶段报告 | 全部保留 | 记录历史失败、证据边界、迁移与回退关系，不参与运行时 |
| 临时调试内容 | 删除核心回归中的 `--diagnose-map-kml-coupling` 分支 | 对应诊断已有受控回执，行为由 `recognition-map-kml-capability-regression` 永久覆盖 |

未发现可以在本阶段安全删除的生产解析器、完整性判断或诊断采集。它们虽有相邻职责，但输入输出和调用阶段不同；直接合并会改变优先级或失去原始证据。`buildPreD1RecognitionAcquisitionEvidence` 继续作为差分与回退入口保留，待后续迁移完成后再退役。

### 地图与 KML 独立能力契约

地图与 KML 不再互为开关。有效、有限、可定位的 EPSG:4326 几何在身份和版本一致且不存在图片、点号、行序、坐标、CRS、轴序或来源硬冲突时，可以提供待核对地图。KML 还必须满足其独立的点号/边界及授权条件；缺少权威点号时只关闭 KML。无效几何、不可转换 CRS、硬完整性冲突或身份/版本不一致继续阻断相应输出。`REVIEW_REQUIRED` 本身既不自动开放地图，也不构成正式授权。

### 验证边界与待生产资格验证

离线门禁证明的是合成及固定证据下的解析、字段保真、阻断传递、身份版本和能力契约，不证明真实 OCR/Provider 的数字准确率、生产延迟、浏览器视觉体验、计费结算或任意图片可识别。历史生产图片缺少完整原始 Provider/OCR 回执的部分继续标记 UNKNOWN。D1 合并后仍需独立生产资格阶段验证真实请求链路；本阶段不上传图片、不调用真实 Provider、不部署。

### 收口门禁结果

| 门禁 | 结果 |
| --- | --- |
| 地图/KML独立能力 | 7/7 PASS |
| 完整性输出 | 13/13 PASS |
| 识别审阅结果 | PASS |
| 采集证据 | PASS |
| 多表示本地HTTP | PASS；模拟Provider调用1，真实Provider调用0 |
| 源坐标详情 | 32/32 PASS |
| 审阅输出契约 | 39/39 PASS |
| 完整离线验收矩阵 | 134/134 PASS |
| 核心离线回归 | 42/42 PASS |

本次恢复回执根目录为 `Temp/recognition-d1-closeout-recovery-20261001`，位于Git忽略目录中；历史回执未覆盖。全部测试在Provider、Supabase及使用次数相关密钥为空且离线网络保护启用的进程中执行。真实Provider调用为0。
