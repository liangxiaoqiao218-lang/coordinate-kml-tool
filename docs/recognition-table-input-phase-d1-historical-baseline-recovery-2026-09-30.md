# 阶段D1 HTTP历史基线分类恢复报告

日期：2026-09-30

## 结论

PARTIAL / 阶段D1未完成。历史缺行JSON基线已按点位预览类别严格校验通过；唯一一次恢复运行随后在当前入口的缺行场景失败，已立即停止。不是旧基线分类再次失败，也不是超时。

失败断言：`json_missing: blocked map`。
当前真实本地HTTP响应为200、`mapReady=true`、`kmlReady=false`；统一采集决策已经保留`SOURCE_LABELS_NONCONTIGUOUS`并给出`mayProceedToGeometryValidation=false`。这违反本阶段对新入口缺行负例的严格阻断要求。

旧链路允许不完整候选的有限WGS84点位预览，而本阶段要求缺行场景关闭地图；该既有政策差异必须在产品最终决策中明确协调，不能继续通过修改历史分类或降低负例断言解决。本轮没有修改产品代码或输出政策。

报告按文档技能要求区分实测、源码解释与未知。全部请求为本地合成输入，不是生产图片重放。

## 基线和操作边界

- 工作树：C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9
- 分支：`codex/recognition-architecture-audit`
- HEAD：`86ea4ac44a6d44a2332d7aaaffb1926504a73dca`，未改变。
- 本轮仅修改两个测试工具文件，新增本报告及独立运行回执。
- 未暂存、提交、推送、创建PR、合并或部署；未修改生产配置、密钥、数据库、账户或计费政策。
- 未上传生产图片或创建生产识别任务。HTTP只向127.0.0.1提交程序生成的空白合成PNG。
- 测试进程清空真实Provider、Supabase及使用次数相关凭证，禁用dotenv环境文件，继承拒绝非loopback socket的离线guard。假凭证仅供本地mock，不是真实密钥。
- 本轮实际localhost HTTP请求1次、mock Provider调用1次、真实Provider调用0、真实Supabase调用0。历史回执中的1次mock不计入本轮。

## 本轮修改

| 文件 | 变化 |
| --- | --- |
| scripts/recognition-diagnostics-http-regression.js | 历史点位预览与无候选失败分开校验；历史分类只能用于baseline；精确哈希复用缺行旧回执；保留当前负例严格关闭地图/KML；增加失败阶段、独立历史校验记录、本轮mock计数和恢复跳过清单 |
| scripts/recognition-architecture-audit-runner.js | 新增一次性恢复入口`--resume-d1-historical-baseline`和独立回执目录；从缺行HTTP恢复，保留首失败/超时停止 |
| 本报告 | 记录已确认路径、实际结果、未知和下一步边界 |

没有固定成功桩。请求仍执行真实Express处理器和产品采集、最终授权函数，仅OCR和Provider I/O为本地模拟。现有身份、版本、CRS、坐标/原文、来源、完整性、几何、权限及扣次断言均保留；没有让空诊断或任意失败通过。

历史预览校验逐项检查真实观察到的采集及授权返回值、原始JSON、点号与行序、经纬度字面值、逐点十进制值、引擎点、响应坐标、MultiPoint、EPSG:4326及轴序、结果身份/版本/几何哈希、pending、未授权和未扣次。该类别只记录历史，不能作为当前缺行通过依据。

## 历史与当前结果必须分别解读

| 检查项 | 已保存旧入口 | 本轮当前入口 |
| --- | --- | --- |
| 输入点号 | 1、3、4；缺2 | 同一合成输入 |
| 统一采集候选数 | 0 | 3 |
| 统一采集状态 | NO_COORDINATE_EVIDENCE | COMPLETED |
| 采集允许进入几何校验 | false | false |
| 缺行证据 | 旧恢复候选组保留NONCONTIGUOUS | 统一采集保留SOURCE_LABELS_NONCONTIGUOUS |
| 最终HTTP | 200 / ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED | 相同 |
| 最终几何 | 3点MultiPoint，不是Polygon | 相同3点MultiPoint |
| 地图/KML | true / false | true / false，违反当前严格地图阻断断言 |
| 确认/授权 | pending / REVIEW_REQUIRED，未正式授权 | 相同 |
| usageConsumed / userUsageConsumed | false / false | false / false，仅离线模式证据 |
| 本轮验证性质 | VALIDATED_HISTORICAL_ONLY | FAIL，不计作通过 |

旧回执路径：
`Temp/recognition-table-phase-d1-markdown-recovery/requests/json_missing-baseline.json`

已校验原文件SHA256：
`cb7b9d42e18e458f6bd8ef943bb137ca0bcdfcea815241ba9c839810b24e6bdd`

旧文件未修改；新目录中的副本添加了复用元数据，因此副本哈希不同，不冒充原文件。
旧入口对照只装载精确HEAD的server.js和recognition-first-acquisition.js，其余依赖来自隔离工作树；不能称为完整生产发布目录重放。

## 已确认的阻断丢失路径

1. `buildRecognitionAcquisitionEvidence`已经恢复3条DMS候选，保持点号1、3、4与方向，并记录缺行原因。地理方向证据为`EXPLICIT_DMS_AXIS_DIRECTIONS`、`datumExplicit=false`、`reviewOnly=true`；不是把本次失败归因为笼统缺少CRS。
2. `evaluateUnifiedRecognitionAcquisition`在recognition-first-acquisition.js:748和:776按reviewReasons得出不允许进入几何校验，地图/KML为CLOSED。此处识别到了问题。
3. server.js:18335的`groupedProviderReviewApplies`把含点号问题的DMS候选接入旧分组预览路径；:18413调用`keepRecognizedCoordinatesAsPointReview`未指定`blockMap:true`。
4. server.js:14753的点位降级函数默认`blockMap=false`，将有限有效点构成Point/MultiPoint，保留pending和KML关闭。这是历史预览设计，不是生成了完整Polygon。
5. 最终授权函数recognition-first-acquisition.js:617、:623、:638按照响应的地图门禁及有效待核对几何决定临时地图；该临时地图条件没有承接采集层的具体完整性阻断。`hasUnifiedEvidence`也只表示候选/状态等结构存在，不等于完整性通过。
6. 实际函数观察值：`hasUnifiedEvidence=true`、`acquisitionIncomplete=false`、`mapGatePassed=true`、`mapReady=true`、`authorized=false`。HTTP响应经server.js:15972的统一附加层仍开放地图，严格负例因此失败。

上述关键采集与最终决策数值来自本轮回执；旧分组分支与最终门禁的连接由返回的precisionMode、几何及只读源码解释。未额外发起请求获得更细事件轨迹。当前带诊断请求尚未运行，不能宣称该失败场景的完整诊断采集/业务等价性已经通过。

本次不是JSON再次丢失全部候选，不是零坐标失真，也没有发现本请求正式授权或KML开放。核心是“采集层严格阻断”与“后置点位预览政策”尚未统一。不能把所有REVIEW_REQUIRED或采集层mapStatus=CLOSED一律当作硬阻断，因为完整待核对DMS路径也可能先关闭再由最终层提供临时输出；下一步必须使用具体失败条件协调，避免伤及正常完整结果。

## 本轮验收结果

执行一次：`node scripts/recognition-architecture-audit-runner.js --resume-d1-historical-baseline`

| 项目 | 结果 |
| --- | --- |
| 两个修改JS文件语法检查、修改前置差异检查 | PASS |
| 指定旧回执SHA256和逐点历史预览分类 | PASS，仅历史证据 |
| 当前缺行JSON真实HTTP，未启用诊断 | FAIL：mapReady=true；退出1，timeout=false，HTTP回归运行728ms |
| 当前缺行JSON带诊断和业务等价性 | 未执行，首失败后停止 |
| 额外字段、方向冲突HTTP | 未执行 |
| 历史39项入口、25项下游、历史4项、普通/Markdown/紧凑JSON/冗长JSON HTTP | 按要求未重跑；不把历史通过记为本轮通过 |
| 完整阶段C影子对照 | 未执行 |
| 阶段B诊断及HTTP | 未执行 |
| 原计划10个专项 | 未执行 |
| 134项完整离线验收 | 未执行，不报告134/134 |
| 42项核心离线回归 | 未执行，不报告42/42 |

10个专项仍为：投影CRS证据、多表示证据、Markdown表格、识别审阅结果、采集证据、多表示HTTP、p08h核对UI、源坐标详情、地图/KML输出契约、投影授权。

新目录：`Temp/recognition-table-phase-d1-historical-baseline-recovery/`

- results.json：退出码、未超时、耗时及失败尾部。
- http-results.json：历史单独校验结果、当前失败阶段、本轮mock计数及跳过清单。
- requests/json_missing-baseline.json：哈希校验后复用的旧请求证据。
- requests/json_missing-disabled.json：断言前保存的当前真实HTTP结果、实际函数观察、候选和最终身份。

当前失败回执SHA256：
`45c9a6d0145b4df7bdebcf88b86a41e5da848c42251202e171a5c53acdfa7544`

## 保护校验与Git状态

本轮开始记录的137份既有修改文件和历史材料已按内容哈希复核：仅上述两个测试工具文件改变，缺失0；所有既有产品修改、报告、历史失败回执和样本未变。本轮新回执使用独立目录，未覆盖历史。

最终工作区仍有4个历史tracked修改；加本报告共22个untracked文件，无暂存。HEAD和分支不变。4个产品文件相对HEAD的既有差异不是本轮新增修改。

## 剩余风险与下一步

- 阶段D1仍不可判定验收完成，不能发布。
- 新入口已恢复字段证据，但硬阻断尚未贯穿旧预览降级与最终响应。
- 额外字段、方向冲突等后续HTTP场景仍未知；直接函数通过不能替代真实HTTP。
- 历史生产03原始Provider响应缺失保持UNKNOWN。合成回执只能定位这一分支，不能证明生产03根因全部相同或已修复。
- 离线未扣次不证明真实数据库结算；本阶段不修改计费政策。
- 源坐标原文和派生数值均保存在回执；缺行预览仍走旧的十进制展示，尚未验证该负例全部字段诊断及展示契约。

需要新的窄范围产品授权：将已明确确认的缺行/字段/方向/点序/来源等硬阻断传递到现有点位降级和最终输出门禁，保留完整待核对结果原有能力。不能只为通过本例硬编码点数、点号或具体值，不能一律关闭所有待核对结果，也不能更改正式授权、身份、版本或计费政策。先补真实函数及HTTP回归，再从本次缺行失败恢复一次，之后按既定顺序完成C/B/专项/134/42；首失败继续停止。

