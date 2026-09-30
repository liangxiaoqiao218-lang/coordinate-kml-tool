# 阶段D1完整性阻断贯穿修复与离线验收报告

日期：2026-09-30

## 结论

PARTIAL。已修复并通过本地验证的具体问题是：统一采集已发现缺行，但旧点位预览和最终输出仍开放地图。新增真实产品函数回归13/13通过；缺行与额外字段的真实本地HTTP场景均通过，包括启用诊断前后的业务等价性。

随后在方向冲突的旧入口历史分类断言失败，立即停止，没有自动重试，也没有继续改产品或测试。当前方向冲突HTTP尚未执行，阶段C/B、10个专项、134项与42项均未启动；阶段D1整体未完成，不可发布。

本报告按文档技能区分实测、源码解释与未知。输入均为本地合成资料，不是生产图片重放，不能声称生产03问题已解决。

## 基线及操作边界

- 工作树：C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9
- 分支：codex/recognition-architecture-audit
- HEAD：86ea4ac44a6d44a2332d7aaaffb1926504a73dca，保持不变。
- 全部既有未提交修改、历史报告和回执保留，无暂存、提交、推送、PR、合并或部署。
- 无生产图片上传、生产识别任务、生产配置/密钥/数据库/账户修改。
- 真实Provider调用0，真实Supabase调用0。
- 本轮本地HTTP实际6次，每次mock Provider仅1次，共6次模拟调用；复用的旧缺行回执不计入本轮调用。
- Provider、Supabase及使用次数真实凭证在离线进程清空，dotenv指向不存在的测试环境文件，继承拒绝非loopback连接的guard。测试固定假凭证仅用于mock。
- 按Supabase技能落实凭证隔离，本轮没有实施或验证数据库功能，也没有更改扣次政策。

## 已确认原因及最小修复

原采集决策已记录SOURCE_LABELS_NONCONTIGUOUS，mayProceedToGeometryValidation=false。但分组DMS恢复路径仍调用默认允许点位预览的keepRecognizedCoordinatesAsPointReview，最终地图门禁主要查看可用几何及待核对身份，没有承接具体行完整性失败。

本轮在现有recognition-first-acquisition.js增加getRecognitionAcquisitionIntegrityBlockReasons，只收集既有字段/点号/行序/来源失败原因及被拒绝、未绑定行的证据，不解析新坐标，也不新增识别引擎。普通REVIEW_REQUIRED、早期mapStatus=CLOSED、GENERIC_REVIEW_ONLY和单独CRS_EVIDENCE_MISSING不自动变成完整性硬阻断。

同一集合在三个现有位置消费：

1. server.js:18345：旧分组点位预览设置blockMap，并在阻断时给出“完整性检查未通过”提示，不再说可查看地图。
2. server.js:15733：统一响应整理层禁止完整性失败被DMS临时输出提升路径重新放开。
3. recognition-first-acquisition.js:644：最终Map/KML门禁否决明确完整性失败，具体原因进入finalAuthorizationReasons。

没有修改采集解析优先级、坐标值、原始文本、字段来源、CRS转换、结果身份/版本、核对权限或计费规则。完整结果沿用原有正式授权条件；本轮只向既有输出门禁传递原本就应生效的完整性失败，没有新增授权方式。阻断不等于删除原始证据或篡改坐标。

## 行为变化和不变项

| 范围 | 以前 | 本轮实测 |
| --- | --- | --- |
| 缺行JSON真实HTTP | 200；MultiPoint；地图开、KML关 | 200；保留同一逐点坐标、点号和顺序；地图/KML均关 |
| 缺行具体原因 | 采集层有，未贯穿最终地图门禁 | SOURCE_LABELS_NONCONTIGUOUS进入最终门禁 |
| 额外字段JSON真实HTTP | 历史无候选失败422 | 当前仍422；EXTRA_ROW_FIELD保留，地图/KML均关 |
| 完整JSON/普通表格/Markdown的最终门禁 | 有效待核对几何允许临时输出 | 与旧最终门禁返回逐字段相等，地图及未确认KML仍可用，未正式授权 |
| 普通待核对/早期地图关闭 | 不能单独决定最终不可用 | 保持；回归专门验证没有一律关闭 |
| 点位降级 | 保留结果身份/版本/几何 | 增加blockMap后身份、版本、几何、哈希、CRS、pending保持 |
| 使用次数 | 离线测试不扣次 | 所有已发起本地请求usageConsumed和userUsageConsumed均false |

完整输入的不退化结论来自真实产品函数门禁回归，不冒充本轮完整JSON端到端HTTP重测。先前已通过且本轮要求跳过的HTTP没有重跑。缺行、额外字段HTTP的diagnostics开关对照验证了业务字段、候选证据、函数观察、结果版本/几何/状态及扣次等价；每次请求身份仍独立生成，不要求跨请求UUID相同。

## 修改文件

| 文件 | 本轮变化 |
| --- | --- |
| server/recognition/recognition-first-acquisition.js | 收集明确完整性阻断；最终地图/KML门禁共用并保留具体原因 |
| server.js | 分组预览blockMap、最终响应整理及条件提示共用上述阻断；不改解析/计费 |
| scripts/recognition-integrity-output-regression.js | 新增13项真实产品函数行为回归、旧最终门禁对照与独立检查点 |
| scripts/recognition-diagnostics-http-regression.js | 独立完整性恢复模式；强化当前负例的最终门禁、逐点/原文/几何与具体原因断言；未放宽历史分类 |
| scripts/recognition-architecture-audit-runner.js | --d1-integrity入口，先新增回归再从缺行恢复HTTP，独立目录、首失败停止 |
| 本报告 | 独立记录本轮范围、结果、未完成事项和下一步 |

历史recognition-candidate-evidence.js、source-coordinate-representation.js、阶段A/B/C文件等未被本轮改变。未更新依赖或package锁文件。

## 本轮运行结果

执行一次：node scripts/recognition-architecture-audit-runner.js --d1-integrity

| 项目 | 结果 |
| --- | --- |
| 5个修改/新增JS文件语法检查，差异空白检查 | PASS，运行门禁前完成 |
| 完整性输出真实函数回归 | 13/13 PASS，238ms，退出0 |
| 复用缺行旧回执及历史逐点预览核验 | PASS，仅历史对照 |
| 缺行当前HTTP，无诊断和带诊断 | PASS；地图/KML均关闭，候选/原文/字段来源、身份/版本、逐点几何、具体原因及未扣次断言通过 |
| 额外字段历史与当前HTTP | PASS；当前422无候选失败，诊断非空且开关业务等价 |
| 方向冲突旧入口HTTP | FAIL，历史分类的engine.sourceCrs断言期望EPSG:4326，实际null |
| 方向冲突当前HTTP | 未执行，不能用函数级方向冲突通过替代 |
| 完整阶段C影子及阶段B诊断和HTTP | 未执行 |
| 原计划10个受影响专项 | 未执行 |
| 134项完整离线验收 | 未执行，不报告134/134 |
| 42项核心离线回归 | 未执行，不报告42/42 |

HTTP套件2645ms，退出1、timeout=false。仅完成其中2个场景，整个套件不是PASS。
没有重跑历史39项入口、25项下游及历史4项、普通/Markdown/紧凑JSON/冗长JSON已通过HTTP。

13项具体覆盖：保存的失败证据不变；完整JSON、普通表格、Markdown门禁与旧实现相等；缺行、重复点号、点序冲突、额外字段、缺字段、方向冲突不能重新开放输出；未绑定来源阻断；普通待核对不被一律阻断；点位降级保持身份/版本/几何并尊重阻断。

## 本次停止的精确原因

失败发生在json_direction / historical_baseline，不在当前入口。位置：
scripts/recognition-diagnostics-http-regression.js:318，verifyHistoricalPointPreview。

已经保存的旧入口事实：

- 原始合成JSON含4行，第一行纬度字段被改为E，形成方向冲突。
- 统一采集没有候选，但旧DMS恢复分支保留点号2、3、4；第一行未进入最终点集。
- 引擎sourceCrs为null，点级source_crs也为null。
- 最终仍为3点MultiPoint，最终对象crs标记EPSG:4326/longitude_latitude。
- HTTP200，地图true、KMLfalse，REVIEW_REQUIRED、pending、未正式授权、离线未扣次。
- 原因保留DMS_DIRECTION_CONFLICT、SOURCE_LABELS_NONCONTIGUOUS、CRS_EVIDENCE_MISSING等。

现有历史分类只支持“完整来源的点位预览”，要求引擎CRS为EPSG:4326并按全部原始行逐点对照。方向冲突的旧响应属于“拒绝冲突行后仍开放剩余点位”的不完整历史输出，不符合这个类别。不能只删除CRS断言或把全部旧HTTP200视作合法预览；必须将丢弃行、保留行及CRS层间不一致明确记录为历史风险。

最终对象标记EPSG:4326只是旧响应事实，不证明缺失的原始CRS证据已得到补全。本轮未修改该历史响应，也没有把其预览当作新入口安全标准。

静态检查还发现测试verifyFinal把统一采集0候选直接归入422无候选失败；在存在后置候选保留路径时，这个前提需要按响应事实确认适用范围。当前方向冲突请求尚未运行，不推断它必然返回200或422，更不据此预先宣称安全通过。

## 回执及保护检查

新目录：Temp/recognition-table-phase-d1-integrity-recovery/

- results.json：两个套件退出码、耗时、未超时和错误尾部。
- integrity-results.json及13个检查点：真实函数输入、前后结果和断言状态。
- http-results.json：旧新分别记录、2个PASS、方向旧基线失败阶段、本轮mock计数。
- requests/json_missing-disabled.json与captured文件：缺行当前真实HTTP和私有诊断。
- requests/json_extra-*：额外字段旧/新HTTP及诊断。
- requests/json_direction-baseline.json：方向冲突历史失败回执。

方向旧回执SHA256：
7a6b1414c7825402c230c8a9d2959e82c29a6db909b11acbfbadfc1d2c0e4bec

本轮http-results.json SHA256：
85808c7b8f718366de602d7f92c99082447f4dff248d05186df0742348d60b63

原缺行基线SHA256保持：
cb7b9d42e18e458f6bd8ef943bb137ca0bcdfcea815241ba9c839810b24e6bdd

本轮开始保护的142份既有文件已按哈希比对：仅本轮授权的server.js、recognition-first-acquisition.js、HTTP测试和runner共4份改变；缺失0，其余历史回执、报告、样本及未提交内容未变。另新增函数回归脚本、本报告和独立运行回执。

Git状态：4个tracked文件仍修改，其中2个本轮继续修改、2个只是历史修改；24个untracked（含本轮脚本及报告），无暂存，分支及精确HEAD不变。

## 尚未解决及下一步

当前已验证缺行、额外字段阻断，但方向冲突端到端HTTP、后续矩阵和其他专项仍未知。范围有限的函数/HTTP通过不能代表识别准确率或生产可用性。历史生产03原始Provider响应缺失继续标记UNKNOWN，不要求再次上传图片试错。

下一步应仅修正HTTP历史分类及必要runner恢复入口，给“方向冲突后保留部分点位的旧响应”建立受严格证据约束的独立历史类别。先完整核查剩余状态断言的适用范围，保留新入口硬阻断及逐字段/身份/版本/CRS/几何/计费检查，不再逐个把旧路径误套为完整成功或无候选失败。

校验并复用本轮方向旧回执，从方向冲突恢复一次，不重跑已通过13项和缺行、额外字段HTTP。之后继续C/B/10项/134/42。若出现当前产品输出未阻断或无法解释的坐标/来源/CRS状态，则停止，不能把历史不安全行为作为通过依据。此下一步不需要新的产品修改、生产操作或真实Provider调用授权。

