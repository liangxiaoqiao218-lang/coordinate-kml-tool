# 坐标识别全链路架构审计与收敛方案

日期：2026-09-30。性质：已授权审计、离线诊断及无行为变化清理；不是发布说明。

## 1. 结论与证据边界

问题不是只有“03这张图片难识别”，而是识别、证据绑定、候选选择、最终授权、展示及扣次各有自己的成功定义。补丁增加了局部能力，却未把这些定义收敛到一个结果契约。继续按图片追加兜底，仍会重复出现“有坐标但不可用”“有CRS却提示缺CRS”“测试通过而真实任务失败”。

不能保证本次穷尽所有潜在缺陷；本报告区分源码已确认、离线复现、生产观测和未知。不能据此宣称识别准确率达标或当前生产已经修好。

- 基线/远程目标分支/内部生产版本均核对为 `86ea4ac44a6d44a2332d7aaaffb1926504a73dca`。结束前再次读取远程分支与公开 `/api/version` 的 `runtimeIdentity.commit/branch`，仍一致。
- 生产：`/opt/geokitlab/app` 指向 `/opt/geokitlab/releases/hotfix-86ea4ac`，`geokitlab.service` 为 active；本次未重启。
- 目标分支：`hotfix/production-generic-dms-review-recovery`。
- 隔离目录：`C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9`。
- 隔离分支：`codex/recognition-architecture-audit`。既有33个工作树（含本次新建）均只读检查；原有3个脏工作树的1/1/35项修改未触碰。
- 原调用目录仍在旧提交 `4e21a3b`，没有把它误当生产基线，也未切换它。
- 离线依赖复用既有 pr70-postmerge 的 node_modules junction；本次无安装、升级或锁文件修改。已有离线 eng.traineddata 复制到忽略路径；不下载训练数据。
- 只读生产日志、源码、现有样本和用户提供的结果用于审计。没有上传图片、创建生产识别任务、访问数据库明细或调用真实Provider。

本文文件/函数定位以精确基线为准；清理后行号会前移，函数名为稳定定位。

## 2. “03”五个问题的回答

### 2.1 为什么16行仍关闭地图/KML？

生产观测（09-30 12:54:35–12:54:51中国时间）：本地OCR分类未建立强结构；Provider一次HTTP200；DMS诊断16行COMPLETE、投影诊断16行COMPLETE、统一候选16/bound16；最终 REVIEW_REQUIRED/needs_review，地图与KML CLOSED，usageCommitted/userUsageConsumed 为true。

这里的COMPLETE只是各解析器自己确认“可解析的行齐全”，不是字段真实准确、表示相符、几何及权限都通过。

源码：server.js 的投影分支在通用DMS返回前运行。`completeProviderDmsSupersedesProjected` 要求完整DMS再加投影交叉检查或投影引擎成功；不是“有完整DMS就选它”。`buildExplicitProjectedBoundaryAutoReleaseEngine` 同时检查行数/点号/轴序/CRS/数值/几何/DMS参考，任何失败均返回null。

null以后投影兜底仍保留X/Y供显示，却把引擎 `source_crs`、点的lat/lon、projection清空。最终授权会再次看到“没有可转换的CRS/轴序/几何”，而非原先的具体失败。这是已确认的失败原因丢失与状态折叠路径。

### 2.2 哪个具体字段阻断这次生产请求？

现有日志不足以还原这一次请求的准确叶子原因。日志未保留可重放Provider原文、同次本地OCR文本、逐字段冲突、选中候选与完整finalAuthorizationReasons，不能排除源绑定、轴序、数值交叉检查或几何中的任一项。

已经定位需要观测的具体检查：

| 检查位置 | 输入字段/条件 | 现在的失败表现 |
| --- | --- | --- |
| projected-source-evidence.js / bindProjectedEvidenceToSourceContext | image_sha256、same_request、CRS、axisOrder | 清空或降级CRS证据 |
| multi-representation-source-evidence.js / bindProviderRepresentationsToSource | 点号、行序、X/Y、DMS及源表 | CONFLICT/INCOMPLETE；投影可能被降级 |
| server.js / hasContinuousProjectedPointNumbers | 1开始连续数字点号，3个以上，标签1–3位 | null，未说明具体缺点/排序 |
| server.js / projectedDmsReferencesMatch | 各行referenceDms，与转换后lat/lon差≤1e-6度 | null，未保留残差及冲突行 |
| server.js / hasNonDegenerateProjectedBoundary | 有限值、重复点、面积、自交 | null，未区分原因 |
| recognition-first-acquisition.js / evaluateUnifiedRecognitionFinalAuthorization | 当前候选、引擎source_crs/axis、finalized几何/版本 | CRS/AXIS/geometry等阻断，可能是上游信息丢失的次生原因 |

离线诊断调用实际转换及检查函数：在3个区带、南北半球的合成边界中，完整一致可返回引擎；仅改变一行DMS参考、缺参考、缺轴序、点序冲突或自交均返回null。不存在“返回null就一定缺CRS”的逻辑依据。这是机制复现，不是03生产原始请求重放。

### 2.3 数字差异最早出现在哪？

本地读取原图目视核对与用户贴出的当前输出：

| 字段 | 原图可见 | 用户当前输出 | 结论 |
| --- | --- | --- | --- |
| 点3 Y | 9721182,351 | 9721188.351 | 不只是小数标点规范化 |
| 点4 Y | 9721181,948 | 9721188.1948 | 位数和值均不同 |
| 点6 X | 778980,724 | 778880.724 | 值不同 |

已确认终端值存在差异；最早产生于视觉识别、Provider序列化、规范化或候选选择的哪个阶段仍未知。禁止直接用上述真值替换代码或加入样本特例。

另发现活动代码 `normalizeProjectedColumn` 会在无小数点、数值越界且其他行精度一致时根据整列精度插入小数点。这是推断式修复风险，但不能据此认定它导致上述具体错误（这些输出本身已有小数点）。

### 2.4 完整DMS和投影为何不形成一致输出？

“完整”在当前实现里不是“两个表示相互一致”。同一响应被多次解析：DMS诊断、分组DMS规范化、投影解析、本地多表示绑定、统一候选。生产日志里DMS诊断COMPLETE而分组规范化NO_COORDINATE_EVIDENCE就体现了各自契约差异。

绑定成功时统一候选优先使用投影拼接文本，再次解析；不同表示不在一条持久统一行记录中携带相互独立的字段证据。再加上提前return与交叉检查门槛，后面的DMS路径可能没有执行机会。

正确方向不是强制相信DMS、强制相信X/Y或放开按钮，而是每行保留两种表示及来源、比较残差、给出冲突单元格；冲突未解决时不得导出为完整边界。

### 2.5 为什么接受16行却不可用，还记录扣次？

“接受行”当前表示语法/结构上接收的候选，并非任务成功。`coordinate-usage-atomicity.js` 的投影review authority和acquisition review authority专门允许无最终几何/KML关闭的可核对文本结果进入eligible结算。生产日志commit与此规则一致。

这是产品交付口径与计费口径不一致，不等于已经证明重复扣次、数据库账务错误或恶意扣费。本次未查账、未改计费。应明确将“原文可提取”“可定位预览”“可导出未确认KML”“正式授权输出”分开统计和对用户说明。

## 3. 当前链路与冲突位置

```text
上传/异步任务身份 → 图像安全检查 → 本地OCR分类/overview+footer复合图
  → 单次Provider请求 → 原始响应提取/一次性格式契约
  ├─ Provider DMS诊断
  ├─ 分组DMS规范化
  ├─ 投影表解析 → 本地CRS/轴序绑定
  └─ 本地多表示源表绑定
  → unified acquisition（仍将选中证据变成文本再解析）
  → 多个早退分支/家族路由/投影优先/通用DMS等
  → V2归一化/轴序评分/几何 → verification/finalizer
  → res.json装饰器再次协调acquisition/final authorization/注册结果
  → 独立review billing authority + usage settle
  → 识别页状态、地图消费、KML下载、详情、Toast、核对动作
```

server.js约2.1万行、index.html约1.6万行，不是单凭行数就有错；问题在于同一业务事实被多个阶段重复解释，职责边界穿透，缺少完整、可重放的中间结果。

## 4. 模块/重复逻辑清单

生产标记“活动”表示从当前服务或页面有可达调用，不表示每次请求都走；“条件”表示依赖路由/开关。覆盖列仅列相关测试，不代表充分。

| 文件及函数/模块 | 入口/调用方；输入→输出 | 状态所有者/生产用途 | 覆盖与保留/合并/退役依据 |
| --- | --- | --- | --- |
| coordinate-image-safety.js / createCoordinateImageIdentity、validateCoordinateImageUpload | recognize入口；文件→hash/尺寸/校验 | 服务端入口；活动 | image-safety/core；保留，扩展page/region/table，不以图像hash代替表身份 |
| recognition-acquisition-job-runtime.js | async路由；任务→结果/截止时间 | 任务运行时；活动 | async-v5/v6；保留，统一resultRevision，迁移时测并发/取消/晚到结果 |
| projected-source-evidence.js / createLocalOcrClassificationImage | 本地OCR前；原图→overview+bottom28%复合图 | OCR采集；活动 | projected-crs；保留现功能，提出区域定位迁移；固定页底策略可能漏掉页中表 |
| server.js / Provider调用与extractProviderMessageText | recognize；响应→text | Provider适配层；活动 | recovery/http；保留一次调用边界，收敛为版本化响应适配器 |
| server.js / extractProviderDmsReviewEvidence、parseProviderDmsPair | rawText→逐行DMS诊断 | DMS候选；活动 | review/acquisition/recovery；与下两项重叠，先差分再迁移，不直接删除 |
| recognition-review-result.js / normalizeProviderDmsReviewResult | rawText→分组review结果 | DMS组模型；活动 | review-v2；保留群组能力，移到统一table IR适配器 |
| recognition-candidate-evidence.js / 候选行提取 | raw或重新拼接文本→候选/拒绝项 | unified acquisition；活动 | markdown/acquisition；目标禁止已解析值反复文本化重解析 |
| local-ocr-map-layout-classifier.js / extractProviderProjectedCoordinateEvidence | rawText→X/Y、CRS、DMS参考、counts | 投影候选；活动 | projected-crs/recovery；保留解析能力，输出字段span和拒绝原因 |
| projected-source-evidence.js / extractProjectedSourceContext、bindProjectedEvidenceToSourceContext | OCR+Provider+image→CRS/axis绑定 | 源证据；活动 | projected-crs；同图检查保留，增加table/region限定、datum独立字段；不默认UTM就是WGS84 |
| multi-representation-source-evidence.js / normalizeProjectedColumn、orderContinuousRows、bindProviderRepresentationsToSource | 本地完整表+候选→绑定/冲突 | 多表示证据；活动 | multi-representation；不能安全删除，先取消无来源数字推断的迁移方案，保留原顺序证据 |
| recognition-first-acquisition.js / buildRecognitionAcquisitionEvidence | 多解析输出→再解析统一候选 | 采集状态；活动 | acquisition-v3/v4/v6；合并目标是直接消费IR，而不是再创一套解析器 |
| server.js / buildExplicitProjectedBoundaryAutoReleaseEngine | 投影候选→engine或null | 投影放行；活动 | recovery；保留校验，迁移为具名决策/字段原因，禁止静默null |
| family-primary-routing.js、server.js家族路由 | 文本/版式→结构与优先候选 | 历史路由；条件活动 | recovery/golden；多处国家/家族判断仍存在，本次不新增或删改；必须完整矩阵再退役 |
| server.js / getCoordinateEngineV2ContextualProfile、buildCoordinateEngineV2ValidationReport | 类型/原文/可含文件名上下文/几何→评分及轴序解释 | V2验证；活动 | recovery/authority；存在国家上下文评分和候选换轴，迁移时与显式axis证据划清边界 |
| projection/utm.js、bftm.js；index.html投影函数 | 源值→WGS84 | 服务端转换及前端手工路径 | projected-authorization/core；服务端数学模块保留；前端不能静默重算图片最终结果，手工输入需独立受控入口 |
| coordinate-finalizer/、verification/ | V2/legacy→identity/geometry/gates | 最终授权核心；活动 | output-contract/finalizer/core；优先复用，不新造第三套finalizer |
| server.js / attachUnifiedRecognitionAcquisition | res.json前；payload→重协调/可能再finalize | 最终response；活动 | final-fail-close/acquisition；目标只消费一次决策，迁移前保留双重检查 |
| coordinate-usage-atomicity.js、agentic-coordinate-usage-authority.js | 输出/身份→eligible/settle | 计费；普通活动、agentic条件 | atomicity/core/http；保留幂等/封装，交付口径需产品确认后迁移 |
| index.html / status、showMessage、showToast、uploadSupport、review panel | 各种响应和消息→UI | 多个前端状态所有者；活动 | p08h/display/output；统一viewModel，一次渲染一个提示；字符串反推状态退役前要覆盖真实事件链 |
| index.html地图/KML/核对、map-preview-adapter.js | result tuple→地图/下载/ack | 消费者；活动 | output-contract/HTTP；保留版本/几何hash，统一服务端capabilities，ack不能变成授权 |
| scripts/production-recognition-recovery-p0-regression.js | 函数VM+HTTP mocks→134项断言 | 测试，无生产入口 | 保留；其prompt测试调用生产未调用的buildProjectedTableOcrAcquisitionPrompt，不能当该prompt上线证明 |
| scripts/p08h-*、recognition-projected-authorization-v8-* | 抽取函数/简化DOM→UI断言 | 测试 | 保留；历史沙箱缺依赖暴露耦合，增加真实浏览器事件流，不只改字符串契约 |
| docs/CANONICAL_COORDINATE_RESULT_CONTRACT.md等 | 旧提交治理描述 | 历史契约，不是运行状态 | 保留审计记录；旧文档pending阻断KML与当前未确认输出不同，需版本化迁移，不覆盖历史事实 |
| scripts/sr08c-build-release-evidence.js及release-governance | 发布证据→绑定/核验 | 发布工具，不属识别入口 | 保留；本次未找到能完整复现最近原子发布流程的统一版本化部署脚本，不能宣称所有临时脚本已盘清 |

不是所有旧模块都冗余：OCR与视觉模型是不同证据；DMS与X/Y是不同表示；服务端安全校验与前端版本核验也不是可任意删掉的重复。

## 5. 已发现问题/未来风险（按严重程度）

| 级别 | 问题与证据 | 未来表现 / 处理方向 |
| --- | --- | --- |
| 高 | 字段数字差异存在，缺逐阶段来源记录 | 新图片错误数字可成为“接受行”；先来源链及差分，不能相信行数 |
| 高 | 多种投影失败被折叠为null，再清空CRS | 反复修错位置；应返回具体字段、原因、残差和原证据 |
| 高 | 同图不等于同表：CRS/axis全局文本匹配，无稳定region/table归属 | 多表、多个CRS、页眉页脚混入时误绑定或误阻断 |
| 高 | UTM zone+hemisphere直接映射326/327，未强制独立datum证据 | 非WGS84的UTM图可能定位偏移；需要明确datum/单位/转换支持矩阵 |
| 高 | 候选选择多处优先级和早退，完整DMS不必能到最终输出 | Provider输出格式微变就走另一条路径；应一次统一仲裁 |
| 高 | 解析成功可计次但用户定位/下载失败 | 用户反复付费试错；独立定义任务交付指标、计费资格与恢复策略 |
| 高 | 缺原始响应/中间产物的受控重放包 | 当前故障无法精确归因；下一阶段只做最小诊断而不是立刻重构 |
| 中 | 固定底部28%OCR增强；布局变体未分层测量 | 页中、旋转、多栏、跨页表可能取不到完整源证据 |
| 中 | 1..n数字点号门槛、自动排序、同列小数推断、历史家族评分 | 字母/非连续但真实点号、闭合重复首点、精度混合可能误处理；需显式语义策略 |
| 中 | 1e-6度固定交叉容差未携带源精度/舍入记录 | 合法舍入差可被阻断；不能先放宽阈值，需误差预算设计与验证 |
| 中 | 空间事实不可用与坐标有效混在一个面板 | 图能画但中心/点数破折号，用户分不清主任务失败与附加信息缺失 |
| 中 | status/Toast/ack各自驱动，旧文档与新UI语义不一致 | 核对完成仍弹“核对”，刷新/返回/晚到请求重复提示 |
| 中 | 静态字符串断言/VM常量缺失/只测候选不测下载产物 | 测试绿不能发现真实端到端失败；测试结构本身需治理 |
| 中 | 发布过程有临时命令，runtimeSourceSha不是整树完整性证明 | 元数据看似一致但工件/启动命令不同；将来需内容清单、权限分离、版本与回滚测试 |
| 待证实 | 并发重复上传、过期revision、断线重连、过期结果、重启丢缓存、取消后晚到response、超时成本 | 当前未证明有新故障，需故障注入和真实浏览器生命周期测试 |
| 待证实 | 模型随机性/截断/说明文字/低清/反光/多语言/隐私留存 | 离线mock不能说明真实Provider准确率；将来另行有界资格验证，不能此阶段调用 |

安全阻断本身并非坏事。不能以“按钮可点”为验收标准，更不能把错误的完整Polygon导出作为问题解决。

## 6. 统一框架（设计，不是本次行为修改）

沿用已有finalizer、结果仓库、projection数学模块、图像身份和使用次数幂等实现。不要再新增一个并行“V4/V5引擎”与旧链路竞争。通过适配器逐步让一个版本化结果成为唯一事实源。

### 6.1 拟议结果模型

```text
CoordinateResult (新主版本；命名在迁移评审时确定)
  schemaVersion / resultId / resultRevision / evidenceRevision / geometryHash
  source: imageHash, pageId, regionId, tableId, acquisitionId, provider/model version
  evidence: 原始响应及OCR的受控引用/hash、源span/bbox、表头、源行序
  rows[]:
    rowId, originalRowIndex, pointLabel(raw/normalized/sourceRef)
    representations: projected{x,y}, geographic{latitude,longitude}
    每字段: rawLexeme, normalizedValue, unit, sourceRef, parseAction, conflictRefs
    acceptance: parsed / sourceBound / consistent / rejected / unresolved
  sourceReference: projection, datum, zone, hemisphere, axisOrder, unit, evidenceRefs
  transform: sourceReferenceRef, target EPSG:4326, algorithm/version, inputHash, residuals
  completeness: observed, expected(if evidenced), accepted, rejected, missingIds, orderConflicts
  geometry: type, coordinates, completeness, validationReasons
  decision: map, unverifiedKml, officialExport, blockers[], warnings[], recoveries[]
  review: required, acknowledged{resultId,revision,geometryHash}, formalAuthorization
  delivery: extracted / previewable / downloadable / blocked / failed
  billing: eligibilityPolicyVersion, eligible, commitState, idempotencyKey
```

不存储密钥、认证头或账户信息到证据。原始响应默认不进入公共日志；受限本地重放包的访问/保留期限/删除规则先定，再实施。内容hash不能代替原文重放，也不能代替空间归属。

### 6.2 唯一数据流与职责

1. 采集适配器：只采集一次，保留响应封装，不决定Map/KML。
2. 表格规范化：JSON/普通/Markdown是序列化适配器，输出同一行模型；只记录有根据的格式规范化，禁止补数字。
3. 证据绑定：image+page+region+table+pointLabel+sourceOrder，缺身份不能按行数补配。
4. 表示协调：同一行比较DMS与XY，冲突不覆盖；CRS/axis/单位单独来源，转换有记录。
5. 几何构造：完整边界/不完整预览明确区分，缺行不自动闭合Polygon。未知geometry保持Unknown。
6. 最终决策：在现有finalizer边界内实现一次纯决策；上游各模块只出事实/诊断，下游不得追加授权解释或重解析已定稿文本。
7. 前端单一viewModel：坐标编辑区仅源坐标；一个状态提示；具体问题行和恢复动作；地图/下载仅消费同一identity/revision/capability。
8. 使用次数：事务/幂等实现不变；资格由明确的交付策略决定，不由COMPLETE字段巧合推导。

### 6.3 状态表与恢复闭环

| 状态 | 地图/KML | 唯一主提示/动作 | 权限 |
| --- | --- | --- | --- |
| 同图同表证据完整、转换和几何有效、待核对 | 临时地图+未确认KML | 轻量核对；ack绑定当前版本 | REVIEW_REQUIRED/pending/needs_review不提升正式授权 |
| 用户已核对相同版本 | 与上行一致 | 已核对，仍是未确认输出 | ack不是formalAuthorization，也不是导出完成 |
| 缺行/值/方向/CRS冲突 | 无完整Polygon/KML；若另行设计不完整预览，必须显著标注 | 具体行/字段、修改或重识别/协助；无核对完成按钮 | 不以点击确认绕过校验 |
| 采集失败 | 无当前新结果 | 失败原因类别、未扣次/实际结算状态、单一恢复入口 | 不把上次成功身份混作本次结果 |
| 修订/重新识别 | 旧ack失效，按新revision重算 | 当前修订说明 | 旧下载/地图请求拒绝过期版本 |

原始投影CRS与输出EPSG:4326分列显示；DMS直接选中时不得谎称该DMS是由XY转换而来。双表示交叉校验与实际转换路径分别列明。

建议交付口径：可准确提取但不能定位=部分交付；支持范围内完整结果能预览且未确认KML实际下载成功=核心任务可用。计费是否包含部分交付须明确产品决策，不能在这次清理中暗改。

## 7. 有证据的清理与保留

先完成以上架构清单与目标设计，再执行以下清理。只删除server.js四个本地函数声明：

| 旧函数 | 实际调用/动态入口检查 | 保留实现/映射 | 等价理由 |
| --- | --- | --- | --- |
| groupEveryFourDmsLinesWhenLikely | 全仓库仅声明，未导出、非路由/事件回调 | 无替代调用；保留现有显式分组路径 | 未被调用的4行猜组函数，删除不改变执行路径 |
| looksLikeProjectedContext | 同上 | 无替代调用；保留extractProviderProjectedCoordinateEvidence等活动检测 | 不把语义不同检测合并冒充等价 |
| hasCompleteProviderDmsPair | 同上 | 保留parseProviderDmsPair及实际调用者 | 删除未使用Boolean包装 |
| normalizeCommaDmsCoordinateDisplayOrder | 同上 | 保留现有DMS展示和源表示模块 | 无调用的≥8行触发重排旧函数，不迁移其规则 |

除了全仓库引用搜索，还检查了server.js ESM无导出这些函数、无eval/new Function或按名global分派、路由/事件使用及scripts抽取函数用途。生产文件无读取server.js源码后反射调用的入口。测试有抽取全部声明的VM，但没有调用这四个函数。新增等价检验从git基线精确移除四个声明，要求与清理后server.js全文一致，防止误删邻近代码。

不清理：buildProjectedTableOcrAcquisitionPrompt仍被有效回归直接测试；其生产未调用不是删除测试契约的充分条件。其他孤立函数、家族解析、前端转换、Agentic/V3/兼容路径、release脚本、历史文档、回滚材料均保留：需要更广动态使用证据或行为迁移授权。

## 8. 验证记录与局限

准备阶段离线探针曾因VM遗漏产品常量coordinateEngineV2CountryProfiles、spatialKnowledgeBaseCache发生两次启动异常，未进入断言、非产品失败、非超时。已纳入实际产品常量和只读空间知识依赖，无固定成功桩。此事实本身说明抽取函数沙箱脆弱；不能用它取代完整HTTP/浏览器验收。

正式验收顺序执行一次，共14个进程，全部退出码0，无断言失败、无实际超时。runner保存末尾输出、退出码、时长和超时标志，不因输出通道中断丢失终态；没有自动重试。

| 验证 | 结果 |
| --- | --- |
| server.js语法检查 | PASS |
| recognition-architecture-cleanup-regression | 四声明精确删除、其余全文不变；18/18跨区带/半球行为对照PASS |
| projected-crs-source-evidence-regression | PASS；含既有本地OCR样本 |
| multi-representation-source-evidence-regression | PASS，6种Provider输出变体 |
| coordinate-markdown-table-regression | PASS |
| recognition-first-review-result-v2-regression | PASS |
| recognition-first-acquisition-evidence-v3-regression | PASS |
| multi-representation-http-regression | PASS；本地mock一次，真实Provider 0 |
| p08h-confirmation-ui-lifecycle-regression | 22项PASS |
| source-coordinate-review-display-regression | 32/32 PASS |
| review-output-contract-regression | 39/39 PASS |
| recognition-projected-authorization-v8-regression | PASS |
| production-recognition-recovery-p0-regression（未跳过HTTP） | 134/134 PASS |
| production-core-capability-closure-p0-regression | 42/42 PASS |

离线进程清空继承的Provider/Supabase/使用次数等密钥变量，并禁止dotenv载入真实环境文件；现有HTTP回归自行使用的local-mock-only虚拟字符串不是实际密钥。审计preload拒绝非loopback socket并向替换env的Node子进程传递防护。真实Provider调用0、生产识别任务0、图片上传0；本次只读版本/日志访问不属于Provider调用。没有改动生产环境文件。

结果回执：本隔离目录 `Temp/recognition-architecture-audit/results.json`（忽略路径，保留未删除）。SHA-256：`cea468b232e194d9ad92490a2dfc69652cf94ba7477f7c7c17041dfe16b56459`。

修改文件共6个（均未暂存/未提交）：

- `server.js`：仅61行删除，四个无调用声明；没有新增产品规则。
- `docs/recognition-architecture-audit-2026-09-30.md`：本报告。
- `scripts/recognition-architecture-probe.js`：执行现有产品函数的离线诊断，非生产入口。
- `scripts/recognition-architecture-cleanup-regression.js`：基线全文等价及安全分支差分断言。
- `scripts/recognition-architecture-audit-runner.js`：顺序、一次、有界输出及退出码回执。
- `scripts/recognition-audit-offline-guard.cjs`：测试进程网络隔离辅助，不被生产导入。

Git HEAD仍为精确基线，分支 `codex/recognition-architecture-audit`；`git diff --check`通过，package.json/package-lock.json/index.html未改。没有提交、推送、PR、合并、部署或删除既有用户文件。新增测试脚本数量不是新增生产实现；它们仅由本次离线runner调用，不进入应用启动链。

即使134/134与42/42通过，也只证明这些回归和输入下行为保持，不证明03生产请求已被重放，不证明任意图片准确率，不证明真实Provider稳定性，不证明最终用户可用性。Mock Provider调用不是实际Provider调用。

## 9. 分阶段实施路线及退出标准

| 阶段 | 实施范围 | 明确退出标准 / 回退 |
| --- | --- | --- |
| A 当前审计与无行为清理 | 本报告、4个死函数、等价证据、离线回归 | 全文差分仅声明删除；既有门禁通过；不改生产。Git可恢复四声明，无外部状态迁移 |
| B 诊断和可重放性 | 统一阶段快照、字段级reason、来源hash/span、受控离线重放包；不变判定/路由/计费 | 一条失败可定位最早差异；原输入→归一化→绑定→选择→最终决定可重放；敏感信息检查；新旧结果决策完全一致。先本地，禁真实调用/发布 |
| C 结果契约与影子仲裁 | 同一表IR及唯一decision设计；先影子并行，不授予生产权限 | 覆盖各支持格式/CRS/方向/行序/版本/冲突的差分，所有差异分类；无未解释安全差异；保留旧路径可切回 |
| D 小步迁移 | 一类适配器一次迁移，移除重复文本化及冲突后清CRS，UI只读统一状态 | 真实浏览器从上传mock到文件下载/返回/修订全链路通过；错误定位字段可修复；没有过期输出；每步独立可回退 |
| E 计费口径协调 | 明确部分交付/可用交付，保留事务幂等 | 超时/重复提交/失败/修订/恢复无重复结算；用户可见结果与费用一致；需独立产品确认 |
| F 资格与发布 | 代表性保留集、盲测、单次Provider有界资格，统一发布脚本/工件校验 | 预先定义整表/逐字段准确率、可用率、误放行、误阻断、时延/成本；不得以回归数量替代。真实Provider、计费、生产发布另行授权 |
| G 旧路径退役 | 删除已迁移适配器/重复状态/过期临时工具 | 调用图、生产观测、契约及回归均证明无消费者；先列精确清单，保留审计/Golden/回滚材料 |

下一步优先B，不是再改03的坐标或强行开放按钮。新图不应需要一份产品规则；应落在已定义表示/版式/证据契约内，失败可解释可恢复。没有足够证据时应承认未知，而不是继续靠生产上传碰运气。
