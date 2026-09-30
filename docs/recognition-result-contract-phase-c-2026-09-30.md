# 阶段C统一坐标结果契约与离线影子对照报告

日期：2026-09-30。结论：本次授权的离线观测契约及受测对照通过，尚未迁移生产识别入口，也不是03识别问题修复或发布完成。

新增36项回归通过。32组正常观测包各执行9个既有产品阶段调用重放，共288项结果匹配；另4组验证状态隔离、篡改检测、未知证据及隐私隔离。所有受测适配差异有解释，正常对照中未解释安全差异为0。134/134完整离线验收、42/42核心回归和受影响专项均通过。真实Provider调用0。

最重要的新证据是：同一合成JSON在分组DMS入口可得16行，在原始统一候选入口得0行；混合表格在两个入口呈相反的覆盖差异。它证实入口契约不同，而不是证明03生产那一次请求走了哪个入口。不能据此宣布历史根因已经找到，也不能把所有入口改为取行数最多者。

## 1 保护基线及本次边界

隔离工作树：`C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9`。

分支：`codex/recognition-architecture-audit`。HEAD保持 `86ea4ac44a6d44a2332d7aaaffb1926504a73dca`。本次开始时的12个未提交文件完整保留；除授权扩展的离线runner外，原有文件内容哈希保持一致。生产代码、前端、依赖和阶段A/B报告与回执均未修改。

未暂存、提交、推送、创建PR、合并或部署；未访问生产服务、上传生产图片、创建生产识别任务或调用真实Provider/Supabase。离线runner清空继承的Provider、Supabase、使用次数相关密钥变量，禁止dotenv读取生产环境文件，沿用非loopback网络拒绝防护。既有HTTP测试的 `local-mock-only` 是虚拟值，不是真实密钥。

## 2 统一契约的定义

当前实现版本为 `coordinate_result_observation_v1`，模式固定为 `OFFLINE_OBSERVATION_ONLY`。这是统一的观测模型，不是替代finalizer，也不具有授予地图、KML、核对或计费权限的能力。

每个字段统一表示为：

```text
status   OBSERVED 或 UNKNOWN
presence PRESENT 或 ABSENT
value    原产品记录的值，保持类型和精度，不修补或重新解析
source   会话下标、事件下标、阶段、操作名、JSON Pointer
reason   未采集、显式null、证据删减等原因
```

`false`、`0`、空数组是已观察到的值，不会当作缺失；显式null与不存在分别记录。OBSERVED只表示“在这个产品阶段看到该值”，不表示“经原图独立验证正确”。字段来源首先精确指向产品输出；没有原始字符位置证据的，字符span保持UNKNOWN。

| 契约部分 | 来源与内容 | 不允许的推导 |
| --- | --- | --- |
| identity | image_identity中的图片hash、page、regionId、tableId | 不把图片hash当物理表ID；region/table缺失保持未知 |
| rawEvidence | 原Provider内容封装与extractProviderMessageText结果分开 | 不以最终坐标文本冒充原始响应 |
| localOcr | 原有本地OCR文本和来源证明 | 不执行新的OCR，不用样本真值替换数字 |
| candidateInput | 原始文本、实际候选输入、既有选择来源、可见CRS | 记录选中的路径，但不改变它 |
| rowSets | 分组DMS、投影提取、投影绑定、统一候选、既有多表示协调分别成组 | 不把不同解析器的行数相加，不按行数或数组位置合并两种表示 |
| rows.fields | 逐字段保留点号、源行号、值、源字符串、轴序及既有附加字段 | 不取近似值替换源值，不增补缺点、点号或方向 |
| crs | Provider原始CRS、绑定后CRS、区带、半球、datum、轴序、交付时原始与目标CRS | 不从EPSG名称反向补造原图datum字段；缺datum明示未知 |
| conversions | 原产品转换记录、目标EPSG:4326、输出点、几何检查、DMS残差及容差 | 不把DMS直接选用说成由X/Y转换；不重新转换选取另一坐标 |
| bindings与stageRecords | 既有同图/表示绑定及全部保留阶段记录 | 后续清空CRS不覆盖上游原始CRS；多次交付不抹掉前一次身份 |

`rowSets.rows.originalSourceOrder`保留的是既有解析器报告的`sourceLineNumber`，不是经过图像定位验证的物理行号。尤其JSON解析器中的行号可能是结构化数组序号。`observedOrder`仅为该次输出顺序，两者不互相替代。物理表ID、原图单元格位置和精确字符span尚未建立，已在契约中显式标为未知。

当前不强行合并这些rowSets为一个已验证物理表。未来的统一表适配器应在同图、同区域、同表、点号及行序都有来源时建立关联；证据缺失时保留各自表示及冲突，不制造确定性。

## 3 最终决策各维度独立

| 维度 | 字段与来源 | 含义 |
| --- | --- | --- |
| technical | 原finalized geometry、crs、technicalKmlReady及转换检查 | 技术可用性观察，不等于授权 |
| evidenceIntegrity | 原候选诊断、拒绝行、未绑定行、reviewReasons、hasUnifiedEvidence | 语法接受、行绑定、值正确性不能混为一谈 |
| temporaryOutputs | 最终交付mapReady/kmlReady/status与授权函数计算值分别记录 | 不默认开放，不自行生成KML |
| userReview | 原confirmationStatus、requiresReview；ack未采集则未知 | 待核对与用户已核对分开；不提升正式授权 |
| formalAuthorization | 原authorized、decisionState、qualityGateStatus、sourceAuthority及原因 | 只有观察值，无新的授权判定 |
| usage | 原usageConsumed/userUsageConsumed | 是返回字段，不代表查过账或验证了事务 |
| resultIdentity | 原resultId、resultRevision、geometryHash | 多次交付冲突可见，不合并成同一结果 |
| userTaskSuccess | 当前为UNKNOWN | 可提取、可定位、下载完成、任务成功和扣次不是同一个状态 |

离线反例验证：在真实授权函数对合成待核对几何的计算中，临时地图/KML可用、正式授权为false、pending及已扣次字段能同时保留；适配器没有把它们互相替代。另验证多个交付事件的身份或版本不同时报告冲突，交付能力与最近记录的授权能力不一致时报DIFFERENT。没有模拟正式授权成功。

这些一致性检查仅产生诊断，不修改产品结果。多个交付身份的冲突只是被解释并记录，不意味着过期请求在生产已经通过新工具获得阻断。

## 4 离线工具与对照方式

`scripts/coordinate-result-shadow.js`只接受阶段B诊断重放包，不解析任意坐标文本。它没有新的图像、坐标或CRS识别引擎，也不被生产文件导入。

工具先检查诊断包的隐私及结构限制，逐字段映射到统一契约，再调用阶段B重放器执行原产品函数，验证记录的阶段输出。最后分别报告适配差异、产品重放差异、跨阶段矛盾和未知证据。源码内容摘要缺失/不匹配返回UNKNOWN；截断或删减返回PARTIAL；改变坐标字段或权限字段会被检测为DIFFERENT。仅当受录阶段与适配保持一致时，返回MATCHED_OBSERVATIONS，不叫“生产识别成功”。

对照局限：适配结构检查复用同一声明式映射，不能独立证明每个未来字段都被正确设计。因此新增测试还独立比较每行每字段与原产品输出、执行旧基线和当前采集模块等价对照，并注入值/权限篡改验证检测能力。即便如此，仍不能替代完整生产路由和浏览器验证。

本地命令示例，下面命令没有对生产执行任何操作：

```powershell
node scripts/coordinate-result-shadow.js --missing-history
node scripts/coordinate-result-shadow.js --input "<受控本地阶段B重放包.json>" --out "<新的本地输出路径.json>"
```

只允许本地文件，输入上限8MiB，输出使用不覆盖写入；标准输出只显示状态和计数，不打印原始坐标或证据。显式指定输出路径时，文件会包含经过阶段B隐私检查的原始证据，仍应本地受控保存，不能发往公开日志。原自由文本筛选不构成“任何个人信息都不可能出现”的证明。

本次新增测试是产品阶段调用的离线组合，不冒充完整recognizeCoordinatesHandler路由。多数采集样例以空finalized body调用授权函数，出现的“Provider次数缺失、几何未完成”等原因只表示该测试没有提供最终路由上下文，不作为生产缺陷证据。另行通过的阶段B HTTP回归执行真实本地HTTP处理器，但没有因此宣称新契约覆盖了所有HTTP/异步时序。

## 5 受测差异及未知清单

| 项目 | 实际结果与解释 | 分类 |
| --- | --- | --- |
| 普通DMS | 分组与原始候选入口均保留完整行；不产生适配值变化 | 一致 |
| 紧凑JSON、带说明的冗长JSON | 分组DMS各16行，原始候选入口各0行；前者有结构化JSON解析，后者只按行处理；原始候选最终授权记录采集不完整 | 已解释覆盖差异，未修复 |
| 普通X/Y及缺CRS的X/Y | DMS专用入口0行、原始候选16行；格式职责不同，不能比较后直接选较大值 | 已解释覆盖差异 |
| 混合普通表、混合Markdown | 分组DMS0行、原始候选16行；原始候选支持带X/Y附加列的DMS行，分组解析入口契约不同 | 已解释覆盖差异，迁移时必须明确 |
| 缺行、重复点号 | 既有候选reviewReasons保留；多表示绑定记录行数冲突 | 原产品阻断证据原样保留 |
| 点序冲突 | 保留原序；既有连续点序或表示绑定检查失败，不自动重排 | 原产品阻断证据原样保留 |
| 方向冲突、额外字段 | 原解析器拒绝行及拒绝原因保留；不丢弃额外字段后冒称成功 | 原产品拒绝证据原样保留 |
| 数字近似但不同 | 改动一个合成X值，提取值仍是改动后的值；绑定冲突及DMS残差检查可定位，不以源值覆盖 | 已解释数值冲突 |
| CRS冲突 | 原Provider EXPLICIT CRS保留，后续绑定CONFLICT/未绑定同时保留 | 已解释CRS冲突 |
| 轴序冲突 | 记录axisConflict与未绑定，不能改写成“唯一缺CRS” | 已解释轴序冲突 |
| 图片身份冲突 | 不同hash的源上下文记录IMAGE_IDENTITY_MISMATCH | 已解释身份冲突 |
| 跨3区带、南北半球、6/9/16行 | 完整投影输入原校验返回引擎；参考差异、缺轴、缺CRS、点序异常分别保留原检查与字段 | 15组原产品结果一致，无新增点数规则 |
| 结果身份/版本冲突 | 2次合成交付的ID/revision差异明确报告；两次原记录都保留 | 负例检测通过 |
| 值/权限篡改、阶段输出篡改 | 非等价值产生DIFFERENT；不以记录存在替代正确性 | 负例检测通过 |
| 缺历史、源码漂移、证据删减 | 分别UNKNOWN、UNKNOWN、PARTIAL | 缺失不等于通过 |
| 区域、物理表ID、原图单元格和字符span | 当前诊断未提供，适配器不能恢复 | 已知证据缺口，需后续采集设计 |
| 真实核对点击、下载完成、计费事务 | 当前诊断未记录完整生命周期 | UNKNOWN，不推断任务成功 |

32组正常观测包的全部逐项结果保存在 `Temp/recognition-result-phase-c/comparison-results.json`。原产品检查失败不等于适配器失败：本阶段要求这些失败保持、可定位，不要求把它们放行为可用结果。4组反例中预期DIFFERENT/UNKNOWN是测试成功条件，没有将这些条件改为忽略。

现有证据下，没有观察到未解释的适配值改变或授权提升。未执行或证据不足的风险不在这个结论内：历史03最早数字差异、复杂多表视觉分区、所有支持CRS/格式的穷尽覆盖、并发晚到结果、用户编辑后的浏览器行为及真实结算，均未因此完成验证。

## 6 全部验收结果

本次只执行一次 `node scripts/recognition-architecture-audit-runner.js --phase-c`。独立回执路径，顺序执行，首次非零退出/实际超时即停止，无自动重试。23个进程全部退出0、无超时。

| 项目 | 结果 |
| --- | --- |
| 2项新增脚本语法及6项原阶段B语法 | 8项PASS |
| coordinate-result-shadow-regression | 36/36 PASS |
| recognition-diagnostics-regression | PASS；18基线安全、10采集、7投影及隐私/重放/隔离 |
| recognition-diagnostics-http-regression | PASS；DMS/X/Y基线、关闭/开启诊断业务一致，mock 6次 |
| projected-crs-source-evidence-regression | PASS；既有25dpi转70dpi警告，不是失败 |
| multi-representation-source-evidence-regression | PASS；6种变体 |
| coordinate-markdown-table-regression | PASS |
| recognition-first-review-result-v2-regression | PASS |
| recognition-first-acquisition-evidence-v3-regression | PASS；既有日志名v4 |
| multi-representation-http-regression | PASS；mock 1次 |
| p08h-confirmation-ui-lifecycle-regression | 22项PASS |
| source-coordinate-review-display-regression | 32/32 PASS |
| review-output-contract-regression | 39/39 PASS |
| recognition-projected-authorization-v8-regression | PASS |
| production-recognition-recovery-p0-regression | 134/134 PASS，24344ms |
| production-core-capability-closure-p0-regression | 42/42 PASS，12359ms |

真实Provider调用0；真实Supabase调用0。以上数量是受测输入和契约门禁，不是识别准确率、生产可用率或真实模型稳定性。

回执及SHA-256：

- `Temp/recognition-result-phase-c/results.json`：`e5464750b08f3c091292d30cf6fe7beeaaa175be0c6cd219c85c2ff429c65806`。
- `Temp/recognition-result-phase-c/comparison-results.json`：`90a96c30a70e8395fd2d0b35e300d46010a8545b9d1727af3820c8861095b057`。
- 原阶段A、阶段B失败及阶段B恢复回执哈希与开始时相同，全部保留。

## 7 修改文件和保留模块

本次只新增或修改4个离线文件：

| 文件 | 本次变化 |
| --- | --- |
| scripts/coordinate-result-shadow.js | 新增统一观测契约、来源指针、离线适配、产品重放对照及CLI |
| scripts/coordinate-result-shadow-regression.js | 新增36项合成行为级对照和篡改/未知/隐私反例 |
| scripts/recognition-architecture-audit-runner.js | 新增互斥的阶段C入口、独立回执与执行计划，旧A/B入口保留 |
| docs/recognition-result-contract-phase-c-2026-09-30.md | 本报告、契约、差异和迁移方案 |

Git共15个未提交文件：2个既有已跟踪产品修改、13个未跟踪文件。原12个文件保留；新增2脚本和1报告。`git diff --check`通过，暂存区为空，分支/HEAD不变。`server.js`、采集模块、诊断模块、index.html、package.json及package-lock.json内容哈希均与本阶段开始时相同。

| 既有模块 | 当前适配位置 | 保留或未来迁移方式 |
| --- | --- | --- |
| extractProviderMessageText | rawEvidence | 保留单次采集与内容封装，未来统一格式入口直接消费原证据 |
| normalizeProviderDmsReviewResult | DMS_GROUP/DMS_UNBOUND rowSets | 保留JSON与显式分组能力；先迁移序列化适配，不能整段删除 |
| extractRecognitionCandidateEvidence | UNIFIED_CANDIDATES rowSets | 保留格式/拒绝/行序能力；后续改为消费结构化行，逐步消除文本化重解析 |
| 投影提取与bindProjectedEvidenceToSourceContext | PROJECTED_EXTRACTED/BOUND及crs | 保留校验，原始CRS与绑定失败并存；不得因安全拒绝覆盖原证据 |
| bindProviderRepresentationsToSource | bindings、PRODUCT_RECONCILED、LOCAL_SOURCE_TABLE | 保留实际绑定结果，需新增物理表及字段来源后才能合并生产行模型 |
| buildExplicitProjectedBoundaryAutoReleaseEngine、utmToWgs84等 | conversions和recordedChecks | 复用实际转换与几何校验，不在影子模型重写算法 |
| evaluateUnifiedRecognitionFinalAuthorization及finalizer | decision各维度 | 未来一次最终决策的保留边界；本次不替换、不额外授予能力 |
| 现有计费、地图/KML、核对UI | 只观察已记录返回字段 | 本次完全不改；下载/编辑/过期版本/核对需HTTP与浏览器验证后迁移 |

没有删除任何仍活动代码、有效回归、样本或回滚材料。新文件不导入生产链路，也不成为另一套识别引擎。

## 8 下一阶段迁移与退役方案

建议进入阶段D1的一个入口迁移，而非整站重写或立即发布。顺序如下：

| 小阶段 | 改动范围 | 退出标准 | 回退与退役 |
| --- | --- | --- | --- |
| D1 序列化输入及字段来源 | JSON/普通表/Markdown输出同一字段证据行；只迁移一个采集入口，保留旧入口作差分基线 | 原始字面值及来源可追踪，完整表不因序列化丢行；缺字段/冲突不补值；所有行为差异有明确理由 | 旧入口保持可调用；回退该入口接线，不改最终权限；暂不删旧解析器 |
| D2 同图同表表示协调 | 给同图、表/区域、点号、行序绑定明确来源；两种表示保留原始值及残差 | 不按数量配对；含重复/多表/错位/方向/CRS冲突负例；无未解释安全差异 | 保留旧绑定路径至行为对照及真实浏览器门禁完成 |
| D3 唯一最终决策及前端消费 | 复用现有finalizer，前端只消费同一identity/revision及明确能力 | 本地HTTP到地图、下载、返回、编辑、过期请求全链路通过；ack不提升正式权限 | 一次迁移一个消费者；旧路径仅在无人引用且回归覆盖后列清单退役 |
| E 计费口径 | 单独确认部分交付与可用交付的扣次政策 | 幂等、重复提交、失败/恢复/修订无重复结算；账务口径另行确认 | 不随D阶段暗改；保留现有事务实现 |
| F与G 资格发布及旧路径清理 | 代表性保留集、资格测试、发布验证，之后清理已迁移路径 | 预先定义整表/字段准确率、误放行/误阻断与延迟目标；生产操作另行授权 | 精确发布回滚；删除前再次核对动态调用、兼容入口及测试用途 |

本次阶段C退出标准仅在上述受测范围内满足：观测适配无未解释安全差异，差异及未知均明确报告。并不代表完整目标架构已落地。下一阶段需要新的、有界的D1产品入口修改授权；本次不继续实施。用户不需要再次上传图片试错。
