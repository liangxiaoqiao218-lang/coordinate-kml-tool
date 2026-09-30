# 阶段D1 Markdown字段绑定修复与HTTP停止报告

日期：2026-09-30

## 结论

Markdown下游消费修复已通过本地行为验证：本轮25/25通过，历史4项按要求跳过。恢复HTTP验证后，紧凑JSON、冗长JSON均通过；随后在缺行场景的旧入口基线请求上出现断言失败，立即停止，没有重试。阶段D1整体仍未完成，不可发布。

本报告按文档技能区分实测结果、源码解释与未知。所有输入均为已保存的本地合成证据，不是生产图片重放。历史生产03的原始Provider响应仍缺失，不能声称生产识别问题已经解决。

## 基线与边界

- 隔离工作树：C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9
- 分支：codex/recognition-architecture-audit
- HEAD：86ea4ac44a6d44a2332d7aaaffb1926504a73dca
- 无暂存、提交、推送、PR、合并或部署；无生产配置、数据库、密钥、账户或计费政策修改。
- 原有缺失值安全处理、源坐标保真改动以及阶段A/B/C/D1材料均保留。
- 真实Provider调用0，真实Supabase调用0。离线进程清空真实相关凭证、禁用dotenv环境文件，并预加载拒绝非loopback连接的guard。
- 本轮localhost HTTP实际6次，每次只有1次mock Provider调用；另校验复用1份历史请求，其历史mock计数不计入本轮。测试用固定假凭证不是生产密钥。
- 无生产图片上传、无生产识别任务。HTTP仅向本机提交程序生成的空白合成图。

## 修复内容与原始原因

候选提取在解析时去掉Markdown行首尾的竖线，却在sourceText保留原始行。下游适配器之前直接解析这个原始行，并要求它精确等于普通表格的拼接字符串；因此字段完整的Markdown候选仍被拒绝。

本轮把候选提取器已有的表格外壳规范化抽为共享函数，采集与下游复用完全相同的处理。消费时仍先逐行核对分组、点号、行序、原始sourceText及经纬度字段，再比较规范化后的每个单元格、完整字段数、DMS方向和轴序；不丢弃空单元格，不仅凭行数匹配。

原始Markdown及其分隔符保留在候选sourceText、引擎point.raw和源坐标展示中。没有改动已有类型的解析优先级、CRS转换、最终身份/版本、输出权限、核对权限或计费规则。共享函数仅提取原有表达式，没有新增识别引擎。

## 本轮修改文件

| 文件 | 本轮改动 |
| --- | --- |
| server/recognition/recognition-candidate-evidence.js | 导出已有表格外壳规范化函数，原采集解析器复用它；顺序及规则不变 |
| server.js | 下游用同一规范化函数进行字段级绑定，原始行独立保留 |
| scripts/recognition-architecture-probe.js | 沙箱导入真实共享产品函数，不使用固定成功桩 |
| scripts/recognition-acquisition-downstream-regression.js | 校验历史回执哈希后跳过4项，从Markdown恢复；断言前保存输入、候选、消费/最终化和展示；负例补充Markdown绑定与额外/空单元格检查 |
| scripts/recognition-architecture-audit-runner.js | 独立恢复入口 --resume-d1-markdown，独立目录和首失败停止 |
| scripts/recognition-diagnostics-http-regression.js | 仅让回执目录消费本轮runner指定路径；本轮没有改变HTTP安全断言 |
| 本报告 | 独立记录结果、停止原因及下一步 |

## 本轮测试结果

执行命令：node scripts/recognition-architecture-audit-runner.js --resume-d1-markdown

| 检查 | 结果 | 范围 |
| --- | --- | --- |
| 6个本轮JS文件语法检查及git diff --check | PASS | 运行门禁前完成 |
| 候选到下游几何及展示 | 25/25 PASS | 本轮298ms，退出0，未超时 |
| 历史4项 | 历史PASS，本轮未重跑 | 原因定位、紧凑JSON、冗长JSON、普通表格；校验原回执哈希 |
| HTTP紧凑JSON | PASS | 精确历史基线422且无候选；新入口200，有效逐点几何及完整原文保留；启用诊断前后业务等价 |
| HTTP冗长JSON | PASS | 本轮旧入口422/新入口200；诊断开关业务等价、字段来源及身份/状态/使用次数断言通过 |
| HTTP缺行JSON | FAIL，立即停止 | 旧入口基线mapReady=true，与blocked map断言冲突；本轮2767ms，退出1，未超时 |
| HTTP缺行新入口、额外字段、方向冲突 | 未执行 | 旧入口失败后停止，没有自动重试 |
| 完整阶段C影子、阶段B诊断及HTTP | 未执行 | 后续顺序门禁未启动 |
| 10个受影响专项 | 未执行 | 不把历史通过替代本轮验证 |
| 134项完整离线验收 | 未执行 | 不报告134/134 |
| 42项核心回归 | 未执行 | 不报告42/42 |

25项涵盖：Markdown逐点/原文一致性，DMS点号前缀不误读成十进制，null/undefined/空串/空白/NaN/Infinity/非法字符串/布尔/数组/对象不变成零坐标，合法0与字符串0保留，缺行/重复点号/点序/方向/额外/缺字段阻断，字段内容/组顺序/来源行冲突不覆盖，近似数值不替换，既有类型解析优先级不变。

缺行直接函数测试已通过，但不能代替真实HTTP新入口缺行测试；后者尚未运行。完整JSON的HTTP新结果为有效MultiPoint待核对结果，地图和KML均关闭，原因仍为既有GENERIC_REVIEW_ONLY合同门禁。没有提升为Polygon、正式授权或导出完成。

## HTTP失败的证据与解释

失败位置：scripts/recognition-diagnostics-http-regression.js:218，run中的blocked map断言。

失败请求是json_missing/baseline，不是新入口。实测记录：

- 输入点号1、3、4，缺少2。
- 旧采集证据：candidateCoordinates为空、acquisitionStatus=NO_COORDINATE_EVIDENCE。
- 旧采集决策：shouldReturnFailure=true，mayProceedToGeometryValidation=false。
- 另一条Provider DMS恢复路径最终保留点号1、3、4，生成有限且有实际坐标的MultiPoint。
- HTTP 200，code=ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED，authorizationStatus=REVIEW_REQUIRED，mapReady=true，kmlReady=false。
- 最终确认pending、未正式授权；userUsageConsumed=false、usageConsumed=false。

只读源码解释：groupedProviderReviewApplies可接管存在点号问题的DMS候选；其keepRecognizedCoordinatesAsPointReview调用没有blockMap=true，既有最终授权函数允许这类有限WGS84点位预览。测试却将options.blocked同时用于旧、新请求，且后续verifyOldJsonFailure还假定所有旧JSON输入都是422无候选。缺行旧路径不符合这个假定。

因此不能把本次失败归因于Markdown修复，也不能直接放宽新入口负例来通过。旧链路是点位预览而非完整Polygon/KML，是否保留这种恢复方式属于输出政策，当前没有改变它。已确认的是统一采集结论与另一条恢复路径的能力状态不一致，不能用单个“无候选”状态代表整个旧链路。

历史对照装载精确HEAD的server.js和recognition-first-acquisition.js，其余依赖使用当前隔离目录；这是旧入口对照，不是整个生产发布目录的精确重放。两份通过的HTTP完整JSON对照也适用这一限定。

## 回执与保护校验

新目录：Temp/recognition-table-phase-d1-markdown-recovery/

- results.json：精确退出码、耗时、timeout=false、末尾输出。
- downstream-results.json：本轮25项、历史4项及每项检查点路径。
- http-results.json：2个完整通过场景及具体失败请求/堆栈。
- requests/json_missing-baseline.json：失败前保存的HTTP状态、业务结果、候选证据、最终身份和全部实际采集决策。
- 其余逐项JSON：断言前输入、候选、消费、几何和源展示。undefined与非有限值使用诊断类型标记，避免JSON自动变null而丢失差别。

历史4项回执SHA256：
7acb416e10263aad682c1db725602631594c900b71d689ef61436739037b56c4

本轮缺行旧入口失败回执SHA256：
cb7b9d42e18e458f6bd8ef943bb137ca0bcdfcea815241ba9c839810b24e6bdd

本轮开始记录的40份既有未提交文件及recognition历史回执已比对：5份发生预期改动，缺失0，历史回执与报告未变。recognition-candidate-evidence.js此前干净，本轮新增为tracked修改，差异仅共享规范化提取。新增本报告。源坐标展示和recognition-first-acquisition.js的既有未提交内容本轮未变；package.json、package-lock.json不在Git差异中。

最终Git：HEAD和分支不变，暂存区为空；4个tracked修改、21个untracked文件，包含此前阶段累计内容，不是本轮新增25个文件。

## 剩余风险与下一步

1. 新HTTP入口缺行是否始终关闭地图/KML仍未知，直接函数通过不能代替它。
2. 旧入口的采集与恢复状态分歧必须保留在架构差异清单；不得伪称旧JSON全部失败，或将点位预览自动视为完整边界成功。
3. 其余HTTP负例、B/C、专项、134/42尚未验证，整体仍未满足退出标准。
4. 真实生产03的数字失真、模型输出稳定性、原始CRS证据绑定仍不能由本轮合成测试确定。没有真实Provider或生产准确率结论。

下一阶段建议只调整历史HTTP基线的分类与恢复入口，不先改产品：核验并复用已保存缺行基线，精确记录其点位预览状态，不要求它冒充422；新入口缺行、额外字段和方向冲突仍必须满足原有严格负例。跳过本轮已通过25项及2个完整JSON HTTP场景，恢复一次后继续剩余门禁。若新入口也违反阻断条件，立即停止，报告具体产品路径以及是否需要权限范围变更，不能通过放宽断言或静默改门禁完成验收。
