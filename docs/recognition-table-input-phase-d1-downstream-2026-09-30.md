# 阶段D1候选下游消费修复及停止回执

日期：2026-09-30

## 结论

本阶段未完成，不可发布。新增离线行为回归仅运行一次：4项通过，第5项 Markdown 失败后立即停止；没有重试、没有后续 HTTP、阶段 B/C、专项、134项或42项验收。

本轮修复了候选消费、缺失数值转换和源坐标展示的实现，但尚未完整通过验证。紧凑 JSON、冗长 JSON、普通表格的候选到几何和原文保留检查通过；不能将其扩大为 HTTP 链路或生产识别通过。

## 基线与边界

- 工作树：C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9
- 分支：codex/recognition-architecture-audit
- HEAD：86ea4ac44a6d44a2332d7aaaffb1926504a73dca
- 既有分支、工作树、未提交修改与历史回执保留。
- 未暂存、未提交、未推送、未创建PR、未合并、未部署。
- 未修改生产配置、密钥、数据库或计费政策；未上传图片、未创建生产任务。
- 本轮真实 Provider 0，mock Provider 0，真实 Supabase 0。测试在运行真实产品函数的离线沙箱中执行，尚未进入 localhost HTTP/mock 请求阶段。
- runner 清空离线进程真实 Provider、Supabase、usage 相关凭证，禁用 dotenv 文件加载并预加载外部 socket 阻断器。

## 已定位的原始失真路径

保存的合成 HTTP 请求包含完整 DMS 候选。原先路径是：

采集候选 → one-shot-acquisition-contract-review 流程标记 → inferCoordinateEngineV2Type 返回空类型 → parseCoordinateEngineV2PointLine 未进入现有 standard_dms_table 分支 → lat/lon 保持初始 null → 点位降级函数把 Number(null) 当作0。

本轮第1项回归用精确基线中的真实函数验证了：
- 同一 payload 的旧类型推断为空；
- 旧分组中每行 lat/lon 都是 null；
- 同一首行传入空类型时解析失败，传入现有 standard_dms_table 类型时能解析；
- 历史合成回执最终几何确为零坐标 MultiPoint。

这确定了该合成案例的首个失真接口。不等于确认历史生产“印尼03”的首个错误；生产原始响应缺失仍为 UNKNOWN。

## 本轮修改及验证范围

| 文件 | 修改 | 当前验证状态 |
| --- | --- | --- |
| server.js | 在既有类型为空的采集审阅入口增加逐行候选消费，检查组绑定、点号、行序、原始字段和DMS方向，复用 parseDmsSourceCoordinateRow；不覆盖已有类型解析器 | JSON、冗长JSON、普通表格通过；Markdown失败 |
| server.js | 点位降级复用 finiteNumberOrNull；拒绝非数值类型和缺失值；存在任何无效点时不构造部分几何 | 已实现；合法普通点路径通过，专门缺失值/零值负例尚未执行 |
| server/source-coordinate-representation.js | 十进制解析要求整行匹配；不再用未证实的备用表示覆盖原有坐标文本 | 已通过前三种输入的原文保留；专门前缀/额外字段项尚未执行 |
| scripts/recognition-acquisition-downstream-regression.js | 新增29项计划回归，执行真实采集、消费、最终化、确认运行时和源表示函数 | 执行至第5项停止 |
| scripts/recognition-diagnostics-http-regression.js | 修正 CRS_EVIDENCE_MISSING 预期并检查完整方向证据、datumExplicit=false、reviewOnly=true；逐点验证既有点位降级策略；增加源文与引擎一致性检查 | 本轮尚未执行 |
| scripts/recognition-architecture-audit-runner.js | 新 --d1-downstream 顺序入口，独立回执、首失败停止，不重跑39项及D1旧普通/Markdown HTTP | 停止行为已验证 |
| scripts/coordinate-result-shadow-regression.js | 可从本轮 runner 指定独立输出目录，不覆盖历史阶段C回执 | 语法通过，尚未执行 |
| 本报告 | 独立保存结果、失败原因和下一步 | 新增 |

这些改动不调整候选选择优先级、CRS转换、输出门禁、正式授权、结果身份/版本或计费规则。真实 localhost HTTP 仍须验证这些边界，不能仅凭静态差异证明全部行为等价。

本轮 HTTP 断言按已观察到的现有合同路径区分：
- 无候选、空几何的失败；
- 候选恢复但合同阻断输出的点位审阅；
- 有效待核对输出。

只有明确的既有合同原因及点位策略才允许“REVIEW_REQUIRED但输出关闭”；不把任意失败放行，也不要求所有几何都是 Polygon。上述 HTTP 断言修改尚未执行，不能报告通过。

## 本次失败的具体原因

失败项：markdown_candidate_geometry_source_preservation  
位置：scripts/recognition-acquisition-downstream-regression.js:97  
断言：逐行经度必须与原始 DMS 换算值一致，误差小于1e-10。

已执行4项：
1. retained_evidence_and_first_null_location：PASS。
2. json_candidate_geometry_source_preservation：PASS。
3. verbose_json_candidate_geometry_source_preservation：PASS。
4. plain_candidate_geometry_source_preservation：PASS。

第5项 Markdown：FAIL。后续24项未执行。前三种正例同时检查了行值、点号、raw、输入证据不被修改、最终 MultiPoint 各点、几何哈希、身份和版本保留、pending、地图/KML关闭以及原文展示。

只读源码定位：
- recognition-candidate-evidence.js 的 parseCoordinateCandidates 在解析时移除行首尾 Markdown 分隔符，但重新写回 sourceText 时保留原始行。
- 新 getAcquisitionDmsConsumerPoints 对保留外层分隔符的原始行调用标签解析，并要求原始整行与“点号 | 纬度 | 经度”的字符串相等。
- 对合法 Markdown，字段证据虽完整，原始行却带外层分隔符；标签/字面串比较不能成立，消费适配返回 null，下游保留空数值。

这是本轮新增适配器没有正确区分原始序列化与字段证据的实现遗漏，不是测试文案问题或 Provider 故障。失败后没有修改代码，也没有另行运行探针；本次失败回执只保存断言位置，没有保存该场景完整中间对象。具体运行时值不得伪造，恢复时应在断言前记录。

后续应按既有表格字段证据和来源检查整行内容，而不是再增加一种固定字符串模板，也不能删除点号/顺序/额外字段校验来取得通过。

## 测试与回执

执行一次：
node scripts/recognition-architecture-audit-runner.js --d1-downstream

runner：退出码1，timedOut=false，261ms。语法检查6个文件通过；修改前运行的 git diff --check 通过。

独立回执：
- Temp/recognition-table-phase-d1-downstream/results.json
- Temp/recognition-table-phase-d1-downstream/downstream-results.json

本轮未执行：HTTP恢复、完整阶段C影子、阶段B诊断和HTTP、全部计划专项、134项离线矩阵、42项核心回归。旧有通过项只作为历史证据，没有重跑，也不计作本轮通过。

## 保护校验与Git状态

对本轮开始记录的238个代码、脚本、文档及包文件内容哈希进行复核：
- 仅5个已存在文件变化：server.js、source-coordinate-representation.js及上述3个既有测试/runner脚本。
- 缺失文件0；原有 recognition-first-acquisition.js 未再修改；package.json、package-lock.json、index.html未变。
- 新增 downstream 回归和本报告。
- 本轮哈希清单未覆盖Temp目录。另分别核对了两份本次引用的历史请求回执，均与先前哈希相同：
  - 旧JSON基线：f7d34cf4337d49699d5b355aa48318c81e35b2b62e02501104f1eede8ea51c73
  - 已恢复候选的旧失败回执：94d5dea9cf7b40d867f198e9d248c1e9fec98341716aa18efebd415a02caa50c
- HEAD和分支不变，暂存区为空；新增报告后工作区为3个tracked修改、20个untracked文件。这包括此前阶段保留内容，不是本轮新增23个修改。

## 未解决风险与下一阶段

1. Markdown原文/字段绑定尚未正确消费，完整入口等价目标未满足。
2. 专门缺失值、合法零值、额外字段、方向冲突、点序冲突测试尚未执行；不能声称安全修复已全部验证。
3. 新HTTP断言和真实handler行为未验证，阶段B/C和验收尚未完成。
4. 原始CRS和技术可转换性与授权合同仍是不同状态层；本阶段保持原规则，不能以候选完整为理由直接开放输出。
5. 旧 renderCanonicalEngineGroups 已不被当前源展示入口调用，尚未退役；首失败后没有再进行清理。
6. 历史生产03原始响应仍缺失，本轮无生产资格或识别准确率结论。

建议下一阶段范围：
- 仅修复消费适配器对原始序列化和字段证据的绑定方式，复用既有表格规范化/字段来源，不新增识别引擎、不改优先级。
- 保留原始行；逐字段、逐点号、逐行序核对，完整消费全部内容，额外字段或冲突仍阻断。
- 在测试断言前保存输入、候选字段、消费拒绝位置及引擎点，禁止空诊断通过。
- 从失败的Markdown场景恢复一次，继续其余24项及原计划后续门禁；独立目录，保留已通过和失败回执。
- 任一断言失败或实际超时停止，不自动重试，不降低断言。
- 仍不提交、推送、合并、部署，不访问真实Provider，不上传生产图片。

