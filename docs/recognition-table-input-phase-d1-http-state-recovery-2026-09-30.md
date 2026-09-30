# 阶段 D1 HTTP 最终状态分类恢复报告（2026-09-30）

## 结论与操作边界

本阶段未完成。仅恢复运行一次，在紧凑 JSON 新入口请求的候选证据断言处失败后立即停止。没有重试，没有执行后续测试，没有修改产品代码。不能把本次结果报告为 HTTP 通过、阶段 D1 完成或可发布。

分支：`codex/recognition-architecture-audit`  
HEAD：`86ea4ac44a6d44a2332d7aaaffb1926504a73dca`  
工作树：`C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9`

本轮只修改 HTTP 测试状态分类、runner 恢复入口，并新增本报告。原有产品修改、阶段 A/B/C/D1 报告及历史失败回执均保留。没有暂存、提交、推送、PR、合并、部署或生产配置操作。

## 本轮改动

1. `scripts/recognition-diagnostics-http-regression.js`
   - 完整检查无候选失败、候选恢复但输出关闭、可用待核对输出三类断言。
   - 无候选旧入口保留精确 422、失败业务码、零候选、无几何、无输出、无扣次断言。
   - 不再对所有最终对象无条件要求 REVIEW_REQUIRED；输出阻断必须关联实测校验依据。
   - 保留坐标字面值、点号、顺序、来源、完整性、身份、版本、CRS、轴序、几何、输出权限、扣次与真实诊断断言。
   - 增加独立恢复模式，校验并复用旧紧凑 JSON 回执，不重复请求旧入口。
2. `scripts/recognition-architecture-audit-runner.js`
   - 新增 `--resume-d1-state`，只从紧凑 JSON HTTP 项恢复，写入独立目录。
   - 保留首失败停止、实际超时停止、外网阻断、凭证隔离和历史结果。
3. 本报告。

## 执行结果

执行命令：`node scripts/recognition-architecture-audit-runner.js --resume-d1-state`

| 项目 | 结果 |
| --- | --- |
| 分支、HEAD、未暂存状态核对 | 符合基线 |
| 修改前后脚本语法检查、diff 空白检查 | 通过 |
| 已保存旧 JSON 基线哈希校验 | 通过 |
| 旧 JSON 无候选失败分类 | 复用回执后对应断言通过；非新请求 |
| 新紧凑 JSON / disabled 请求 | 实际本地 HTTP 200，随后断言失败 |
| 新紧凑 JSON / captured 请求 | 未执行 |
| 后续冗长 JSON 与负例 HTTP | 未执行 |
| 已通过的 39 项入口、普通与 Markdown HTTP | 按授权不重跑；仅保留历史结果 |
| 完整阶段 C 历史旧入口影子对照 | 本轮未执行 |
| 阶段 B 诊断及 HTTP 回归 | 本轮未执行 |
| 原计划 10 项专项 | 本轮未执行 |
| 134 项完整离线验收 | 本轮未执行，不报告 134/134 |
| 42 项核心离线回归 | 本轮未执行，不报告 42/42 |

runner 退出码 1，`timedOut: false`，该步骤耗时 557 ms。没有发生实际超时。

未运行的 10 项专项：projected-crs-source-evidence、multi-representation-source-evidence、coordinate-markdown-table、recognition-first-review-result-v2、recognition-first-acquisition-evidence-v3、multi-representation-http、p08h-confirmation-ui-lifecycle、source-coordinate-review-display、review-output-contract、recognition-projected-authorization-v8。

本轮真实 Provider 调用 0；新 localhost 请求使用 mock Provider 1 次。复用旧回执中的 providerCallCount=1 属于历史 mock 请求，不是本轮新调用。真实 Supabase 与生产识别请求均为 0。真实 Provider、Supabase、usage 相关凭证在离线子进程中清空；mock 占位凭证只用于受控本地桩，外部 socket 由离线 guard 拒绝。

## 本次直接失败原因：测试预期错误

位置：`scripts/recognition-diagnostics-http-regression.js:392`，verifyCandidates。

断言要求候选 reviewReasons 必须等于：
`["CRS_EVIDENCE_MISSING"]`

实际为：`[]`。

现有产品 `server/recognition/recognition-candidate-evidence.js:521-523` 只在没有 visible CRS 且 geographicCrsEvidence.complete 不为 true 时增加该原因。本次候选 DMS 方向明确且 complete=true，因此实际空数组符合现有候选证据规则。

同时，实际证据仍保留 `datumExplicit:false`、`reviewOnly:true`、`authority:EVIDENCE_ONLY`。不能把“无该原因码”解释为明确了测地基准、已正式授权或可正式导出。

这条旧测试预期本轮没有修改。按首失败停止要求，失败后未修改测试或产品、未重跑。

## 重要发现：不能只改这一条断言就宣称完成

以下来自本次实际本地 HTTP 响应，在断言前已保存。输入为合成的四行明确 DMS，绝非生产图片或历史生产响应重放。

### 1. 采集入口已经恢复完整候选，但下游引擎没有得到有效坐标

已执行至失败前的断言验证了：
- 四行候选与输入 DMS 字面值逐行一致；
- 点号 1→2→3→4、规范化行序、DMS 表示、latitude_longitude 轴序一致；
- acquisition/normalization 均 COMPLETED；
- 一个候选组，boundRowCount=4，拒绝行与未绑定行均为 0。

但同一响应的 engine.groups[0].points 中四个点的 lat/lon/x/y 均为 null，并带“point 解析失败”等警告。

已确认产品路径：`server.js:18477-18485` 将候选行放入 contractReviewPayload，交给 buildCoordinateEngineV2ShadowResult，再调用点位降级函数。该引擎又走类型推断及分组构建，说明新采集行并未作为完整规范化坐标被下游一致消费。

未知：具体是类型推断、行序列化消费还是更深的解析分支导致 null，当前没有运行新的探针或测试，尚不能宣称定位到唯一首个失真函数。

### 2. null 被点位降级函数变成零坐标

实际 finalized.geometry 为四个 [0,0] 的 MultiPoint，与原始非零 DMS 候选不一致。

静态代码证据：`server.js:14703-14708` 的 keepRecognizedCoordinatesAsPointReview 用 `Number(point?.lon)`、`Number(point?.lat)` 后再验证有限数值。JavaScript 中 Number(null) 为 0，这一检查无法识别缺失值。结合本次引擎 null 和最终零坐标，定位到了明确的危险转换路径。

本次 mapReady/kmlReady 均为 false，未导出错误点，未扣次。不能据此推断所有调用方都安全，也不能把它直接等同于生产 03 的根因。

### 3. 源坐标展示备用路径误读 DMS 数字前缀

原候选/response.coordinates 是完整 DMS，但 sourceCoordinateRepresentation.rows/displayText 实际变成：
`1,18`、`2,18`、`3,18`、`4,18`；
sourceEquivalence 为 missing_rows_or_engine_points，axisOrder/family 均为空。

代码链：
- `server/source-coordinate-representation.js:89-99` 的 parseDecimalCoordinateLine 只匹配数字前缀，未要求整行匹配；
- `173-194` 的 groupsFromEngine 在无有效引擎点时回退解析 coordinateDisplayText；
- `438-460` 以原 JSON 尝试 DMS 源结构校验，随后可能用备用 canonicalEngineDisplay 替换已存在的 DMS displayText。

这使“点号＋纬度度数”可以成为展示坐标。观察到的是错误的响应字段；本轮没有浏览器请求，不宣称已验证该字段在生产页面的最终渲染效果。

### 4. 状态仍有多个所有者，不能用单个 REVIEW_REQUIRED 代表所有能力

本次 acquisition 决策为：
- mayProceedToGeometryValidation=true；
- dmsGeographicReviewEligible=true；
- acquisition contractReasons=[]。

最终授权决策则有：
- contractRequiresReview=true；
- finalAuthorizationReasons=["ACQUISITION_CONTRACT_NOT_CONFORMANT"]；
- map/kml gates=false。

响应 finalized 同时是 decisionState=REVIEW_REQUIRED、technicalKmlReady=false、kmlAuthorityBlocked=true、map/kml=false。

静态代码 `server.js:15776-15791` 在生成 alignedFinalizedCoordinateResult 后，再按旧 finalized decisionState 覆盖新 decisionState/gate.decisionState。它保留旧“待核对”语义，并不等于输出开放。因此后续须区分证据审阅状态与技术/权限能力，不能仅把测试改成任意状态均通过，也不能因有候选就开放输出。

本轮失败发生在 verifyCandidates，尚未执行该新请求的 verifyFinal、几何逐点一致性及 disabled/captured 业务等价性断言。以上仅是保存响应与只读源码核对，不冒充这些断言已通过。

## 证据、回执与保护校验

新目录：`Temp/recognition-table-phase-d1-http-state-recovery/`
- results.json：runner 退出码、时长、错误尾部；
- http-results.json：合成来源标志、失败、请求与实际业务结果；
- requests/json_compact-baseline.json：复用的历史旧入口响应及来源标记；
- requests/json_compact-disabled.json：本轮新入口真实 localhost 响应。

旧基线原路径：
`Temp/recognition-table-phase-d1-http-recovery/requests/json_compact-baseline.json`
SHA256：
`f7d34cf4337d49699d5b355aa48318c81e35b2b62e02501104f1eede8ea51c73`

本轮新请求回执 SHA256：
`94d5dea9cf7b40d867f198e9d248c1e9fec98341716aa18efebd415a02caa50c`

本轮开始保存了 123 个文件内容哈希，包括现有 server 模块、server.js、index.html、package.json/lock、报告、测试和历史 recognition 回执。结束比对仅两个授权脚本变化，缺失文件 0。原有产品修改未动，历史报告与回执未覆盖。新增独立报告后，Git 为 2 个既有 tracked 修改及 18 个 untracked 文件；暂存区为空，HEAD 不变。

## 剩余风险与下一步

阶段 D1 退出条件未满足，存在实际坐标失真和未完成的端到端验证，禁止发布。历史 03 原始 Provider 响应和完整诊断仍缺失，生产根因继续 UNKNOWN，不要求用户再次上传图片。

下一阶段不能继续仅协调测试文案。应限定为“恢复候选到下游坐标消费、源坐标保真与缺失值安全处理”：
1. 用本次已有合成回执定位首个 null 产生阶段；只修现有消费/适配缺口，不新增引擎，不改变解析优先级。
2. 使用现有安全数值工具拒绝 null/undefined/空串/非法值，保证真实零坐标仍可表达；不得把缺失值制造成零坐标。
3. 防止十进制前缀解析吞掉 DMS 或额外字段；不得用错误备用表示覆盖原始完整坐标。
4. 记录各状态所有者、具体阻断依据，保留输出、正式授权和扣次规则；若必须改权限规则则另行停止报告。
5. 修正 CRS 原因码的错误测试预期，同时验证实际 DMS 方向证据、datumExplicit=false、reviewOnly=true，不弱化逐点、身份及安全断言。
6. 在新独立回执中运行新增安全回归及剩余 HTTP/C/B/专项/134/42；首失败或实际超时停止。
7. 仍不提交、推送、合并、部署或访问真实 Provider，不修改生产。

即便完成上述离线工作，也只能证明受测输入链路改善，不能把离线合成样本作为生产识别准确率或历史问题已解决的证明。

