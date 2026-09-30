# D1 HTTP 差分基线恢复回执

日期：2026-09-30。状态：**未完成；首次断言失败后已停止**。

## 结论

本次纠正了“旧 JSON 必须 HTTP 200”的错误前提，但新增的 `verifyFinal` 又把“存在最终结果对象”误等同于“最终状态必须 REVIEW_REQUIRED”。旧入口确实生成一个不可用的最终结果对象，其几何为空、最终状态为 BLOCKED。这是本轮新增测试断言的错误，不是已经证实的新产品故障。

只执行了紧凑 JSON 的一个旧基线请求。新版请求、诊断捕获请求及所有后续门禁尚未执行。不得声称新入口 HTTP 已验证、诊断业务等价已通过或生产识别问题已修复。

## 基线和范围

- 隔离工作树：`C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9`。
- 分支：`codex/recognition-architecture-audit`。
- HEAD：`86ea4ac44a6d44a2332d7aaaffb1926504a73dca`，未变。
- 本轮只修改两个离线测试/runner 文件，另新增本报告；没有修改产品、依赖、配置、解析优先级、权限或计费规则。
- 开始前保存的 151 个文件内容哈希已重新对照，只有上述两个脚本发生变化。既有产品修改、历史报告、样本和历史失败/通过回执均保留且内容未变。
- 未暂存、提交、推送、创建 PR、合并、部署或调用生产接口。

## 本轮修改

| 文件 | 改动与当前状态 |
| --- | --- |
| scripts/recognition-diagnostics-http-regression.js | 将已成功输入与 JSON 候选恢复分开；通过测试加载器观察实际采集/决策函数返回值；先落盘状态、业务原因、候选、最终状态再断言；新增字段、行序、来源、几何、身份、版本、权限、扣次检查。新增最终状态断言仍有错误，尚未通过。 |
| scripts/recognition-architecture-audit-runner.js | 增加显式 `--resume-d1-http`，使用独立回执目录，从紧凑 JSON 开始，跳过已通过入口和普通/Markdown HTTP 项；首次失败即停。 |
| docs/recognition-table-input-phase-d1-http-recovery-2026-09-30.md | 本报告，不覆盖历史报告。 |

测试加载器只在本地子进程包装真实函数并记录其实际返回值，未用固定成功结果代替实现。原始 OCR/Provider I/O 仍使用合成样本和 mock；本次不是生产重放。

## 实测旧基线

执行命令：`node scripts/recognition-architecture-audit-runner.js --resume-d1-http`。

| 项目 | 实际值 |
| --- | --- |
| 场景 / 模式 | json_compact / baseline |
| HTTP | 422 |
| 业务 code / reason | COORDINATE_RECOGNITION_FAILED_CLOSED / recognition_failed_closed |
| 采集状态 | NO_COORDINATE_EVIDENCE |
| 候选坐标 / 行 / 分组 | 0 / 0 / 0 |
| 业务授权 / 结果状态 | NOT_ESTABLISHED / failed |
| 最终结果身份 | 实际结果 ID 非空，版本 1，未确认 |
| 几何 / 哈希 | null / null |
| 最终 decisionState | BLOCKED |
| 地图 / KML | CLOSED / CLOSED；ready 均 false |
| 正式授权 | false |
| usageConsumed / userUsageConsumed | false / false |
| mock Provider | 1 次；无自动重试 |
| 真实 Provider / Supabase | 0 / 0 |

实际授权函数保留的原因包括：

- ACQUISITION_CONTRACT_NOT_CONFORMANT
- UNIFIED_RECOGNITION_ACQUISITION_INCOMPLETE
- UNIFIED_RECOGNITION_EVIDENCE_INCOMPLETE

它不是任意 422、空回执或被误放行的负例。HTTP 状态、失败原因、0 候选和关闭输出断言已通过；随后在 `verifyFinal` 的无条件 `decisionState === REVIEW_REQUIRED` 断言停止，实际值是 BLOCKED。

失败位置：`scripts/recognition-diagnostics-http-regression.js:299`，调用方 `:420`。退出码 1，运行 1030 ms，`timedOut=false`。错误应在测试中按真实状态类别纠正，不能为了测试变绿把产品 BLOCKED 改成 REVIEW_REQUIRED。

## 测试结果与未执行项目

| 测试 | 本次结果 |
| --- | --- |
| git diff --check | 通过 |
| HTTP 测试、runner 语法检查 | 通过 |
| D1 HTTP 恢复 | 失败：上面的测试契约错误；无重试 |
| 39 项 D1 入口回归 | 历史 39/39，通过记录未变，本次未重跑 |
| 普通表格、Markdown D1 HTTP | 历史通过记录未变，本次未重跑 |
| 新版紧凑 JSON、诊断开启请求 | 未执行 |
| 冗长 JSON、缺行、额外字段、方向冲突 HTTP | 未执行 |
| 完整阶段 C 历史影子对照 | 未执行 |
| 阶段 B 诊断及 HTTP 回归 | 未执行 |
| projected-crs-source-evidence-regression | 未执行 |
| multi-representation-source-evidence-regression | 未执行 |
| coordinate-markdown-table-regression | 未执行 |
| recognition-first-review-result-v2-regression | 未执行 |
| recognition-first-acquisition-evidence-v3-regression | 未执行 |
| multi-representation-http-regression | 未执行 |
| p08h-confirmation-ui-lifecycle-regression | 未执行 |
| source-coordinate-review-display-regression | 未执行 |
| review-output-contract-regression | 未执行 |
| recognition-projected-authorization-v8-regression | 未执行 |
| 134 项完整离线验收 | 未执行，不能报告 134/134 |
| 42 项核心回归 | 未执行，不能报告 42/42 |

历史旧入口对照不替代新入口验证。当前 D1 退出标准未满足。

## 新回执

均位于 `Temp/recognition-table-phase-d1-http-recovery/`，没有覆盖原 `recognition-table-phase-d1/` 或 A/B/C 的任何结果：

- `results.json`：退出码、耗时、超时标志及错误末尾。
- `http-results.json`：本次失败和已执行请求证据。
- `requests/json_compact-baseline.json`：断言前保存的真实本地旧 HTTP 响应投影、候选及决策函数输出。

尚无新入口或 captured 请求回执。旧请求的观测值来自真实函数，但不是阶段 B 的诊断捕获成功证明。

## 剩余风险

1. 新入口真实 HTTP 的候选恢复、字段来源和最终权限仍未验收；不得用历史 39 项单入口测试替代。
2. 测试必须区分失败 BLOCKED、候选已采集但输出阻断、可用待核对输出；存在结果对象本身不代表具有地图/KML 能力。
3. D1 不迁移投影原始提取器、物理表/图片绑定和后续消费者；它不能证明生产 03 数字、CRS 或输出问题已解决。
4. 历史生产原始响应仍缺失，错数最早阶段继续 UNKNOWN。不需要用户再次上传生产图片试错。
5. 离线网络守卫禁止非本机连接，真实凭证已在测试进程置空；只有明确的无效本地 mock 标识。不修改生产环境文件。此次不验证真实计费、OCR/Provider 准确率或生产资格。

## Git 和下一步

共 19 个未提交文件：2 个已跟踪修改、17 个未跟踪文件，包含各阶段既有修改；暂存区为空。相较本轮开始，只有两个既有测试脚本内容变动和本报告新增，无文件删除。

下一步仍是 D1 HTTP 测试契约恢复，不是 D2 或发布：先完整审查该测试中的终态分类，以真实几何、具体拒绝条件和现有授权函数为依据，修正无条件 REVIEW_REQUIRED 断言。可校验并复用此次旧基线回执，继续未执行的新请求；如重新执行则仅一次，使用新的独立恢复目录，保留本失败回执。HTTP 全部通过后才继续原有未执行门禁；任何新断言失败或实际超时仍立即停止。不得修改产品实现或放宽任何安全校验。
