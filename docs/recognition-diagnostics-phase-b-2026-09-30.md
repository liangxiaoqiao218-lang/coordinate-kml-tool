# 阶段B：字段级诊断与离线重放——实施及恢复验收报告

日期：2026-09-30。当前状态：**授权范围内的本地离线验收通过；未发布，03历史生产根因仍为UNKNOWN**。

首次HTTP采集失败记录完整保留。本次仅修正HTTP测试作用域并新增独立runner恢复入口；未修改产品代码或放宽断言。HTTP诊断、10项剩余专项、134/134完整离线验收及42/42核心回归全部通过，13个进程均退出0、无超时。该结论不等于生产03已复现或修复。

## 1. 基线和边界

工作树：`C:/Users/Mir-1/.codex/worktrees/recognition-architecture-audit/geokitlab-recognition-projected-header-terminal-v9`。

分支：`codex/recognition-architecture-audit`。HEAD：`86ea4ac44a6d44a2332d7aaaffb1926504a73dca`。

保留阶段A的四个无调用函数删除、审计报告、探针、回归和隔离辅助脚本。没有提交、暂存、推送、PR、合并、部署或生产配置修改，没有再次上传用户图片、创建生产识别任务或访问真实Provider/Supabase。本阶段HTTP测试只向本机127.0.0.1提交内存生成的合成灰色PNG，OCR与Provider输入均由测试桩提供。

## 2. 已实施的诊断框架

新增 `recognition_diagnostic_v1`，作为独立记录，不加入HTTP业务响应、地图/KML结果模型或公开日志。只有显式进程内 `withRecognitionDiagnostics` 作用域可以采集；没有环境变量、请求参数、请求头或新公开接口能够启用采集。生产默认不采集、不持久化。

| 阶段 | 记录内容 | 与现有业务关系 |
| --- | --- | --- |
| 图片身份 | 原图SHA-256、尺寸、页；区域/表格身份未建立时显式未知 | 不创建或改变图片身份 |
| 本地OCR | 源文本、上下文、轴序文本及既有来源证明 | 不再执行OCR，不替换任何字符 |
| Provider内容 | 原始message.content安全投影及实际extractProviderMessageText结果 | 不保存认证/HTTP头，不发起Provider调用 |
| 规范化与候选 | 现有DMS、投影解析函数的输入/输出；统一候选实际选用的文本及拒绝行 | 原始证据和规范化结果分开，不修改选择优先级 |
| 同图绑定和表示协调 | Provider证据、OCR上下文、同图证明、实际绑定结果；协调前后证据 | 不凭行数构造点号、表格身份或坐标 |
| 投影/转换/几何 | 实际失败条件、原始CRS、轴序、输入值、转换点、DMS差值和既有容差 | 仍执行原产品函数，条件和容差保持不变 |
| 最终决定 | 现有最终授权函数输入/输出；最终发送的身份、版本、几何哈希、Map/KML和扣次返回字段 | 只观察，不调用结算、不提升权限 |

投影诊断可区分 `INPUT_COMPLETENESS`、`ROW_CONTRACT`、`SOURCE_AXIS_ORDER`、`SOURCE_CRS_SELECTION`、`PROJECTED_VALUE_RANGE`、`TRANSFORM_POINT_COUNT`、`GEOMETRY_VALIDITY`、`DMS_REFERENCE_MATCH`、`TRANSFORM_EXCEPTION`。行序和数值范围有逐行检查记录；DMS参考不一致可以输出行下标、点号、纬度/经度字段、实际差值及容差。上游CRS保留在对应阶段证据中，后续业务清空CRS不会覆盖这些记录。

这是诊断侧的细分，不是对用户可见错误、转换算法、行序判断、坐标值或授权的修复。原有笼统业务提示本阶段不改。

## 3. 控制、隐私和重放边界

每个作用域最多16个会话，每会话96事件、2MiB、单文本65536字符、数组512项、递归24层；截断、隐私删减和不可序列化数据明确标为不完整。采集器自身异常不应传播到业务，已验证异常隔离和并发作用域隔离。

默认源文本仅保存摘要/长度；只有显式本地受控采集允许保留文本。调用点不传req、认证头、用户、配额或环境对象；第二层过滤密钥、token、账户、邮箱、Cookie等字段和可疑文本。敏感内容整段扣留，不伪装成完整可重放的原文。自由文本过滤不是任意个人信息的数学证明；真实外部证据仍需受控筛选，本阶段没有启用生产原文采集。

离线工具为 `scripts/recognition-diagnostic-replay.js`，只允许本地文件和固定产品函数白名单，不执行输入文件携带的代码，不调用Provider/OCR/数据库。原内联函数从当前仓库代码加载，模块函数直接导入，未创建平行生产识别引擎。执行时禁外网套接字。显式本地保存使用不覆盖写入，没有默认存储。

源码版本由server目录、server.js、空间知识数据、锁文件及工具内容哈希和Node版本绑定。采集开始时未提供源码摘要，或摘要与当前源码不符，重放标记UNKNOWN；不在保存时冒称历史证据属于当前版本。来源SYNTHETIC/CAPTURED_EVIDENCE只是记录声明，不是生产来源认证。

`MATCHED_CAPTURED_STAGES`仅表示记录下来的产品阶段调用结果一致，不表示完整生产请求重放。缺原始响应为UNKNOWN，删减/截断为PARTIAL，输出差异为DIFFERENT。OCR执行、真实Provider、完整路由时序、计费事务和浏览器生命周期均明确不在阶段调用重放覆盖内。

受控本地命令示例（本次恢复未另行执行）：

```powershell
node scripts/recognition-diagnostic-replay.js --missing-history
node scripts/recognition-diagnostic-replay.js --input "<经筛选的本地重放包.json>"
```

历史03请求缺失原始Provider响应、本地OCR完整快照及阶段绑定记录，仍为UNKNOWN。截图/用户粘贴的最终16行不能冒充原始响应；尚不能据此判定第4行等数字差异最早由OCR、Provider还是哪一个整理阶段产生，也不能宣称03已经复现或修复。

## 4. 实际执行的验证与停止点

### 4.1 历史首次执行：失败后停止，回执保留

执行一次 `node scripts/recognition-architecture-audit-runner.js --phase-b`。顺序运行、有界末尾输出、保留退出码，独立回执不覆盖阶段A。离线子进程清空继承的Provider、Supabase、使用次数等密钥并禁止dotenv加载生产文件，子进程继承非loopback socket拒绝防护。HTTP测试仅使用明确的 `local-mock-only` 虚拟值。

| 验证 | 首次执行结果，不代表恢复后的当前状态 |
| --- | --- |
| server及5个新增/受影响文件语法 | 6项PASS |
| recognition-diagnostics-regression | PASS，退出0，约6.4秒 |
| 精确86ea基线/current投影安全对照 | 18组一致 |
| 基线/current/开启诊断的统一候选对照 | 10组一致：普通表、Markdown、结构化JSON、缺行、重复、逆序、方向冲突、额外字段、空内容、非坐标文本 |
| 投影诊断开启/关闭 | 7组一致：完整、DMS不符、参考缺失、缺轴序、缺CRS、数值越界、点序冲突 |
| 阶段重放、安全和隔离 | 产品调用输出匹配、源码漂移UNKNOWN、缺历史UNKNOWN、篡改输出DIFFERENT、未知操作拒绝、隐私扣留、观察器异常、并发隔离PASS |
| recognition-diagnostics-http-regression | FAIL，退出1，11.805秒；不是实际超时 |
| 后续受影响专项 | 未运行（按首次失败停止） |
| 134/134完整离线验收矩阵 | 本阶段未运行，不沿用阶段A结果充作本阶段通过 |
| 42/42核心回归 | 本阶段未运行，不沿用阶段A结果充作本阶段通过 |

首次失败断言位于当时的 `scripts/recognition-diagnostics-http-regression.js:127`：`diagnostics.sessions.length` 应为1，实际为0。测试先完成DMS旧基线与当前关闭采集的业务字段相等断言；第三个开启采集请求HTTP200、mock调用1次、响应及公开日志未出现私有诊断schema的断言通过，然后在诊断会话数断言失败。当时尚未执行第三请求的最终业务相等比较，也未进入X/Y HTTP组，因此首次执行不能证明HTTP等价性整体通过。

已确认测试包只在 `http.Server.prototype.emit('request')` 外包AsyncLocalStorage作用域；真实路由先执行deadline中间件和异步 `upload.single('image')`，再执行recognizeCoordinatesHandler。处理器入口的诊断工厂没有在所期待的采集作用域中创建记录。最可能的接入问题是外层HTTP事件作用域未覆盖后续异步上传/处理器边界；具体哪个异步回调丢失上下文尚未通过再次执行定位，不把推测写成已证实的产品解析根因。

首次停止时建议只在测试加载器中，将作用域放到实际recognizeCoordinatesHandler执行入口，并等待响应完成后读诊断。不修改产品路由/中间件、不过早关闭作用域、不增加公开采集开关、不把“期望1份”改为“允许0份”。该方案获用户明确授权后，按下一节执行。

首次HTTP实际mock调用3次（DMS基线/关闭/开启各1次）；真实Provider调用0、真实Supabase调用0、生产识别任务0、生产图片上传0。首次失败后至新授权前没有修改产品/测试代码或重跑，只做只读核对和报告。

回执：`Temp/recognition-diagnostics-phase-b/results.json`。
SHA-256：`c01f74e0a68ec1f029999f765da64f2720225b2d1056f4f0e4eb8bb29e931075`。

### 4.2 授权恢复：处理器作用域及剩余离线验收通过

HTTP测试移除最外层 `http.Server.prototype.emit('request')` 采集包装，改为在测试加载器中、实际处理器声明之后且路由注册之前包装 `recognizeCoordinatesHandler`。包装只调用原处理器，在进入处理器时建立诊断作用域，并等待处理器返回及响应 `finish` 后读取诊断。没有修改产品文件、原处理器正文或返回固定业务成功的替代函数；OCR/Provider仍只在I/O边界使用合成证据。

保留且通过的断言包括：每个采集请求恰有1个诊断会话、包含Provider文本提取与候选提取事件、包含最终交付事件，以及交付事件的结果身份、版本、几何哈希、地图/KML和扣次返回字段与同一HTTP响应相等。DMS、X/Y两组均分别执行旧基线、当前关闭采集、当前开启采集，业务字段投影完全一致；不是完整原始HTTP字节相等或真实计费事务验证。该回归mock Provider共6次，每请求1次，真实调用0。

执行一次 `node scripts/recognition-architecture-audit-runner.js --resume-phase-b-http`，只从先前失败项恢复；不重跑此前通过的6项语法检查与诊断单元回归。恢复入口使用独立目录并拒绝覆盖已有回执。其离线环境隔离与首次运行相同，禁止加载生产环境文件和访问非loopback网络。

| 恢复验收项目 | 结果 | 耗时 |
| --- | --- | --- |
| recognition-diagnostics-http-regression | PASS；两组HTTP诊断捕获及业务等价 | 2714ms |
| projected-crs-source-evidence-regression | PASS | 2588ms |
| multi-representation-source-evidence-regression | PASS；6种输出变体 | 150ms |
| coordinate-markdown-table-regression | PASS | 120ms |
| recognition-first-review-result-v2-regression | PASS | 164ms |
| recognition-first-acquisition-evidence-v3-regression | PASS；既有日志名称为v4 | 153ms |
| multi-representation-http-regression | PASS；mock 1次，真实0 | 694ms |
| p08h-confirmation-ui-lifecycle-regression | PASS | 107ms |
| source-coordinate-review-display-regression | PASS；32/32 | 113ms |
| review-output-contract-regression | PASS；39/39 | 156ms |
| recognition-projected-authorization-v8-regression | PASS | 1726ms |
| production-recognition-recovery-p0-regression | PASS；134/134 | 25234ms |
| production-core-capability-closure-p0-regression | PASS；42/42 | 12247ms |

全部13个进程退出码0，`timedOut=false`，没有自动重试。投影CRS专项有既有OCR分辨率警告（25dpi按70dpi处理），该进程断言通过，无超时。

新回执：`Temp/recognition-diagnostics-phase-b-http-recovery/results.json`。
SHA-256：`57fb09dada9fd4bffcd856fc3e561680c33d489e9a1b6043871c519ebebfce94`。
旧失败回执及阶段A回执内容哈希与恢复前相同，均未覆盖。本次真实Provider调用0、真实Supabase调用0、生产识别任务0、生产图片上传0。

这些结果证明受测本地HTTP路径可捕获诊断、诊断不改变受测业务结果。它们不证明历史03请求的失败原因、真实模型识别准确率、所有异步任务边界或完整浏览器/真实计费生命周期；未采集到的历史证据继续标记UNKNOWN。

## 5. 修改清单和Git状态

本阶段涉及9个文件：

- `server.js`：独立阶段观察点、投影失败细分；保留阶段A四声明删除。
- `server/recognition/recognition-first-acquisition.js`：可选观察器，记录实际候选输入和输出。
- `server/recognition/recognition-diagnostics.js`：新增版本化、受限、隔离采集器。
- `scripts/recognition-architecture-probe.js`：将现有产品函数加载器导出供重放复用，原characterize仍保留。
- `scripts/recognition-architecture-audit-runner.js`：新增独立阶段B执行计划和HTTP恢复入口/回执；默认阶段A计划保留。
- `scripts/recognition-diagnostic-replay.js`：新增受控本地阶段重放、摘要校验和失败字段汇总。
- `scripts/recognition-diagnostics-regression.js`：新增等价、隐私、来源和重放回归。
- `scripts/recognition-diagnostics-http-regression.js`：新增实际HTTP对照；恢复后在真实处理器入口采集并等待响应完成，断言通过。
- `docs/recognition-diagnostics-phase-b-2026-09-30.md`：本报告。

此前的阶段A审计报告、cleanup-regression及offline-guard没有再修改或删除。`package.json`、`package-lock.json`、`index.html`无修改，依赖未安装/升级。Git暂存区为空，HEAD和分支不变；包含阶段A成果共12个未提交文件（2个已跟踪修改、10个未跟踪文件）。

原阶段A cleanup回归断言的是“全文仅删除四声明”，不适用于新增诊断后的全文；该历史回归及通过回执原样保留。本阶段另设行为对照，不通过放宽该历史断言取得PASS。

本次恢复仅修改上述HTTP回归、runner及本报告3个文件。恢复前后的 `server.js`、`server/recognition/recognition-first-acquisition.js`、`server/recognition/recognition-diagnostics.js`、`package.json`、`package-lock.json`、`index.html` 内容SHA-256逐一一致。既有未提交产品诊断改动保留，本次未增加产品改动。`git diff --check`通过，暂存区仍为空。

## 6. 下一步和退出标准

阶段B本次授权要求的本地HTTP诊断门禁及剩余离线回归已完成。不提交、不发布；无需用户再次上传图片试错。

仍不能满足“历史03请求完整重放并确认最早数字差异”的目标，因为原始Provider响应、本地OCR快照及中间状态并不存在于本次可用证据中。不能以合成测试通过替代这项缺失证据，也不能宣称架构或识别准确性问题已经解决。

建议下一阶段C先做“统一结果契约及离线影子对照”：定义版本化表/字段/证据/CRS/最终决定契约，使用现有产品函数及受控合成/留存证据作只读适配和差分分类，不接生产、不改变实际解析优先级或授权/计费。退出标准是已覆盖格式、CRS、方向、行序、身份、版本和冲突的差分均有解释，没有未解释安全差异；不以缺失值默认为成功。旧生产路径保留，任何业务迁移另行授权。
