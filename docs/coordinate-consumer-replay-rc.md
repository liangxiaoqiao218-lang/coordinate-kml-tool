# R2/V2 历史结果消费者：本地主审与后续交接

## 当前结论与权限边界

2026-10-12，主线按用户最新指令接手，L 保持停手。保留其已有客户端修改，仅完成以下 7 个既有路径的修复和无网络增量验收：

1. `scripts/coordinate-consumer-replay-client.ps1`
2. `scripts/coordinate-consumer-replay-client-regression.ps1`
3. `server/recognition/coordinate-consumer-replay.js`
4. `index.html`（仅 R2/V2 常量、查询参数及既有隔离标识）
5. `scripts/coordinate-consumer-replay-gate.ps1`
6. `scripts/coordinate-consumer-replay-regression.js`
7. 本文件。

基线 HEAD：`51a780c196ebdb83c6eaa6c19fe27724bbd5b5bd`。修复仍为未提交工作树。
本阶段结论：**LIMITED_OFFLINE_CANDIDATE_PASS**；不是已部署、真实地图绘制或真实浏览器下载通过。

本阶段提交、推送、真实客户端启动、部署、浏览器认证、外部服务操作、模型调用、数据库写入、业务扣次、预算增加均为 0；没有恢复定时跟进，也没有唤醒 L。此前额外部署（包括 `dep-db5tn9id0e5s738m5jr0`）仍记为越界，不追认、不抵扣后续额度。R1/R7 不恢复、不重新检查或关闭。

以下外部执行交接是准备材料，**本轮不执行**；须由用户解除“暂不操作外部服务”边界后才能继续，不把旧批准当作现在即可启动的许可。

## 修复及主审

| 已查明问题 | 本地修复 | 验证边界 |
| --- | --- | --- |
| 未放行即读取输入，早先排队的回车可能越过等待 | 先进入 WAITING_RELEASE；仅轮询本地 manifest，不读输入、不请求 HTTP。实际 validator 校验 R2/V2、候选、PID、进程创建时间、实例 nonce、放行时间和严格布尔值 | 缺失、旧实例、旧轮次、错误候选、字符串布尔、未来/过早时间均停止 |
| 客户端已退出仍继续启用或认证 | 新增本地只读 Test-ReplayWaitingClient：核对预先捕获的 PID+创建时间+nonce、候选、精确阶段、总期限、无终态回执/失败；进程不存在或 PID 复用即拒绝 | 执行方必须在启用、要求认证及写 release 前即时运行；状态文件存在本身不是存活证明。它不是跨工具操作的原子锁 |
| 15 分钟只覆盖放行等待 | 总期限取实际 PowerShell 子进程创建时间+15分钟，包含预检、部署、人工交接、公共版本读取、密码、注册、证据及物理 KML 核查；阻塞 Read-Host 改为同期限内的遮蔽 SecureString 控制台循环 | 输入、HTTP 及消费等待均有界；关闭清理不受执行期限阻断 |
| 状态写失败会跳过 close，外层 catch 把已用次数清零 | 一份共享回执在动作前记录尝试；CLOSING 写失败仍执行唯一 close；终态状态或回执持久化失败不抹除真实计数，不重写/重试 | 注册未尝试 close=0；尝试过注册（含响应不明）close<=1 |
| 原始异常可能进入回执 | 客户端失败码严格固定白名单，HTTP/浏览器/XML 原始错误不写入回执；阶段也白名单化 | 未识别错误保留通用失败码，不输出消息正文；close/状态失败另有字段 |
| 本地原子更新和时间反序列化不兼容 | File.Replace 使用真正的 null string，不传空备份路径；状态/manifest/HTTP JSON 使用 DateKind String 保留时间精度 | 实际临时文件原子更新链已运行；无真实凭据 |
| R2 绑定及期限衔接 | 客户端、服务模块、浏览器、物理 gate 统一 R2/V2；服务端拒绝无有效浏览器会话的注册，handoff 不超过8分钟及既有认证期限；客户端再取自身15分钟截止的较早者 | server.js 未改；服务端并不知道本地进程期限，依靠客户端截止、close及执行方停止操作共同约束，不宣称服务端独立强制本地15分钟 |

正常状态链最多 7 次原子写入尝试：
`PREPARING → WAITING_RELEASE → RUNTIME_CHECK → WAITING_PASSWORD → RUNNING → CLOSING → COMPLETED`。
失败以 FAILED 替代后续状态；持久化失败后不再尝试状态写入。最终回执最多写一次。写入失败时仅保留脱敏控制台回执，必须报告证据缺口，不能宣称落盘成功。

放行后先读取无凭据的公共 runtime identity，要求精确候选、enabled=true（真实布尔）及 R2 run。
然后调用原生 FlushConsoleInputBuffer 丢弃排队输入，不读取/回显其字符；原生控制台不支持则失败，不换方式重启。通过后才显示遮蔽密码提示。空密码本地拒绝，用户输入内容不写入状态、manifest、回执、URL或日志。

## 新增离线证据

按影响范围定向执行，不重跑旧 R1 的 9 个客户端用例和其余已通过消费者用例。修改后验证结果按最后一次对应定向运行汇总，不把早期失败运行算通过：

- PowerShell R2 入口及故障矩阵：31 项通过。实际使用生产入口编排、manifest validator、状态迁移与原子写、带截止时间的 SecureString 输入循环、协议以及真实物理 KML parser。控制台键盘、时钟、HTTP 及故障注入边界使用合成适配器。覆盖放行前无读取/请求、错误 runtime、空输入/输入超时、注册/证据/物理 gate 超时、浏览器失败、状态/终态/回执/close 失败及真实 PID 的存活检查。
- Node R2 绑定/期限/完整内部消费者链：4 项通过。动态加载实际 server.js session/register/status/receive/stage/evidence/map/close handlers，实际 confirmation registry、Map adapter、产品 installer、地图 gate、downloadKml wrapper，再把产品生成的 Blob 写入临时目录交由真实 PowerShell parser 对照保存几何。
- 三个 PowerShell 文件 AST 无语法错误；git diff --check 无差异格式错误。
- 离线链中的外部 HTTP 由内存 dispatcher 代替，不监听本机端口。地图 renderer 是外部边界 stub，Blob 落入测试临时目录不是浏览器实际下载。
- 测试只创建各自唯一临时目录；删除前核对绝对路径仍是系统 Temp 下对应前缀目录，测试后删除这些可再生临时产物，不删除业务文件。
- 回执中的零模型/零数据库等是这条隔离消费路径的边界声明；不是对真实外部环境/账单的远程核验。

原生终端可操作性、实际启用部署、浏览器认证、OpenFreeMap 可见底图、实际点击下载及物理落盘仍须后续唯一真实验收；本地测试不能替代这些结果。

离线入口（只用于后续确有修改影响时定向运行，不要求现在重跑）：

```powershell
& 'C:\Users\Mir-1\.cache\codex-runtimes\codex-primary-runtime\dependencies\native\powershell\pwsh.exe' -NoProfile -File scripts/coordinate-consumer-replay-client-regression.ps1 -HistoricalArtifactPath 'C:\Users\Mir-1\Documents\Codex\2026-10-12\coordinate-three-case-rc-r7-evidence\r7-sanitized-receipt.json.validated-results.json' -R2Incremental -CaseFilter '<受影响用例名>'
$env:CONSUMER_REPLAY_ARTIFACT_PATH = 'C:\Users\Mir-1\Documents\Codex\2026-10-12\coordinate-three-case-rc-r7-evidence\r7-sanitized-receipt.json.validated-results.json'
node scripts/coordinate-consumer-replay-regression.js --r2-incremental
```

运行时固定为随 Codex 提供的 PowerShell 7.5+（需要 DateKind String），不要改用旧 Windows PowerShell 5.1。

## 冻结的数据边界

- Run：`coordinate-indonesia-consumer-replay-20261012-r2`
- Batch：`coordinate-indonesia-consumer-replay-20261012-v2`
- Evidence class：`HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY`
- 历史结果 SHA-256：`0de3ed5542962d8203e15e654bc2e908dc659745b72b55c5c1518075f94f3522`
- 原图 SHA-256：`41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2`
- 固定业务访客：`rc-b-release-20261010-4f125a9`
- 旧 R7 结果、run、batch、commit 仅作来源证明；仅建立新的内存 replay identity，不恢复旧授权、会话或账本。

只验证同一保存几何经过既有真实产品地图、产品 KML 按钮和物理 parser。隔离 replay-only 免业务扣次，不改变普通用户权限，也不算“新图识别→地图→下载”普通用户全链通过。MGRS/BFTM 不在本轮。

## 后续执行的固定上限（未重新授权或消耗）

| 操作 | 本方案上限 |
| --- | --- |
| 候选提交、推送 | 各1 |
| 合并启用、唯一关闭部署 | 各1；未启用则不做关闭部署 |
| 可见客户端启动、实际执行 | 各1 |
| 浏览器页面、人工认证 | 各1 |
| 放行 manifest、最终回执 | 各1 |
| 状态原子更新尝试 | 最多7 |
| 历史结果注册、接收、产品地图查看 | 各1 |
| 产品 KML 下载、落盘 XML/几何核对 | 各1 |
| 浏览器消费证据 POST | 1 |
| 客户端 close 请求 | 注册尝试前0；尝试后最多1，响应未知不重试 |
| 初始化/数据库写入/模型/业务扣次/预算增加 | 全部0 |
| 自动重试、额外配置部署、补偿部署 | 全部0 |

部署前核实推送不会自动触发正式站或额外 RC 部署。当前本地接手阶段没有使用上述外部次数；历史越界记录也不冲抵这些次数。

## 交接顺序：以后一次说清位置与操作

1. **先准备且不启动**：完成本地主审、冻结候选、外部执行获准后完成唯一提交/推送及必要只读预检。确定新的专用证据目录、空下载目标和用户可连续配合。不得先启动再等用户回来。L 现在保持停手，后续唯一执行人需要主线明确。
2. **唯一客户端**：在用户可操作终端用固定 PowerShell 子进程启动一次。停在 **WAITING_RELEASE（暂不输入密码）**；这不是密码提示，不在这里粘贴密码。记录本次 PID、创建时间和 nonce，向用户只请求本轮可操作确认。15分钟从这个子进程创建起计。
3. **启用前存活门禁**：只读调用 Test-ReplayWaitingClient，要求 WAITING_RELEASE、正确实例、无最终回执/失败且未超时。任一未知/退出/失败：不启用、不要求认证，不补开进程。
4. **唯一合并启用**：只在原 RC 开启消费者开关并绑定 R2，旧 R7/旧印尼开关保持关闭。核验唯一部署 LIVE、精确候选和消费者 runtime identity。不得额外配置部署。
5. **浏览器先认证**：再次通过同一存活门禁。唯一页面为 `https://coordinate-kml-tool-rc.onrender.com/coordinate?coordinate-consumer-replay=r2`。用户在页面“历史结果消费者验证（零模型）”的“管理员密码”框私下输入一次，点击一次“验证受控浏览器”。确认固定身份、权限、OpenFreeMap 及准确下载目录准备妥当。代理不操作密码框、不读取剪贴板；不要刷新/重开/重复认证。
6. **一次 release**：执行人再次核验本次进程仍在 WAITING_RELEASE、没有终态回执，然后根据已核验的唯一 enable deploy 证据原子写一次非秘密 manifest。deployId 正则只验证格式，不证明部署 LIVE；LIVE/候选/浏览器就绪由执行人根据真实证据确认，不得自行填入假授权。
7. **终端后输入**：客户端自行校验 manifest→公共 runtime→原生清空排队输入，才显示遮蔽提示。执行人核对同一实例处于 WAITING_PASSWORD 且仍在期限内后，通知用户回到原 R2 终端，在该提示后私下输入同一密码一次并回车。不得让用户输入 READY，也不收聊天密码。
8. **一次消费**：客户端注册冻结结果后，用户在同一浏览器依次点击“接收历史结果”→“查看地图”，确认真实 OpenFreeMap 底图和几何可见→一次产品“下载 KML”。不能重复解析编辑框、重新识别、修改点序/边界、确认坐标或脚本另造文件。
9. **结果与关闭**：客户端对实际落盘 coordinates.kml 核对 XML、类型、经纬顺序、分组/环/坐标和 SHA；最晚在较早截止前完成业务步骤。注册曾尝试就 finally close 一次，关闭清理允许越过业务截止但有30秒请求上限。无论业务/close 成败，只要已经触发启用动作，就执行唯一关闭部署；不得补偿部署或数据库补写。核对真实关闭状态，不把进程退出/过期等同入口关闭。
10. **交付**：同结果产品地图画面、产品实际下载 KML、几何匹配及 SHA、身份绑定、实际操作次数、内存会话撤销与唯一关闭部署证据。失败明确卡在接收/地图/下载/关闭哪一步，后续动作停止，不续期、不重启。

release 文件只允许以下非秘密字段（值必须来自这一次实际证据，不复制旧实例）：

```json
{
  "schemaVersion": "coordinate_consumer_replay_release_v1",
  "runId": "coordinate-indonesia-consumer-replay-20261012-r2",
  "batchId": "coordinate-indonesia-consumer-replay-20261012-v2",
  "expectedCommit": "<本次唯一候选40位SHA>",
  "clientProcessId": 0,
  "clientProcessStartedAt": "<本次state原样UTC时间>",
  "clientInstanceNonce": "<本次32位nonce>",
  "enableDeployId": "<本次已核验LIVE的唯一dep-id>",
  "releaseAuthorized": true,
  "releasedAt": "<实际放行UTC时间>"
}
```

放行时间必须位于进程创建之后、当前时间之前且仍在原15分钟内；会话最多30分钟，注册handoff最多8分钟，任何有效期不续签。

## 历史消费通过后的剩余主线

复用本轮产品 installer、同 identity 地图和 KML 下载/校验消费者，不另起验证工程。仍需回接新图片的一次真实模型结果、业务身份/权限及正常扣次路径，核验代表性坐标类型；本轮不做新的识别或 MGRS/BFTM 门禁改动。历史回放通过不能替代普通用户路径或正式发布审批，正式站保持不动。
