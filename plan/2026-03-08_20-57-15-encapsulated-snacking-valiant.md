---
mode: plan
cwd: /home/opensource/golang/tdx-api
task: 以 /root/.claude/plans/encapsulated-snacking-valiant.md 为主的配置驱动实时行情采集与存储系统实施计划
complexity: complex
planning_method: builtin
created_at: 2026-03-08T20:57:15+08:00
---

# Plan: 配置驱动的实时行情采集与存储系统

🎯 任务概述

目标是在现有 `web` 服务中落地“服务端独立行情采集与存储链路”，使行情采集不再依赖在线 WebSocket 客户端，而由配置驱动持续运行，并形成“当日增量表 + 盘后归档表”的双层存储结构。

本计划以 `/root/.claude/plans/encapsulated-snacking-valiant.md` 为主设计依据：配置表固定使用 `sys_config`，表结构、`ConfigPoller → QuoteCollector → BatchWriter → ArchivalTask` 链路、以及归档压缩逻辑均优先参照该文档；仓库内已有实现只作为复用落点与接入约束。

✨ 关键亮点

- **新增文件目录结构必须与源文档一致**
  - 新增实现文件固定落在 `web/quote/config.go`、`web/quote/collector.go`、`web/quote/writer.go`、`web/quote/archival.go`、`web/quote/system.go`。
  - 新增 API 文件固定为 `web/server_api_quote_storage.go`。
  - 新增数据库迁移文件固定为 `migrations/001_quote_storage_schema.sql`。
  - 修改入口文件固定为 `web/server.go`，避免随意分散到其他目录。

- **数据库表结构必须与源文档一致**
  - 配置表固定复用 `sys_config`，不新建同义配置表。
  - `quote_tick_realtime`、`quote_tick_history` 的字段、唯一键、索引、`quote_ticks` 压缩字段和配置初始化 SQL，均以源文档 DDL 为准。
  - 若实现阶段发现仓库现状与源文档冲突，应优先对齐到源文档，而不是私自改动表结构口径。

📋 执行计划

1. 固化边界与命名口径
   - 以“独立 Quote Collector / Recorder”作为实现目标，不把写库逻辑绑定到 `/ws/quote` 连接生命周期。
   - 配置表名称固定为 `sys_config`，并以源文档中的 `QUOTE_WS_*`、`QUOTE_STORAGE_*` 配置项为准，不再引入 `system_config` 别名，避免实现分叉。

2. 设计配置与存储契约
   - 按源文档固化配置项集合：`QUOTE_WS_SUBSCRIBE_CODES`、`QUOTE_WS_TRADE_SESSIONS`、`QUOTE_STORAGE_ENABLED`、`QUOTE_STORAGE_SOURCE`、`QUOTE_STORAGE_BATCH_SIZE`、`QUOTE_STORAGE_FLUSH_INTERVAL_MS`。
   - 具体表结构以源文档为主：`quote_tick_realtime` 包含 `code/exchange/trade_date/event_ts/seq`、成交量额、内外盘、五档盘口和唯一键 `uk_code_event_seq(code, event_ts, seq)`；`quote_tick_history` 包含日级汇总字段与 `quote_ticks` JSON 压缩结果，唯一键为 `uk_code_trade_date(code, trade_date)`。

3. 搭建 MySQL 与配置访问层
   - 新增文件目录结构严格对齐源文档，优先在 `web/quote/` 下拆分 `config.go`、`collector.go`、`writer.go`、`archival.go`、`system.go`，并新增 `web/server_api_quote_storage.go` 与 `migrations/001_quote_storage_schema.sql`。
   - 在 `web` 模块增加独立的存储子系统初始化入口，复用现有 `xorm.NewEngine` 与 `NewSessionFunc` 模式实现表同步、事务与批量写入。
   - 新增配置仓储/DAO，专门轮询 `sys_config` 并生成运行时快照；MySQL DSN、`update_ts` 检测、默认值和降级策略优先参照源文档，缺配置时保持“功能关闭但服务可启动”。

4. 实现配置轮询与运行时状态机
   - 按源文档落地 `ConfigPoller`：60 秒轮询 `sys_config`，基于 `update_ts` 感知变更，并通过回调把最新 `QuoteSubscriptionConfig` 下发给采集/写入链路。
   - 将订阅表达式交给 `normalizeStockCodes` 解析，交易时间窗解析逻辑遵循 `QUOTE_WS_TRADE_SESSIONS` 语义，并补齐启停状态、采样参数的默认值与校验。

5. 实现采集层与写入层闭环
   - 采集层按源文档的 `QuoteCollector` 结构实现：真实行情复用 `fetchQuotesBatched`，模拟行情复用 `mockPusher.generateQuote`，并把 `protocol.Quote` 转成 `QuoteTick` 后入队。
   - 写入层按 `BatchWriter` 设计实现：基于 `batchSize` + `flushInterval` 刷盘，复用 `tdx.NewSessionFunc` 事务模式，对 `quote_tick_realtime` 执行批量插入；幂等策略以 `(code, event_ts, seq)` 唯一键和重复插入忽略为主。

6. 集成任务化控制与服务启动
   - 复用 `TaskManager` 暴露采集任务、归档任务的状态查询、启动、停止与手动触发接口。
   - 在 `web/server.go` 中以最小侵入方式挂载存储子系统初始化、状态 API 与任务 API，接入点优先参照源文档中的 `quoteStorageSystem.Start()` 组织方式，同时保持现有 REST/WS 路由不回归。

7. 实现盘后归档与清理策略
   - 按源文档的 `ArchivalTask` 结构实现工作日 16:00 归档：从 `quote_tick_realtime` 读取当日数据，按 `code + trade_date` 聚合，写入 `quote_tick_history`。
   - `quote_ticks` 压缩逻辑参照源文档引用的 `QuoteMinuteAggregator.java` 思路：保留五档时间序列，使用 `number` 差分数组压缩；归档链路需保证“写历史表 → 校验 → 标记/清理实时表”的幂等顺序。

8. 补齐测试、观测与文档
   - 增加围绕配置解析、diff 更新、批量写入、幂等约束、归档流程的单测/集成测试；数据库相关测试优先做可选或 mock 化，避免阻塞 `go test ./...`。
   - 补充运行文档：环境变量、配置项、表结构、运维接口、常见故障与回退方式。

9. 按 `verify-env` 执行容器内验收回归
   - 编译：`docker exec -i go-dev-container bash -c "cd /app/tdx-api/web && go build -o tdx-api ."`
   - 测试：`docker exec -i go-dev-container bash -c "cd /app/tdx-api/web && go test ./..."`
   - 启动：`docker exec -i go-dev-container bash -c "cd /app/tdx-api/web && go run ."`
   - 健康检查：`curl http://localhost:8080/api/health`；若新增状态/运维接口，再补充针对性回归（例如配置状态、任务状态、手动归档触发）。

⚠️ 风险与注意事项

- 本计划已固定采用 `sys_config`；后续编码、SQL、文档若继续出现 `system_config`，应视为需要修正的偏差。
- 当前实施约束是“目录结构对齐源文档、表结构对齐源文档”，因此不应把新增逻辑随意塞回现有 WS 文件或临时拆到其他目录。
- `web` 当前没有现成的 MySQL 初始化与配置轮询入口；若把数据库连接做成启动强依赖，会直接影响现有 `go run .` 与健康检查回归。
- 源文档中“移除 `seq` 字段”的描述与后续 DDL/唯一键 `(code, event_ts, seq)` 存在冲突；实施时应以其 DDL 与去重策略为准，并在设计说明里显式修正这一点。
- 高频行情写库的核心风险在于唯一键设计、批次大小、队列背压与失败补偿；这部分必须优先做可观测日志与幂等策略。
- 盘后归档涉及“写历史表 → 校验成功 → 清理实时表”的顺序保证，第一版不建议直接物理删除。
- 验收环境必须遵循 `AGENTS.md` 在 `go-dev-container` 中执行；若容器异常，应停止回归并先修复环境，而不是偷偷切回宿主机验证。

📎 参考

- `/root/.claude/plans/encapsulated-snacking-valiant.md:52`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:66`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:102`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:148`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:223`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:275`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:329`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:379`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:622`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:637`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:673`
- `/root/.claude/plans/encapsulated-snacking-valiant.md:695`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:34`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:54`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:63`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:73`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:155`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:196`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:244`
- `docs/ws-quote-subscription-storage-plan-2026-03-07.md:323`
- `web/server.go:45`
- `web/server.go:72`
- `web/server.go:731`
- `web/ws_quote_pusher.go:24`
- `web/ws_quote_pusher.go:139`
- `web/ws_quote.go:177`
- `web/ws_mock_quote.go:22`
- `web/ws_mock_quote.go:155`
- `web/tasks.go:31`
- `web/server_api_extended.go:885`
- `extend/pull-kline-mysql.go:14`
- `codes.go:339`
- `manage.go:156`
- `web/go.mod:14`
