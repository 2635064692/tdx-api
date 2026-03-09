# Quote Storage 运行说明

## 1. 配置入口

### 环境变量

- `QUOTE_STORAGE_ENABLED`：启动期开关，建议放在 `.env`。默认按开启处理；显式设为 `0/false/off` 时，直接禁用采集子系统初始化，此时即使未配置 MySQL 也不会影响其他接口和服务启动。
- `QUOTE_STORAGE_MYSQL_DSN`：MySQL 连接串，建议放在 `.env`；仅当 `QUOTE_STORAGE_ENABLED` 未关闭时才需要。
  - 验证环境示例：`root:infini_rag_flow@tcp(192.168.144.3:3306)/vnpy?charset=utf8mb4&parseTime=True&loc=Asia%2FShanghai`
- `TDX_WAIT_TIMEOUT`：沿用现有 TDX 超时配置。

`.env` 示例：

```bash
QUOTE_STORAGE_ENABLED=1
QUOTE_STORAGE_MYSQL_DSN=root:infini_rag_flow@tcp(192.168.144.3:3306)/vnpy?charset=utf8mb4&parseTime=True&loc=Asia%2FShanghai
```

若只想关闭采集且不配置 MySQL：

```bash
QUOTE_STORAGE_ENABLED=0
```

### sys_config 配置项

> `sys_config` 负责运行期采集参数；`.env` 中的 `QUOTE_STORAGE_ENABLED` 负责启动期总开关。只有启动期未关闭时，才会继续读取数据库和轮询 `sys_config`。

| key | 默认值 | 说明 |
| --- | --- | --- |
| `QUOTE_WS_SUBSCRIBE_CODES` | `603*` | 订阅表达式，支持复用 `/ws/quote` 的通配符能力 |
| `QUOTE_WS_TRADE_SESSIONS` | `09:15-11:30,13:00-15:00` | 交易时间窗 |
| `QUOTE_STORAGE_ENABLED` | `0` | 1=启用，0=关闭 |
| `QUOTE_STORAGE_SOURCE` | `real` | `real` 或 `mock` |
| `QUOTE_STORAGE_BATCH_SIZE` | `1000` | 批量写入阈值 |
| `QUOTE_STORAGE_FLUSH_INTERVAL_MS` | `5000` | 定时 flush 间隔 |

## 2. 迁移与数据库结构检查

```bash
mysql "$QUOTE_STORAGE_MYSQL_DSN" < /app/tdx-api/migrations/001_quote_storage_schema.sql
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SHOW CREATE TABLE quote_tick_realtime\G"
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SHOW CREATE TABLE quote_tick_history\G"
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SHOW INDEX FROM quote_tick_realtime"
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SHOW INDEX FROM quote_tick_history"
```

预期检查点：

- 迁移只初始化缺失的 `sys_config` 默认项，不覆盖已存在的业务配置值。

- `quote_tick_realtime` 存在 `uk_code_event_seq(code,event_ts,seq)`。
- `quote_tick_history` 存在 `uk_code_trade_date(code,trade_date)`。
- `sys_config` 已包含 `QUOTE_WS_*`、`QUOTE_STORAGE_*` 默认项。

## 3. 服务接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/api/quote-storage/status` | GET | 查看系统 ready/running/config/writer 状态 |
| `/api/quote-storage/start` | POST | 手动启动采集链路 |
| `/api/quote-storage/stop` | POST | 手动停止采集链路 |
| `/api/quote-storage/stats` | GET | 查看 writer 统计与 collector 代码集合 |
| `/api/quote-storage/tasks` | GET | 查看采集伪任务状态与归档任务列表 |
| `/api/quote-storage/archival?date=YYYY-MM-DD` | POST | 手动触发指定交易日归档 |

## 4. 验证步骤（verify-env）

> 必须在 `go-dev-container` 内执行编译/测试/运行命令。

```bash
docker exec -i go-dev-container bash -lc 'cd /app/tdx-api/web && go build -o tdx-api .'
docker exec -i go-dev-container bash -lc 'cd /app/tdx-api/web && go test ./...'
docker exec -i go-dev-container bash -lc 'cd /app/tdx-api/web && go run .'
curl http://localhost:8080/api/health
curl http://localhost:8080/api/quote-storage/status
```

## 5. 性能观察口径

### 批量写入观测

1. 打开 `QUOTE_STORAGE_ENABLED=1`，设置：
   - `QUOTE_STORAGE_SOURCE=mock`
   - `QUOTE_STORAGE_BATCH_SIZE=1000`
   - `QUOTE_STORAGE_FLUSH_INTERVAL_MS=5000`
2. 通过 `/api/quote-storage/stats` 观察：
   - `writer.total_received`
   - `writer.total_written`
   - `writer.total_failed`
   - `queue_size`
3. 关注 5-10 分钟窗口内：
   - `total_failed` 应维持 0 或可解释的小值。
   - `queue_size` 不应持续单调增长。
   - `total_written / total_received` 应接近 1（考虑重复键时允许轻微偏差）。

### 数据落盘抽查

```bash
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SELECT code, COUNT(*) FROM quote_tick_realtime GROUP BY code ORDER BY COUNT(*) DESC LIMIT 10"
mysql "$QUOTE_STORAGE_MYSQL_DSN" -e "SELECT code, trade_date, LENGTH(quote_ticks) FROM quote_tick_history ORDER BY archived_at DESC LIMIT 10"
```

## 6. 常见故障

- **关闭采集但未配置 MySQL**：把 `.env` 中 `QUOTE_STORAGE_ENABLED=0`，主服务和非 quote-storage 功能可正常启动。
- **状态显示未就绪**：确认 `.env` 未把 `QUOTE_STORAGE_ENABLED` 关闭，再检查 `QUOTE_STORAGE_MYSQL_DSN` 是否配置、MySQL 是否可达。
- **配置更新未生效**：检查 `sys_config.update_ts` 是否刷新；轮询周期默认 60 秒。
- **归档后无历史数据**：先检查目标交易日是否存在 `quote_tick_realtime` 数据，再看 `/api/quote-storage/tasks` 的归档任务错误信息。
- **队列积压**：降低订阅规模或增大 `QUOTE_STORAGE_BATCH_SIZE`，同时检查数据库吞吐与唯一键冲突率。
