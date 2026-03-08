# Quote Storage 运行说明

## 1. 配置入口

### 环境变量

- `QUOTE_STORAGE_MYSQL_DSN`：MySQL 连接串；未配置时主服务仍启动，但行情存储接口会返回“未就绪”。
- `TDX_WAIT_TIMEOUT`：沿用现有 TDX 超时配置。

### sys_config 配置项

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

- **状态显示未就绪**：优先检查 `QUOTE_STORAGE_MYSQL_DSN` 是否配置、MySQL 是否可达。
- **配置更新未生效**：检查 `sys_config.update_ts` 是否刷新；轮询周期默认 60 秒。
- **归档后无历史数据**：先检查目标交易日是否存在 `quote_tick_realtime` 数据，再看 `/api/quote-storage/tasks` 的归档任务错误信息。
- **队列积压**：降低订阅规模或增大 `QUOTE_STORAGE_BATCH_SIZE`，同时检查数据库吞吐与唯一键冲突率。
