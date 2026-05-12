package quote

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"xorm.io/xorm"
)

const (
	ConfigKeySubscribeCodes   = "QUOTE_WS_SUBSCRIBE_CODES"
	ConfigKeyTradeSessions    = "QUOTE_WS_TRADE_SESSIONS"
	ConfigKeyStorageEnabled   = "QUOTE_STORAGE_ENABLED"
	ConfigKeyStorageSource    = "QUOTE_STORAGE_SOURCE"
	ConfigKeyStorageBatchSize = "QUOTE_STORAGE_BATCH_SIZE"
	ConfigKeyStorageFlushMs   = "QUOTE_STORAGE_FLUSH_INTERVAL_MS"
	ConfigKeyMarketHolidays   = "QUOTE_MARKET_HOLIDAYS"
	DefaultPollInterval       = time.Minute
)

var watchedConfigKeys = []string{
	ConfigKeyStorageEnabled,
	ConfigKeyStorageSource,
	ConfigKeySubscribeCodes,
	ConfigKeyTradeSessions,
	ConfigKeyStorageBatchSize,
	ConfigKeyStorageFlushMs,
	ConfigKeyMarketHolidays,
}

const configQuerySQL = `
SELECT config_key, config_value, update_ts
FROM sys_config
WHERE config_key IN (
    'QUOTE_STORAGE_ENABLED',
    'QUOTE_STORAGE_SOURCE',
    'QUOTE_WS_SUBSCRIBE_CODES',
    'QUOTE_WS_TRADE_SESSIONS',
    'QUOTE_STORAGE_BATCH_SIZE',
    'QUOTE_STORAGE_FLUSH_INTERVAL_MS',
    'QUOTE_MARKET_HOLIDAYS'
)`

type ConfigPoller struct {
	db            *xorm.Engine
	interval      time.Duration
	currentConfig QuoteSubscriptionConfig
	lastUpdateTs  int64
	onChange      func(QuoteSubscriptionConfig)
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	started       bool
}

func NewConfigPoller(db *xorm.Engine, interval time.Duration, onChange func(QuoteSubscriptionConfig)) *ConfigPoller {
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	poller := &ConfigPoller{
		db:            db,
		interval:      interval,
		currentConfig: DefaultConfig(),
		onChange:      onChange,
	}
	poller.resetContext()
	return poller
}

func (p *ConfigPoller) Start() error {
	if p.db == nil {
		return nil
	}
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.resetContext()
	ctx := p.ctx
	p.started = true
	p.mu.Unlock()
	if err := p.Poll(); err != nil {
		p.Stop()
		return err
	}
	go p.run(ctx)
	return nil
}

func (p *ConfigPoller) run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.Poll(); err != nil {
				log.Printf("[quote-storage] config poll failed: %v", err)
			}
		}
	}
}

func (p *ConfigPoller) Stop() {
	p.mu.Lock()
	p.started = false
	cancel := p.cancel
	p.cancel = nil
	p.ctx = nil
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (p *ConfigPoller) Poll() error {
	rows, err := p.loadRows()
	if err != nil {
		return err
	}
	cfg, updateTs, err := p.parseRows(rows)
	if err != nil {
		return err
	}
	if !p.shouldApply(updateTs, cfg) {
		return nil
	}
	p.apply(cfg, updateTs)
	return nil
}

func (p *ConfigPoller) Current() QuoteSubscriptionConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentConfig.Clone()
}

func (p *ConfigPoller) resetContext() {
	if p.cancel != nil {
		p.cancel()
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
}

func (p *ConfigPoller) loadRows() ([]map[string]string, error) {
	if p.db == nil {
		return nil, nil
	}
	return p.db.QueryString(configQuerySQL)
}

func (p *ConfigPoller) parseRows(rows []map[string]string) (QuoteSubscriptionConfig, int64, error) {
	values := make(map[string]string, len(rows))
	var updateTs int64
	for _, row := range rows {
		key := strings.TrimSpace(row["config_key"])
		if key == "" {
			continue
		}
		values[key] = strings.TrimSpace(row["config_value"])
		ts, err := strconv.ParseInt(strings.TrimSpace(row["update_ts"]), 10, 64)
		if err == nil && ts > updateTs {
			updateTs = ts
		}
	}
	cfg, err := parseConfig(values, updateTs)
	return cfg, updateTs, err
}

func (p *ConfigPoller) shouldApply(updateTs int64, cfg QuoteSubscriptionConfig) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if updateTs > p.lastUpdateTs {
		return true
	}
	return updateTs == 0 && !configsEqual(p.currentConfig, cfg)
}

func (p *ConfigPoller) apply(cfg QuoteSubscriptionConfig, updateTs int64) {
	p.mu.Lock()
	p.currentConfig = cfg.Clone()
	p.lastUpdateTs = updateTs
	p.mu.Unlock()
	if p.onChange != nil {
		p.onChange(cfg.Clone())
	}
}

func parseConfig(values map[string]string, updateTs int64) (QuoteSubscriptionConfig, error) {
	cfg := DefaultConfig()
	cfg.Enabled = parseEnabled(values[ConfigKeyStorageEnabled])
	cfg.Source = parseSource(values[ConfigKeyStorageSource])
	cfg.Codes = splitConfigList(values[ConfigKeySubscribeCodes])
	cfg.TradeSessions = normalizeWhitespace(values[ConfigKeyTradeSessions])
	cfg.UpdatedAt = updateTs

	batchSize, err := parsePositiveInt(values[ConfigKeyStorageBatchSize], DefaultBatchSize)
	if err != nil {
		return cfg, fmt.Errorf("invalid %s: %w", ConfigKeyStorageBatchSize, err)
	}
	cfg.BatchSize = batchSize

	flushMs, err := parsePositiveInt(values[ConfigKeyStorageFlushMs], DefaultFlushIntervalMs)
	if err != nil {
		return cfg, fmt.Errorf("invalid %s: %w", ConfigKeyStorageFlushMs, err)
	}
	cfg.FlushIntervalMs = flushMs

	ranges, err := parseTradeSessions(cfg.TradeSessions)
	if err != nil {
		return cfg, fmt.Errorf("invalid %s: %w", ConfigKeyTradeSessions, err)
	}
	cfg.Ranges = ranges
	cfg.Holidays = parseHolidays(values[ConfigKeyMarketHolidays])
	return cfg, nil
}

func parseTradeSessions(sessions string) ([]TimeRange, error) {
	sessions = normalizeWhitespace(sessions)
	if sessions == "" {
		return nil, nil
	}
	parts := strings.Split(sessions, ",")
	ranges := make([]TimeRange, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		r, err := parseTimeRange(item)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

func parseTimeRange(raw string) (TimeRange, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return TimeRange{}, fmt.Errorf("invalid range %q", raw)
	}
	start, err := parseClock(parts[0])
	if err != nil {
		return TimeRange{}, err
	}
	end, err := parseClock(parts[1])
	if err != nil {
		return TimeRange{}, err
	}
	if start >= end {
		return TimeRange{}, fmt.Errorf("start must be earlier than end")
	}
	return TimeRange{StartMinutes: start, EndMinutes: end}, nil
}

func parseClock(raw string) (int, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid clock %q", raw)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, fmt.Errorf("invalid clock %q", raw)
	}
	return hour*60 + minute, nil
}

func parsePositiveInt(raw string, def int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return value, nil
}

func parseEnabled(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parseSource(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "mock":
		return "mock"
	default:
		return "real"
	}
}

func splitConfigList(raw string) []string {
	raw = strings.NewReplacer("\n", ",", ";", ",", "\t", ",").Replace(raw)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func normalizeWhitespace(raw string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(raw)), "")
}

func configsEqual(a, b QuoteSubscriptionConfig) bool {
	if a.Enabled != b.Enabled || a.Source != b.Source || a.TradeSessions != b.TradeSessions || a.BatchSize != b.BatchSize || a.FlushIntervalMs != b.FlushIntervalMs || !holidaysEqual(a.Holidays, b.Holidays) {
		return false
	}
	if len(a.Codes) != len(b.Codes) || len(a.Ranges) != len(b.Ranges) {
		return false
	}
	for i := range a.Codes {
		if a.Codes[i] != b.Codes[i] {
			return false
		}
	}
	for i := range a.Ranges {
		if a.Ranges[i] != b.Ranges[i] {
			return false
		}
	}
	return true
}

func parseHolidays(raw string) map[string]bool {
	dates := splitConfigList(raw)
	if len(dates) == 0 {
		return nil
	}
	m := make(map[string]bool, len(dates))
	for _, d := range dates {
		if d != "" {
			m[d] = true
		}
	}
	return m
}

func holidaysEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
