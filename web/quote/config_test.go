package quote

import (
	"strings"
	"testing"
)

func TestParseConfigUsesSysConfigContract(t *testing.T) {
	if !strings.Contains(configQuerySQL, "FROM sys_config") {
		t.Fatalf("expected sys_config in query: %s", configQuerySQL)
	}
	if strings.Contains(configQuerySQL, "system_config") {
		t.Fatalf("unexpected system_config in query: %s", configQuerySQL)
	}
}

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(map[string]string{}, 12)
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.Enabled {
		t.Fatalf("expected disabled by default")
	}
	if cfg.Source != "real" {
		t.Fatalf("unexpected source: %s", cfg.Source)
	}
	if cfg.BatchSize != DefaultBatchSize || cfg.FlushIntervalMs != DefaultFlushIntervalMs {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.UpdatedAt != 12 {
		t.Fatalf("unexpected update ts: %d", cfg.UpdatedAt)
	}
}

func TestParseConfigSupportsRequiredKeys(t *testing.T) {
	cfg, err := parseConfig(map[string]string{
		ConfigKeyStorageEnabled:   "1",
		ConfigKeyStorageSource:    "mock",
		ConfigKeySubscribeCodes:   "603000, 000001",
		ConfigKeyTradeSessions:    "09:15-11:30,13:00-15:00",
		ConfigKeyStorageBatchSize: "128",
		ConfigKeyStorageFlushMs:   "2500",
	}, 100)
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if !cfg.Enabled || cfg.Source != "mock" {
		t.Fatalf("unexpected enable/source: %+v", cfg)
	}
	if got := strings.Join(cfg.Codes, ","); got != "603000,000001" {
		t.Fatalf("unexpected codes: %s", got)
	}
	if len(cfg.Ranges) != 2 {
		t.Fatalf("unexpected ranges: %+v", cfg.Ranges)
	}
	if cfg.BatchSize != 128 || cfg.FlushIntervalMs != 2500 {
		t.Fatalf("unexpected tuning: %+v", cfg)
	}
}

func TestParseTradeSessionsRejectsInvalidWindow(t *testing.T) {
	if _, err := parseTradeSessions("11:30-09:15"); err == nil {
		t.Fatalf("expected invalid session error")
	}
}
