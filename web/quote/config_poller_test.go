package quote

import (
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"xorm.io/xorm"
)

func TestConfigPollerPollAppliesChangesByUpdateTS(t *testing.T) {
	db := newSQLiteConfigDB(t)
	insertConfigRow(t, db, ConfigKeyStorageEnabled, "1", 100)
	insertConfigRow(t, db, ConfigKeyStorageSource, "mock", 100)
	insertConfigRow(t, db, ConfigKeySubscribeCodes, "603000", 100)
	insertConfigRow(t, db, ConfigKeyTradeSessions, "09:15-11:30", 100)

	var mu sync.Mutex
	var applied []QuoteSubscriptionConfig
	poller := NewConfigPoller(db, time.Minute, func(cfg QuoteSubscriptionConfig) {
		mu.Lock()
		defer mu.Unlock()
		applied = append(applied, cfg)
	})

	if err := poller.Poll(); err != nil {
		t.Fatalf("first poll failed: %v", err)
	}
	if err := poller.Poll(); err != nil {
		t.Fatalf("second poll failed: %v", err)
	}
	insertConfigRow(t, db, ConfigKeyStorageSource, "real", 200)
	if err := poller.Poll(); err != nil {
		t.Fatalf("third poll failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(applied) != 2 {
		t.Fatalf("expected 2 applied configs, got %d", len(applied))
	}
	if applied[0].Source != "mock" || applied[1].Source != "real" {
		t.Fatalf("unexpected sources: %+v", applied)
	}
}

func TestConfigPollerPollRejectsInvalidConfig(t *testing.T) {
	db := newSQLiteConfigDB(t)
	insertConfigRow(t, db, ConfigKeyTradeSessions, "11:30-09:15", 100)
	poller := NewConfigPoller(db, time.Minute, nil)
	if err := poller.Poll(); err == nil {
		t.Fatalf("expected invalid config error")
	}
	if poller.Current().TradeSessions != "" {
		t.Fatalf("current config should stay at defaults after invalid poll")
	}
}

func TestConfigPollerStartStopCanRestart(t *testing.T) {
	db := newSQLiteConfigDB(t)
	poller := NewConfigPoller(db, time.Hour, nil)
	if err := poller.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	poller.Stop()
	if err := poller.Start(); err != nil {
		t.Fatalf("restart Start returned error: %v", err)
	}
	poller.Stop()
}

func newSQLiteConfigDB(t *testing.T) *xorm.Engine {
	t.Helper()
	db, err := xorm.NewEngine("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("new sqlite engine failed: %v", err)
	}
	if _, err := db.Exec(`
CREATE TABLE sys_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    remark TEXT,
    update_ts BIGINT NOT NULL
)`); err != nil {
		t.Fatalf("create sys_config failed: %v", err)
	}
	return db
}

func insertConfigRow(t *testing.T, db *xorm.Engine, key, value string, updateTs int64) {
	t.Helper()
	if _, err := db.Exec(`
INSERT INTO sys_config(config_key, config_value, remark, update_ts)
VALUES (?, ?, '', ?)
ON CONFLICT(config_key) DO UPDATE SET config_value=excluded.config_value, update_ts=excluded.update_ts`, key, value, updateTs); err != nil {
		t.Fatalf("upsert config row failed: %v", err)
	}
}
