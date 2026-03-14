//go:build integration

package quote

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestArchivalVerifyOneMinute_603977_20260312(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("QUOTE_STORAGE_MYSQL_DSN"))
	if dsn == "" {
		var err error
		dsn, err = loadEnvValue(".env", "QUOTE_STORAGE_MYSQL_DSN")
		if err != nil {
			t.Fatalf("QUOTE_STORAGE_MYSQL_DSN is empty and not found in .env: %v", err)
		}
	}

	db, err := OpenMySQLEngine(dsn)
	if err != nil {
		t.Fatalf("open mysql engine failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tradeDate, err := time.ParseInLocation("2006-01-02", "2026-03-12", time.UTC)
	if err != nil {
		t.Fatalf("parse trade date failed: %v", err)
	}

	rows := make([]QuoteTickRealtime, 0)
	if err := db.Table(QuoteTickRealtime{}.TableName()).
		Where("DATE(trade_date) = ? AND code = ?", tradeDateKey(tradeDate), "603977").
		Asc("event_ts", "seq").
		Find(&rows); err != nil {
		t.Fatalf("load realtime rows failed: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("no realtime rows found for code=603977 date=2026-03-12")
	}

	minuteStartTs := (rows[0].EventTs / 60_000) * 60_000
	if raw := strings.TrimSpace(os.Getenv("ARCHIVAL_MINUTE_START_TS")); raw != "" {
		override, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid ARCHIVAL_MINUTE_START_TS=%q: %v", raw, err)
		}
		minuteStartTs = (override / 60_000) * 60_000
	}
	minuteEndTs := minuteStartTs + 60_000
	oneMinute := make([]QuoteTickRealtime, 0)
	for i := range rows {
		if rows[i].EventTs < minuteStartTs {
			continue
		}
		if rows[i].EventTs >= minuteEndTs {
			break
		}
		oneMinute = append(oneMinute, rows[i])
	}
	if len(oneMinute) == 0 {
		t.Fatalf("no rows found within first minute window start=%d", minuteStartTs)
	}

	history, err := aggregateQuoteTicks(oneMinute)
	if err != nil {
		t.Fatalf("aggregateQuoteTicks failed: %v", err)
	}

	var payload archivalPayload
	if err := json.Unmarshal([]byte(history.QuoteTicks), &payload); err != nil {
		t.Fatalf("unmarshal archival payload failed: %v", err)
	}
	if payload.Code != "603977" {
		t.Fatalf("unexpected payload code: %q", payload.Code)
	}
	if len(payload.Minutes) != 1 {
		t.Fatalf("expected exactly 1 minute bucket, got %d", len(payload.Minutes))
	}
	if payload.Minutes[0].Ts != minuteStartTs {
		t.Fatalf("expected minuteStartTs=%d, got %d", minuteStartTs, payload.Minutes[0].Ts)
	}

	outPath := fmt.Sprintf("/tmp/archival_603977_2026-03-12_minute_%d.json", minuteStartTs)
	pretty, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	if err := os.WriteFile(outPath, pretty, 0o644); err != nil {
		t.Fatalf("write json file failed: %v", err)
	}
	t.Logf("wrote_json=%s", outPath)
	t.Logf("minuteStartTs=%d ticks=%d buyLevels=%d sellLevels=%d", minuteStartTs, len(oneMinute), len(payload.Minutes[0].BuyLevel), len(payload.Minutes[0].SellLevel))
}

func loadEnvValue(relEnvFile, key string) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", errors.New("key is empty")
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	candidates := []string{
		filepath.Join(wd, relEnvFile),
		filepath.Join(wd, "..", relEnvFile),
		filepath.Join(wd, "..", "..", relEnvFile),
	}

	var lastErr error
	for _, path := range candidates {
		raw, err := os.ReadFile(path)
		if err != nil {
			lastErr = err
			continue
		}
		val, ok := parseDotEnv(string(raw), key)
		if ok && strings.TrimSpace(val) != "" {
			return strings.TrimSpace(val), nil
		}
		lastErr = fmt.Errorf("key %s not found in %s", key, path)
	}
	if lastErr == nil {
		lastErr = errors.New("no .env candidates checked")
	}
	return "", lastErr
}

func parseDotEnv(content, key string) (string, bool) {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		if k != key {
			continue
		}
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"'`)
		return v, true
	}
	return "", false
}
