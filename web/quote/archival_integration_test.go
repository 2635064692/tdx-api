//go:build integration

package quote

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestArchivalVerifyOneMinute_603977_20260312(t *testing.T) {
	dsn := os.Getenv("QUOTE_STORAGE_MYSQL_DSN")
	if dsn == "" {
		t.Fatalf("QUOTE_STORAGE_MYSQL_DSN is empty; set it in the test environment")
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

	t.Logf("minuteStartTs=%d ticks=%d buyLevels=%d sellLevels=%d", minuteStartTs, len(oneMinute), len(payload.Minutes[0].BuyLevel), len(payload.Minutes[0].SellLevel))
	t.Logf("archival_payload_json=%s", history.QuoteTicks)
}

