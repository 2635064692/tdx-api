package quote

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"xorm.io/xorm"
)

func TestArchivalTaskArchivesAndCleansRealtime(t *testing.T) {
	db := newArchivalDB(t)
	tradeDate := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	insertRealtime(t, db, sampleRealtime("603000", tradeDate, 1000, 1, 100, 1000))
	insertRealtime(t, db, sampleRealtime("603000", tradeDate, 2000, 2, 120, 1005))
	insertRealtime(t, db, sampleRealtime("000001", tradeDate, 1500, 1, 80, 900))

	task := NewArchivalTask(db)
	if err := task.Run(context.Background(), tradeDate); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	historyCount, err := db.Table("quote_tick_history").Count()
	if err != nil {
		t.Fatalf("history count failed: %v", err)
	}
	if historyCount != 2 {
		t.Fatalf("expected 2 history rows, got %d", historyCount)
	}
	realtimeCount, err := db.Table("quote_tick_realtime").Count()
	if err != nil {
		t.Fatalf("realtime count failed: %v", err)
	}
	if realtimeCount != 0 {
		t.Fatalf("expected realtime cleanup, got %d rows", realtimeCount)
	}

	var history QuoteTickHistory
	ok, err := db.Table("quote_tick_history").Where("code = ?", "603000").Get(&history)
	if err != nil || !ok {
		t.Fatalf("load history failed: %v ok=%v", err, ok)
	}
	var payload archivalPayload
	if err := json.Unmarshal([]byte(history.QuoteTicks), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload.TotalHand != 120 || payload.Amount != 12.5 {
		t.Fatalf("unexpected payload totals: %+v", payload)
	}
}

func TestArchivalTaskIsIdempotent(t *testing.T) {
	db := newArchivalDB(t)
	tradeDate := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	insertRealtime(t, db, sampleRealtime("603000", tradeDate, 1000, 1, 100, 1000))

	task := NewArchivalTask(db)
	if err := task.Run(context.Background(), tradeDate); err != nil {
		t.Fatalf("first Run returned error: %v", err)
	}
	if err := task.Run(context.Background(), tradeDate); err != nil {
		t.Fatalf("second Run returned error: %v", err)
	}
	count, err := db.Table("quote_tick_history").Count()
	if err != nil {
		t.Fatalf("history count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one history row after repeat archival, got %d", count)
	}
}

func newArchivalDB(t *testing.T) *xorm.Engine {
	t.Helper()
	db, err := xorm.NewEngine("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("new sqlite engine failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	for _, ddl := range []string{`
CREATE TABLE quote_tick_realtime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    exchange TEXT NOT NULL,
    trade_date DATETIME NOT NULL,
    event_ts BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    volume BIGINT NOT NULL,
    amount DOUBLE NOT NULL,
    inside_vol INT NOT NULL,
    outside_vol INT NOT NULL,
    bid1_price INT, bid1_vol INT,
    bid2_price INT, bid2_vol INT,
    bid3_price INT, bid3_vol INT,
    bid4_price INT, bid4_vol INT,
    bid5_price INT, bid5_vol INT,
    ask1_price INT, ask1_vol INT,
    ask2_price INT, ask2_vol INT,
    ask3_price INT, ask3_vol INT,
    ask4_price INT, ask4_vol INT,
    ask5_price INT, ask5_vol INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, event_ts, seq)
)`, `
CREATE TABLE quote_tick_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    exchange TEXT NOT NULL,
    trade_date DATETIME NOT NULL,
    volume BIGINT NOT NULL,
    amount DOUBLE NOT NULL,
    inside_vol INT NOT NULL,
    outside_vol INT NOT NULL,
    quote_ticks TEXT NOT NULL,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, trade_date)
)`} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("exec ddl failed: %v", err)
		}
	}
	return db
}

func insertRealtime(t *testing.T, db *xorm.Engine, row QuoteTickRealtime) {
	t.Helper()
	if _, err := db.Table(row.TableName()).Insert(&row); err != nil {
		t.Fatalf("insert realtime failed: %v", err)
	}
}

func sampleRealtime(code string, tradeDate time.Time, eventTs int64, seq uint64, volume int64, bid1 int) QuoteTickRealtime {
	return QuoteTickRealtime{
		Code:       code,
		Exchange:   map[bool]string{true: "sh", false: "sz"}[code[0] == '6'],
		TradeDate:  tradeDate,
		EventTs:    eventTs,
		Seq:        seq,
		Volume:     volume,
		Amount:     12.5,
		InsideVol:  40,
		OutsideVol: 60,
		Bid1Price:  bid1,
		Bid1Vol:    11,
		Bid2Price:  bid1 - 1,
		Bid2Vol:    12,
		Bid3Price:  bid1 - 2,
		Bid3Vol:    13,
		Bid4Price:  bid1 - 3,
		Bid4Vol:    14,
		Bid5Price:  bid1 - 4,
		Bid5Vol:    15,
		Ask1Price:  bid1 + 1,
		Ask1Vol:    21,
		Ask2Price:  bid1 + 2,
		Ask2Vol:    22,
		Ask3Price:  bid1 + 3,
		Ask3Vol:    23,
		Ask4Price:  bid1 + 4,
		Ask4Vol:    24,
		Ask5Price:  bid1 + 5,
		Ask5Vol:    25,
	}
}
