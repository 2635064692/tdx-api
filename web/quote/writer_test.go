package quote

import (
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"xorm.io/xorm"
)

func TestBatchWriterFlushesOnBatchSize(t *testing.T) {
	db := newWriterDB(t)
	queue := make(chan *QuoteTick, 4)
	writer := NewBatchWriter(db, queue, 2, time.Hour)
	if err := writer.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer writer.Stop()

	queue <- sampleTick("603000", 1000, 1)
	queue <- sampleTick("603000", 1001, 2)
	waitForCount(t, db, 2)

	stats := writer.Stats()
	if stats.TotalReceived != 2 || stats.TotalWritten != 2 || stats.TotalFailed != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestBatchWriterIgnoresDuplicateRows(t *testing.T) {
	db := newWriterDB(t)
	queue := make(chan *QuoteTick, 4)
	writer := NewBatchWriter(db, queue, 2, time.Hour)
	if err := writer.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer writer.Stop()

	queue <- sampleTick("603000", 1000, 1)
	queue <- sampleTick("603000", 1000, 1)
	waitForCount(t, db, 1)

	stats := writer.Stats()
	if stats.TotalReceived != 2 || stats.TotalFailed != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestBatchWriterFlushesOnInterval(t *testing.T) {
	db := newWriterDB(t)
	queue := make(chan *QuoteTick, 4)
	writer := NewBatchWriter(db, queue, 10, 50*time.Millisecond)
	if err := writer.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer writer.Stop()

	queue <- sampleTick("000001", 2000, 1)
	waitForCount(t, db, 1)
}

func newWriterDB(t *testing.T) *xorm.Engine {
	t.Helper()
	db, err := xorm.NewEngine("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("new sqlite engine failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
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
)`); err != nil {
		t.Fatalf("create quote_tick_realtime failed: %v", err)
	}
	return db
}

func sampleTick(code string, eventTs int64, seq uint64) *QuoteTick {
	return &QuoteTick{
		Code:       code,
		Exchange:   "sh",
		TradeDate:  mustClock("2026-03-08T00:00:00+08:00"),
		EventTs:    eventTs,
		Seq:        seq,
		Volume:     100,
		Amount:     12.5,
		InsideVol:  40,
		OutsideVol: 60,
	}
}

func waitForCount(t *testing.T, db *xorm.Engine, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, err := db.Table("quote_tick_realtime").Count()
		if err == nil && int(count) == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	count, err := db.Table("quote_tick_realtime").Count()
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	t.Fatalf("expected %d rows, got %d", expected, count)
}
