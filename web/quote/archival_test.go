package quote

import (
	"context"
	"encoding/json"
	"slices"
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
	if len(payload.Minutes) != 1 {
		t.Fatalf("expected one minute payload for sample ticks, got %d", len(payload.Minutes))
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

func TestCompressLevelsUsesPriceAggregation(t *testing.T) {
	rows := []QuoteTickRealtime{
		quoteRowAt(1000, 10, []priceLevel{{Price: 1000, Number: 10}, {Price: 999, Number: 9}}, nil),
		quoteRowAt(2000, 10, []priceLevel{{Price: 1001, Number: 8}, {Price: 1000, Number: 10}}, nil),
	}

	buyLevels := compressLevels(rows, true)
	if len(buyLevels) != 3 {
		t.Fatalf("expected 3 aggregated buy levels, got %d", len(buyLevels))
	}

	level1000 := mustLevel(t, buyLevels, 1000)
	if !slices.Equal(level1000.Number, []int{10, -119}) {
		t.Fatalf("expected price 1000 to be aggregated by price with same marker, got %v", level1000.Number)
	}
	if level1000.Ts != 1000 {
		t.Fatalf("expected first ts 1000, got %d", level1000.Ts)
	}
}

func TestCompressLevelsDistinguishesSameAndGapRanges(t *testing.T) {
	rows := []QuoteTickRealtime{
		quoteRowAt(1000, 10, []priceLevel{{Price: 1000, Number: 10}, {Price: 999, Number: 9}}, nil),
		quoteRowAt(2000, 10, []priceLevel{{Price: 1000, Number: 10}}, nil),
		quoteRowAt(3000, 10, []priceLevel{{Price: 1000, Number: 10}}, nil),
		quoteRowAt(4000, 10, []priceLevel{{Price: 1001, Number: 8}}, nil),
		quoteRowAt(5000, 10, []priceLevel{{Price: 1001, Number: 8}}, nil),
	}

	buyLevels := compressLevels(rows, true)

	sameLevel := mustLevel(t, buyLevels, 1000)
	if !slices.Equal(sameLevel.Number, []int{10, -118, -2}) {
		t.Fatalf("expected same-run then gap marker for price 1000, got %v", sameLevel.Number)
	}
	assertSameGapRanges(t, sameLevel.Number)

	gapLevel := mustLevel(t, buyLevels, 999)
	if !slices.Equal(gapLevel.Number, []int{9, -4}) {
		t.Fatalf("expected compressed gap range for price 999, got %v", gapLevel.Number)
	}
	assertSameGapRanges(t, gapLevel.Number)
}

func TestCompressLevelsDoesNotDownsampleWithinSameSecond(t *testing.T) {
	rows := []QuoteTickRealtime{
		quoteRowAt(1000, 10, []priceLevel{{Price: 1000, Number: 10}}, nil),
		quoteRowAt(1500, 10, []priceLevel{{Price: 1000, Number: 10}}, nil),
		quoteRowAt(1999, 10, []priceLevel{{Price: 1000, Number: 10}}, nil),
	}

	buyLevels := compressLevels(rows, true)
	level := mustLevel(t, buyLevels, 1000)
	if !slices.Equal(level.Number, []int{10, -118}) {
		t.Fatalf("expected same marker to reflect 3 ticks in same second, got %v", level.Number)
	}
	assertSameGapRanges(t, level.Number)
}

func TestAggregateQuoteTicksKeepsNegativeMarkersAboveMinus120(t *testing.T) {
	rows := []QuoteTickRealtime{
		quoteRowAt(1000, 10, []priceLevel{{Price: 1000, Number: 10}, {Price: 999, Number: 9}}, []priceLevel{{Price: 1001, Number: 20}}),
		quoteRowAt(2000, 12, []priceLevel{{Price: 1000, Number: 10}}, []priceLevel{{Price: 1001, Number: 20}, {Price: 1002, Number: 18}}),
		quoteRowAt(3000, 14, []priceLevel{{Price: 1000, Number: 10}}, []priceLevel{{Price: 1002, Number: 18}}),
		quoteRowAt(4000, 16, []priceLevel{{Price: 1003, Number: 7}}, []priceLevel{{Price: 1002, Number: 18}}),
	}

	history, err := aggregateQuoteTicks(rows)
	if err != nil {
		t.Fatalf("aggregateQuoteTicks returned error: %v", err)
	}

	var payload archivalPayload
	if err := json.Unmarshal([]byte(history.QuoteTicks), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	for _, minute := range payload.Minutes {
		for _, level := range append(append([]compressedLevel{}, minute.BuyLevel...), minute.SellLevel...) {
			assertSameGapRanges(t, level.Number)
		}
	}
}

func TestAggregateQuoteTicksBucketsByMinute(t *testing.T) {
	rows := []QuoteTickRealtime{
		quoteRowAt(59_000, 10, []priceLevel{{Price: 1000, Number: 10}}, []priceLevel{{Price: 1001, Number: 20}}),
		quoteRowAt(60_000, 12, []priceLevel{{Price: 1000, Number: 10}}, []priceLevel{{Price: 1001, Number: 20}}),
		quoteRowAt(61_000, 14, []priceLevel{{Price: 1000, Number: 10}}, []priceLevel{{Price: 1001, Number: 20}}),
	}

	history, err := aggregateQuoteTicks(rows)
	if err != nil {
		t.Fatalf("aggregateQuoteTicks returned error: %v", err)
	}

	var payload archivalPayload
	if err := json.Unmarshal([]byte(history.QuoteTicks), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if len(payload.Minutes) != 2 {
		t.Fatalf("expected 2 minute buckets, got %d", len(payload.Minutes))
	}
	if payload.Minutes[0].Ts != 0 {
		t.Fatalf("expected first minuteStartTs=0, got %d", payload.Minutes[0].Ts)
	}
	if payload.Minutes[1].Ts != 60_000 {
		t.Fatalf("expected second minuteStartTs=60000, got %d", payload.Minutes[1].Ts)
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

func quoteRowAt(eventTs int64, volume int64, buys []priceLevel, sells []priceLevel) QuoteTickRealtime {
	row := sampleRealtime("603000", time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC), eventTs, uint64(eventTs/1000), volume, 1000)
	applyLevels(&row, buys, true)
	applyLevels(&row, sells, false)
	return row
}

func applyLevels(row *QuoteTickRealtime, levels []priceLevel, buy bool) {
	for idx := range 5 {
		price, num := 0, 0
		if idx < len(levels) {
			price = levels[idx].Price
			num = levels[idx].Number
		}
		setLevel(row, idx, buy, price, num)
	}
}

func setLevel(row *QuoteTickRealtime, idx int, buy bool, price int, num int) {
	if buy {
		switch idx {
		case 0:
			row.Bid1Price, row.Bid1Vol = price, num
		case 1:
			row.Bid2Price, row.Bid2Vol = price, num
		case 2:
			row.Bid3Price, row.Bid3Vol = price, num
		case 3:
			row.Bid4Price, row.Bid4Vol = price, num
		default:
			row.Bid5Price, row.Bid5Vol = price, num
		}
		return
	}
	switch idx {
	case 0:
		row.Ask1Price, row.Ask1Vol = price, num
	case 1:
		row.Ask2Price, row.Ask2Vol = price, num
	case 2:
		row.Ask3Price, row.Ask3Vol = price, num
	case 3:
		row.Ask4Price, row.Ask4Vol = price, num
	default:
		row.Ask5Price, row.Ask5Vol = price, num
	}
}

func mustLevel(t *testing.T, levels []compressedLevel, price int) compressedLevel {
	t.Helper()
	for _, level := range levels {
		if level.Price == price {
			return level
		}
	}
	t.Fatalf("price %d not found in levels: %+v", price, levels)
	return compressedLevel{}
}

func assertSameGapRanges(t *testing.T, numbers []int) {
	t.Helper()
	for _, num := range numbers {
		if num >= 0 {
			continue
		}
		if num <= -120 {
			t.Fatalf("expected negative marker > -120, got %d in %v", num, numbers)
		}
		if num >= -59 {
			continue
		}
		if num >= -119 && num <= -60 {
			continue
		}
		t.Fatalf("expected marker to be in gap[-59,-1] or same[-119,-60], got %d in %v", num, numbers)
	}
}
