package quote

import (
	"sync/atomic"
	"time"

	"github.com/injoyai/tdx/protocol"
)

const (
	DefaultBatchSize       = 1000
	DefaultFlushIntervalMs = 5000
	DefaultQueueSize       = 10000
)

type QuoteSubscriptionConfig struct {
	Enabled         bool        `json:"enabled"`
	Source          string      `json:"source"`
	Codes           []string    `json:"codes"`
	TradeSessions   string      `json:"trade_sessions"`
	BatchSize       int         `json:"batch_size"`
	FlushIntervalMs int         `json:"flush_interval_ms"`
	UpdatedAt       int64       `json:"updated_at"`
	Ranges          []TimeRange `json:"ranges,omitempty"`
}

func DefaultConfig() QuoteSubscriptionConfig {
	return QuoteSubscriptionConfig{
		Source:          "real",
		BatchSize:       DefaultBatchSize,
		FlushIntervalMs: DefaultFlushIntervalMs,
	}
}

func (c QuoteSubscriptionConfig) Clone() QuoteSubscriptionConfig {
	dup := c
	dup.Codes = append([]string(nil), c.Codes...)
	dup.Ranges = append([]TimeRange(nil), c.Ranges...)
	return dup
}

type TimeRange struct {
	StartMinutes int `json:"start_minutes"`
	EndMinutes   int `json:"end_minutes"`
}

func (r TimeRange) Contains(t time.Time) bool {
	minute := t.Hour()*60 + t.Minute()
	return minute >= r.StartMinutes && minute <= r.EndMinutes
}

type QuoteTick struct {
	Code       string    `json:"code"`
	Exchange   string    `json:"exchange"`
	TradeDate  time.Time `json:"trade_date"`
	EventTs    int64     `json:"event_ts"`
	Seq        uint64    `json:"seq"`
	Volume     int64     `json:"volume"`
	Amount     float64   `json:"amount"`
	InsideVol  int       `json:"inside_vol"`
	OutsideVol int       `json:"outside_vol"`
	BidPrices  [5]int    `json:"bid_prices"`
	BidVols    [5]int    `json:"bid_vols"`
	AskPrices  [5]int    `json:"ask_prices"`
	AskVols    [5]int    `json:"ask_vols"`
}

func NewQuoteTick(q *protocol.Quote, seq uint64, now time.Time) QuoteTick {
	tick := QuoteTick{
		Code:       q.Code,
		Exchange:   q.Exchange.String(),
		TradeDate:  time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC),
		EventTs:    now.UnixMilli(),
		Seq:        seq,
		Volume:     int64(q.TotalHand),
		Amount:     q.Amount,
		InsideVol:  q.InsideDish,
		OutsideVol: q.OuterDisc,
	}
	for i := 0; i < 5; i++ {
		tick.BidPrices[i] = int(q.BuyLevel[i].Price)
		tick.BidVols[i] = q.BuyLevel[i].Number
		tick.AskPrices[i] = int(q.SellLevel[i].Price)
		tick.AskVols[i] = q.SellLevel[i].Number
	}
	return tick
}

type QuoteTickRealtime struct {
	ID         int64     `xorm:"pk autoincr 'id'" json:"id"`
	Code       string    `xorm:"'code'" json:"code"`
	Exchange   string    `xorm:"'exchange'" json:"exchange"`
	TradeDate  time.Time `xorm:"'trade_date'" json:"trade_date"`
	EventTs    int64     `xorm:"'event_ts'" json:"event_ts"`
	Seq        uint64    `xorm:"'seq'" json:"seq"`
	Volume     int64     `xorm:"'volume'" json:"volume"`
	Amount     float64   `xorm:"'amount'" json:"amount"`
	InsideVol  int       `xorm:"'inside_vol'" json:"inside_vol"`
	OutsideVol int       `xorm:"'outside_vol'" json:"outside_vol"`
	Bid1Price  int       `xorm:"'bid1_price'" json:"bid1_price"`
	Bid1Vol    int       `xorm:"'bid1_vol'" json:"bid1_vol"`
	Bid2Price  int       `xorm:"'bid2_price'" json:"bid2_price"`
	Bid2Vol    int       `xorm:"'bid2_vol'" json:"bid2_vol"`
	Bid3Price  int       `xorm:"'bid3_price'" json:"bid3_price"`
	Bid3Vol    int       `xorm:"'bid3_vol'" json:"bid3_vol"`
	Bid4Price  int       `xorm:"'bid4_price'" json:"bid4_price"`
	Bid4Vol    int       `xorm:"'bid4_vol'" json:"bid4_vol"`
	Bid5Price  int       `xorm:"'bid5_price'" json:"bid5_price"`
	Bid5Vol    int       `xorm:"'bid5_vol'" json:"bid5_vol"`
	Ask1Price  int       `xorm:"'ask1_price'" json:"ask1_price"`
	Ask1Vol    int       `xorm:"'ask1_vol'" json:"ask1_vol"`
	Ask2Price  int       `xorm:"'ask2_price'" json:"ask2_price"`
	Ask2Vol    int       `xorm:"'ask2_vol'" json:"ask2_vol"`
	Ask3Price  int       `xorm:"'ask3_price'" json:"ask3_price"`
	Ask3Vol    int       `xorm:"'ask3_vol'" json:"ask3_vol"`
	Ask4Price  int       `xorm:"'ask4_price'" json:"ask4_price"`
	Ask4Vol    int       `xorm:"'ask4_vol'" json:"ask4_vol"`
	Ask5Price  int       `xorm:"'ask5_price'" json:"ask5_price"`
	Ask5Vol    int       `xorm:"'ask5_vol'" json:"ask5_vol"`
	CreatedAt  time.Time `xorm:"created 'created_at'" json:"created_at"`
}

func (QuoteTickRealtime) TableName() string { return "quote_tick_realtime" }

func (t QuoteTick) RealtimeRow() QuoteTickRealtime {
	return QuoteTickRealtime{
		Code:       t.Code,
		Exchange:   t.Exchange,
		TradeDate:  t.TradeDate,
		EventTs:    t.EventTs,
		Seq:        t.Seq,
		Volume:     t.Volume,
		Amount:     t.Amount,
		InsideVol:  t.InsideVol,
		OutsideVol: t.OutsideVol,
		Bid1Price:  t.BidPrices[0],
		Bid1Vol:    t.BidVols[0],
		Bid2Price:  t.BidPrices[1],
		Bid2Vol:    t.BidVols[1],
		Bid3Price:  t.BidPrices[2],
		Bid3Vol:    t.BidVols[2],
		Bid4Price:  t.BidPrices[3],
		Bid4Vol:    t.BidVols[3],
		Bid5Price:  t.BidPrices[4],
		Bid5Vol:    t.BidVols[4],
		Ask1Price:  t.AskPrices[0],
		Ask1Vol:    t.AskVols[0],
		Ask2Price:  t.AskPrices[1],
		Ask2Vol:    t.AskVols[1],
		Ask3Price:  t.AskPrices[2],
		Ask3Vol:    t.AskVols[2],
		Ask4Price:  t.AskPrices[3],
		Ask4Vol:    t.AskVols[3],
		Ask5Price:  t.AskPrices[4],
		Ask5Vol:    t.AskVols[4],
	}
}

type QuoteTickHistory struct {
	ID         int64     `xorm:"pk autoincr 'id'" json:"id"`
	Code       string    `xorm:"'code'" json:"code"`
	Exchange   string    `xorm:"'exchange'" json:"exchange"`
	TradeDate  time.Time `xorm:"'trade_date'" json:"trade_date"`
	Volume     int64     `xorm:"'volume'" json:"volume"`
	Amount     float64   `xorm:"'amount'" json:"amount"`
	InsideVol  int       `xorm:"'inside_vol'" json:"inside_vol"`
	OutsideVol int       `xorm:"'outside_vol'" json:"outside_vol"`
	QuoteTicks string    `xorm:"'quote_ticks'" json:"quote_ticks"`
	ArchivedAt time.Time `xorm:"created 'archived_at'" json:"archived_at"`
}

func (QuoteTickHistory) TableName() string { return "quote_tick_history" }

type WriterStats struct {
	totalReceived atomic.Uint64
	totalWritten  atomic.Uint64
	totalFailed   atomic.Uint64
	lastFlushAt   atomic.Int64
}

type WriterStatsSnapshot struct {
	TotalReceived uint64 `json:"total_received"`
	TotalWritten  uint64 `json:"total_written"`
	TotalFailed   uint64 `json:"total_failed"`
	LastFlushAt   int64  `json:"last_flush_at"`
}

func (s *WriterStats) Snapshot() WriterStatsSnapshot {
	if s == nil {
		return WriterStatsSnapshot{}
	}
	return WriterStatsSnapshot{
		TotalReceived: s.totalReceived.Load(),
		TotalWritten:  s.totalWritten.Load(),
		TotalFailed:   s.totalFailed.Load(),
		LastFlushAt:   s.lastFlushAt.Load(),
	}
}

type SystemStatus struct {
	Ready          bool                    `json:"ready"`
	Running        bool                    `json:"running"`
	QueueSize      int                     `json:"queue_size"`
	QueueCapacity  int                     `json:"queue_capacity"`
	LastError      string                  `json:"last_error,omitempty"`
	Config         QuoteSubscriptionConfig `json:"config"`
	Writer         WriterStatsSnapshot     `json:"writer"`
	CollectorCodes []string                `json:"collector_codes,omitempty"`
}
