package quote

import (
	"context"
	"testing"
	"time"

	"github.com/injoyai/tdx/protocol"
)

func TestCollectorUpdateConfigExpandsCodes(t *testing.T) {
	collector := NewQuoteCollector(make(chan *QuoteTick, 4), CollectorDependencies{
		NormalizeCodes: func(codes []string) ([]string, error) {
			return []string{"603000", "000001"}, nil
		},
	})
	if err := collector.UpdateConfig(QuoteSubscriptionConfig{Enabled: true, Codes: []string{"603*"}}); err != nil {
		t.Fatalf("UpdateConfig returned error: %v", err)
	}
	if got := collector.ExpandedCodes(); len(got) != 2 || got[0] != "603000" {
		t.Fatalf("unexpected expanded codes: %+v", got)
	}
}

func TestCollectorCollectsRealQuotes(t *testing.T) {
	queue := make(chan *QuoteTick, 4)
	collector := NewQuoteCollector(queue, CollectorDependencies{
		NormalizeCodes: func(codes []string) ([]string, error) { return codes, nil },
		FetchRealQuotes: func(codes []string) ([]*protocol.Quote, error) {
			return []*protocol.Quote{sampleQuote(codes[0], protocol.ExchangeSH)}, nil
		},
		Now: func() time.Time { return mustClock("2026-03-08T09:30:00+08:00") },
	})
	cfg := QuoteSubscriptionConfig{
		Enabled: true,
		Source:  "real",
		Codes:   []string{"603000"},
		Ranges:  []TimeRange{{StartMinutes: 9*60 + 15, EndMinutes: 11*60 + 30}},
	}
	if err := collector.UpdateConfig(cfg); err != nil {
		t.Fatalf("UpdateConfig returned error: %v", err)
	}
	if err := collector.collect(context.Background(), collector.deps.Now()); err != nil {
		t.Fatalf("collect returned error: %v", err)
	}
	select {
	case tick := <-queue:
		if tick.Code != "603000" || tick.Exchange != "sh" || tick.Seq == 0 {
			t.Fatalf("unexpected tick: %+v", tick)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected tick in queue")
	}
}

func TestCollectorSkipsOutsideTradeWindow(t *testing.T) {
	queue := make(chan *QuoteTick, 1)
	collector := NewQuoteCollector(queue, CollectorDependencies{
		NormalizeCodes: func(codes []string) ([]string, error) { return codes, nil },
		FetchRealQuotes: func(codes []string) ([]*protocol.Quote, error) {
			return []*protocol.Quote{sampleQuote(codes[0], protocol.ExchangeSH)}, nil
		},
	})
	cfg := QuoteSubscriptionConfig{
		Enabled: true,
		Source:  "real",
		Codes:   []string{"603000"},
		Ranges:  []TimeRange{{StartMinutes: 9*60 + 15, EndMinutes: 11*60 + 30}},
	}
	if err := collector.UpdateConfig(cfg); err != nil {
		t.Fatalf("UpdateConfig returned error: %v", err)
	}
	if err := collector.collect(context.Background(), mustClock("2026-03-08T08:30:00+08:00")); err != nil {
		t.Fatalf("collect returned error: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("expected empty queue outside trade window")
	}
}

func TestCollectorCollectsMockQuotes(t *testing.T) {
	queue := make(chan *QuoteTick, 2)
	collector := NewQuoteCollector(queue, CollectorDependencies{
		NormalizeCodes: func(codes []string) ([]string, error) { return codes, nil },
		FetchMockQuote: func(code string) (*protocol.Quote, error) {
			return sampleQuote(code, protocol.ExchangeSZ), nil
		},
	})
	cfg := QuoteSubscriptionConfig{Enabled: true, Source: "mock", Codes: []string{"000001"}}
	if err := collector.UpdateConfig(cfg); err != nil {
		t.Fatalf("UpdateConfig returned error: %v", err)
	}
	if err := collector.collect(context.Background(), mustClock("2026-03-08T10:00:00+08:00")); err != nil {
		t.Fatalf("collect returned error: %v", err)
	}
	if len(queue) != 1 {
		t.Fatalf("expected one mock tick, got %d", len(queue))
	}
}

func sampleQuote(code string, exchange protocol.Exchange) *protocol.Quote {
	return &protocol.Quote{
		Exchange:   exchange,
		Code:       code,
		Amount:     12.5,
		TotalHand:  100,
		InsideDish: 40,
		OuterDisc:  60,
		BuyLevel: protocol.PriceLevels{
			{Price: 1000, Number: 11},
			{Price: 999, Number: 12},
			{Price: 998, Number: 13},
			{Price: 997, Number: 14},
			{Price: 996, Number: 15},
		},
		SellLevel: protocol.PriceLevels{
			{Price: 1001, Number: 21},
			{Price: 1002, Number: 22},
			{Price: 1003, Number: 23},
			{Price: 1004, Number: 24},
			{Price: 1005, Number: 25},
		},
	}
}

func mustClock(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return t
}
