package quote

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/injoyai/tdx/protocol"
)

const collectorInterval = time.Second

type CollectorDependencies struct {
	NormalizeCodes  func([]string) ([]string, error)
	FetchRealQuotes func([]string) ([]*protocol.Quote, error)
	FetchMockQuote  func(string) (*protocol.Quote, error)
	Now             func() time.Time
}

type QuoteCollector struct {
	deps          CollectorDependencies
	queue         chan *QuoteTick
	config        QuoteSubscriptionConfig
	expandedCodes []string
	seq           atomic.Uint64
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	started       bool
}

func NewQuoteCollector(queue chan *QuoteTick, deps CollectorDependencies) *QuoteCollector {
	collector := &QuoteCollector{queue: queue, deps: deps}
	collector.resetContext()
	if collector.deps.Now == nil {
		collector.deps.Now = time.Now
	}
	return collector
}

func (c *QuoteCollector) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.resetContext()
	ctx := c.ctx
	c.started = true
	c.mu.Unlock()
	go c.run(ctx)
	return nil
}

func (c *QuoteCollector) run(ctx context.Context) {
	ticker := time.NewTicker(collectorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.collect(ctx, c.deps.Now())
		}
	}
}

func (c *QuoteCollector) Stop() {
	c.mu.Lock()
	c.started = false
	cancel := c.cancel
	c.cancel = nil
	c.ctx = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *QuoteCollector) UpdateConfig(cfg QuoteSubscriptionConfig) error {
	next := cfg.Clone()
	expanded, err := c.expandCodes(next)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.config = next
	c.expandedCodes = expanded
	c.mu.Unlock()
	return nil
}

func (c *QuoteCollector) ExpandedCodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.expandedCodes...)
}

func (c *QuoteCollector) collect(ctx context.Context, now time.Time) error {
	cfg, codes := c.snapshot()
	if !cfg.Enabled || len(codes) == 0 || !isInTimeWindow(now, cfg.Ranges) {
		return nil
	}
	quotes, err := c.fetchQuotes(cfg.Source, codes)
	if err != nil {
		return err
	}
	for _, item := range quotes {
		tick := NewQuoteTick(item, c.seq.Add(1), now)
		if err := c.enqueue(ctx, &tick); err != nil {
			return err
		}
	}
	return nil
}

func (c *QuoteCollector) snapshot() (QuoteSubscriptionConfig, []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Clone(), append([]string(nil), c.expandedCodes...)
}

func (c *QuoteCollector) expandCodes(cfg QuoteSubscriptionConfig) ([]string, error) {
	if !cfg.Enabled || len(cfg.Codes) == 0 {
		return nil, nil
	}
	if c.deps.NormalizeCodes == nil {
		return nil, fmt.Errorf("collector normalize dependency is nil")
	}
	return c.deps.NormalizeCodes(cfg.Codes)
}

func (c *QuoteCollector) fetchQuotes(source string, codes []string) ([]*protocol.Quote, error) {
	if source == "mock" {
		return c.fetchMockQuotes(codes)
	}
	if c.deps.FetchRealQuotes == nil {
		return nil, fmt.Errorf("collector real quote dependency is nil")
	}
	return c.deps.FetchRealQuotes(codes)
}

func (c *QuoteCollector) fetchMockQuotes(codes []string) ([]*protocol.Quote, error) {
	if c.deps.FetchMockQuote == nil {
		return nil, fmt.Errorf("collector mock quote dependency is nil")
	}
	quotes := make([]*protocol.Quote, 0, len(codes))
	for _, code := range codes {
		quote, err := c.deps.FetchMockQuote(code)
		if err != nil {
			return nil, err
		}
		quotes = append(quotes, quote)
	}
	return quotes, nil
}

func (c *QuoteCollector) enqueue(ctx context.Context, tick *QuoteTick) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.queue <- tick:
		return nil
	}
}

func (c *QuoteCollector) resetContext() {
	if c.cancel != nil {
		c.cancel()
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
}

func isInTimeWindow(now time.Time, ranges []TimeRange) bool {
	if len(ranges) == 0 {
		return true
	}
	for _, item := range ranges {
		if item.Contains(now) {
			return true
		}
	}
	return false
}
