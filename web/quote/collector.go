package quote

import (
	"context"
	"time"

	"github.com/injoyai/tdx/protocol"
)

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
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewQuoteCollector(queue chan *QuoteTick, deps CollectorDependencies) *QuoteCollector {
	ctx, cancel := context.WithCancel(context.Background())
	return &QuoteCollector{queue: queue, deps: deps, ctx: ctx, cancel: cancel}
}

func (c *QuoteCollector) Start() error { return nil }
func (c *QuoteCollector) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}
func (c *QuoteCollector) UpdateConfig(cfg QuoteSubscriptionConfig) error {
	c.config = cfg.Clone()
	c.expandedCodes = append([]string(nil), cfg.Codes...)
	return nil
}
func (c *QuoteCollector) ExpandedCodes() []string { return append([]string(nil), c.expandedCodes...) }
