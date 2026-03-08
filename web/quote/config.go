package quote

import (
	"context"
	"time"

	"xorm.io/xorm"
)

type ConfigPoller struct {
	db            *xorm.Engine
	interval      time.Duration
	currentConfig QuoteSubscriptionConfig
	lastUpdateTs  int64
	onChange      func(QuoteSubscriptionConfig)
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewConfigPoller(db *xorm.Engine, interval time.Duration, onChange func(QuoteSubscriptionConfig)) *ConfigPoller {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigPoller{db: db, interval: interval, onChange: onChange, ctx: ctx, cancel: cancel}
}

func (p *ConfigPoller) Start() error { return nil }
func (p *ConfigPoller) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}
func (p *ConfigPoller) Poll() error                      { return nil }
func (p *ConfigPoller) Current() QuoteSubscriptionConfig { return p.currentConfig.Clone() }
