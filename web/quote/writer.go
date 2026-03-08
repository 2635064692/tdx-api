package quote

import (
	"context"
	"time"

	"xorm.io/xorm"
)

type BatchWriter struct {
	db            *xorm.Engine
	queue         chan *QuoteTick
	batchSize     int
	flushInterval time.Duration
	stats         *WriterStats
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewBatchWriter(db *xorm.Engine, queue chan *QuoteTick, batchSize int, flushInterval time.Duration) *BatchWriter {
	ctx, cancel := context.WithCancel(context.Background())
	return &BatchWriter{db: db, queue: queue, batchSize: batchSize, flushInterval: flushInterval, stats: &WriterStats{}, ctx: ctx, cancel: cancel}
}

func (w *BatchWriter) Start() error { return nil }
func (w *BatchWriter) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
func (w *BatchWriter) UpdateConfig(cfg QuoteSubscriptionConfig) {
	if cfg.BatchSize > 0 {
		w.batchSize = cfg.BatchSize
	}
	if cfg.FlushIntervalMs > 0 {
		w.flushInterval = time.Duration(cfg.FlushIntervalMs) * time.Millisecond
	}
}
func (w *BatchWriter) Stats() WriterStatsSnapshot { return w.stats.Snapshot() }
