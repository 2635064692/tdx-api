package quote

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"time"

	"xorm.io/xorm"
)

type SystemDependencies struct {
	Collector CollectorDependencies
}

type QuoteStorageSystem struct {
	db           *xorm.Engine
	configPoller *ConfigPoller
	collector    *QuoteCollector
	writer       *BatchWriter
	archival     *ArchivalTask
	queue        chan *QuoteTick
	running      bool
	lastError    string
}

func NewQuoteStorageSystem(db *xorm.Engine, deps SystemDependencies) *QuoteStorageSystem {
	queue := make(chan *QuoteTick, DefaultQueueSize)
	s := &QuoteStorageSystem{
		db:        db,
		queue:     queue,
		collector: NewQuoteCollector(queue, deps.Collector),
		writer:    NewBatchWriter(db, queue, DefaultBatchSize, time.Duration(DefaultFlushIntervalMs)*time.Millisecond),
		archival:  NewArchivalTask(db),
	}
	s.configPoller = NewConfigPoller(db, time.Minute, s.onConfigChange)
	return s
}

func (s *QuoteStorageSystem) Start() error {
	s.running = true
	return nil
}

func (s *QuoteStorageSystem) Stop() error {
	s.running = false
	return nil
}

func (s *QuoteStorageSystem) Status() SystemStatus {
	return SystemStatus{
		Ready:          s.db != nil,
		Running:        s.running,
		QueueSize:      len(s.queue),
		QueueCapacity:  cap(s.queue),
		LastError:      s.lastError,
		Writer:         s.writer.Stats(),
		CollectorCodes: s.collector.ExpandedCodes(),
	}
}

func (s *QuoteStorageSystem) UpdateConfig(cfg QuoteSubscriptionConfig) error {
	s.onConfigChange(cfg)
	return nil
}

func (s *QuoteStorageSystem) RunArchival(ctx context.Context, tradeDate time.Time) error {
	return s.archival.Run(ctx, tradeDate)
}

func (s *QuoteStorageSystem) onConfigChange(cfg QuoteSubscriptionConfig) {
	_ = s.collector.UpdateConfig(cfg)
	s.writer.UpdateConfig(cfg)
}
