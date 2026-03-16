package quote

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	tdx "github.com/injoyai/tdx"
	"xorm.io/core"
	"xorm.io/xorm"
)

type SystemDependencies struct {
	Collector     CollectorDependencies
	PollInterval  time.Duration
	QueueSize     int
	MigrationFile string
}

type QuoteStorageSystem struct {
	mu           sync.RWMutex
	db           *xorm.Engine
	configPoller *ConfigPoller
	collector    *QuoteCollector
	writer       *BatchWriter
	archival     *ArchivalTask
	queue        chan *QuoteTick
	running      bool
	lastError    string
}

func OpenMySQLEngine(dsn string) (*xorm.Engine, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("quote storage mysql dsn is empty")
	}
	engine, err := xorm.NewEngine("mysql", dsn)
	if err != nil {
		return nil, err
	}
	engine.SetMapper(core.SameMapper{})
	return engine, nil
}

func ApplyMigration(db *xorm.Engine, path string) error {
	if db == nil {
		return fmt.Errorf("quote storage db is nil")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, statement := range splitStatements(string(content)) {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func NewQuoteStorageSystem(db *xorm.Engine, deps SystemDependencies) *QuoteStorageSystem {
	queueSize := deps.QueueSize
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	queue := make(chan *QuoteTick, queueSize)
	archival := NewArchivalTask(db)
	archival.SetProgressFunc(func(current, total int, message string) {
		log.Printf("[归档进度] %d/%d - %s", current, total, message)
	})
	s := &QuoteStorageSystem{
		db:        db,
		queue:     queue,
		collector: NewQuoteCollector(queue, deps.Collector),
		writer:    NewBatchWriter(db, queue, DefaultBatchSize, time.Duration(DefaultFlushIntervalMs)*time.Millisecond),
		archival:  archival,
	}
	s.configPoller = NewConfigPoller(db, deps.PollInterval, s.onConfigChange)
	if path := strings.TrimSpace(deps.MigrationFile); path != "" && db != nil {
		if err := ApplyMigration(db, path); err != nil {
			s.setLastError(err)
		}
	}
	return s
}

func (s *QuoteStorageSystem) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.mu.Unlock()
	if err := s.writer.Start(); err != nil {
		return s.failStart(err)
	}
	if err := s.collector.Start(); err != nil {
		return s.failStart(err)
	}
	if err := s.configPoller.Start(); err != nil {
		return s.failStart(err)
	}
	return nil
}

func (s *QuoteStorageSystem) failStart(err error) error {
	s.mu.Lock()
	s.running = false
	s.lastError = err.Error()
	s.mu.Unlock()
	_ = s.Stop()
	return err
}

func (s *QuoteStorageSystem) Stop() error {
	s.mu.Lock()
	wasRunning := s.running
	s.running = false
	s.mu.Unlock()
	if !wasRunning {
		return nil
	}
	s.configPoller.Stop()
	s.collector.Stop()
	s.writer.Stop()
	return nil
}

func (s *QuoteStorageSystem) Close() error {
	_ = s.Stop()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *QuoteStorageSystem) Status() SystemStatus {
	s.mu.RLock()
	status := SystemStatus{
		Ready:         s.db != nil,
		Running:       s.running,
		QueueSize:     len(s.queue),
		QueueCapacity: cap(s.queue),
		LastError:     s.lastError,
		Writer:        s.writer.Stats(),
	}
	s.mu.RUnlock()
	status.Config = s.configPoller.Current()
	status.CollectorCodes = s.collector.ExpandedCodes()
	return status
}

func (s *QuoteStorageSystem) ReportError(err error) {
	s.setLastError(err)
}

func (s *QuoteStorageSystem) UpdateConfig(cfg QuoteSubscriptionConfig) error {
	s.onConfigChange(cfg)
	return nil
}

func (s *QuoteStorageSystem) RunArchival(ctx context.Context, tradeDate time.Time) error {
	return s.archival.Run(ctx, tradeDate)
}

func (s *QuoteStorageSystem) Drain(ctx context.Context) error {
	for len(s.queue) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil
}

func (s *QuoteStorageSystem) onConfigChange(cfg QuoteSubscriptionConfig) {
	if err := s.collector.UpdateConfig(cfg); err != nil {
		s.setLastError(err)
		return
	}
	s.writer.UpdateConfig(cfg)
	s.clearLastError()
}

func (s *QuoteStorageSystem) setLastError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}

func (s *QuoteStorageSystem) clearLastError() {
	s.mu.Lock()
	s.lastError = ""
	s.mu.Unlock()
}

func splitStatements(content string) []string {
	parts := strings.Split(content, ";")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

func NewSession(db *xorm.Engine, fn func(session *xorm.Session) error) error {
	return tdx.NewSessionFunc(db, fn)
}
