package quote

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"xorm.io/xorm"
)

type BatchWriter struct {
	db            *xorm.Engine
	queue         chan *QuoteTick
	batchSize     int
	flushInterval time.Duration
	buffer        []*QuoteTick
	stats         *WriterStats
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	started       bool
}

func NewBatchWriter(db *xorm.Engine, queue chan *QuoteTick, batchSize int, flushInterval time.Duration) *BatchWriter {
	writer := &BatchWriter{
		db:            db,
		queue:         queue,
		batchSize:     positiveOrDefault(batchSize, DefaultBatchSize),
		flushInterval: durationOrDefault(flushInterval, time.Duration(DefaultFlushIntervalMs)*time.Millisecond),
		stats:         &WriterStats{},
	}
	writer.resetContext()
	return writer
}

func (w *BatchWriter) Start() error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return nil
	}
	w.resetContext()
	ctx := w.ctx
	w.started = true
	w.mu.Unlock()
	go w.run(ctx)
	return nil
}

func (w *BatchWriter) run(ctx context.Context) {
	timer := time.NewTimer(w.currentFlushInterval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = w.FlushNow()
			return
		case tick := <-w.queue:
			w.push(tick)
			if w.shouldFlush() {
				_ = w.FlushNow()
				resetTimer(timer, w.currentFlushInterval())
			}
		case <-timer.C:
			_ = w.FlushNow()
			resetTimer(timer, w.currentFlushInterval())
		}
	}
}

func (w *BatchWriter) Stop() {
	w.mu.Lock()
	w.started = false
	cancel := w.cancel
	w.cancel = nil
	w.ctx = nil
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *BatchWriter) UpdateConfig(cfg QuoteSubscriptionConfig) {
	w.mu.Lock()
	w.batchSize = positiveOrDefault(cfg.BatchSize, w.batchSize)
	w.flushInterval = durationOrDefault(time.Duration(cfg.FlushIntervalMs)*time.Millisecond, w.flushInterval)
	w.mu.Unlock()
}

func (w *BatchWriter) Stats() WriterStatsSnapshot { return w.stats.Snapshot() }

func (w *BatchWriter) FlushNow() error {
	batch := w.drainBuffer()
	if len(batch) == 0 {
		return nil
	}
	written, err := w.insertBatch(batch)
	w.stats.lastFlushAt.Store(time.Now().UnixMilli())
	if err != nil {
		w.stats.totalFailed.Add(uint64(len(batch)))
		return err
	}
	w.stats.totalWritten.Add(uint64(written))
	return nil
}

func (w *BatchWriter) push(tick *QuoteTick) {
	if tick == nil {
		return
	}
	w.mu.Lock()
	w.buffer = append(w.buffer, tick)
	w.mu.Unlock()
	w.stats.totalReceived.Add(1)
}

func (w *BatchWriter) shouldFlush() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.buffer) >= w.batchSize
}

func (w *BatchWriter) drainBuffer() []*QuoteTick {
	w.mu.Lock()
	defer w.mu.Unlock()
	batch := append([]*QuoteTick(nil), w.buffer...)
	w.buffer = w.buffer[:0]
	return batch
}

func (w *BatchWriter) insertBatch(batch []*QuoteTick) (int, error) {
	if w.db == nil {
		return 0, fmt.Errorf("quote storage db is nil")
	}
	written := 0
	err := NewSession(w.db, func(session *xorm.Session) error {
		for _, tick := range batch {
			row := tick.RealtimeRow()
			result, err := session.Table(row.TableName()).Insert(&row)
			if err == nil {
				written += int(result)
				continue
			}
			if isDuplicateErr(err) {
				continue
			}
			return err
		}
		return nil
	})
	return written, err
}

func (w *BatchWriter) currentFlushInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.flushInterval
}

func (w *BatchWriter) resetContext() {
	if w.cancel != nil {
		w.cancel()
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func positiveOrDefault(value, def int) int {
	if value <= 0 {
		return def
	}
	return value
}

func durationOrDefault(value, def time.Duration) time.Duration {
	if value <= 0 {
		return def
	}
	return value
}

func isDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate") || strings.Contains(message, "unique constraint")
}
