package main

import (
	"encoding/binary"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/injoyai/tdx/protocol"
)

const (
	wsQuoteInterval         = 1000 * time.Millisecond
	wsQuoteSnapshotInterval = 30 * time.Second
	wsQuoteBatchSize        = 80
)

var (
	quotePusher     = NewQuotePusher(quoteHub)
	quotePusherOnce sync.Once
)

func startQuotePusher() {
	quotePusherOnce.Do(func() {
		go quotePusher.Run()
	})
}

type QuotePusher struct {
	hub *QuoteHub

	interval         time.Duration
	snapshotInterval time.Duration

	lastSnapshot time.Time
	lastHash     map[string]uint64
}

func NewQuotePusher(hub *QuoteHub) *QuotePusher {
	return &QuotePusher{
		hub:              hub,
		interval:         wsQuoteInterval,
		snapshotInterval: wsQuoteSnapshotInterval,
		lastHash:         make(map[string]uint64),
	}
}

func (p *QuotePusher) Run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for range ticker.C {
		p.tick()
	}
}

func (p *QuotePusher) tick() {
	clientCodes, union := p.hub.Snapshot()
	if len(union) == 0 {
		return
	}

	quotes, err := fetchQuotesBatched(union)
	if err != nil {
		log.Printf("[ws/quote] pusher fetch failed: codes=%d %v", len(union), err)
		return
	}

	quotesByCode := make(map[string]*protocol.Quote, len(quotes))
	changed := make(map[string]struct{}, len(quotes))

	unionSet := make(map[string]struct{}, len(union))
	for _, code := range union {
		unionSet[code] = struct{}{}
	}
	for code := range p.lastHash {
		if _, ok := unionSet[code]; !ok {
			delete(p.lastHash, code)
		}
	}

	for _, q := range quotes {
		quotesByCode[q.Code] = q
		h := quoteHash(q)
		if p.lastHash[q.Code] != h {
			changed[q.Code] = struct{}{}
			p.lastHash[q.Code] = h
		}
	}

	now := time.Now()
	needSnapshot := p.lastSnapshot.IsZero() || now.Sub(p.lastSnapshot) >= p.snapshotInterval
	if needSnapshot {
		p.lastSnapshot = now
	}

	for c, codes := range clientCodes {
		if len(codes) == 0 {
			continue
		}

		data := make([]*protocol.Quote, 0, len(codes))
		if needSnapshot {
			for _, code := range codes {
				if q, ok := quotesByCode[code]; ok {
					data = append(data, q)
				}
			}
		} else {
			for _, code := range codes {
				if _, ok := changed[code]; !ok {
					continue
				}
				if q, ok := quotesByCode[code]; ok {
					data = append(data, q)
				}
			}
		}

		if len(data) == 0 {
			continue
		}

		msgType := "delta"
		if needSnapshot {
			msgType = "snapshot"
		}

		wsSendJSON(c, wsServerMessage{
			Type: msgType,
			Ts:   now.UnixMilli(),
			Seq:  wsQuoteSeq.Add(1),
			Data: data,
		})
	}
}

func fetchQuotesBatched(codes []string) ([]*protocol.Quote, error) {
	if len(codes) == 0 {
		return nil, nil
	}

	all := make([]*protocol.Quote, 0, len(codes))
	for i := 0; i < len(codes); i += wsQuoteBatchSize {
		end := i + wsQuoteBatchSize
		if end > len(codes) {
			end = len(codes)
		}

		part := codes[i:end]
		quotes, err := client.GetQuote(part...)
		if err != nil {
			return nil, err
		}
		all = append(all, quotes...)
	}
	return all, nil
}

func quoteHash(q *protocol.Quote) uint64 {
	h := fnv.New64a()

	var buf [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}

	put(uint64(q.K.Last))
	put(uint64(q.K.Open))
	put(uint64(q.K.High))
	put(uint64(q.K.Low))
	put(uint64(q.K.Close))
	put(uint64(q.TotalHand))
	put(uint64(q.Intuition))
	put(uint64(q.InsideDish))
	put(uint64(q.OuterDisc))
	put(uint64(q.Amount * 100))

	for i := 0; i < 5; i++ {
		put(uint64(q.BuyLevel[i].Price))
		put(uint64(q.BuyLevel[i].Number))
		put(uint64(q.SellLevel[i].Price))
		put(uint64(q.SellLevel[i].Number))
	}

	return h.Sum64()
}
