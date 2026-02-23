package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/injoyai/tdx/protocol"
)

var (
	mockQuoteHub   = NewQuoteHub()
	mockPusher     *MockQuotePusher
	mockPusherOnce sync.Once
)

func startMockQuotePusher() {
	mockPusherOnce.Do(func() {
		mockPusher = NewMockQuotePusher(mockQuoteHub)
		go mockPusher.Run()
	})
}

type mockQuoteState struct {
	exchange protocol.Exchange
	last     protocol.Price
	open     protocol.Price
	high     protocol.Price
	low      protocol.Price
	close    protocol.Price
	volume   int
	amount   float64
	inside   int
	outside  int
}

type MockQuotePusher struct {
	hub              *QuoteHub
	interval         time.Duration
	snapshotInterval time.Duration
	lastSnapshot     time.Time
	lastHash         map[string]uint64
	mu               sync.Mutex
	states           map[string]*mockQuoteState
}

func NewMockQuotePusher(hub *QuoteHub) *MockQuotePusher {
	return &MockQuotePusher{
		hub:              hub,
		interval:         wsQuoteInterval,
		snapshotInterval: wsQuoteSnapshotInterval,
		lastHash:         make(map[string]uint64),
		states:           make(map[string]*mockQuoteState),
	}
}

func (p *MockQuotePusher) Run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for range ticker.C {
		p.tick()
	}
}

func (p *MockQuotePusher) tick() {
	clientCodes, union := p.hub.Snapshot()
	if len(union) == 0 {
		return
	}

	quotes := make([]*protocol.Quote, 0, len(union))
	for _, code := range union {
		quotes = append(quotes, p.generateQuote(code))
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

func (p *MockQuotePusher) generateQuote(code string) *protocol.Quote {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.states[code]
	if !ok {
		var ex protocol.Exchange
		if strings.HasPrefix(code, "6") {
			ex = 1
		}
		base := protocol.Price(8000 + rand.Intn(22000))
		base = base / 10 * 10
		s = &mockQuoteState{
			exchange: ex,
			last:     base,
			open:     base + protocol.Price(rand.Intn(200)-100),
			high:     base,
			low:      base,
			close:    base,
		}
		p.states[code] = s
	}

	step := int(s.close/400) + 1
	s.close += protocol.Price(rand.Intn(2*step+1) - step)
	if s.close < 100 {
		s.close = 100
	}
	if s.close > s.high {
		s.high = s.close
	}
	if s.close < s.low {
		s.low = s.close
	}

	vol := rand.Intn(5000) + 100
	s.volume += vol
	s.amount += float64(vol*100) * s.close.Float64()
	inPart := rand.Intn(vol + 1)
	s.inside += inPart
	s.outside += vol - inPart

	now := time.Now()
	serverTime := fmt.Sprintf("%02d%02d%02d%02d", now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1e7)

	q := &protocol.Quote{
		Exchange:       s.exchange,
		Code:           code,
		Active1:        uint16(rand.Intn(10000)),
		K:              protocol.K{Last: s.last, Open: s.open, High: s.high, Low: s.low, Close: s.close},
		ServerTime:     serverTime,
		ReversedBytes0: int(now.Unix()),
		ReversedBytes1: -int(s.close),
		TotalHand:      s.volume,
		Intuition:      vol,
		Amount:         s.amount,
		InsideDish:     s.inside,
		OuterDisc:      s.outside,
	}
	q.Active2 = q.Active1

	spread := protocol.Price(10)
	for i := 0; i < 5; i++ {
		q.BuyLevel[i] = protocol.PriceLevel{
			Buy:    true,
			Price:  s.close - spread*protocol.Price(i+1),
			Number: rand.Intn(10000) + 100,
		}
		q.SellLevel[i] = protocol.PriceLevel{
			Buy:    false,
			Price:  s.close + spread*protocol.Price(i+1),
			Number: rand.Intn(10000) + 100,
		}
	}

	return q
}

func handleWSMockQuote(w http.ResponseWriter, r *http.Request) {
	conn, err := wsQuoteUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws mock upgrade failed: %v", err)
		return
	}

	wsClient := NewWSClient(conn)
	mockQuoteHub.Register(wsClient)

	go wsQuoteWritePump(wsClient)
	wsMockQuoteReadPump(wsClient)
}

func wsMockQuoteReadPump(c *WSClient) {
	defer func() {
		mockQuoteHub.Unregister(c)
		c.CloseSend()
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(64 * 1024)

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var req wsClientMessage
		if err := json.Unmarshal(message, &req); err != nil {
			wsSendError(c, 1000, "invalid json")
			continue
		}

		action := strings.ToLower(strings.TrimSpace(req.Action))
		switch action {
		case "ping":
			wsSendPong(c)
		case "subscribe":
			codes, err := normalizeStockCodes(req.Codes)
			if err != nil {
				wsSendError(c, 1001, err.Error())
				continue
			}
			mockQuoteHub.Subscribe(c, codes)
			wsMockSendSnapshot(c)
		case "unsubscribe":
			codes, err := normalizeStockCodes(req.Codes)
			if err != nil {
				wsSendError(c, 1002, err.Error())
				continue
			}
			mockQuoteHub.Unsubscribe(c, codes)
		default:
			wsSendError(c, 1003, "invalid action")
		}
	}
}

func wsMockSendSnapshot(c *WSClient) {
	codes := mockQuoteHub.ClientCodes(c)
	if len(codes) == 0 {
		return
	}

	data := make([]*protocol.Quote, 0, len(codes))
	for _, code := range codes {
		data = append(data, mockPusher.generateQuote(code))
	}

	wsSendJSON(c, wsServerMessage{
		Type: "snapshot",
		Ts:   time.Now().UnixMilli(),
		Seq:  wsQuoteSeq.Add(1),
		Data: data,
	})
}

