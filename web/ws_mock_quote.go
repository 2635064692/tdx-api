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
	exchange  protocol.Exchange
	last      protocol.Price
	open      protocol.Price
	high      protocol.Price
	low       protocol.Price
	close     protocol.Price
	volume    int
	amount    float64
	inside    int
	outside   int
	bidPrices [5]protocol.Price
	askPrices [5]protocol.Price
	bidVols   [5]int
	askVols   [5]int
	bookInit  bool
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

	const tick = protocol.Price(10) // 10厘 = 0.01元

	s, ok := p.states[code]
	if !ok {
		var ex protocol.Exchange
		if strings.HasPrefix(code, "6") {
			ex = 1
		}
		base := protocol.Price(8000+rand.Intn(22000)) / 10 * 10
		open := base + protocol.Price(rand.Intn(200)-100)
		high := base
		if open > high {
			high = open
		}
		low := base
		if open < low {
			low = open
		}
		s = &mockQuoteState{
			exchange: ex,
			last:     base,
			open:     open,
			high:     high,
			low:      low,
			close:    base,
		}
		p.states[code] = s
	}

	// 初始化五档持久状态
	if !s.bookInit {
		for i := 0; i < 5; i++ {
			s.bidPrices[i] = s.close - tick*protocol.Price(i)
			s.askPrices[i] = s.close + tick*protocol.Price(i+1)
			s.bidVols[i] = rand.Intn(10000) + 100
			s.askVols[i] = rand.Intn(10000) + 100
		}
		s.bookInit = true
	}

	// 概率驱动价格变动: 50%不变 / 30%±1档 / 15%±2档 / 5%±3档
	oldClose := s.close
	moveTicks := 0
	switch r := rand.Intn(100); {
	case r < 50:
	case r < 80:
		moveTicks = 1
	case r < 95:
		moveTicks = 2
	default:
		moveTicks = 3
	}
	if moveTicks != 0 && rand.Intn(2) == 0 {
		moveTicks = -moveTicks
	}

	s.close += protocol.Price(moveTicks) * tick
	if s.close < 100 {
		s.close = 100
	}
	if s.close > s.high {
		s.high = s.close
	}
	if s.close < s.low {
		s.low = s.close
	}

	// 更新五档挂单量
	shift := int((s.close - oldClose) / tick)
	if shift == 0 {
		// 价格未变：对每档挂单量施加 ±5-15% 微扰
		for i := 0; i < 5; i++ {
			s.bidVols[i] = jitterVol(s.bidVols[i])
			s.askVols[i] = jitterVol(s.askVols[i])
		}
	} else {
		var newBid, newAsk [5]int
		abs := shift
		if abs < 0 {
			abs = -abs
		}
		for i := 0; i < 5; i++ {
			if shift > 0 { // 价格上涨：近端ask被吃掉，bid近端补新档
				if src := i + abs; src < 5 {
					newAsk[i] = s.askVols[src]
				} else {
					newAsk[i] = rand.Intn(10000) + 100
				}
				if src := i - abs; src >= 0 {
					newBid[i] = s.bidVols[src]
				} else {
					newBid[i] = rand.Intn(10000) + 100
				}
			} else { // 价格下跌：近端bid被吃掉，ask近端补新档
				if src := i + abs; src < 5 {
					newBid[i] = s.bidVols[src]
				} else {
					newBid[i] = rand.Intn(10000) + 100
				}
				if src := i - abs; src >= 0 {
					newAsk[i] = s.askVols[src]
				} else {
					newAsk[i] = rand.Intn(10000) + 100
				}
			}
		}
		s.bidVols = newBid
		s.askVols = newAsk
	}

	// 重建价格阶梯 (bid1=close, ask1=close+tick)
	for i := 0; i < 5; i++ {
		s.bidPrices[i] = s.close - tick*protocol.Price(i)
		s.askPrices[i] = s.close + tick*protocol.Price(i+1)
		if s.bidVols[i] < 50 {
			s.bidVols[i] = 50
		}
		if s.askVols[i] < 50 {
			s.askVols[i] = 50
		}
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

	for i := 0; i < 5; i++ {
		q.BuyLevel[i] = protocol.PriceLevel{
			Buy:    true,
			Price:  s.bidPrices[i],
			Number: s.bidVols[i],
		}
		q.SellLevel[i] = protocol.PriceLevel{
			Buy:    false,
			Price:  s.askPrices[i],
			Number: s.askVols[i],
		}
	}

	return q
}

// jitterVol 对挂单量施加 ±5-15% 随机微扰，下限50，上限50000
func jitterVol(v int) int {
	delta := v * (rand.Intn(11) + 5) / 100
	if delta < 1 {
		delta = 1
	}
	if rand.Intn(2) == 0 {
		v += delta
	} else {
		v -= delta
	}
	if v < 50 {
		v = 50
	}
	if v > 50000 {
		v = 50000
	}
	return v
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

