package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/injoyai/tdx/protocol"
)

var (
	quoteHub = NewQuoteHub()

	wsQuoteSeq atomic.Uint64

	wsQuoteUpgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	wsCodeRe = regexp.MustCompile(`^[0-9]{6}$`)
)

type wsClientMessage struct {
	Action string   `json:"action"`
	Codes  []string `json:"codes"`
}

type wsServerMessage struct {
	Type    string            `json:"type"`
	Ts      int64             `json:"ts"`
	Seq     uint64            `json:"seq,omitempty"`
	Data    []*protocol.Quote `json:"data,omitempty"`
	Code    int               `json:"code,omitempty"`
	Message string            `json:"message,omitempty"`
}

func handleWSQuote(w http.ResponseWriter, r *http.Request) {
	conn, err := wsQuoteUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}

	wsClient := NewWSClient(conn)
	quoteHub.Register(wsClient)

	go wsQuoteWritePump(wsClient)
	wsQuoteReadPump(wsClient)
}

func wsQuoteReadPump(c *WSClient) {
	defer func() {
		quoteHub.Unregister(c)
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
			quoteHub.Subscribe(c, codes)
			wsSendSnapshot(c)
		case "unsubscribe":
			codes, err := normalizeStockCodes(req.Codes)
			if err != nil {
				wsSendError(c, 1002, err.Error())
				continue
			}
			quoteHub.Unsubscribe(c, codes)
		default:
			wsSendError(c, 1003, "invalid action")
		}
	}
}

func wsQuoteWritePump(c *WSClient) {
	for msg := range c.send {
		_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func wsSendPong(c *WSClient) {
	wsSendJSON(c, wsServerMessage{
		Type: "pong",
		Ts:   time.Now().UnixMilli(),
	})
}

func wsSendSnapshot(c *WSClient) {
	codes := quoteHub.ClientCodes(c)
	if len(codes) == 0 {
		return
	}

	quotes, err := client.GetQuote(codes...)
	if err != nil {
		wsSendError(c, 2001, fmt.Sprintf("get quote failed: %v", err))
		return
	}

	wsSendJSON(c, wsServerMessage{
		Type: "snapshot",
		Ts:   time.Now().UnixMilli(),
		Seq:  wsQuoteSeq.Add(1),
		Data: quotes,
	})
}

func wsSendError(c *WSClient, code int, message string) {
	wsSendJSON(c, wsServerMessage{
		Type:    "error",
		Ts:      time.Now().UnixMilli(),
		Code:    code,
		Message: message,
	})
}

func wsSendJSON(c *WSClient, v wsServerMessage) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	_ = c.TrySend(b)
}

func normalizeStockCodes(codes []string) ([]string, error) {
	if len(codes) == 0 {
		return nil, fmt.Errorf("codes is required")
	}
	if len(codes) > 200 {
		return nil, fmt.Errorf("too many codes")
	}

	set := make(map[string]struct{}, len(codes))
	out := make([]string, 0, len(codes))

	for _, raw := range codes {
		code := strings.TrimSpace(raw)
		if code == "" {
			return nil, fmt.Errorf("code is empty")
		}
		if code == "*" || strings.Contains(code, ":") {
			return nil, fmt.Errorf("unsupported code syntax")
		}
		if !wsCodeRe.MatchString(code) {
			return nil, fmt.Errorf("invalid code format")
		}
		if _, ok := set[code]; ok {
			continue
		}
		set[code] = struct{}{}
		out = append(out, code)
	}

	return out, nil
}
