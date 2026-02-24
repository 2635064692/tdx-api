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
		return nil, fmt.Errorf("too many input patterns (max 200)")
	}

	set := make(map[string]struct{})
	out := make([]string, 0)

	for _, raw := range codes {
		pattern := strings.TrimSpace(raw)
		if pattern == "" {
			return nil, fmt.Errorf("code is empty")
		}

		expanded, err := expandWildcardPattern(pattern)
		if err != nil {
			return nil, err
		}

		for _, code := range expanded {
			if _, ok := set[code]; !ok {
				set[code] = struct{}{}
				out = append(out, code)
			}
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no valid codes matched")
	}
	if len(out) > 2000 {
		return nil, fmt.Errorf("too many codes after expansion (limit 2000, got %d)", len(out))
	}

	return out, nil
}

func expandWildcardPattern(pattern string) ([]string, error) {
	if strings.Contains(pattern, "*") {
		return expandWildcard(pattern)
	}

	if strings.Contains(pattern, ":") {
		return expandExchangePattern(pattern)
	}

	if wsCodeRe.MatchString(pattern) {
		fullCode := protocol.AddPrefix(pattern)
		if manager.Codes.Get(fullCode) != nil {
			return []string{pattern}, nil
		}
		return nil, fmt.Errorf("code %s not found", pattern)
	}

	return nil, fmt.Errorf("invalid pattern: %s", pattern)
}

func expandWildcard(pattern string) ([]string, error) {
	var exchange, codePattern string

	if strings.Contains(pattern, ":") {
		parts := strings.SplitN(pattern, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid exchange pattern: %s", pattern)
		}
		exchange = strings.ToLower(strings.TrimSpace(parts[0]))
		codePattern = strings.TrimSpace(parts[1])

		if exchange != "sh" && exchange != "sz" && exchange != "bj" {
			return nil, fmt.Errorf("invalid exchange: %s", exchange)
		}
	} else {
		codePattern = pattern
	}

	if codePattern == "*" {
		if exchange == "" {
			return nil, fmt.Errorf("exchange required for full wildcard (*)")
		}
		return getStocksByExchange(exchange), nil
	}

	prefix := strings.TrimSuffix(codePattern, "*")
	if len(prefix) == 0 || len(prefix) > 6 {
		return nil, fmt.Errorf("invalid wildcard pattern: %s", pattern)
	}

	if !regexp.MustCompile(`^[0-9]+$`).MatchString(prefix) {
		return nil, fmt.Errorf("wildcard prefix must be numeric: %s", prefix)
	}

	return matchCodesByPrefix(exchange, prefix), nil
}

func expandExchangePattern(pattern string) ([]string, error) {
	parts := strings.SplitN(pattern, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid exchange pattern: %s", pattern)
	}

	exchange := strings.ToLower(strings.TrimSpace(parts[0]))
	code := strings.TrimSpace(parts[1])

	if exchange != "sh" && exchange != "sz" && exchange != "bj" {
		return nil, fmt.Errorf("invalid exchange: %s", exchange)
	}

	if !wsCodeRe.MatchString(code) {
		return nil, fmt.Errorf("invalid code format: %s", code)
	}

	fullCode := exchange + code
	if manager.Codes.Get(fullCode) != nil {
		return []string{code}, nil
	}

	return nil, fmt.Errorf("code %s not found in exchange %s", code, exchange)
}

func getStocksByExchange(exchange string) []string {
	result := make([]string, 0)
	for fullCode, model := range manager.Codes.Map {
		if model.Exchange != exchange {
			continue
		}
		if protocol.IsStock(fullCode) {
			result = append(result, model.Code)
		}
	}
	return result
}

func matchCodesByPrefix(exchange, prefix string) []string {
	result := make([]string, 0)

	if exchange != "" {
		for fullCode, model := range manager.Codes.Map {
			if model.Exchange != exchange {
				continue
			}
			if strings.HasPrefix(model.Code, prefix) && protocol.IsStock(fullCode) {
				result = append(result, model.Code)
			}
		}
	} else {
		inferredExchange := inferExchangeFromPrefix(prefix)
		for fullCode, model := range manager.Codes.Map {
			if inferredExchange != "" && model.Exchange != inferredExchange {
				continue
			}
			if strings.HasPrefix(model.Code, prefix) && protocol.IsStock(fullCode) {
				result = append(result, model.Code)
			}
		}
	}

	return result
}

func inferExchangeFromPrefix(prefix string) string {
	if len(prefix) == 0 {
		return ""
	}
	first := prefix[0]
	if first == '6' {
		return "sh"
	}
	if first == '0' || (len(prefix) >= 2 && prefix[:2] == "30") {
		return "sz"
	}
	if first == '8' || (len(prefix) >= 2 && (prefix[:2] == "92" || prefix[:2] == "43")) {
		return "bj"
	}
	return ""
}
