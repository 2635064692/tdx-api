package main

import (
	"log"
	"sort"
	"sync"

	"github.com/gorilla/websocket"
)

type WSClient struct {
	conn *websocket.Conn
	send chan []byte

	closeOnce sync.Once
}

func NewWSClient(conn *websocket.Conn) *WSClient {
	return &WSClient{
		conn: conn,
		send: make(chan []byte, 256),
	}
}

func (c *WSClient) CloseSend() {
	c.closeOnce.Do(func() {
		close(c.send)
	})
}

func (c *WSClient) TrySend(msg []byte) bool {
	select {
	case c.send <- msg:
		return true
	default:
		log.Printf("[ws/quote] send buffer full, dropped: %s", c.conn.RemoteAddr())
		return false
	}
}

type QuoteHub struct {
	mu sync.RWMutex

	clients     map[*WSClient]struct{}
	clientCodes map[*WSClient]map[string]struct{}
	codeClients map[string]map[*WSClient]struct{}
}

func NewQuoteHub() *QuoteHub {
	return &QuoteHub{
		clients:     make(map[*WSClient]struct{}),
		clientCodes: make(map[*WSClient]map[string]struct{}),
		codeClients: make(map[string]map[*WSClient]struct{}),
	}
}

func (h *QuoteHub) Register(c *WSClient) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[c] = struct{}{}
	if _, ok := h.clientCodes[c]; !ok {
		h.clientCodes[c] = make(map[string]struct{})
	}
}

func (h *QuoteHub) Unregister(c *WSClient) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[c]; !ok {
		return
	}

	for code := range h.clientCodes[c] {
		clients := h.codeClients[code]
		delete(clients, c)
		if len(clients) == 0 {
			delete(h.codeClients, code)
		}
	}

	delete(h.clientCodes, c)
	delete(h.clients, c)
}

func (h *QuoteHub) Subscribe(c *WSClient, codes []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[c]; !ok {
		return
	}

	set, ok := h.clientCodes[c]
	if !ok {
		set = make(map[string]struct{})
		h.clientCodes[c] = set
	}

	for _, code := range codes {
		if _, ok := set[code]; ok {
			continue
		}
		set[code] = struct{}{}

		clients, ok := h.codeClients[code]
		if !ok {
			clients = make(map[*WSClient]struct{})
			h.codeClients[code] = clients
		}
		clients[c] = struct{}{}
	}
}

func (h *QuoteHub) Unsubscribe(c *WSClient, codes []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.clientCodes[c]
	if !ok {
		return
	}

	for _, code := range codes {
		delete(set, code)

		clients := h.codeClients[code]
		delete(clients, c)
		if len(clients) == 0 {
			delete(h.codeClients, code)
		}
	}
}

func (h *QuoteHub) ClientCodes(c *WSClient) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	set, ok := h.clientCodes[c]
	if !ok {
		return nil
	}

	codes := make([]string, 0, len(set))
	for code := range set {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func (h *QuoteHub) Snapshot() (map[*WSClient][]string, []string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	clientCodes := make(map[*WSClient][]string, len(h.clients))
	for c := range h.clients {
		set := h.clientCodes[c]
		codes := make([]string, 0, len(set))
		for code := range set {
			codes = append(codes, code)
		}
		sort.Strings(codes)
		clientCodes[c] = codes
	}

	union := make([]string, 0, len(h.codeClients))
	for code := range h.codeClients {
		union = append(union, code)
	}
	sort.Strings(union)

	return clientCodes, union
}
