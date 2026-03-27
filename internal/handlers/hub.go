package handlers

import (
	"context"
	"sync"

	"github.com/krateoplatformops/events-presenter/internal/telemetry"
)

type EventHub struct {
	mu      sync.RWMutex
	clients map[chan ResourceEvent]struct{}
	metrics *telemetry.Metrics
}

func NewEventHub(metrics *telemetry.Metrics) *EventHub {
	return &EventHub{
		clients: make(map[chan ResourceEvent]struct{}),
		metrics: metrics,
	}
}

func (h *EventHub) Subscribe() chan ResourceEvent {
	ch := make(chan ResourceEvent, 16) // buffer!
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	h.metrics.IncSSEClientConnected(context.Background())
	return ch
}

func (h *EventHub) Unsubscribe(ch chan ResourceEvent) {
	h.mu.Lock()
	delete(h.clients, ch)
	close(ch)
	h.mu.Unlock()
	h.metrics.IncSSEClientDisconnected(context.Background())
}

func (h *EventHub) Broadcast(ev ResourceEvent) {
	h.mu.RLock()
	delivered := int64(0)
	dropped := int64(0)

	for ch := range h.clients {
		select {
		case ch <- ev:
			delivered++
		default:
			// client lento → drop
			dropped++
		}
	}
	h.mu.RUnlock()

	h.metrics.AddSSEBroadcast(context.Background(), delivered)
	h.metrics.AddSSEDropped(context.Background(), dropped)
}
