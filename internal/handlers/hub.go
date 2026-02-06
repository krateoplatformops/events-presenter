package handlers

import "sync"

type EventHub struct {
	mu      sync.RWMutex
	clients map[chan ResourceEvent]struct{}
}

func NewEventHub() *EventHub {
	return &EventHub{
		clients: make(map[chan ResourceEvent]struct{}),
	}
}

func (h *EventHub) Subscribe() chan ResourceEvent {
	ch := make(chan ResourceEvent, 16) // buffer!
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *EventHub) Unsubscribe(ch chan ResourceEvent) {
	h.mu.Lock()
	delete(h.clients, ch)
	close(ch)
	h.mu.Unlock()
}

func (h *EventHub) Broadcast(ev ResourceEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.clients {
		select {
		case ch <- ev:
		default:
			// client lento → drop
		}
	}
}
