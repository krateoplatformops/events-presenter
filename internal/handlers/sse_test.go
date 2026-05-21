package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEventsSSEHandler_FiltersByCompositionID(t *testing.T) {
	hub := NewEventHub(nil)
	handler := EventsSSEHandler(hub, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/notifications?composition_id=comp-a", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	waitSubscribers(t, hub, 1)

	compA := "comp-a"
	compB := "comp-b"
	hub.Broadcast(ResourceEvent{GlobalUID: "1", CompositionID: &compA})
	hub.Broadcast(ResourceEvent{GlobalUID: "2", CompositionID: &compB})
	hub.Broadcast(ResourceEvent{GlobalUID: "3"})

	cancel()
	waitDone(t, done)

	body := rec.Body.String()
	if !strings.Contains(body, `"global_uid":"1"`) {
		t.Fatalf("expected event with matching composition_id to be streamed, body=%q", body)
	}
	if strings.Contains(body, `"global_uid":"2"`) {
		t.Fatalf("did not expect event with different composition_id, body=%q", body)
	}
	if strings.Contains(body, `"global_uid":"3"`) {
		t.Fatalf("did not expect event without composition_id, body=%q", body)
	}
}

func TestEventsSSEHandler_WithoutFilterStreamsAllEvents(t *testing.T) {
	hub := NewEventHub(nil)
	handler := EventsSSEHandler(hub, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/notifications", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	waitSubscribers(t, hub, 1)

	compA := "comp-a"
	hub.Broadcast(ResourceEvent{GlobalUID: "1", CompositionID: &compA})
	hub.Broadcast(ResourceEvent{GlobalUID: "2"})

	// wait until both appear
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		body := rec.Body.String()
		if strings.Contains(body, `"global_uid":"1"`) && strings.Contains(body, `"global_uid":"2"`) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	waitDone(t, done)

	body := rec.Body.String()
	if !strings.Contains(body, `"global_uid":"1"`) {
		t.Fatalf("expected first event to be streamed, body=%q", body)
	}
	if !strings.Contains(body, `"global_uid":"2"`) {
		t.Fatalf("expected second event to be streamed, body=%q", body)
	}
}

func waitSubscribers(t *testing.T, hub *EventHub, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		n := len(hub.clients)
		hub.mu.RUnlock()

		if n >= want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for %d subscriber(s)", want)
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not terminate")
	}
}
