package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func EventsSSEHandler(hub *EventHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ch := hub.Subscribe()
		defer hub.Unsubscribe(ch)

		ctx := r.Context()

		for {
			select {
			case ev := <-ch:
				data, _ := json.Marshal(ev)
				fmt.Fprintf(w, "event: k8s-event\n")
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()

			case <-ctx.Done():
				return
			}
		}
	}
}
