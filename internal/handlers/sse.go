package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func EventsSSEHandler(hub *EventHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		compositionID := strings.TrimSpace(r.URL.Query().Get("composition_id"))

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
				if compositionID != "" {
					if ev.CompositionID == nil || *ev.CompositionID != compositionID {
						continue
					}
				}

				data, _ := json.Marshal(ev)
				fmt.Fprintf(w, "event: krateo\n")
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()

			case <-ctx.Done():
				return
			}
		}
	}
}
