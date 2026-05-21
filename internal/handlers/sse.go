package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

func EventsSSEHandler(hub *EventHub, log *slog.Logger) http.HandlerFunc {
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
				log.Debug("event received from hub", slog.Any("ev", ev))
				if compositionID != "" {
					if ev.CompositionID == nil || *ev.CompositionID != compositionID {
						log.Debug("event skipped because it does not belong to requested composition",
							slog.String("requested_composition_id", compositionID),
							slog.Any("event_composition_id", ev.CompositionID),
						)
						continue
					}
				}

				data, _ := json.Marshal(ev)
				fmt.Fprintf(w, "event: krateo\n")
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()

				log.Debug("event sent to client", slog.Any("ev", ev))

			case <-ctx.Done():
				return
			}
		}
	}
}
