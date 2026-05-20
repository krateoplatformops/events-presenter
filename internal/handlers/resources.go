package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krateoplatformops/events-presenter/internal/sql"
	"github.com/krateoplatformops/events-presenter/internal/telemetry"
)

type ResourceEvent struct {
	EventID           *string   `json:"event_id,omitempty"`
	GlobalUID         string    `json:"global_uid"`
	ClusterName       string    `json:"cluster_name"`
	Namespace         string    `json:"namespace"`
	ResourceKind      string    `json:"resource_kind"`
	ResourceName      string    `json:"resource_name"`
	InvolvedObjectUID *string   `json:"involved_object_uid,omitempty"`
	EventType         string    `json:"event_type"`
	Reason            *string   `json:"reason,omitempty"`
	Message           *string   `json:"message,omitempty"`
	CompositionID     *string   `json:"composition_id,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	Raw               string    `json:"raw,omitempty"` // JSON as string
}

type ResourcesResponse struct {
	Resources []ResourceEvent   `json:"resources"`
	Cursor    sql.EncodedCursor `json:"cursor,omitempty"`
}

func ResourcesHandler(db *pgxpool.Pool, metrics *telemetry.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		method := r.Method
		statusCode := http.StatusOK
		defer func() {
			metrics.RecordEventsRequest(r.Context(), method, statusCode, time.Since(started))
		}()

		fail := func(stage, msg string, status int) {
			statusCode = status
			metrics.IncEventsError(r.Context(), method, stage, status)
			http.Error(w, msg, status)
		}

		var (
			params sql.ResourcesQueryParams
			err    error
		)

		switch r.Method {
		case http.MethodGet:
			params, err = sql.ResourcesQueryParamsFromHTTPRequest(r)
		case http.MethodPost:
			params, err = sql.ResourcesQueryJSONFromHTTPRequest(r)
		default:
			w.Header().Set("Allow", "GET, POST")
			fail("method_not_allowed", "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err != nil {
			fail("invalid_params", fmt.Sprintf("invalid params: %v", err), http.StatusBadRequest)
			return
		}

		// build query
		query, args, err := sql.BuildResourcesQuery(params)
		if err != nil {
			fail("invalid_cursor", fmt.Sprintf("invalid cursor: %v", err), http.StatusBadRequest)
			return
		}

		// execute
		rows, err := db.Query(r.Context(), query, args...)
		if err != nil {
			fail("query_error", fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// map results
		var resources []ResourceEvent
		for rows.Next() {
			var e ResourceEvent
			var rawJSON []byte
			if err := rows.Scan(
				&e.EventID,
				&e.GlobalUID,
				&e.ClusterName,
				&e.Namespace,
				&e.ResourceKind,
				&e.ResourceName,
				&e.InvolvedObjectUID,
				&e.EventType,
				&e.Reason,
				&e.Message,
				&e.CompositionID,
				&e.CreatedAt,
				&rawJSON,
			); err != nil {
				fail("scan_error", fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
				return
			}
			e.Raw = string(rawJSON)
			resources = append(resources, e)
		}
		if rows.Err() != nil {
			fail("rows_error", fmt.Sprintf("rows error: %v", rows.Err()), http.StatusInternalServerError)
			return
		}

		// respond JSON
		resp := map[string]any{
			"resources": resources,
			"cursor":    lastRowCursor(resources),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			metrics.IncEventsError(r.Context(), method, "encode_error", http.StatusOK)
			return
		}
		metrics.AddEventsResourcesReturned(r.Context(), method, int64(len(resources)))
	}
}

func lastRowCursor(rows []ResourceEvent) sql.EncodedCursor {
	if len(rows) == 0 {
		return ""
	}

	last := rows[len(rows)-1]

	rc := sql.ResourcesCursor{
		CreatedAt: last.CreatedAt,
		GlobalUID: last.GlobalUID,
	}

	return sql.EncodeCursor(&rc)
}
