package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krateoplatformops/events-presenter/internal/sql"
)

type ResourceEvent struct {
	GlobalUID         string    `json:"global_uid"`
	ClusterName       string    `json:"cluster_name"`
	Namespace         string    `json:"namespace"`
	ResourceKind      string    `json:"resource_kind"`
	ResourceName      string    `json:"resource_name"`
	InvolvedObjectUID string    `json:"involved_object_uid,omitempty"`
	EventType         string    `json:"event_type"`
	Reason            *string   `json:"reason,omitempty"`
	Message           *string   `json:"message,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	Raw               string    `json:"raw,omitempty"` // JSON as string
}

type ResourcesResponse struct {
	Resources []ResourceEvent   `json:"resources"`
	Cursor    sql.EncodedCursor `json:"cursor,omitempty"`
}

func ResourcesHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err != nil {
			http.Error(w, fmt.Sprintf("invalid params: %v", err), http.StatusBadRequest)
			return
		}

		// build query
		query, args, err := sql.BuildResourcesQuery(params)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid cursor: %v", err), http.StatusBadRequest)
			return
		}

		// execute
		rows, err := db.Query(r.Context(), query, args...)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// map results
		var resources []ResourceEvent
		for rows.Next() {
			var e ResourceEvent
			var rawJSON []byte
			if err := rows.Scan(
				&e.GlobalUID,
				&e.ClusterName,
				&e.Namespace,
				&e.ResourceKind,
				&e.ResourceName,
				&e.InvolvedObjectUID,
				&e.EventType,
				&e.Reason,
				&e.Message,
				&e.CreatedAt,
				&rawJSON,
			); err != nil {
				http.Error(w, fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
				return
			}
			e.Raw = string(rawJSON)
			resources = append(resources, e)
		}
		if rows.Err() != nil {
			http.Error(w, fmt.Sprintf("rows error: %v", rows.Err()), http.StatusInternalServerError)
			return
		}

		// respond JSON
		resp := map[string]any{
			"resources": resources,
			"cursor":    lastRowCursor(resources),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
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
