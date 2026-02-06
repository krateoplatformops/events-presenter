package sql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type EncodedCursor string

type ResourcesCursor struct {
	CreatedAt time.Time `json:"created_at"`
	GlobalUID string    `json:"global_uid"`
}

// ResourcesQueryParams represents the set of supported query
// parameters for the /resources endpoint.
type ResourcesQueryParams struct {
	Cluster   string
	Namespace string
	Kind      string
	Name      string
	// Labels is a JSON object (encoded as string) used to filter
	// Kubernetes labels via JSONB containment (@>).
	Labels string // raw JSON string

	// Since filters events created at or after this timestamp.
	Since *time.Time

	// Limit is the maximum number of resources to return.
	Limit int

	// CursorCreatedAt and CursorGlobalUID implement keyset
	// pagination for stable ordering.
	Cursor EncodedCursor
}

// ResourcesQueryParamsFromHTTPRequest parses query parameters
// from an HTTP request and returns a ResourcesQueryParams struct.
//
// Supported parameters:
//   - cluster
//   - namespace
//   - kind
//   - name
//   - labels (JSON object, e.g. {"app":"nginx"})
//   - since (RFC3339 timestamp)
//   - limit
//   - cursor_created_at (RFC3339 timestamp)
//   - cursor_global_uid
//
// Invalid timestamps result in an error.
func ResourcesQueryParamsFromHTTPRequest(r *http.Request) (ResourcesQueryParams, error) {
	q := r.URL.Query()

	var since *time.Time
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return ResourcesQueryParams{}, err
		}
		since = &t
	}

	limit := 50
	if v := q.Get("limit"); v != "" {
		fmt.Sscan(v, &limit)
	}
	if limit <= 0 {
		limit = 50
	}

	res := ResourcesQueryParams{
		Cluster:   q.Get("cluster"),
		Namespace: q.Get("namespace"),
		Kind:      q.Get("kind"),
		Name:      q.Get("name"),
		Labels:    q.Get("labels"),
		Since:     since,
		Limit:     limit,
		Cursor:    EncodedCursor(q.Get("cursor")),
	}

	if res.Labels != "" {
		var tmp map[string]any
		if err := json.Unmarshal([]byte(res.Labels), &tmp); err != nil {
			return res, fmt.Errorf("invalid labels JSON")
		}
	}

	return res, nil
}

func BuildResourcesQuery(p ResourcesQueryParams) (string, []any, error) {
	cte := NewBuilder() // filtri "di contenuto"
	out := NewBuilder() // keyset + order + limit

	// ---- FILTRI (dentro la CTE) ----

	if p.Cluster != "" {
		cte.Where("cluster_name = ?", p.Cluster)
	}

	if p.Namespace != "" {
		cte.Where("namespace = ?", p.Namespace)
	}

	if p.Kind != "" {
		cte.Where("resource_kind = ?", p.Kind)
	}

	if p.Name != "" {
		cte.Where("resource_name ILIKE ?", "%"+p.Name+"%")
	}

	if p.Since != nil {
		cte.Where("created_at >= ?", *p.Since)
	}

	if p.Labels != "" {
		cte.Where("raw->'involvedObject'->'labels' @> ?::jsonb", p.Labels)
	}

	// ---- KEYSET (fuori dalla CTE) ----

	// ----- Keyset pagination -----
	if p.Cursor != "" {
		cur, err := DecodeCursor(p.Cursor)
		if err != nil {
			return "", nil, fmt.Errorf("invalid cursor: %w", err)
		}
		if cur != nil {
			out.Where("(created_at, global_uid) < (?, ?)", cur.CreatedAt, cur.GlobalUID)
		}
	}

	out.OrderBy("created_at DESC, global_uid DESC")
	out.Limit(p.Limit)

	// ---- RENDER ----
	cteSQL, cteArgs := cte.Render("")
	outSQL, outArgs := out.Render("")

	query := fmt.Sprintf(resourcesBaseSQL, cteSQL, outSQL)

	return query, append(cteArgs, outArgs...), nil
}

const resourcesBaseSQL = `
WITH latest AS (
    SELECT DISTINCT ON (global_uid)
        global_uid,
        cluster_name,
        namespace,
        resource_kind,
        resource_name,
        event_type,
        reason,
        message,
        created_at,
        raw
    FROM k8s_events
    %s
    ORDER BY global_uid, created_at DESC
)
SELECT *
FROM latest
%s
`
