package sql

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

type EncodedCursor string

type ResourcesCursor struct {
	CreatedAt time.Time `json:"created_at"`
	GlobalUID string    `json:"global_uid"`
}

// ResourcesQueryParams represents the set of supported query
// parameters for the /events endpoint.
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
//   - cursor (base64 encoded JSON)
//
// Invalid timestamps or labels JSON result in an error.
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

type resourcesQueryJSONPayload struct {
	Cluster   string         `json:"cluster"`
	Namespace string         `json:"namespace"`
	Kind      string         `json:"kind"`
	Name      string         `json:"name"`
	Labels    map[string]any `json:"labels"`
	Since     *time.Time     `json:"since"`
	Limit     *int           `json:"limit"`
	Cursor    EncodedCursor  `json:"cursor"`
}

// ResourcesQueryJSONFromHTTPRequest parses a JSON body from an HTTP request
// and returns a ResourcesQueryParams struct.
//
// Supported fields:
//   - cluster
//   - namespace
//   - kind
//   - name
//   - labels (JSON object)
//   - since (RFC3339 timestamp)
//   - limit
//   - cursor (base64 encoded JSON)
//
// Unknown fields and invalid JSON return an error.
func ResourcesQueryJSONFromHTTPRequest(r *http.Request) (ResourcesQueryParams, error) {
	defer r.Body.Close()

	var payload resourcesQueryJSONPayload
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&payload); err != nil {
		if err == io.EOF {
			return ResourcesQueryParams{}, fmt.Errorf("empty JSON body")
		}
		return ResourcesQueryParams{}, fmt.Errorf("invalid JSON body: %w", err)
	}

	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return ResourcesQueryParams{}, fmt.Errorf("invalid JSON body: multiple JSON values")
	}

	limit := 50
	if payload.Limit != nil {
		limit = *payload.Limit
	}
	if limit <= 0 {
		limit = 50
	}

	res := ResourcesQueryParams{
		Cluster:   payload.Cluster,
		Namespace: payload.Namespace,
		Kind:      payload.Kind,
		Name:      payload.Name,
		Since:     payload.Since,
		Limit:     limit,
		Cursor:    payload.Cursor,
	}

	if payload.Labels != nil {
		rawLabels, err := json.Marshal(payload.Labels)
		if err != nil {
			return res, fmt.Errorf("invalid labels JSON")
		}
		res.Labels = string(rawLabels)
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
	outSQL = shiftParamPlaceholders(outSQL, len(cteArgs))

	query := fmt.Sprintf(resourcesBaseSQL, cteSQL, outSQL)

	return query, append(cteArgs, outArgs...), nil
}

var dollarParamRE = regexp.MustCompile(`\$(\d+)`)

func shiftParamPlaceholders(query string, delta int) string {
	if delta <= 0 {
		return query
	}

	return dollarParamRE.ReplaceAllStringFunc(query, func(m string) string {
		n, err := strconv.Atoi(m[1:])
		if err != nil {
			return m
		}
		return fmt.Sprintf("$%d", n+delta)
	})
}

const resourcesBaseSQL = `
WITH latest AS (
    SELECT DISTINCT ON (global_uid)
        global_uid,
        cluster_name,
        namespace,
        resource_kind,
        resource_name,
        involved_object_uid,
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
