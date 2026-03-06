package sql

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBuildResourcesQuery_Empty(t *testing.T) {
	q := ResourcesQueryParams{
		Limit: 10,
	}

	sql, args, _ := BuildResourcesQuery(q)

	dumpQuery(t, "empty", sql, args)
}

func TestBuildResourcesQuery_WithFilters(t *testing.T) {
	since := time.Date(2026, 1, 9, 10, 0, 0, 0, time.UTC)

	q := ResourcesQueryParams{
		Cluster:   "cluster-a",
		Namespace: "prod",
		Kind:      "Deployment",
		Name:      "api",
		Since:     &since,
		Limit:     20,
	}

	sql, args, _ := BuildResourcesQuery(q)

	dumpQuery(t, "filters", sql, args)
}

func TestBuildResourcesQuery_WithLabels(t *testing.T) {
	q := ResourcesQueryParams{
		Labels: `{"app":"nginx","tier":"backend"}`,
		Limit:  5,
	}

	sql, args, _ := BuildResourcesQuery(q)

	dumpQuery(t, "labels", sql, args)
}

func TestBuildResourcesQuery_WithCursor(t *testing.T) {
	rc := ResourcesCursor{
		CreatedAt: time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC),
		GlobalUID: "cluster-a:pod-123",
	}

	q := ResourcesQueryParams{
		Cursor: EncodeCursor(&rc),
		Limit:  10,
	}

	sql, args, _ := BuildResourcesQuery(q)

	dumpQuery(t, "cursor", sql, args)
}

func TestBuildResourcesQuery_Full(t *testing.T) {
	rc := ResourcesCursor{
		CreatedAt: time.Date(2026, 1, 9, 12, 30, 0, 0, time.UTC),
		GlobalUID: "cluster-a:pod-123",
	}

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := EncodeCursor(&rc)

	EncodeCursor(&ResourcesCursor{})
	q := ResourcesQueryParams{
		Cluster:   "cluster-a",
		Namespace: "prod",
		Kind:      "Pod",
		Name:      "nginx",
		Labels:    `{"app":"nginx"}`,
		Since:     &since,
		Cursor:    cursor,
		Limit:     25,
	}

	sql, args, _ := BuildResourcesQuery(q)

	dumpQuery(t, "full", sql, args)
}

func TestBuildResourcesQuery_PlaceholderIndexing(t *testing.T) {
	rc := ResourcesCursor{
		CreatedAt: time.Date(2026, 1, 9, 12, 30, 0, 0, time.UTC),
		GlobalUID: "cluster-a:pod-123",
	}
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	q := ResourcesQueryParams{
		Cluster:   "cluster-a",
		Namespace: "prod",
		Kind:      "Pod",
		Name:      "nginx",
		Labels:    `{"app":"nginx"}`,
		Since:     &since,
		Cursor:    EncodeCursor(&rc),
		Limit:     25,
	}

	sql, args, err := BuildResourcesQuery(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, "(created_at, global_uid) < ($7, $8)") {
		t.Fatalf("expected shifted cursor placeholders in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT $9") {
		t.Fatalf("expected shifted limit placeholder in SQL, got:\n%s", sql)
	}
	if len(args) != 9 {
		t.Fatalf("expected 9 args, got %d", len(args))
	}
}

func dumpQuery(t *testing.T, name, sql string, args []any) {
	t.Helper()

	t.Logf("==== %s ====", name)
	t.Log(sql)
	t.Logf("ARGS (%d):", len(args))
	for i, a := range args {
		t.Logf("  $%d = %#v", i+1, a)
	}
}

func TestResourcesQueryJSONFromHTTPRequest_OK(t *testing.T) {
	body := `{
		"cluster":"cluster-a",
		"namespace":"prod",
		"kind":"Deployment",
		"name":"api",
		"labels":{"app":"payments","tier":"backend"},
		"since":"2026-03-01T00:00:00Z",
		"limit":100,
		"cursor":"abc"
	}`

	req := httptest.NewRequest("POST", "/events", strings.NewReader(body))
	got, err := ResourcesQueryJSONFromHTTPRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Cluster != "cluster-a" {
		t.Fatalf("unexpected cluster: %q", got.Cluster)
	}
	if got.Namespace != "prod" {
		t.Fatalf("unexpected namespace: %q", got.Namespace)
	}
	if got.Kind != "Deployment" {
		t.Fatalf("unexpected kind: %q", got.Kind)
	}
	if got.Name != "api" {
		t.Fatalf("unexpected name: %q", got.Name)
	}
	if got.Labels == "" {
		t.Fatal("labels should not be empty")
	}
	if got.Since == nil || got.Since.Format(time.RFC3339) != "2026-03-01T00:00:00Z" {
		t.Fatalf("unexpected since: %#v", got.Since)
	}
	if got.Limit != 100 {
		t.Fatalf("unexpected limit: %d", got.Limit)
	}
	if got.Cursor != "abc" {
		t.Fatalf("unexpected cursor: %q", got.Cursor)
	}
}

func TestResourcesQueryJSONFromHTTPRequest_DefaultLimit(t *testing.T) {
	req := httptest.NewRequest("POST", "/events", strings.NewReader(`{"limit":0}`))
	got, err := ResourcesQueryJSONFromHTTPRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Limit != 50 {
		t.Fatalf("expected default limit 50, got %d", got.Limit)
	}
}

func TestResourcesQueryJSONFromHTTPRequest_UnknownField(t *testing.T) {
	req := httptest.NewRequest("POST", "/events", strings.NewReader(`{"bad":"x"}`))
	_, err := ResourcesQueryJSONFromHTTPRequest(req)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestResourcesQueryJSONFromHTTPRequest_EmptyBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/events", strings.NewReader(""))
	_, err := ResourcesQueryJSONFromHTTPRequest(req)
	if err == nil {
		t.Fatal("expected error for empty body")
	}
}
