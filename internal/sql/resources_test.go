package sql

import (
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

func dumpQuery(t *testing.T, name, sql string, args []any) {
	t.Helper()

	t.Logf("==== %s ====", name)
	t.Log(sql)
	t.Logf("ARGS (%d):", len(args))
	for i, a := range args {
		t.Logf("  $%d = %#v", i+1, a)
	}
}
