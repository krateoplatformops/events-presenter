package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestResourcesPagination_MultiPage(t *testing.T) {
	db, cleanup := setupTestPostgres(t)
	defer cleanup()

	applySchema(t, db)

	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

	seedEvents(t, db, seedOptions{
		Cluster:       "cluster-a",
		Namespace:     "default",
		Kind:          "Pod",
		Resources:     500,
		EventsPerRes:  2,
		StartTime:     now,
		DeltaPerEvent: time.Second,
	})

	handler := ResourcesHandler(db, nil)

	const pageSize = 100

	var (
		cursor string
		total  int
		page   int
	)

	for {
		resp := callResources(t, handler, cursor, pageSize)

		items := extractItems(t, resp)

		cursorVal, _ := resp["cursor"].(string)

		t.Logf("page=%d items=%d cursor=%s", page, len(items), cursorVal)

		total += len(items)
		page++

		// ⬇️ condizione di uscita CORRETTA
		if len(items) == 0 {
			if cursorVal != "" {
				t.Fatal("cursor must be empty on final page")
			}
			break
		}

		// ⬇️ se non è l'ultima pagina
		if len(items) == pageSize {
			if cursorVal == "" {
				t.Fatal("missing cursor on non-final page")
			}
			cursor = cursorVal
			continue
		}

		// ⬇️ ultima pagina parziale
		if cursorVal != "" {
			t.Fatal("cursor must be empty on last partial page")
		}

		break
	}

	if total != 500 {
		t.Fatalf("expected 500 resources, got %d", total)
	}
}

func TestResourcesPostPagination_MultiPage(t *testing.T) {
	db, cleanup := setupTestPostgres(t)
	defer cleanup()

	applySchema(t, db)

	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

	seedEvents(t, db, seedOptions{
		Cluster:       "cluster-a",
		Namespace:     "default",
		Kind:          "Pod",
		Resources:     500,
		EventsPerRes:  2,
		StartTime:     now,
		DeltaPerEvent: time.Second,
	})

	handler := ResourcesHandler(db, nil)

	const pageSize = 100

	var (
		cursor string
		total  int
		page   int
	)

	for {
		payload := map[string]any{
			"cluster":   "cluster-a",
			"namespace": "default",
			"kind":      "Pod",
			"limit":     pageSize,
		}
		if cursor != "" {
			payload["cursor"] = cursor
		}

		resp := callResourcesPOST(t, handler, payload)

		items := extractItems(t, resp)
		cursorVal, _ := resp["cursor"].(string)

		t.Logf("page=%d items=%d cursor=%s", page, len(items), cursorVal)

		total += len(items)
		page++

		if len(items) == 0 {
			if cursorVal != "" {
				t.Fatal("cursor must be empty on final page")
			}
			break
		}

		if len(items) == pageSize {
			if cursorVal == "" {
				t.Fatal("missing cursor on non-final page")
			}
			cursor = cursorVal
			continue
		}

		if cursorVal != "" {
			t.Fatal("cursor must be empty on last partial page")
		}

		break
	}

	if total != 500 {
		t.Fatalf("expected 500 resources, got %d", total)
	}
}

func TestResourcesHandler_MethodNotAllowed(t *testing.T) {
	handler := ResourcesHandler(nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/events", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}

	if allow := rec.Header().Get("Allow"); allow != "GET, POST" {
		t.Fatalf("expected Allow header %q, got %q", "GET, POST", allow)
	}
}

func TestResourcesHandler_CompositionIDPresentAndAbsent(t *testing.T) {
	db, cleanup := setupTestPostgres(t)
	defer cleanup()

	applySchema(t, db)

	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	createDailyPartition(t, db, now)

	compID := "11111111-1111-1111-1111-111111111111"

	insertEventWithCompositionID(
		t,
		db,
		now,
		"cluster-a",
		"uid-with-comp",
		"default",
		"Pod",
		"res-with-comp",
		"1",
		map[string]string{"app": "Pod"},
		&compID,
	)

	insertEventWithCompositionID(
		t,
		db,
		now.Add(-time.Second),
		"cluster-a",
		"uid-no-comp",
		"default",
		"Pod",
		"res-no-comp",
		"1",
		map[string]string{"app": "Pod"},
		nil,
	)

	handler := ResourcesHandler(db, nil)
	resp := callResources(t, handler, "", 10)
	items := extractItems(t, resp)

	if len(items) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(items))
	}

	foundWith := false
	foundWithout := false

	for _, it := range items {
		obj, ok := it.(map[string]any)
		if !ok {
			t.Fatalf("resource is not an object: %#v", it)
		}

		name, _ := obj["resource_name"].(string)
		switch name {
		case "res-with-comp":
			if v, ok := obj["event_id"].(string); !ok || v == "" {
				t.Fatalf("expected event_id for %q, got %#v", name, obj["event_id"])
			}
			v, ok := obj["composition_id"].(string)
			if !ok {
				t.Fatalf("expected composition_id for %q", name)
			}
			if v != compID {
				t.Fatalf("unexpected composition_id for %q: %q", name, v)
			}
			foundWith = true
		case "res-no-comp":
			if _, ok := obj["composition_id"]; ok {
				t.Fatalf("composition_id must be omitted when null for %q", name)
			}
			foundWithout = true
		}
	}

	if !foundWith {
		t.Fatal("resource with composition_id not found")
	}
	if !foundWithout {
		t.Fatal("resource without composition_id not found")
	}
}

func setupTestPostgres(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("skipping integration test: docker not available (%v)", r)
		}
	}()

	container, err := postgres.RunContainer(ctx,
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),

		// ⬇️ QUESTO È IL FIX
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// ⬇️ IMPORTANTISSIMO: disabilita TLS
	connStr += "&sslmode=disable"

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}

	// 🔥 Ping esplicito
	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return db, cleanup
}

func callResources(
	t *testing.T,
	handler http.Handler,
	cursor string,
	limit int,
) map[string]any {
	t.Helper()

	url := fmt.Sprintf("/resources?limit=%d", limit)
	if cursor != "" {
		url += "&cursor=" + cursor
	}

	req := httptest.NewRequest("GET", url, nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	return resp
}

func callResourcesPOST(
	t *testing.T,
	handler http.Handler,
	payload map[string]any,
) map[string]any {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	return resp
}

type seedOptions struct {
	Cluster       string
	Namespace     string
	Kind          string
	Resources     int // numero di risorse distinte
	EventsPerRes  int // eventi per risorsa
	StartTime     time.Time
	DeltaPerEvent time.Duration
}

func seedEvents(t *testing.T, db *pgxpool.Pool, opt seedOptions) {
	t.Helper()

	createDailyPartition(t, db, opt.StartTime)

	for i := 0; i < opt.Resources; i++ {
		uid := fmt.Sprintf("uid-%04d", i)

		for ev := 0; ev < opt.EventsPerRes; ev++ {
			createdAt := opt.StartTime.
				Add(-time.Duration(i*opt.EventsPerRes+ev) * opt.DeltaPerEvent)

			insertEvent(
				t,
				db,
				createdAt,
				opt.Cluster,
				uid,
				opt.Namespace,
				opt.Kind,
				fmt.Sprintf("res-%04d", i),
				fmt.Sprintf("%d", ev+1),
				map[string]string{
					"app": opt.Kind,
					"idx": fmt.Sprintf("%d", i),
				},
			)
		}
	}
}

func applySchema(t *testing.T, db *pgxpool.Pool) {
	_, err := db.Exec(context.Background(), `
CREATE TABLE IF NOT EXISTS k8s_events (
	created_at TIMESTAMPTZ NOT NULL,
	event_id TEXT NULL,
	cluster_name TEXT NOT NULL,
	uid TEXT NOT NULL,
	global_uid TEXT NOT NULL,
	namespace TEXT NOT NULL,
	resource_kind TEXT NOT NULL,
	resource_name TEXT NOT NULL,
	involved_object_uid TEXT NULL,
	event_type TEXT NOT NULL,
	reason TEXT NULL,
	message TEXT NULL,
	composition_id UUID NULL,
	raw JSONB NOT NULL,
	resource_version TEXT NOT NULL
) PARTITION BY RANGE (created_at);
`)
	if err != nil {
		t.Fatal(err)
	}
}

func createDailyPartition(t *testing.T, db *pgxpool.Pool, day time.Time) {
	start := day.Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)

	sql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS k8s_events_%s
PARTITION OF k8s_events
FOR VALUES FROM ('%s') TO ('%s');
`,
		start.Format("20060102"),
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
	)

	_, err := db.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
}

func insertEvent(
	t *testing.T,
	db *pgxpool.Pool,
	createdAt time.Time,
	cluster, uid, ns, kind, name, rv string,
	labels map[string]string,
) {
	raw := map[string]any{
		"involvedObject": map[string]any{
			"labels": labels,
		},
	}

	rawJSON, _ := json.Marshal(raw)

	_, err := db.Exec(context.Background(), `
INSERT INTO k8s_events (
	created_at,
	event_id,
	cluster_name,
	uid,
	global_uid,
	namespace,
	resource_kind,
	resource_name,
	involved_object_uid,
	event_type,
	raw,
	resource_version
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'Normal',$10,$11)
`,
		createdAt,
		fmt.Sprintf("event-%s-%s", uid, rv),
		cluster,
		uid,
		cluster+":"+uid,
		ns,
		kind,
		name,
		uid,
		rawJSON,
		rv,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func insertEventWithCompositionID(
	t *testing.T,
	db *pgxpool.Pool,
	createdAt time.Time,
	cluster, uid, ns, kind, name, rv string,
	labels map[string]string,
	compositionID *string,
) {
	raw := map[string]any{
		"involvedObject": map[string]any{
			"labels": labels,
		},
	}

	rawJSON, _ := json.Marshal(raw)

	_, err := db.Exec(context.Background(), `
INSERT INTO k8s_events (
	created_at,
	event_id,
	cluster_name,
	uid,
	global_uid,
	namespace,
	resource_kind,
	resource_name,
	involved_object_uid,
	event_type,
	composition_id,
	raw,
	resource_version
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'Normal',$10::uuid,$11,$12)
`,
		createdAt,
		fmt.Sprintf("event-%s-%s", uid, rv),
		cluster,
		uid,
		cluster+":"+uid,
		ns,
		kind,
		name,
		uid,
		compositionID,
		rawJSON,
		rv,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func extractItems(t *testing.T, resp map[string]any) []any {
	t.Helper()

	raw, ok := resp["resources"]
	if !ok || raw == nil {
		return nil
	}

	items, ok := raw.([]any)
	if !ok {
		t.Fatalf("resources is not an array: %#v", raw)
	}

	return items
}
