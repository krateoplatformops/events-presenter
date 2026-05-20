# `/events` Search Guide

This document explains how to query `/events` in this project.

Supported methods:

- `GET /events` with query parameters
- `POST /events` with a JSON body

## What `/events` Returns

`/events` returns a list named `resources`, where each item is the latest event for a resource (`global_uid`).

Internally, the query keeps only the newest row per `global_uid`, then applies filters, pagination, and ordering.

When available, each returned item also includes `event_id`, the identifier of
the underlying event row. The endpoint semantics do not change: `/events` still
returns the latest row per resource, not a full event feed.

## Query Parameters

All filters are optional and are combined with `AND`.

| Parameter | Type | Behavior |
| --- | --- | --- |
| `cluster` | string | Exact match on `cluster_name`. |
| `namespace` | string | Exact match on `namespace`. |
| `kind` | string | Exact match on `resource_kind`. |
| `name` | string | Case-insensitive partial match on `resource_name` (`ILIKE %name%`). |
| `labels` | JSON object (string) | JSONB containment on `raw->involvedObject->labels` (`@>`). Must be valid JSON. |
| `since` | RFC3339 timestamp | Includes events with `created_at >= since`. |
| `limit` | integer | Page size. Default: `50`. If `<= 0`, default `50` is used. |
| `cursor` | base64 string | Keyset cursor for pagination (opaque token returned by previous response). |

If `since` is invalid RFC3339 or `labels` is invalid JSON, the API returns `400`.
If `cursor` is invalid base64/JSON, the API returns `400`.

## POST JSON Body

For `POST /events`, use the same fields in JSON format.

Example body:

```json
{
  "cluster": "cluster-a",
  "namespace": "prod",
  "kind": "Deployment",
  "name": "api",
  "labels": {
    "app": "payments"
  },
  "since": "2026-03-01T00:00:00Z",
  "limit": 100,
  "cursor": "<cursor-from-previous-page>"
}
```

Notes:

- Unknown JSON fields return `400`.
- Empty body returns `400`.
- `limit` defaults to `50` when omitted or `<= 0`.

## Search Examples

### 1) GET: no filters (first page)

```bash
curl "http://localhost:8083/events"
```

### 2) GET: filter by cluster + namespace + kind

```bash
curl --get "http://localhost:8083/events" \
  --data-urlencode "cluster=cluster-a" \
  --data-urlencode "namespace=default" \
  --data-urlencode "kind=Pod"
```

### 3) GET: search by resource name (contains, case-insensitive)

```bash
curl --get "http://localhost:8083/events" \
  --data-urlencode "name=api"
```

Matches names like `api`, `API`, `my-api-service`.

### 4) GET: filter by labels

```bash
curl --get "http://localhost:8083/events" \
  --data-urlencode 'labels={"app":"nginx","tier":"backend"}'
```

`labels` uses JSON containment: all provided key/value pairs must exist in the resource labels.

### 5) GET: filter by time (`since`)

```bash
curl --get "http://localhost:8083/events" \
  --data-urlencode "since=2026-03-01T00:00:00Z"
```

### 6) GET: combine filters

```bash
curl --get "http://localhost:8083/events" \
  --data-urlencode "cluster=cluster-a" \
  --data-urlencode "namespace=prod" \
  --data-urlencode "kind=Deployment" \
  --data-urlencode "name=api" \
  --data-urlencode 'labels={"app":"payments"}' \
  --data-urlencode "since=2026-03-01T00:00:00Z" \
  --data-urlencode "limit=100"
```

## Pagination with `cursor`

Use keyset pagination:

1. First call without `cursor`.
2. Read `cursor` from response.
3. Send it back in the next call.

Example:

```bash
# page 1
curl --get "http://localhost:8083/events" \
  --data-urlencode "limit=100"

# page 2
curl --get "http://localhost:8083/events" \
  --data-urlencode "limit=100" \
  --data-urlencode "cursor=<cursor-from-page-1>"
```

The cursor is built from the last returned row (`created_at`, `global_uid`) and is opaque to clients.

### Re-send the same query with cursor (manual next page)

Keep the same filters and `limit`, and only add/update `cursor`.

```bash
# first request
curl --get "http://localhost:8083/events" \
  --data-urlencode "cluster=cluster-a" \
  --data-urlencode "namespace=prod" \
  --data-urlencode "kind=Deployment" \
  --data-urlencode "limit=2"

# suppose response contains: "cursor": "<CURSOR_PAGE_1>"

# second request (same filters + cursor from page 1)
curl --get "http://localhost:8083/events" \
  --data-urlencode "cluster=cluster-a" \
  --data-urlencode "namespace=prod" \
  --data-urlencode "kind=Deployment" \
  --data-urlencode "limit=2" \
  --data-urlencode "cursor=<CURSOR_PAGE_1>"

# third request uses cursor returned by page 2, and so on
```

If you change filters between pages, pagination continuity is broken.

### Automatic pagination loop (Bash + jq)

This example keeps fetching pages until `cursor` is empty.

```bash
BASE_URL="http://localhost:8083/events"
CURSOR=""

while true; do
  if [ -n "$CURSOR" ]; then
    RESP=$(curl --silent --get "$BASE_URL" \
      --data-urlencode "cluster=cluster-a" \
      --data-urlencode "namespace=prod" \
      --data-urlencode "kind=Deployment" \
      --data-urlencode "limit=100" \
      --data-urlencode "cursor=$CURSOR")
  else
    RESP=$(curl --silent --get "$BASE_URL" \
      --data-urlencode "cluster=cluster-a" \
      --data-urlencode "namespace=prod" \
      --data-urlencode "kind=Deployment" \
      --data-urlencode "limit=100")
  fi

  # process current page
  echo "$RESP" | jq '.resources[]'

  # read next cursor
  CURSOR=$(echo "$RESP" | jq -r '.cursor // ""')
  [ -z "$CURSOR" ] && break
done
```

### Re-send the same POST query with cursor

Keep the same filters and `limit`, and only replace `cursor`.

```bash
# page 1
curl --silent --request POST "http://localhost:8083/events" \
  --header "Content-Type: application/json" \
  --data '{
    "cluster":"cluster-a",
    "namespace":"prod",
    "kind":"Deployment",
    "limit":2
  }'

# page 2 (use cursor from page 1 response)
curl --silent --request POST "http://localhost:8083/events" \
  --header "Content-Type: application/json" \
  --data '{
    "cluster":"cluster-a",
    "namespace":"prod",
    "kind":"Deployment",
    "limit":2,
    "cursor":"<CURSOR_PAGE_1>"
  }'
```

## Sorting

Sorting is implemented but not user-configurable.

Fixed order is:

- `created_at DESC`
- `global_uid DESC`

There is no query parameter to change sort field or direction.
