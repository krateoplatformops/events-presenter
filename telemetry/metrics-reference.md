# Events Presenter Metrics Reference

This document describes the OpenTelemetry metrics emitted by `events-presenter`.

## Naming note

In code, metric names use dots (for example `events_presenter.startup.success`).
In Prometheus, names are typically normalized with underscores (for example `events_presenter_startup_success`), and counters may be exposed with `_total`.

## Metrics

| Metric | Type | Unit | Description | Emitted from | PromQL example |
|---|---|---|---|---|---|
| `events_presenter.startup.success` | Counter | count | Service startup completed successfully. | `main.go` | `sum(increase(events_presenter_startup_success_total[1h]))` |
| `events_presenter.startup.failure` | Counter | count | Service startup failed. | `main.go` | `sum(increase(events_presenter_startup_failure_total[1h]))` |
| `events_presenter.db.connect.duration_seconds` | Histogram | seconds | Time spent waiting for PostgreSQL readiness. | `main.go` | `histogram_quantile(0.95, sum by (le) (rate(events_presenter_db_connect_duration_seconds_bucket[5m])))` |
| `events_presenter.http.events.requests` | Counter | requests | Number of `/events` requests. Labels: `method`, `status_code`. | `internal/handlers/resources.go` | `sum by (method) (rate(events_presenter_http_events_requests_total[5m]))` |
| `events_presenter.http.events.duration_seconds` | Histogram | seconds | `/events` request latency. Labels: `method`, `status_code`. | `internal/handlers/resources.go` | `histogram_quantile(0.95, sum by (le, method) (rate(events_presenter_http_events_duration_seconds_bucket[5m])))` |
| `events_presenter.http.events.resources_returned` | Counter | resources | Number of resources returned by `/events`. Label: `method`. | `internal/handlers/resources.go` | `sum(rate(events_presenter_http_events_resources_returned_total[5m]))` |
| `events_presenter.http.events.errors` | Counter | errors | Errors in `/events` flow. Labels: `method`, `stage`, `status_code`. | `internal/handlers/resources.go` | `sum by (stage) (rate(events_presenter_http_events_errors_total[5m]))` |
| `events_presenter.listener.notifications.received` | Counter | notifications | PostgreSQL notifications received from LISTEN/NOTIFY. | `internal/handlers/listener.go` | `sum(rate(events_presenter_listener_notifications_received_total[5m]))` |
| `events_presenter.listener.jobs.enqueued` | Counter | jobs | Jobs enqueued after notifications. | `internal/handlers/listener.go` | `sum(rate(events_presenter_listener_jobs_enqueued_total[5m]))` |
| `events_presenter.listener.load_latest.duration_seconds` | Histogram | seconds | Duration of latest-events DB fetch after notification. | `internal/handlers/listener.go` | `histogram_quantile(0.95, sum by (le) (rate(events_presenter_listener_load_latest_duration_seconds_bucket[5m])))` |
| `events_presenter.listener.load_latest.failure` | Counter | failures | Failures in latest-events DB fetch path. | `internal/handlers/listener.go` | `sum(increase(events_presenter_listener_load_latest_failure_total[1h]))` |
| `events_presenter.listener.connect.failure` | Counter | failures | Failures while connecting/reconnecting listener to PostgreSQL. | `internal/handlers/listener.go` | `sum(increase(events_presenter_listener_connect_failure_total[1h]))` |
| `events_presenter.listener.disconnects` | Counter | disconnects | Listener disconnect events (unexpected listener loop exits). | `internal/handlers/listener.go` | `sum(increase(events_presenter_listener_disconnects_total[1h]))` |
| `events_presenter.sse.clients.connected` | Counter | clients | Total SSE client subscriptions opened. | `internal/handlers/hub.go` | `sum(increase(events_presenter_sse_clients_connected_total[1h]))` |
| `events_presenter.sse.clients.disconnected` | Counter | clients | Total SSE client subscriptions closed. | `internal/handlers/hub.go` | `sum(increase(events_presenter_sse_clients_disconnected_total[1h]))` |
| `events_presenter.sse.clients.active` | Gauge | clients | Current number of active SSE subscribers. | `internal/handlers/hub.go` | `max(events_presenter_sse_clients_active)` |
| `events_presenter.sse.events.broadcast` | Counter | events | Events delivered to SSE client channels. | `internal/handlers/hub.go` | `sum(rate(events_presenter_sse_events_broadcast_total[5m]))` |
| `events_presenter.sse.events.dropped` | Counter | events | Events dropped due to slow SSE clients. | `internal/handlers/hub.go` | `sum(rate(events_presenter_sse_events_dropped_total[5m]))` |

## Cardinality guidance

- Avoid high-cardinality labels (`global_uid`, `resource_name`, dynamic IDs).
- Keep metrics at service-level and low-cardinality.
- Current labels are bounded: `method`, `status_code`, `stage`.
