# events-presenter

**events-presenter** is a lightweight Go service that listens for events from PostgreSQL and exposes them over HTTP, providing both:

- **Server-Sent Events (SSE)** for real-time notifications
- **REST endpoints** for querying event resources
- **Health probes** for Kubernetes deployments

It is designed to be cloud-native, resilient, and easy to deploy inside Kubernetes.


## Architecture Overview

The service acts as a bridge between PostgreSQL and connected clients:

1. Connects to PostgreSQL and waits until the database is ready.
2. Subscribes to a PostgreSQL `LISTEN/NOTIFY` channel (`events`).
3. Pushes incoming notifications into an internal queue.
4. Broadcasts events to connected SSE clients.
5. Exposes HTTP endpoints for querying stored resources.
6. Provides readiness and liveness probes.
7. Supports graceful shutdown.


## Features

- Real-time event streaming via SSE
- PostgreSQL LISTEN/NOTIFY integration
- Connection pool for efficient DB usage
- Internal worker queue for concurrent processing
- Kubernetes-ready health probes
- Graceful shutdown handling
- Structured logging support
- Configurable via environment variables


## Endpoints

### `GET /notifications`

Server-Sent Events endpoint.

Streams events to connected clients in real time.

**Example:**

```bash
curl -N http://localhost:8083/notifications
```

### `GET /events` and `POST /events`

Returns event-related resources from PostgreSQL.

```sh
curl http://localhost:8083/events
```

```sh
curl --request POST http://localhost:8083/events \
  --header "Content-Type: application/json" \
  --data '{"cluster":"cluster-a","limit":100}'
```

Detailed search and pagination examples are available in [`SEARCH.md`](./SEARCH.md).

## Health Checks

| Endpoint  | Purpose                                                            |
| --------- | ------------------------------------------------------------------ |
| `/livez`  | Liveness probe – indicates the process is running                  |
| `/readyz` | Readiness probe – indicates the service is ready to accept traffic |


## Configuration

| Variable           | Description                       | Default     |
| ------------------ | --------------------------------- | ----------- |
| `PORT`             | HTTP server port                  | `8083`      |
| `DEBUG`            | Enable debug logging              | `false`     |
| `DB_USER`          | Database username                 | —           |
| `DB_PASS`          | Database password                 | —           |
| `DB_NAME`          | Database name                     | —           |
| `DB_HOST`          | Database host                     | `localhost` |
| `DB_PORT`          | Database port                     | `5432`      |
| `DB_PARAMS`        | Extra connection parameters       | —           |
| `DB_READY_TIMEOUT` | Max time to wait for DB readiness | `2m`        |
| `OTEL_ENABLED`     | Enable OpenTelemetry metrics      | `true`      |
| `OTEL_EXPORT_INTERVAL` | Metrics export interval      | `30s`       |

> The service builds a PostgreSQL connection string from these values.

## Kubernetes Deployment

The service is Kubernetes-friendly and supports:

* Readiness / liveness probes
* Graceful shutdown on SIGTERM
* Externalized configuration
* Secret-based credentials

Typical deployment flow:

1. Deploy PostgreSQL.
2. Configure environment variables (or Helm values).
3. Expose via Service / Ingress.
4. Optionally enable TLS with cert-manager.

## Graceful Shutdown

On `SIGINT` or `SIGTERM`, the service:

1. Marks itself as not ready.
2. Stops accepting new HTTP connections.
3. Shuts down the PostgreSQL listener.
4. Drains workers.
5. Terminates cleanly.

This prevents dropped connections and partial event delivery.


## Internal Components

### PostgreSQL Listener

Continuously listens on the `events` channel and reconnects automatically if the connection drops.

### Queue

Buffered worker queue with configurable concurrency to avoid blocking event ingestion.

### Event Hub

Manages SSE clients and broadcasts events safely to all subscribers.

## Production Recommendations

* Use connection pooling (already enabled).
* Store DB credentials in Kubernetes Secrets.
* Place the service behind an ingress with proper timeouts for SSE.
* Enable TLS.
* Monitor readiness probes during deployments.
