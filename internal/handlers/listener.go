package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/krateoplatformops/events-presenter/internal/queue"
	"github.com/krateoplatformops/events-presenter/internal/telemetry"
	"github.com/krateoplatformops/events-presenter/internal/util/pg/retry"
)

type PgListenerConfig struct {
	DSN           string
	Channel       string
	ReconnectWait time.Duration
	Log           *slog.Logger
}

type listenerNotification struct {
	EventID   string
	GlobalUID string
}

func RunPgListener(
	ctx context.Context,
	cfg PgListenerConfig,
	q queue.Queuer,
	db *pgxpool.Pool,
	hub *EventHub,
	metrics *telemetry.Metrics,
) {
	baseBackoff := cfg.ReconnectWait
	if baseBackoff <= 0 {
		baseBackoff = time.Second
	}
	backoff := baseBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		cfg.Log.Debug("pg listener: connecting...")

		conn, err := pgx.Connect(ctx, cfg.DSN)
		if err != nil {
			metrics.IncListenerConnectFailure(ctx)
			backoff = sleepWithBackoff(ctx, backoff)
			cfg.Log.Warn("pg listener: connect failed",
				slog.String("backoff", backoff.String()),
				slog.Any("err", err))
			continue
		}
		backoff = baseBackoff

		err = listenAndServe(ctx, conn, cfg.Channel, q, db, hub, cfg.Log, metrics)
		if err != nil && !errors.Is(err, context.Canceled) {
			metrics.IncListenerDisconnect(ctx)
			cfg.Log.Error("pg listener: disconnected", slog.Any("err", err))
		}

		conn.Close(ctx)
		backoff = sleepWithBackoff(ctx, backoff)
	}
}

func listenAndServe(
	ctx context.Context,
	conn *pgx.Conn,
	channel string,
	q queue.Queuer,
	db *pgxpool.Pool,
	hub *EventHub,
	log *slog.Logger,
	metrics *telemetry.Metrics,
) error {
	_, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
	if err != nil {
		return err
	}

	log.Info("pg listener: LISTEN", slog.String("channel", channel))

	for {
		waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
		n, err := conn.WaitForNotification(waitCtx)
		waitCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
				pingErr := conn.Ping(pingCtx)
				pingCancel()
				if pingErr != nil {
					return pingErr
				}
				continue
			}
			return err
		}

		notification := parseListenerNotification(n.Payload)
		log.Debug("notification received",
			slog.String("global_uid", notification.GlobalUID),
			slog.String("event_id", notification.EventID),
		)

		metrics.IncListenerNotificationReceived(ctx)

		log.Debug("pushing fetch notification job to queue",
			slog.String("global_uid", notification.GlobalUID),
			slog.String("event_id", notification.EventID),
		)
		q.Push(queue.NewJob(notification, func(v any) {
			notification := v.(listenerNotification)
			metrics.IncListenerJobEnqueued(ctx)

			queryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()

			// A longer retry window avoids dropping notifications during
			// short DB outages (pod restart, failover, rolling updates).
			events, err := retry.Do(queryCtx, retry.Config{
				MaxAttempts: 12,
				BaseDelay:   500 * time.Millisecond,
				MaxDelay:    10 * time.Second,
			}, func() ([]ResourceEvent, error) {
				if notification.EventID != "" {
					return loadEventByID(queryCtx, db, notification.EventID, log, metrics)
				}
				return loadLatestEvents(queryCtx, db, notification.GlobalUID, metrics)
			})
			if err != nil {
				attrs := []slog.Attr{
					slog.String("global_uid", notification.GlobalUID),
					slog.String("event_id", notification.EventID),
					slog.Any("err", err),
				}
				log.LogAttrs(queryCtx, slog.LevelError, "query error", attrs...)
				return
			}

			for _, ev := range events {
				hub.Broadcast(ev)
			}
		}))
	}
}

func parseListenerNotification(payload string) listenerNotification {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return listenerNotification{}
	}

	var raw struct {
		EventID   string `json:"event_id"`
		GlobalUID string `json:"global_uid"`
	}
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return listenerNotification{GlobalUID: payload}
	}

	raw.EventID = strings.TrimSpace(raw.EventID)
	raw.GlobalUID = strings.TrimSpace(raw.GlobalUID)
	if raw.EventID == "" {
		return listenerNotification{GlobalUID: raw.GlobalUID}
	}

	return listenerNotification{
		EventID:   raw.EventID,
		GlobalUID: raw.GlobalUID,
	}
}

func sleepWithBackoff(ctx context.Context, d time.Duration) time.Duration {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-ctx.Done():
	}

	next := d * 2
	if next > 30*time.Second {
		next = 30 * time.Second
	}
	return next
}

func loadEventByID(
	ctx context.Context,
	db *pgxpool.Pool,
	eventID string,
	log *slog.Logger,
	metrics *telemetry.Metrics,
) ([]ResourceEvent, error) {
	started := time.Now()
	defer func() {
		metrics.RecordListenerLoadLatestDuration(ctx, time.Since(started))
	}()

	query := `
        SELECT
			event_id,
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
			composition_id
        FROM k8s_events
        WHERE event_id = $1
        LIMIT 1
    `
	rows, err := db.Query(ctx, query, eventID)
	if err != nil {
		metrics.IncListenerLoadLatestFailure(ctx)
		return nil, err
	}
	defer rows.Close()

	log.Debug("loaded events on pg notify",
		slog.String("event_id", eventID),
	)

	var res []ResourceEvent
	for rows.Next() {
		var ev ResourceEvent
		var scannedEventID string
		if err := rows.Scan(
			&scannedEventID,
			&ev.GlobalUID,
			&ev.ClusterName,
			&ev.Namespace,
			&ev.ResourceKind,
			&ev.ResourceName,
			&ev.InvolvedObjectUID,
			&ev.EventType,
			&ev.Reason,
			&ev.Message,
			&ev.CreatedAt,
			&ev.CompositionID,
		); err != nil {
			metrics.IncListenerLoadLatestFailure(ctx)
			return nil, err
		}
		ev.EventID = &scannedEventID
		res = append(res, ev)

		log.Debug("event details", slog.Any("ev", ev))
	}
	if rows.Err() != nil {
		metrics.IncListenerLoadLatestFailure(ctx)
		return nil, rows.Err()
	}

	log.Debug("query completed",
		slog.String("query", query),
		slog.String("event_id", eventID),
		slog.Int("rows", len(res)),
		slog.Duration("duration", time.Since(started)),
	)

	return res, nil
}

func loadLatestEvents(
	ctx context.Context,
	db *pgxpool.Pool,
	globalUID string,
	metrics *telemetry.Metrics,
) ([]ResourceEvent, error) {
	started := time.Now()
	defer func() {
		metrics.RecordListenerLoadLatestDuration(ctx, time.Since(started))
	}()

	rows, err := db.Query(ctx, `
        SELECT
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
			composition_id
        FROM k8s_events
        WHERE global_uid = $1
        ORDER BY created_at DESC
        LIMIT 3
    `, globalUID)
	if err != nil {
		metrics.IncListenerLoadLatestFailure(ctx)
		return nil, err
	}
	defer rows.Close()

	var res []ResourceEvent
	for rows.Next() {
		var ev ResourceEvent
		if err := rows.Scan(
			&ev.GlobalUID,
			&ev.ClusterName,
			&ev.Namespace,
			&ev.ResourceKind,
			&ev.ResourceName,
			&ev.InvolvedObjectUID,
			&ev.EventType,
			&ev.Reason,
			&ev.Message,
			&ev.CreatedAt,
			&ev.CompositionID,
		); err != nil {
			metrics.IncListenerLoadLatestFailure(ctx)
			return nil, err
		}
		res = append(res, ev)
	}
	if rows.Err() != nil {
		metrics.IncListenerLoadLatestFailure(ctx)
		return nil, rows.Err()
	}

	return res, nil
}
