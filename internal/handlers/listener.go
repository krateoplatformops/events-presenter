package handlers

import (
	"context"
	"errors"
	"log/slog"
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

		globalUID := n.Payload
		metrics.IncListenerNotificationReceived(ctx)

		q.Push(queue.NewJob(globalUID, func(v interface{}) {
			uid := v.(string)
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
				return loadLatestEvents(queryCtx, db, uid, metrics)
			})
			if err != nil {
				log.Error("query error",
					slog.String("global_uid", uid),
					slog.Any("err", err))
				return
			}

			for _, ev := range events {
				hub.Broadcast(ev)
			}
		}))
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
            created_at
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
