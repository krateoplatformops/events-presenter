package handlers

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/krateoplatformops/events-presenter/internal/queue"
)

type PgListenerConfig struct {
	DSN           string
	Channel       string
	ReconnectWait time.Duration
}

func RunPgListener(
	ctx context.Context,
	cfg PgListenerConfig,
	q queue.Queuer,
	db *pgxpool.Pool,
	hub *EventHub,
) {
	backoff := cfg.ReconnectWait
	if backoff == 0 {
		backoff = time.Second
	}

	for {
		if ctx.Err() != nil {
			return
		}

		log.Println("pg listener: connecting...")
		conn, err := pgx.Connect(ctx, cfg.DSN)
		if err != nil {
			log.Printf("pg listener: connect failed: %v", err)
			sleep(ctx, backoff)
			continue
		}

		err = listenAndServe(ctx, conn, cfg.Channel, q, db, hub)
		log.Printf("pg listener: disconnected: %v", err)

		conn.Close(ctx)
		sleep(ctx, backoff)
	}
}

func listenAndServe(
	ctx context.Context,
	conn *pgx.Conn,
	channel string,
	q queue.Queuer,
	db *pgxpool.Pool,
	hub *EventHub,
) error {
	_, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
	if err != nil {
		return err
	}

	log.Printf("pg listener: LISTEN %s", channel)

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}

		globalUID := n.Payload

		q.Push(queue.NewJob(globalUID, func(v interface{}) {
			uid := v.(string)

			events, err := loadLatestEvents(ctx, db, uid)
			if err != nil {
				log.Println("query error:", err)
				return
			}

			for _, ev := range events {
				hub.Broadcast(ev)
			}
		}))
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func loadLatestEvents(
	ctx context.Context,
	db *pgxpool.Pool,
	globalUID string,
) ([]ResourceEvent, error) {

	rows, err := db.Query(ctx, `
        SELECT
            global_uid,
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
		return nil, err
	}
	defer rows.Close()

	var res []ResourceEvent
	for rows.Next() {
		var ev ResourceEvent
		if err := rows.Scan(
			&ev.GlobalUID,
			&ev.ResourceKind,
			&ev.Reason,
			&ev.Message,
			&ev.CreatedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, ev)
	}

	return res, nil
}
