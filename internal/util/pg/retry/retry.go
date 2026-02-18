package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func Default() Config {
	return Config{
		MaxAttempts: 5,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}
}

func Do[T any](
	ctx context.Context,
	cfg Config,
	fn func() (T, error),
) (T, error) {

	var zero T
	var err error

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {

		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		var result T
		result, err = fn()
		if err == nil {
			return result, nil
		}

		if !isTransient(err) {
			return zero, err
		}

		delay := computeBackoff(attempt, cfg)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}

	return zero, err
}

func isTransient(err error) bool {

	// Network-level error
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Conn closed / broken
	if errors.Is(err, pgx.ErrTxClosed) {
		return true
	}

	// PostgreSQL error
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// Serialization failure
		case "40001":
			return true
		// Deadlock
		case "40P01":
			return true
		default:
			return false
		}
	}

	return true
}

func computeBackoff(attempt int, cfg Config) time.Duration {
	exp := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	delay := time.Duration(exp)
	if delay > cfg.MaxDelay {
		delay = cfg.MaxDelay
	}

	// jitter 0-25%
	jitter := time.Duration(rand.Int63n(int64(delay / 4)))
	return delay - jitter
}
