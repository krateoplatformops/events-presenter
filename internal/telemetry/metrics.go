package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Config struct {
	Enabled        bool
	ServiceName    string
	ExportInterval time.Duration
}

type Metrics struct {
	startupSuccess   metric.Int64Counter
	startupFailure   metric.Int64Counter
	dbConnectDurSecs metric.Float64Histogram

	httpEventsRequests  metric.Int64Counter
	httpEventsDurSecs   metric.Float64Histogram
	httpEventsResources metric.Int64Counter
	httpEventsErrors    metric.Int64Counter

	listenerNotifications metric.Int64Counter
	listenerJobsEnqueued  metric.Int64Counter
	listenerLoadLatestDur metric.Float64Histogram
	listenerLoadLatestErr metric.Int64Counter
	listenerConnectErr    metric.Int64Counter
	listenerDisconnects   metric.Int64Counter

	sseClientsConnected    metric.Int64Counter
	sseClientsDisconnected metric.Int64Counter
	sseEventsBroadcast     metric.Int64Counter
	sseEventsDropped       metric.Int64Counter
	sseClientsActiveGauge  metric.Int64ObservableGauge
	sseClientsActive       atomic.Int64
}

func Setup(ctx context.Context, log *slog.Logger, cfg Config) (*Metrics, func(context.Context) error, error) {
	if !cfg.Enabled {
		return nil, func(context.Context) error { return nil }, nil
	}

	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	resource, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName)))
	if err != nil {
		return nil, nil, err
	}

	options := []sdkmetric.PeriodicReaderOption{}
	if cfg.ExportInterval > 0 {
		options = append(options, sdkmetric.WithInterval(cfg.ExportInterval))
	}

	reader := sdkmetric.NewPeriodicReader(exporter, options...)
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource),
	)
	otel.SetMeterProvider(provider)

	meter := provider.Meter("github.com/krateoplatformops/events-presenter")
	metrics, err := newMetrics(meter)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, err
	}

	log.Info("OpenTelemetry metrics initialized")
	return metrics, provider.Shutdown, nil
}

func newMetrics(meter metric.Meter) (*Metrics, error) {
	var err error
	m := &Metrics{}

	if m.startupSuccess, err = meter.Int64Counter("events_presenter.startup.success"); err != nil {
		return nil, err
	}
	if m.startupFailure, err = meter.Int64Counter("events_presenter.startup.failure"); err != nil {
		return nil, err
	}
	if m.dbConnectDurSecs, err = meter.Float64Histogram("events_presenter.db.connect.duration_seconds"); err != nil {
		return nil, err
	}

	if m.httpEventsRequests, err = meter.Int64Counter("events_presenter.http.events.requests"); err != nil {
		return nil, err
	}
	if m.httpEventsDurSecs, err = meter.Float64Histogram("events_presenter.http.events.duration_seconds"); err != nil {
		return nil, err
	}
	if m.httpEventsResources, err = meter.Int64Counter("events_presenter.http.events.resources_returned"); err != nil {
		return nil, err
	}
	if m.httpEventsErrors, err = meter.Int64Counter("events_presenter.http.events.errors"); err != nil {
		return nil, err
	}

	if m.listenerNotifications, err = meter.Int64Counter("events_presenter.listener.notifications.received"); err != nil {
		return nil, err
	}
	if m.listenerJobsEnqueued, err = meter.Int64Counter("events_presenter.listener.jobs.enqueued"); err != nil {
		return nil, err
	}
	if m.listenerLoadLatestDur, err = meter.Float64Histogram("events_presenter.listener.load_latest.duration_seconds"); err != nil {
		return nil, err
	}
	if m.listenerLoadLatestErr, err = meter.Int64Counter("events_presenter.listener.load_latest.failure"); err != nil {
		return nil, err
	}
	if m.listenerConnectErr, err = meter.Int64Counter("events_presenter.listener.connect.failure"); err != nil {
		return nil, err
	}
	if m.listenerDisconnects, err = meter.Int64Counter("events_presenter.listener.disconnects"); err != nil {
		return nil, err
	}

	if m.sseClientsConnected, err = meter.Int64Counter("events_presenter.sse.clients.connected"); err != nil {
		return nil, err
	}
	if m.sseClientsDisconnected, err = meter.Int64Counter("events_presenter.sse.clients.disconnected"); err != nil {
		return nil, err
	}
	if m.sseEventsBroadcast, err = meter.Int64Counter("events_presenter.sse.events.broadcast"); err != nil {
		return nil, err
	}
	if m.sseEventsDropped, err = meter.Int64Counter("events_presenter.sse.events.dropped"); err != nil {
		return nil, err
	}
	if m.sseClientsActiveGauge, err = meter.Int64ObservableGauge("events_presenter.sse.clients.active"); err != nil {
		return nil, err
	}

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(m.sseClientsActiveGauge, m.sseClientsActive.Load())
		return nil
	}, m.sseClientsActiveGauge)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) IncStartupSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupSuccess.Add(ctx, 1)
}

func (m *Metrics) IncStartupFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupFailure.Add(ctx, 1)
}

func (m *Metrics) RecordDBConnectDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.dbConnectDurSecs.Record(ctx, d.Seconds())
}

func (m *Metrics) RecordEventsRequest(ctx context.Context, method string, status int, d time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("method", method),
		attribute.Int("status_code", status),
	)
	m.httpEventsRequests.Add(ctx, 1, attrs)
	m.httpEventsDurSecs.Record(ctx, d.Seconds(), attrs)
}

func (m *Metrics) AddEventsResourcesReturned(ctx context.Context, method string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.httpEventsResources.Add(ctx, n,
		metric.WithAttributes(attribute.String("method", method)))
}

func (m *Metrics) IncEventsError(ctx context.Context, method, stage string, status int) {
	if m == nil {
		return
	}
	m.httpEventsErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("method", method),
			attribute.String("stage", stage),
			attribute.Int("status_code", status),
		))
}

func (m *Metrics) IncListenerNotificationReceived(ctx context.Context) {
	if m == nil {
		return
	}
	m.listenerNotifications.Add(ctx, 1)
}

func (m *Metrics) IncListenerJobEnqueued(ctx context.Context) {
	if m == nil {
		return
	}
	m.listenerJobsEnqueued.Add(ctx, 1)
}

func (m *Metrics) RecordListenerLoadLatestDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.listenerLoadLatestDur.Record(ctx, d.Seconds())
}

func (m *Metrics) IncListenerLoadLatestFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.listenerLoadLatestErr.Add(ctx, 1)
}

func (m *Metrics) IncListenerConnectFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.listenerConnectErr.Add(ctx, 1)
}

func (m *Metrics) IncListenerDisconnect(ctx context.Context) {
	if m == nil {
		return
	}
	m.listenerDisconnects.Add(ctx, 1)
}

func (m *Metrics) IncSSEClientConnected(ctx context.Context) {
	if m == nil {
		return
	}
	m.sseClientsConnected.Add(ctx, 1)
	m.sseClientsActive.Add(1)
}

func (m *Metrics) IncSSEClientDisconnected(ctx context.Context) {
	if m == nil {
		return
	}
	m.sseClientsDisconnected.Add(ctx, 1)
	for {
		current := m.sseClientsActive.Load()
		if current <= 0 {
			return
		}
		if m.sseClientsActive.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (m *Metrics) AddSSEBroadcast(ctx context.Context, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.sseEventsBroadcast.Add(ctx, n)
}

func (m *Metrics) AddSSEDropped(ctx context.Context, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.sseEventsDropped.Add(ctx, n)
}
