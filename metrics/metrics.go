package metrics

import (
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/web"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
)

type Metrics struct {
	App *web.Web

	cfg config.MetricsConfig
}

func NewMetrics(cfg config.MetricsConfig, tls config.TLSConfig) *Metrics {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	m := &Metrics{
		cfg: cfg,
	}

	m.App = &web.Web{
		FiberApp: app,
		Path:     "/metrics",
		Port:     cfg.PrometheusPort,
		TLS:      tls,
		Type:     "Prometheus Metrics"}

	m.App.FiberApp.Group(m.App.Path, adaptor.HTTPHandler(promhttp.Handler()))

	return m
}

func (m *Metrics) Start() error {
	if !m.cfg.PrometheusEnabled {
		return nil
	}

	return m.App.Start()
}

func (m *Metrics) Stop() error {
	if m.cfg.PrometheusEnabled {
		return m.App.Stop()
	}

	return nil
}
