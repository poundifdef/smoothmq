package server

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/dashboard"
	"github.com/poundifdef/smoothmq/metrics"
	"github.com/poundifdef/smoothmq/models"
	"github.com/poundifdef/smoothmq/protocols/sqs"
	"github.com/poundifdef/smoothmq/queue/sqlite"
	"github.com/poundifdef/smoothmq/tenants/defaultmanager"
	"github.com/poundifdef/smoothmq/web"
)

func recordTelemetry(message string, disabled bool) {
	if disabled {
		return
	}

	url := "https://telemetry.fly.dev"
	jsonData := []byte(message)

	client := &http.Client{
		Timeout: 100 * time.Millisecond,
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	resp.Body.Close()
}

func Run(tm models.TenantManager, queue models.Queue, cfg config.ServerCommand) {
	recordTelemetry("start", cfg.DisableTelemetry)

	// Initialize default tenant manager
	if tm == nil {
		tm = defaultmanager.NewDefaultTenantManager(cfg.SQS.Keys)
	}

	// Initialize default queue implementation
	if queue == nil {
		queue = sqlite.NewSQLiteQueue(cfg.SQLite)
	}

	dashboardServer := dashboard.NewDashboard(queue, tm, cfg.Dashboard, cfg.TLS)
	sqsServer := sqs.NewSQS(queue, tm, cfg.SQS, cfg.TLS)
	metricsServer := metrics.NewMetrics(cfg.Metrics, cfg.TLS)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	if !cfg.UseSinglePort {
		go func() {
			dashboardServer.Start()
		}()

		go func() {
			sqsServer.Start()
		}()

		go func() {
			metricsServer.Start()
		}()

		<-c // This blocks the main thread until an interrupt is received
		fmt.Println("Gracefully shutting down...")

		dashboardServer.Stop()
		sqsServer.Stop()
		metricsServer.Stop()
	} else {
		app := fiber.New(fiber.Config{
			DisableStartupMessage: true,
		})

		if cfg.SQS.Enabled {
			sqsServer.App.Port = cfg.Port
			sqsServer.App.Path = "/sqs"
			app.Mount("/sqs", sqsServer.App.FiberApp)
			sqsServer.App.OutputPort()
		}

		if cfg.Metrics.PrometheusEnabled {
			// "/metrics" is the standard path for prometheus - no need to add it here
			app.Mount("", metricsServer.App.FiberApp)
			metricsServer.App.Port = cfg.Port
			metricsServer.App.OutputPort()
		}

		// This needs to go last to avoid confliciting with prometheus
		if cfg.Dashboard.Enabled {
			dashboardServer.App.Port = cfg.Port
			app.Mount("/", dashboardServer.App.FiberApp)
			dashboardServer.App.OutputPort()
		}

		web_app := web.Web{FiberApp: app, TLS: cfg.TLS, Port: cfg.Port}
		go func() {
			web_app.Start()
		}()

		<-c // This blocks the main thread until an interrupt is received
		fmt.Println("Gracefully shutting down...")

		web_app.Stop()
	}

	queue.Shutdown()
	recordTelemetry("stop", cfg.DisableTelemetry)
}
