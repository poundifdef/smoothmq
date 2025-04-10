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
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/dashboard"
	"github.com/poundifdef/smoothmq/models"
	"github.com/poundifdef/smoothmq/protocols/sqs"
	"github.com/poundifdef/smoothmq/queue/sqlite"
	"github.com/poundifdef/smoothmq/tenants/defaultmanager"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	dashboardServer := dashboard.NewDashboard(queue, tm, cfg.Dashboard)
	sqsServer := sqs.NewSQS(queue, tm, cfg.SQS)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	if !cfg.UseSinglePort {
		go func() {
			dashboardServer.Start()
		}()

		go func() {
			sqsServer.Start()
		}()

		if cfg.Metrics.PrometheusEnabled {
			fmt.Printf("Prometheus metrics: http://%s:%d%s\n", cfg.Metrics.PrometheusHost, cfg.Metrics.PrometheusPort, cfg.Metrics.PrometheusPath)
			go func() {
				http.Handle(cfg.Metrics.PrometheusPath, promhttp.Handler())
				http.ListenAndServe(fmt.Sprintf("%s:%d", cfg.Metrics.PrometheusHost, cfg.Metrics.PrometheusPort), nil)
			}()
		}

		<-c // This blocks the main thread until an interrupt is received
		fmt.Println("Gracefully shutting down...")

		dashboardServer.Stop()
		sqsServer.Stop()
	} else {
		app := fiber.New(fiber.Config{
			DisableStartupMessage: true,
		})

		if cfg.Dashboard.Enabled {
			app.Mount("/", dashboardServer.App)
			fmt.Printf("Dashboard http://%s:%d\n", cfg.Host, cfg.Port)
		}

		if cfg.SQS.Enabled {
			app.Mount("/sqs", sqsServer.App)
			fmt.Printf("SQS Endpoint http://%s:%d/sqs\n", cfg.Host, cfg.Port)
		}

		if cfg.Metrics.PrometheusEnabled {
			app.Group("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
			fmt.Printf("Prometheus http://%s:%d/metrics\n", cfg.Host, cfg.Port)
		}

		go func() {
			app.Listen(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
		}()

		<-c // This blocks the main thread until an interrupt is received
		fmt.Println("Gracefully shutting down...")

		app.Shutdown()
	}

	queue.Shutdown()
	recordTelemetry("stop", cfg.DisableTelemetry)
}
