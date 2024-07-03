package smoothmq

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"q/dashboard"
	"q/models"
	"q/protocols/sqs"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Run(tm models.TenantManager, queue models.Queue) {
	dashboardServer := dashboard.NewDashboard(queue, tm)
	go func() {
		dashboardServer.Start()
	}()

	sqsServer := sqs.NewSQS(queue, tm)
	go func() {
		sqsServer.Start()
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c // This blocks the main thread until an interrupt is received
	fmt.Println("Gracefully shutting down...")

	dashboardServer.Stop()
	sqsServer.Stop()
	queue.Shutdown()
}
