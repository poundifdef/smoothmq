package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/dashboard"
	"github.com/poundifdef/smoothmq/models"
	"github.com/poundifdef/smoothmq/protocols/sqs"
)

func Run(tm models.TenantManager, queue models.Queue, cfg config.ServerCommand) {
	dashboardServer := dashboard.NewDashboard(queue, tm, cfg.Dashboard)
	go func() {
		dashboardServer.Start()
	}()

	sqsServer := sqs.NewSQS(queue, tm, cfg.SQS)
	go func() {
		sqsServer.Start()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c // This blocks the main thread until an interrupt is received
	fmt.Println("Gracefully shutting down...")

	dashboardServer.Stop()
	sqsServer.Stop()
	queue.Shutdown()
}
