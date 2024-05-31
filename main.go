package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"q/dashboard"
	"q/models"
	"q/protocols/sqs"
	"q/queue/sqlite"
	"syscall"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetTenantFromAWSRequest(r *http.Request) int64 {
	return 1
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}

func Run(tm models.TenantManager) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	queue := sqlite.NewSQLiteQueue()

	dashboardServer := dashboard.NewDashboard(queue, tm)
	go func() {
		dashboardServer.Start()
	}()

	sqsServer := sqs.NewSQS(queue, tm)
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

func main() {
	tenantManager := NewDefaultTenantManager()
	Run(tenantManager)
}
