package main

import (
	"fmt"
	"log"
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

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	return int64(1), "YOUR_SECRET_ACCESS_KEY", nil
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}

func Run(tm models.TenantManager, queue models.Queue) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	queue := sqlite.NewSQLiteQueue()
	Run(tenantManager, queue)
}
