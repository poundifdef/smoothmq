package main

import (
	"q/dashboard"
	"q/models"
	"q/queue/sqlite"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}

func Run(tm models.TenantManager) {
	queue := sqlite.NewSQLiteQueue()
	dashboardServer := dashboard.NewDashboard(queue, tm)

	dashboardServer.Start()
	// new queue with tenant manager
	// new sqs, with queue implementation
}

func main() {
	tenantManager := NewDefaultTenantManager()
	Run(tenantManager)
}
