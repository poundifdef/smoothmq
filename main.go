package main

import (
	"fmt"
	"log"
	"q/cmd/smoothmq"
	"q/cmd/tester"
	"q/config"
	"q/models"
	"q/queue/sqlite"
	"q/tenants/defaultmanager"
)

func Run(command string, cfg *config.CLI, tenantManager models.TenantManager, queue models.Queue) {
	if tenantManager == nil {
		tenantManager = defaultmanager.NewDefaultTenantManager(cfg.Server.SQS.Keys)
	}

	if queue == nil {
		queue = sqlite.NewSQLiteQueue(cfg.Server.SQLite)
	}

	fmt.Println()

	switch command {
	case "tester":
		tester.Run(cfg.Tester.Senders, cfg.Tester.Receivers, cfg.Tester.Messages, cfg.Tester.SqsEndpoint)
	default:
		smoothmq.Run(tenantManager, queue, cfg.Server)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	command, cli, err := config.Load()
	if err != nil {
		panic(err)
	}

	Run(command, cli, nil, nil)
}
