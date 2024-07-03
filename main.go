package main

import (
	"log"
	"q/config"
)

// func Run(command string, config *config.CLI, tenantManager models.TenantManager, queue models.Queue) {
// 	if tenantManager == nil {
// 		tenantManager = defaultmanager.NewDefaultTenantManager()
// 	}

// 	if queue == nil {
// 		queue = sqlite.NewSQLiteQueue()
// 	}

// 	fmt.Println()

// 	switch command {
// 	case "tester":
// 		tester.Run(config.Tester.Senders, config.Tester.Receivers, config.Tester.Messages, config.Tester.SqsEndpoint)
// 	default:
// 		smoothmq.Run(tenantManager, queue, config.Queue)
// 	}
// }

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	command, cli, err := config.Load()
	if err != nil {
		panic(err)
	}

	return

	// Run(command, config, nil, nil)
}
