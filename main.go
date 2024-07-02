package main

import (
	"flag"
	"fmt"
	"log"
	"q/cmd/smoothmq"
	"q/cmd/tester"
	"q/models"
	"q/queue/sqlite"
	"q/tenants/defaultmanager"
)

func Run(tm models.TenantManager, queue models.Queue) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var runTester bool
	var numSenders, numReceivers, numMessagesPerGoroutine int
	var endpoint string

	flag.BoolVar(&runTester, "tester", false, "Run in test mode")

	flag.IntVar(&numSenders, "senders", 0, "Number of send goroutines")
	flag.IntVar(&numMessagesPerGoroutine, "messages", 1, "Number of messages to send per goroutine")
	flag.IntVar(&numReceivers, "receivers", 0, "Number of receive goroutines")
	flag.StringVar(&endpoint, "endpoint", "http://localhost:3001", "SQS endpoint for testing")

	flag.Parse()

	fmt.Println()

	if runTester {
		tester.Run(numSenders, numReceivers, numMessagesPerGoroutine, endpoint)
	} else {
		smoothmq.Run(tm, queue)
	}
}

func main() {
	tenantManager := defaultmanager.NewDefaultTenantManager()
	queue := sqlite.NewSQLiteQueue()

	Run(tenantManager, queue)
}
