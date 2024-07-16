package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/poundifdef/smoothmq/cmd/smoothmq"
	"github.com/poundifdef/smoothmq/cmd/tester"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/poundifdef/smoothmq/queue/sqlite"
	"github.com/poundifdef/smoothmq/tenants/defaultmanager"

	"github.com/rs/zerolog"
	zl "github.com/rs/zerolog/log"
)

func Run(command string, cfg *config.CLI, tenantManager models.TenantManager, queue models.Queue) {
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	// TODO: read log level and format from config
	zl.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Caller().Logger().Level(zerolog.TraceLevel)

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
