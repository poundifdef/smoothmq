package smoothmq

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/poundifdef/smoothmq/cmd/smoothmq/server"
	"github.com/poundifdef/smoothmq/cmd/smoothmq/tester"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Run(command string, cfg *config.CLI, tenantManager models.TenantManager, queue models.Queue) {
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	// TODO: read log level and format from config
	logLevel, _ := zerolog.ParseLevel(cfg.Log.Level)
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Caller().Logger().Level(logLevel)
	if cfg.Log.Pretty {
		log.Logger = log.Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	switch command {
	case "tester":
		tester.Run(cfg.Tester)
	default:
		server.Run(tenantManager, queue, cfg.Server)
	}
}
