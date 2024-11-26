package database

import (
	"os"
	"strings"
	"time"

	"github.com/poundifdef/smoothmq/config"

	"github.com/rs/zerolog/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	_ "github.com/mattn/go-sqlite3"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var queueDiskSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "queue_disk_size",
		Help: "Size of queue data on disk",
	},
)

func NewSQLiteQueue(cfg config.ServerCommand, dbq *DBQueue, gcfg *gorm.Config) {
	var err error
	var path string
	var args string

	if cfg.DB.DSN == "" {
		path = cfg.SQLite.Path
		args = "?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full"
	} else {
		pieces := strings.Split(cfg.DB.DSN, "?")
		path = pieces[0]
		if len(pieces) == 2 {
			args = "?" + pieces[1]
		}
	}

	dbq.Filename = path

	dbq.DBG, err = gorm.Open(sqlite.Open(path+args), gcfg)

	if err != nil {
		log.Fatal().Err(err).Send()
	}

	dbq.ticker = time.NewTicker(1 * time.Second)

	dbq.Migrate()

	go func() {
		for {
			select {
			case <-dbq.ticker.C:
				stat, err := os.Stat(dbq.Filename)
				if err == nil {
					queueDiskSize.Set(float64(stat.Size()))
				}
			}
		}
	}()
}
