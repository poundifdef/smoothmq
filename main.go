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

	_ "embed"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

//go:embed config.yaml
var configData []byte

func Run(command string, cli *config.CLI, cfg *config.Config, tenantManager models.TenantManager, queue models.Queue) {
	if tenantManager == nil {
		tenantManager = defaultmanager.NewDefaultTenantManager(cfg.Server.SQS.Keys)
	}

	if queue == nil {
		queue = sqlite.NewSQLiteQueue()
	}

	fmt.Println()

	switch command {
	case "tester":
		tester.Run(cli.Tester.Senders, cli.Tester.Receivers, cli.Tester.Messages, cli.Tester.SqsEndpoint)
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

	k := koanf.New(".")
	cfg := &config.Config{}
	parser := yaml.Parser()
	var provider koanf.Provider

	if cli.ConfigFile != "" {
		provider = file.Provider(cli.ConfigFile)
	} else {
		provider = rawbytes.Provider(configData)
	}

	err = k.Load(provider, parser)
	if err != nil {
		panic(err)
	}

	err = k.Unmarshal("", cfg)
	if err != nil {
		panic(err)
	}

	Run(command, cli, cfg, nil, nil)
}
