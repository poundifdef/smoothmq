package main

import (
	"log"

	"github.com/poundifdef/smoothmq/cmd/smoothmq"
	"github.com/poundifdef/smoothmq/config"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	command, cli, err := config.Load()
	if err != nil {
		panic(err)
	}

	smoothmq.Run(command, cli, nil, nil)
}
