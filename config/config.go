package config

import (
	"github.com/alecthomas/kong"
)

type CLI struct {
	Server struct{} `cmd:"server" help:"Run queue server"`
	Tester struct {
		SqsEndpoint string `help:"SQS endpoint" name:"endpoint" default:"http://localhost:3001" env:"SQS_ENDPOINT"`
		Senders     int    `help:"" default:"0"`
		Receivers   int    `help:"" default:"0"`
		Messages    int    `help:"" default:"0"`
	} `cmd:"tester" help:"Run tester"`

	ConfigFile string `help:"Configuration file" name:"config" type:"path"`
}

type Config struct {
	Log    LogConfig    `koanf:"log"`
	Server ServerConfig `koanf:"server"`
}

type LogConfig struct {
	Pretty bool   `koanf:"pretty"`
	Level  string `koanf:"level"`
}

type ServerConfig struct {
	SQS       SQSConfig       `koanf:"sqs"`
	Dashboard DashboardConfig `koanf:"dashboard"`
}

type SQSConfig struct {
	Enabled bool     `koanf:"enabled"`
	Port    int      `koanf:"port"`
	Keys    []AWSKey `koanf:"keys"`
}

type AWSKey struct {
	AccessKey string `koanf:"access_key"`
	SecretKey string `koanf:"secret_key"`
}

type DashboardConfig struct {
	Enabled bool `koanf:"enabled"`
	Port    int  `koanf:"port"`
}

func Load() (string, *CLI, error) {
	cli := &CLI{}
	c := kong.Parse(cli)

	return c.Command(), cli, c.Error
}
