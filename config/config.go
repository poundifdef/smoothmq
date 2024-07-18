package config

import (
	"errors"
	"strings"

	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
)

type CLI struct {
	Server ServerCommand `cmd:"server" help:"Run queue server"`
	Tester TesterCommand `cmd:"tester" help:"Run queue test tool"`

	Config kong.ConfigFlag `name:"config" help:"Configuration file"`
	Log    LogConfig       `embed:"" prefix:"log-" name:"log"`
}

type TesterCommand struct {
	SqsEndpoint string `help:"SQS endpoint" name:"endpoint" default:"http://localhost:3001"`
	Senders     int    `help:"" default:"0"`
	Receivers   int    `help:"" default:"0"`
	Messages    int    `help:"" default:"0"`
	BatchSize   int    `help:"" default:"1"`
}

type ServerCommand struct {
	SQS       SQSConfig       `embed:"" prefix:"sqs-" envprefix:"Q_SQS_"`
	Dashboard DashboardConfig `embed:"" prefix:"dashboard-" envprefix:"Q_DASHBOARD_"`
	SQLite    SQLiteConfig    `embed:"" prefix:"sqlite-" envprefix:"Q_SQLITE_"`
}

type LogConfig struct {
	Pretty bool   `name:"pretty" default:"true"`
	Level  string `name:"level" enum:"trace,debug,info,warn,error,fatal,panic" default:"debug" help:"Log level"`
}

type SQLiteConfig struct {
	Path string `name:"path" help:"Path of SQLite file" default:"smoothmq.sqlite" env:"PATH"`
}

type SQSConfig struct {
	Enabled     bool     `name:"enabled" default:"true" help:"Enable SQS protocol for queue" env:"ENABLED"`
	Port        int      `name:"port" default:"3001" help:"HTTP port for SQS protocol" env:"PORT"`
	Keys        []AWSKey `name:"keys" default:"DEV_ACCESS_KEY_ID:DEV_SECRET_ACCESS_KEY" env:"KEYS"`
	ParseCelery bool     `name:"parse-celery" default:"true" env:"PARSE_CELERY" help:"Parse Celery messages. Lets you search by celery message ID and task type."`
}

type AWSKey struct {
	AccessKey string `name:"accesskey"`
	SecretKey string `name:"secretkey"`
}

func (k *AWSKey) Decode(ctx *kong.DecodeContext) error {
	var val string

	err := ctx.Scan.PopValueInto("string", &val)
	if err != nil {
		return err
	}

	tokens := strings.Split(val, ":")
	if len(tokens) != 2 {
		return errors.New("AWS SQS key should be of the form access_key:secret_key")
	}

	k.AccessKey = tokens[0]
	k.SecretKey = tokens[1]

	return nil
}

type DashboardConfig struct {
	Enabled bool `name:"enabled" help:"Enable web dashboard" default:"true" env:"ENABLED"`
	Port    int  `name:"port" help:"HTTP port for dashboard" default:"3000" env:"PORT"`
	Dev     bool `name:"dev" help:"Run dashboard in dev mode, refresh templates from local" default:"false" env:"DEV"`
}

func Load() (string, *CLI, error) {
	cli := &CLI{}
	c := kong.Parse(cli, kong.Configuration(kongyaml.Loader))

	return c.Command(), cli, c.Error
}
