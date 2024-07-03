package config

import (
	"github.com/alecthomas/kong"
)

type CLI struct {
	Queue  struct{} `cmd:"queue" help:"Run queue"`
	Tester struct{} `cmd:"tester" help:"Run tester"`

	ConfigFile string `help:"Configuration file" name:"config" type:"path"`
	// Config `embed:"" envprefix:"TESTER_"`
}

type Config struct {
	// Log LogConfig `envprefix:"LOG_" prefix:"log-" embed:""`
	// Config string    `help:"Configuration file" name:"config" type:"yamlfile"`
	// Config string    `help:"Configuration file" name:"config" type:"path"`
}

type TesterConfig struct {
	SqsEndpoint string `help:"SQS endpoint" default:"http://localhost:3001" env:"SQS_ENDPOINT"`
	Senders     int    `help:"" default:"0"`
	Receivers   int    `help:"" default:"0"`
	Messages    int    `help:"" default:"0"`
}

type QueueConfig struct {
	SQS       SQSConfig       `embed:"" envprefix:"Q_SQS_" prefix:"sqs-"`
	Dashboard DashboardConfig `embed:"" envprefix:"Q_DASHBOARD_" prefix:"dashboard-"`
}

type LogConfig struct {
	Pretty bool   `envprefix:"PRETTY" help:"Pretty-print logs" prefix:"pretty"`
	Level  string `env:"LEVEL" default:"info" enum:"trace,info,debug,error" name:"level"`
}

type SQSConfig struct {
	Enabled bool     `help:"Enable SQS server" default:"true" env:"ENABLED"`
	Port    int      `help:"SQS server port" default:"3001" env:"PORT"`
	Keys    []AWSKey `help:"keys" kong:"-" env:""`
}

type AWSKey struct {
	AccessKey string `name:"access_key"`
	SecretKey string `name:"secret_key"`
}

type DashboardConfig struct {
	Enabled bool `help:"Enable web dashboard" default:"true" env:"ENABLED"`
	Port    int  `help:"Dashboard HTTP port" default:"3000" env:"PORT"`
}

func Load() (string, *CLI, error) {
	cli := &CLI{}
	c := kong.Parse(cli) // kong.NamedMapper("yamlfile", kongyaml.YAMLFileMapper),

	// return c, c.Error
	// kong.Configuration(kongyaml.Loader, "--config"),
	// kong.Bind(&cli.Config)

	// if c.Error != nil {
	// 	return "", nil, c.Error
	// }

	// log.Println(cli)

	// var k = koanf.New(".")
	// log.Println(k.Load(file.Provider("config.yaml"), yaml.Parser()))
	// log.Println(k.All())

	// for _, s := range k.Slices("aws.keys") {
	// 	log.Println(s.String("k"))
	// }

	// k.Load(env.Provider("MQ_", ".", func(s string) string {
	// 	return strings.Replace(strings.ToLower(
	// 		strings.TrimPrefix(s, "MYVAR_")), "_", ".", -1)
	// }), nil)

	// log.Println(k.Bool("aws.enabled"))

	// log.Println(k.Keys())

	// koanf.UnmarshalConf{Tag: "koanf"}

	return c.Command(), cli, c.Error
}
