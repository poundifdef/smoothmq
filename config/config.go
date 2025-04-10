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
	Log    LogConfig       `embed:"" prefix:"log-" name:"log" envprefix:"LOG_"`
}

type TesterCommand struct {
	SqsEndpoint  string `help:"SQS endpoint" name:"endpoint" default:"http://localhost:3001"`
	Senders      int    `help:"" default:"0"`
	Receivers    int    `help:"" default:"0"`
	Messages     int    `help:"" default:"0"`
	BatchSize    int    `help:"" default:"1"`
	DelaySeconds int    `help:"" default:"0"`
	AccessKey    string `help:"" default:"DEV_ACCESS_KEY_ID"`
	SecretKey    string `help:"" default:"DEV_SECRET_ACCESS_KEY"`
}

type ServerCommand struct {
	SQS       SQSConfig       `embed:"" prefix:"sqs-" envprefix:"Q_SQS_"`
	Dashboard DashboardConfig `embed:"" prefix:"dashboard-" envprefix:"Q_DASHBOARD_"`
	SQLite    SQLiteConfig    `embed:"" prefix:"sqlite-" envprefix:"Q_SQLITE_"`
	Metrics   MetricsConfig   `embed:"" prefix:"metrics-" name:"metrics" envprefix:"Q_METRICS_"`

	DisableTelemetry bool   `name:"disable-telemetry" default:"false" env:"DISABLE_TELEMETRY"`
	UseSinglePort    bool   `name:"use-single-port" default:"false" env:"Q_SERVER_USE_SINGLE_PORT" help:"Enables having all HTTP services run on a single port with different endpoints"`
	Port             int    `name:"port" default:"8080" env:"PORT" help:"If use-single-port is enabled, this is the port number for the server"`
	Host             string `name:"host" env:"HOST" help:"If use-single-port is enabled, this is the hostname or ip address for the server"`
}

type LogConfig struct {
	Pretty bool   `name:"pretty" default:"true" env:"PRETTY"`
	Level  string `name:"level" enum:"trace,debug,info,warn,error,fatal,panic" default:"info" help:"Log level" env:"LEVEL"`
}

type MetricsConfig struct {
	PrometheusEnabled bool   `name:"prometheus-enabled" default:"true" env:"PROMETHEUS_ENABLED"`
	PrometheusHost    string `name:"prometheus-host" default:"localhost" env:"PROMETHEUS_HOST"`
	PrometheusPort    int    `name:"prometheus-port" default:"2112" env:"PROMETHEUS_PORT"`
	PrometheusPath    string `name:"prometheus-path" default:"/metrics" env:"PROMETHEUS_PATH"`
}

type SQLiteConfig struct {
	Path string `name:"path" help:"Path of SQLite file" default:"smoothmq.sqlite" env:"PATH"`
}

type SQSConfig struct {
	Enabled          bool     `name:"enabled" default:"true" help:"Enable SQS protocol for queue" env:"ENABLED"`
	Host             string   `name:"host" default:"localhost" help:"hostname or ip address for SQS protocol" env:"HOST"`
	Port             int      `name:"port" default:"3001" help:"HTTP port for SQS protocol" env:"PORT"`
	Keys             []AWSKey `name:"keys" default:"DEV_ACCESS_KEY_ID:DEV_SECRET_ACCESS_KEY" env:"KEYS"`
	ParseCelery      bool     `name:"parse-celery" default:"true" env:"PARSE_CELERY" help:"Parse Celery messages. Lets you search by celery message ID and task type."`
	MaxRequestSize   int      `name:"max-request-size" default:"1048576" env:"MAX_REQUEST_SIZE" help:"Max size of SQS request in bytes"`
	MaxDelaySeconds  int      `name:"max-delay-seconds" default:"30" env:"MAX_DELAY_SECONDS" help:"Max allowed wait time for long polling"`
	DelayRetryMillis int      `name:"delay-retry-millis" default:"1000" env:"DELAY_RETRY_MILLIS" help:"When long polling, how often to request new items"`
	EndpointOverride string   `name:"endpoint-override" default:"" env:"ENDPOINT_OVERRIDE" help:"Endpoint to advertise in queue URLs. Defaults to HTTP hostname."`
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
	Enabled bool   `name:"enabled" help:"Enable web dashboard" default:"true" env:"ENABLED"`
	Host    string `name:"host" help:"hostname or ip address for dashboard" default:"localhost" env:"HOST"`
	Port    int    `name:"port" help:"HTTP port for dashboard" default:"3000" env:"PORT"`
	Dev     bool   `name:"dev" help:"Run dashboard in dev mode, refresh templates from local" default:"false" env:"DEV"`
	User    string `name:"user" help:"Username for auth" default:"" env:"USER"`
	Pass    string `name:"pass" help:"Pass for auth" default:"" env:"PASS"`
}

func Load() (string, *CLI, error) {
	cli := &CLI{}
	c := kong.Parse(cli, kong.Configuration(kongyaml.Loader))

	return c.Command(), cli, c.Error
}
