package dashboard

import (
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"

	"github.com/rs/zerolog/log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/template/html/v2"
)

//go:embed views/*
var viewsfs embed.FS

type Dashboard struct {
	app           *fiber.App
	queue         models.Queue
	tenantManager models.TenantManager

	cfg config.DashboardConfig
}

func NewDashboard(queue models.Queue, tenantManager models.TenantManager, cfg config.DashboardConfig) *Dashboard {
	var engine *html.Engine

	if cfg.Dev {
		engine = html.New("./dashboard/views", ".html")
		engine.Reload(true)
		engine.Debug(true)
	} else {
		http.FS(viewsfs)
		fs2, err := fs.Sub(viewsfs, "views")
		if err != nil {
			log.Fatal().Err(err).Send()
		}
		engine = html.NewFileSystem(http.FS(fs2), ".html")
	}

	engine.AddFunc("b64Json", func(s []byte) string {
		decodedStr, err := base64.StdEncoding.DecodeString(string(s))
		if err == nil {
			if json.Valid(decodedStr) {
				return string(decodedStr)
			}
		}

		return string(s)
	})

	app := fiber.New(fiber.Config{
		Views:                 engine,
		DisableStartupMessage: true,
	})

	if cfg.User != "" && cfg.Pass != "" {
		app.Use(basicauth.New(basicauth.Config{
			Users: map[string]string{
				cfg.User: cfg.Pass,
			},
		}))
	}

	d := &Dashboard{
		app:           app,
		queue:         queue,
		tenantManager: tenantManager,
		cfg:           cfg,
	}

	app.Get("/", d.Queues)
	app.Post("/queues", d.NewQueue)
	app.Get("/queues/:queue", d.Queue)
	app.Post("/queues/:queue/delete", d.DeleteQueue)
	app.Get("/queues/:queue/messages/:message", d.Message)

	return d
}

func (d *Dashboard) Start() error {
	if !d.cfg.Enabled {
		return nil
	}

	fmt.Printf("Dashboard: http://localhost:%d\n", d.cfg.Port)
	return d.app.Listen(fmt.Sprintf(":%d", d.cfg.Port))
}

func (d *Dashboard) Stop() error {
	if d.cfg.Enabled {
		return d.app.Shutdown()
	}

	return nil
}

func (d *Dashboard) Queues(c *fiber.Ctx) error {
	tenantId := d.tenantManager.GetTenant()

	type QueueDetails struct {
		Name  string
		Stats models.QueueStats
		Count int
	}

	queues, err := d.queue.ListQueues(tenantId)

	queueDetails := make([]QueueDetails, len(queues))
	for i, queue := range queues {
		queueStats := d.queue.Stats(tenantId, queue)

		totalMessages := 0
		for _, v := range queueStats.Counts {
			totalMessages += v
		}

		queueDetails[i] = QueueDetails{
			Name:  queue,
			Stats: queueStats,
			Count: totalMessages,
		}
	}

	return c.Render("queues", fiber.Map{"Queues": queueDetails, "Err": err}, "layout")
}

func (d *Dashboard) Queue(c *fiber.Ctx) error {
	queueName := c.Params("queue")

	tenantId := d.tenantManager.GetTenant()
	queueStats := d.queue.Stats(tenantId, queueName)

	filterCriteria := models.FilterCriteria{
		KV: make(map[string]string),
	}
	filterString := c.Query("filter")

	filterFields := strings.Fields(filterString)
	for _, field := range filterFields {
		maybeMessageID, err := strconv.ParseInt(field, 10, 64)
		if err == nil {
			filterCriteria.MessageID = maybeMessageID
		}

		if strings.Contains(field, "=") {
			tokens := strings.Split(field, "=")
			filterCriteria.KV[strings.TrimSpace(tokens[0])] = strings.TrimSpace(tokens[1])
		}

	}

	filteredMessageIDs := d.queue.Filter(tenantId, queueName, filterCriteria)

	messages := make([]*models.Message, 0)
	for _, messageId := range filteredMessageIDs {
		message := d.queue.Peek(tenantId, queueName, messageId)
		if message != nil {
			messages = append(messages, message)
		}
	}

	return c.Render("queue", fiber.Map{"Queue": queueName, "Stats": queueStats, "Messages": messages, "Filter": filterString}, "layout")
}

func (d *Dashboard) Message(c *fiber.Ctx) error {
	queueName := c.Params("queue")
	messageID := c.Params("message")
	tenantId := d.tenantManager.GetTenant()

	// TODO: check for errors
	messageIdInt, err := strconv.ParseInt(messageID, 10, 64)
	if err != nil {
		return err
	}

	message := d.queue.Peek(tenantId, queueName, messageIdInt)
	if message == nil {
		return errors.New("Message not found")
	}

	return c.Render("message", fiber.Map{"Queue": queueName, "Message": message}, "layout")
}

func (d *Dashboard) NewQueue(c *fiber.Ctx) error {
	queueName := c.FormValue("queue")

	tenantId := d.tenantManager.GetTenant()
	err := d.queue.CreateQueue(tenantId, queueName)

	if err != nil {
		return err
	}

	return c.Redirect("/")
}

func (d *Dashboard) DeleteQueue(c *fiber.Ctx) error {
	queueName := c.Params("queue")
	tenantId := d.tenantManager.GetTenant()

	err := d.queue.DeleteQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	return c.Redirect("/")
}
