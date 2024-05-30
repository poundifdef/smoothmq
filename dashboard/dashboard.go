package dashboard

import (
	"q/models"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
)

type Dashboard struct {
	app           *fiber.App
	queue         models.Queue
	tenantManager models.TenantManager
}

func NewDashboard(queue models.Queue, tenantManager models.TenantManager) *Dashboard {
	engine := html.New("./dashboard/views", ".html")
	engine.Reload(true)
	engine.Debug(true)

	app := fiber.New(fiber.Config{
		Views: engine,
	})

	d := &Dashboard{
		app:           app,
		queue:         queue,
		tenantManager: tenantManager,
	}

	app.Get("/", d.Queues)
	app.Post("/queues", d.NewQueue)
	app.Get("/queues/:queue", d.Queue)
	app.Post("/queues/:queue/delete", d.Queue)
	app.Get("/queues/:queue/messages/:message", d.Message)

	return d
}

func (d *Dashboard) Start() error {
	return d.app.Listen(":3000")
}

func (d *Dashboard) Stop() error {
	return d.app.Shutdown()
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

	filterCriteria := models.FilterCriteria{}
	filteredMessageIDs := d.queue.Filter(tenantId, queueName, filterCriteria)

	messages := make([]*models.Message, len(filteredMessageIDs))
	for i, messageId := range filteredMessageIDs {
		messages[i] = d.queue.Peek(tenantId, messageId)
	}

	return c.Render("queue", fiber.Map{"Queue": queueName, "Stats": queueStats, "Messages": messages, "Filter": filterCriteria}, "layout")
}

func (d *Dashboard) Message(c *fiber.Ctx) error {
	queueName := c.Params("queue")
	messageID := c.Params("message")
	tenantId := d.tenantManager.GetTenant()

	// TODO: check for errors
	messageIdInt, _ := strconv.ParseInt(messageID, 10, 64)

	message := d.queue.Peek(tenantId, messageIdInt)

	return c.Render("message", fiber.Map{"Queue": queueName, "Message": message}, "layout")
}

func (d *Dashboard) NewQueue(c *fiber.Ctx) error {
	queueName := c.FormValue("queue")

	tenantId := d.tenantManager.GetTenant()
	err := d.queue.CreateQueue(tenantId, queueName)

	if err != nil {
		return c.Status(fiber.ErrBadRequest.Code).SendString(err.Error())
	}

	return c.Redirect("/")
}
