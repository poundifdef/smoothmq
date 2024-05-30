package sqs

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"q/models"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

type SQS struct {
	app           *fiber.App
	queue         models.Queue
	tenantManager models.TenantManager
}

func NewSQS(queue models.Queue, tenantManager models.TenantManager) *SQS {
	app := fiber.New(fiber.Config{})

	s := &SQS{
		app:           app,
		queue:         queue,
		tenantManager: tenantManager,
	}

	app.All("/*", s.Action)

	return s
}

func (s *SQS) Start() error {
	return s.app.Listen(":3001")
}

func (s *SQS) Stop() error {
	return s.app.Shutdown()
}

func (s *SQS) Action(c *fiber.Ctx) error {
	awsMethodHeader, ok := c.GetReqHeaders()["X-Amz-Target"]
	if !ok {
		return errors.New("X-Amz-Target header not found")
	}
	awsMethod := awsMethodHeader[0]

	var r *http.Request = &http.Request{}
	fasthttpadaptor.ConvertRequest(c.Context(), r, false)

	tenantId := s.tenantManager.GetTenantFromAWSRequest(r)

	switch awsMethod {
	case "AmazonSQS.SendMessage":
		return s.SendMessage(c, tenantId)
	case "AmazonSQS.ReceiveMessage":
		return s.ReceiveMessage(c, tenantId)
	case "AmazonSQS.DeleteMessage":
		return s.DeleteMessage(c, tenantId)
	default:
		return fmt.Errorf("SQS method %s not implemented", awsMethod)
	}

}

func (s *SQS) SendMessage(c *fiber.Ctx, tenantId int64) error {
	// TODO: DelaySeconds
	// TODO: MessageAttributes

	req := &SendMessagePayload{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	messageId, err := s.queue.Enqueue(tenantId, queue, req.MessageBody)
	if err != nil {
		return err
	}

	response := SendMessageResponse{
		MessageId: fmt.Sprintf("%d", messageId),
	}

	return c.JSON(response)
}

func (s *SQS) ReceiveMessage(c *fiber.Ctx, tenantId int64) error {
	req := &ReceiveMessageRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	messages, err := s.queue.Dequeue(tenantId, queue, req.MaxNumberOfMessages)
	if err != nil {
		return err
	}

	response := ReceiveMessageResponse{
		Messages: make([]Message, len(messages)),
	}

	for i, message := range messages {
		response.Messages[i] = Message{
			MessageId:     fmt.Sprintf("%d", message.ID),
			ReceiptHandle: fmt.Sprintf("%d", message.ID),
			Body:          string(message.Message),
		}
	}

	return c.JSON(response)
}

func (s *SQS) DeleteMessage(c *fiber.Ctx, tenantId int64) error {
	req := &DeleteMessageRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	messageId, err := strconv.ParseInt(req.ReceiptHandle, 10, 64)
	if err != nil {
		return err
	}

	err = s.queue.Delete(tenantId, queue, messageId)
	if err != nil {
		return err
	}

	return nil
}
