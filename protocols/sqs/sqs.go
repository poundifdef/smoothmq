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
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/tidwall/gjson"
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

	app.Use(s.authMiddleware)
	app.Post("/*", s.Action)

	return s
}

func (s *SQS) authMiddleware(c *fiber.Ctx) error {
	r, err := adaptor.ConvertRequest(c, false)
	if err != nil {
		return err
	}

	awsHeader, err := ParseAuthorizationHeader(r)
	if err != nil {
		return err
	}

	queueUrl := gjson.GetBytes(c.Body(), "QueueUrl")
	queueTokens := strings.Split(queueUrl.Str, "/")
	tenantIdStr := queueTokens[len(queueTokens)-2]
	tenantId, err := strconv.ParseInt(tenantIdStr, 10, 64)
	if err != nil {
		return err
	}

	secretKey, err := s.tenantManager.GetAWSSecretKey(tenantId, awsHeader.AccessKey, awsHeader.Region)
	if err != nil {
		return err
	}

	err = ValidateAWSRequest(awsHeader, secretKey, r)
	if err != nil {
		return err
	}

	c.Locals("tenantId", tenantId)
	return c.Next()
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

	tenantId := c.Locals("tenantId").(int64)

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

	kv := make(map[string]string)

	messageId, err := s.queue.Enqueue(tenantId, queue, req.MessageBody, kv)
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
