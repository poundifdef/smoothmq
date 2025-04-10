package sqs

/*
Docs:
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
https://docs.aws.amazon.com/cli/latest/reference/sqs/delete-message.html

Testing:
AWS_ACCESS_KEY_ID=DEV_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=DEV_SECRET_ACCESS_KEY aws sqs ...
aws sqs list-queues --endpoint-url http://localhost:3001
aws sqs send-message --queue-url https://sqs.us-east-1.amazonaws.com/1/a --message-body "hello world" --endpoint-url http://localhost:3001
aws sqs receive-message --queue-url https://sqs.us-east-1.amazonaws.com/1/a --endpoint-url http://localhost:3001
aws sqs delete-message --receipt-handle x --queue-url https://sqs.us-east-1.amazonaws.com/1/a --endpoint-url http://localhost:3001
aws sqs create-queue --queue-name b --endpoint-url http://localhost:3001
aws sqs get-queue-attributes --queue-url https://sqs.us-east-1.amazonaws.com/1/a --endpoint-url http://localhost:3001
aws sqs get-queue-url --debug --queue-name test-queue --endpoint-url http://localhost:3001
*/

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"net/http"
	"strconv"
	"strings"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"

	"github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

type SQS struct {
	App           *fiber.App
	queue         models.Queue
	tenantManager models.TenantManager

	cfg config.SQSConfig
}

var requestLatency = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "sqs_request_latency",
		Help:    "Latency of SQS requests",
		Buckets: prometheus.ExponentialBucketsRange(0.05, 1, 5),
	},
	[]string{"tenant_id", "aws_method"},
)

var requestStatus = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "sqs_request_status",
		Help: "Status SQS requests",
	},
	[]string{"tenant_id", "aws_method", "status"},
)

func NewSQS(queue models.Queue, tenantManager models.TenantManager, cfg config.SQSConfig) *SQS {
	s := &SQS{
		queue:         queue,
		tenantManager: tenantManager,
		cfg:           cfg,
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ErrorHandler:          s.errorHandler,
		BodyLimit:             cfg.MaxRequestSize,
	})

	app.Use(fiberzerolog.New(fiberzerolog.Config{
		Logger: &log.Logger,
		Levels: []zerolog.Level{zerolog.ErrorLevel, zerolog.WarnLevel, zerolog.TraceLevel},
	}))

	app.Use(s.authMiddleware)
	app.Post("/*", s.Action)

	s.App = app

	return s
}

func (s *SQS) errorHandler(c *fiber.Ctx, err error) error {
	sqsErr, ok := err.(*SQSError)
	if !ok {
		sqsErr = NewSQSError(500, "InternalFailure", err.Error())
	}

	return c.Status(sqsErr.Code).JSON(sqsErr)
}

func (s *SQS) authMiddleware(c *fiber.Ctx) error {
	r, err := adaptor.ConvertRequest(c, false)
	if err != nil {
		return ErrIncompleteSignature
	}

	awsHeader, err := ParseAuthorizationHeader(r)
	if err != nil {
		return ErrIncompleteSignature
	}

	tenantId, secretKey, err := s.tenantManager.GetAWSSecretKey(awsHeader.AccessKey, awsHeader.Region)
	if err != nil {
		return ErrInvalidClientTokenId
	}

	err = ValidateAWSRequest(awsHeader, secretKey, r)
	if err != nil {
		return ErrIncompleteSignature
	}

	c.Locals("tenantId", tenantId)
	return c.Next()
}

func (s *SQS) Start() error {
	if !s.cfg.Enabled {
		return nil
	}

	fmt.Printf("SQS Endpoint: http://%s:%d\n", s.cfg.Host, s.cfg.Port)
	return s.App.Listen(fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port))
}

func (s *SQS) Stop() error {
	if s.cfg.Enabled {
		return s.App.Shutdown()
	}
	return nil
}

func (s *SQS) Action(c *fiber.Ctx) error {
	log.Trace().Interface("headers", c.GetReqHeaders()).Bytes("body", c.Body()).Send()
	start := time.Now()

	awsMethodHeader, ok := c.GetReqHeaders()["X-Amz-Target"]
	if !ok {
		return errors.New("X-Amz-Target header not found")
	}
	awsMethod := awsMethodHeader[0]

	var r *http.Request = &http.Request{}
	fasthttpadaptor.ConvertRequest(c.Context(), r, false)

	tenantId := c.Locals("tenantId").(int64)

	defer func() {
		requestLatency.WithLabelValues(fmt.Sprintf("%d", tenantId), utils.CopyString(awsMethod)).Observe(time.Since(start).Seconds())
	}()

	var rc error
	switch awsMethod {
	case "AmazonSQS.SendMessage":
		rc = s.SendMessage(c, tenantId)
	case "AmazonSQS.SendMessageBatch":
		rc = s.SendMessageBatch(c, tenantId)
	case "AmazonSQS.ReceiveMessage":
		rc = s.ReceiveMessage(c, tenantId)
	case "AmazonSQS.DeleteMessage":
		rc = s.DeleteMessage(c, tenantId)
	case "AmazonSQS.ListQueues":
		rc = s.ListQueues(c, tenantId)
	case "AmazonSQS.GetQueueUrl":
		rc = s.GetQueueURL(c, tenantId)
	case "AmazonSQS.CreateQueue":
		rc = s.CreateQueue(c, tenantId)
	case "AmazonSQS.GetQueueAttributes":
		rc = s.GetQueueAttributes(c, tenantId)
	case "AmazonSQS.PurgeQueue":
		rc = s.PurgeQueue(c, tenantId)
	case "AmazonSQS.ChangeMessageVisibility":
		rc = s.ChangeMessageVisibility(c, tenantId)
	default:
		rc = NewSQSError(400, "UnsupportedOperation", fmt.Sprintf("SQS method %s not implemented", awsMethod))
	}

	status := "ok"
	if rc != nil {
		status = "error"
	}
	requestStatus.WithLabelValues(fmt.Sprintf("%d", tenantId), utils.CopyString(awsMethod), status).Inc()

	return rc
}

func (s *SQS) PurgeQueue(c *fiber.Ctx, tenantId int64) error {
	req := &PurgeQueueRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	messages := s.queue.Filter(tenantId, queue, models.FilterCriteria{})
	for _, msg := range messages {
		s.queue.Delete(tenantId, queue, msg)
	}

	rc := PurgeQueueResponse{
		Success: true,
	}

	return c.JSON(rc)
}

func (s *SQS) GetQueueAttributes(c *fiber.Ctx, tenantId int64) error {
	req := &GetQueueAttributesRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	stats := s.queue.Stats(tenantId, queue)

	rc := GetQueueAttributesResponse{
		Attributes: map[string]string{
			"ApproximateNumberOfMessages": fmt.Sprintf("%d", stats.TotalMessages),
		},
	}

	return c.JSON(rc)
}

func (s *SQS) CreateQueue(c *fiber.Ctx, tenantId int64) error {
	req := &CreateQueueRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	if len(req.QueueName) > 80 {
		return ErrValidationError
	}

	regex, err := regexp.Compile(`^[a-zA-Z0-9-_]+$`)
	if err != nil {
		return err
	}

	if !regex.MatchString(req.QueueName) {
		return ErrValidationError
	}

	properties := models.QueueProperties{
		Name:              req.QueueName,
		RateLimit:         -1,
		MaxRetries:        -1,
		VisibilityTimeout: 30,
	}
	err = s.queue.CreateQueue(tenantId, properties)

	if errors.Is(err, models.ErrQueueExists) {
		return ErrQueueNameExists
	}

	if err != nil {
		return err
	}

	rc := CreateQueueResponse{
		QueueUrl: s.queueURL(c, tenantId, req.QueueName),
	}

	return c.JSON(rc)
}

func (s *SQS) queueURL(c *fiber.Ctx, tenantId int64, queue string) string {
	host := fmt.Sprintf("%s://%s", c.Protocol(), c.Hostname())

	if s.cfg.EndpointOverride != "" {
		host = s.cfg.EndpointOverride
	}

	return fmt.Sprintf("%s/%d/%s", host, tenantId, queue)

}
func (s *SQS) ListQueues(c *fiber.Ctx, tenantId int64) error {
	req := &ListQueuesRequest{}
	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	queues, err := s.queue.ListQueues(tenantId)
	if err != nil {
		return err
	}

	queueUrls := make([]string, 0)

	for _, queue := range queues {
		if req.QueueNamePrefix != "" && !strings.HasPrefix(queue, req.QueueNamePrefix) {
			continue
		}
		queueUrls = append(queueUrls, s.queueURL(c, tenantId, queue))
	}

	rc := ListQueuesResponse{
		QueueUrls: queueUrls,
	}

	return c.JSON(rc)
}

func (s *SQS) GetQueueURL(c *fiber.Ctx, tenantId int64) error {
	req := &GetQueueURLRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return ErrValidationError
	}

	queues, err := s.queue.ListQueues(tenantId)
	if err != nil {
		return err
	}

	for _, q := range queues {
		if q == req.QueueName {
			response := GetQueueURLResponse{
				QueueURL: s.queueURL(c, tenantId, q),
			}
			return c.JSON(response)
		}
	}

	return ErrQueueDoesNotExist
}

func (s *SQS) SendMessage(c *fiber.Ctx, tenantId int64) error {
	req := &SendMessagePayload{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	kv := make(map[string]string)
	for k, v := range req.MessageAttributes {
		kv[k+"_DataType"] = v.DataType
		if v.DataType == "String" {
			kv[k] = v.StringValue
		} else if v.DataType == "Number" {
			kv[k] = v.StringValue
		} else if v.DataType == "Binary" {
			kv[k] = v.BinaryValue
		}
	}

	// Try to parse celery task and ID
	if s.cfg.ParseCelery {
		// Is our message a JSON string?
		if strings.HasPrefix(req.MessageBody, "ey") {
			jsonStr, err := base64.StdEncoding.DecodeString(req.MessageBody)

			if err == nil {
				res := gjson.GetBytes(jsonStr, "headers.task")
				if res.Exists() {
					kv["celery_task"] = res.Str
				}

				res = gjson.GetBytes(jsonStr, "headers.id")
				if res.Exists() {
					kv["celery_id"] = res.Str
				}
			}
		}
	}

	messageId, err := s.queue.Enqueue(tenantId, queue, req.MessageBody, kv, req.DelaySeconds)
	if err != nil {
		return err
	}

	hasher := md5.New()
	hasher.Write([]byte(req.MessageBody))

	response := SendMessageResponse{
		MessageId:        fmt.Sprintf("%d", messageId),
		MD5OfMessageBody: hex.EncodeToString(hasher.Sum(nil)),
	}

	return c.JSON(response)
}

func (s *SQS) SendMessageBatch(c *fiber.Ctx, tenantId int64) error {
	batchReq := &SendMessageBatchRequest{}

	err := json.Unmarshal(c.Body(), batchReq)
	if err != nil {
		return err
	}

	tokens := strings.Split(batchReq.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	response := &SendMessageBatchResponse{}

	for _, req := range batchReq.Entries {

		kv := make(map[string]string)
		for k, v := range req.MessageAttributes {
			kv[k+"_DataType"] = v.DataType
			if v.DataType == "String" {
				kv[k] = v.StringValue
			} else if v.DataType == "Number" {
				kv[k] = v.StringValue
			} else if v.DataType == "Binary" {
				kv[k] = v.BinaryValue
			}
		}

		// Try to parse celery task and ID
		if s.cfg.ParseCelery {
			// Is our message a JSON string?
			if strings.HasPrefix(req.MessageBody, "ey") {
				jsonStr, err := base64.StdEncoding.DecodeString(req.MessageBody)

				if err == nil {
					res := gjson.GetBytes(jsonStr, "headers.task")
					if res.Exists() {
						kv["celery_task"] = res.Str
					}

					res = gjson.GetBytes(jsonStr, "headers.id")
					if res.Exists() {
						kv["celery_id"] = res.Str
					}
				}
			}
		}

		messageId, err := s.queue.Enqueue(tenantId, queue, req.MessageBody, kv, req.DelaySeconds)

		if err == nil {
			hasher := md5.New()
			hasher.Write([]byte(req.MessageBody))

			response.Successful = append(response.Successful, SendMessageBatchResultEntry{
				ID:               req.ID,
				MessageId:        fmt.Sprintf("%d", messageId),
				MD5OfMessageBody: hex.EncodeToString(hasher.Sum(nil)),
			})
		} else {
			response.Failed = append(response.Failed, BatchResultErrorEntry{
				ID:      req.ID,
				Code:    "InternalFailure",
				Message: err.Error(),
			})
		}
	}

	return c.JSON(response)
}

func (s *SQS) ReceiveMessage(c *fiber.Ctx, tenantId int64) error {
	req := &ReceiveMessageRequest{}

	err := json.Unmarshal(c.Body(), req)
	if err != nil {
		return err
	}

	maxNumberOfMessages := req.MaxNumberOfMessages
	if maxNumberOfMessages == 0 {
		maxNumberOfMessages = 1
	}

	// log.Println(req)

	tokens := strings.Split(req.QueueUrl, "/")
	queue := tokens[len(tokens)-1]

	visibilityTimeout := -1
	if req.VisibilityTimeout != nil {
		visibilityTimeout = *req.VisibilityTimeout
	}

	var messages []*models.Message

	waitTime := min(req.WaitTimeSeconds, s.cfg.MaxDelaySeconds)
	maxRequestTime := time.Now().Add(time.Duration(waitTime) * time.Second)
	sleepIncrement := time.Duration(s.cfg.DelayRetryMillis) * time.Millisecond

	for {
		messages, err = s.queue.Dequeue(tenantId, queue, maxNumberOfMessages, visibilityTimeout)
		if err != nil {
			return err
		}

		if len(messages) > 0 {
			break
		}

		if time.Now().Equal(maxRequestTime) || time.Now().After(maxRequestTime) {
			break
		}

		time.Sleep(sleepIncrement)
	}

	response := ReceiveMessageResponse{
		Messages: make([]Message, len(messages)),
	}

	hasher := md5.New()

	for i, message := range messages {
		hasher.Reset()
		hasher.Write(message.Message)

		response.Messages[i] = Message{
			MessageId:         fmt.Sprintf("%d", message.ID),
			ReceiptHandle:     fmt.Sprintf("%d", message.ID),
			Body:              string(message.Message),
			MessageAttributes: make(map[string]MessageAttribute),
			MD5OfBody:         hex.EncodeToString(hasher.Sum(nil)),
		}

		for k, v := range message.KeyValues {
			if strings.HasSuffix(k, "_DataType") {
				continue
			}
			attr := MessageAttribute{
				DataType: message.KeyValues[k+"_DataType"],
			}
			if attr.DataType == "String" {
				attr.StringValue = v
			} else if attr.DataType == "Number" {
				attr.StringValue = v
			} else if attr.DataType == "Binary" {
				data, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					log.Trace().Int64("message_id", message.ID).Err(err).Msg("Unable to decode binary SQS attribute")
				} else {
					attr.BinaryValue = data
				}
			}

			response.Messages[i].MessageAttributes[k] = attr
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

func (s *SQS) ChangeMessageVisibility(c *fiber.Ctx, tenantId int64) error {
	req := &ChangeMessageVisibilityRequest{}

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

	// Fetch the message
	message := s.queue.Peek(tenantId, queue, messageId)
	if message == nil {
		return NewSQSError(400, "InvalidAddress", "The specified message does not exist.")
	}

	if message.Status() != models.MessageStatusDequeued {
		return NewSQSError(400, "MessageNotInFlight", "The message is not in flight")
	}

	// Calculate the new delivery time in Unix timestamp (seconds)
	newDeliverAt := time.Now().Add(time.Duration(req.VisibilityTimeout) * time.Second).Unix()

	// Update the message's DeliverAt time
	message.DeliverAt = int(newDeliverAt)

	// Update the message in the queue
	err = s.queue.UpdateMessage(tenantId, queue, messageId, message)
	if err != nil {
		return err
	}

	// Return an empty response as per SQS specification
	return c.JSON(ChangeMessageVisibilityResponse{})
}
