package models

import "errors"

var ErrInvalidQueueName = errors.New("Invalid queue name")
var ErrQueueExists = errors.New("Queue already exists")

type FilterCriteria struct {
	MessageID int64

	// 0 means unbounded
	DeliverAtStart int
	DeliverAtEnd   int

	// status is an OR filter
	Status []MessageStatus

	// kv is an AND filter
	KV map[string]string

	// Smallest message ID to return. Message IDs are Snowflake IDs
	MinMessageID int64

	// How many message IDs to return
	Limit int
}

type QueueProperties struct {
	Name              string
	RateLimit         float64
	MaxRetries        int
	VisibilityTimeout int
}

type Queue interface {
	GetQueue(tenantId int64, queueName string) (QueueProperties, error)
	CreateQueue(tenantId int64, properties QueueProperties) error
	UpdateQueue(tenantId int64, queue string, properties QueueProperties) error
	DeleteQueue(tenantId int64, queue string) error
	ListQueues(tenantId int64) ([]string, error)

	Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int) (int64, error)
	Dequeue(tenantId int64, queue string, numToDequeue int, requeueIn int) ([]*Message, error)
	UpdateMessage(tenantId int64, queue string, messageId int64, m *Message) error

	Peek(tenantId int64, queue string, messageId int64) *Message
	Stats(tenantId int64, queue string) QueueStats
	Filter(tenantId int64, queue string, filterCriteria FilterCriteria) []int64

	Delete(tenantId int64, queue string, messageId int64) error
	DeleteBatch(tenantId int64, queue string, messageIds []int64) error

	Shutdown() error
}
