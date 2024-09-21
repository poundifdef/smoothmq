package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/craigpastro/pgmq-go"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/rs/zerolog/log"
)

type Envelope struct {
	Body string `json:"body"`
	Headers map[string]string `json:"headers"`
}

type PGMQQueue struct {
	PGMQ *pgmq.PGMQ
}

func NewPGMQQueue(cfg config.PGMQConfig) (*PGMQQueue, error) {
	log.Info().Msg("Initializing pgmq backend")
	impl, err := pgmq.New(context.Background(), cfg.Uri)
	if err != nil {
		return nil, err
	}
	driver := &PGMQQueue{
		PGMQ: impl,
	}
	return driver, nil
}

func buildTenantQueueName(tenantId int64, queueName string) string {
	return fmt.Sprintf("q_%x_%s", uint64(tenantId), queueName)
}

func toMessage(tenantId int64, in *pgmq.Message) (*models.Message, error) {
	var envelope Envelope
	err := json.Unmarshal(in.Message, &envelope)
	if err != nil {
		return nil, err
	}

	return &models.Message {
		ID:       in.MsgID,
		TenantID: tenantId,
		//QueueID:  message.QueueID,
		//DeliverAt:   int(message.DeliverAt),
		//DeliveredAt: int(message.DeliveredAt),
		//Tries:       message.Tries,
		//MaxTries:    message.MaxTries,
		Message:   []byte(envelope.Body),
		KeyValues: envelope.Headers,
	}, nil
}

func (q *PGMQQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
	queue := models.QueueProperties{}
	return queue, nil
}

func (q *PGMQQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	queueName := buildTenantQueueName(tenantId, properties.Name)
	err := q.PGMQ.CreateQueue(context.TODO(), queueName)
	return err
}

func (q *PGMQQueue) UpdateQueue(tenantId int64, queue string, properties models.QueueProperties) error {
	return nil
}

func (q *PGMQQueue) DeleteQueue(tenantId int64, queue string) error {
	queueName := buildTenantQueueName(tenantId, queue)
	err := q.PGMQ.DropQueue(context.TODO(), queueName)
	return err
}

func (q *PGMQQueue) ListQueues(tenantId int64) ([]string, error) {
	return nil, nil
}

func (q *PGMQQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int) (int64, error) {
	queueName := buildTenantQueueName(tenantId, queue)
	envelope := Envelope{
		Body: message,
		Headers: kv,
	}
	rawMsg, err := json.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	msgId, err := q.PGMQ.Send(context.TODO(), queueName, rawMsg)
	return msgId, err
}

func (q *PGMQQueue) Dequeue(tenantId int64, queue string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	queueName := buildTenantQueueName(tenantId, queue)
	var visibilityTimeoutSeconds int64
	visibilityTimeoutSeconds = 0 // Use default

	if requeueIn > 0 {
		visibilityTimeoutSeconds = int64(requeueIn)
	}

	msgs, err := q.PGMQ.ReadBatch(context.TODO(), queueName, visibilityTimeoutSeconds, int64(numToDequeue))

	if err != nil {
		return nil, err
	}

	out := make([]*models.Message, len(msgs))

	for i, msg := range msgs {
		msg2, err := toMessage(tenantId, msg)
		if err != nil {
			return nil, err
		}
		out[i] = msg2
	}
	return out, nil
}

func (q *PGMQQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
	return nil
}

func (q *PGMQQueue) Stats(tenantId int64, queue string) models.QueueStats {
	stats := models.QueueStats{}
	return stats
}

func (q *PGMQQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	return nil
}

func (q *PGMQQueue) Delete(tenantId int64, queue string, messageId int64) error {
	return nil
}

func (q *PGMQQueue) Shutdown() error {
	q.PGMQ.Close()
	return nil
}
