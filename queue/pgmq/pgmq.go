package pgmq

import (
	"context"
	"github.com/craigpastro/pgmq-go"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/rs/zerolog/log"
)

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

func (q *PGMQQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
	queue := models.QueueProperties{}
	return queue, nil
}

func (q *PGMQQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	return nil
}

func (q *PGMQQueue) UpdateQueue(tenantId int64, queue string, properties models.QueueProperties) error {
	return nil
}

func (q *PGMQQueue) DeleteQueue(tenantId int64, queue string) error {
	return nil
}

func (q *PGMQQueue) ListQueues(tenantId int64) ([]string, error) {
	return nil, nil
}

func (q *PGMQQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int) (int64, error) {
	return 0, nil
}

func (q *PGMQQueue) Dequeue(tenantId int64, queue string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	return nil, nil
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
