package sqlite

import (
	"errors"
	"log"
	"q/models"
)

type SQLiteQueue struct {
	Filename string
}

func NewSQLiteQueue() *SQLiteQueue {
	filename := "queue.db"

	return &SQLiteQueue{
		Filename: filename,
	}
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, queue string) error {
	return errors.New("not implemented")
}

func (q *SQLiteQueue) DeleteQueue(tenantId int64, queue string) error {
	// Delete all messages with the queue, and then the queue itself
	return errors.New("not implemented")
}

func (q *SQLiteQueue) ListQueues(tenantId int64) ([]string, error) {
	return []string{"a", "b"}, nil
}

func (q *SQLiteQueue) Enqueue(tenantId int64, queue string, message string) (int64, error) {
	log.Println(tenantId, queue, message)
	return 1, nil
}

func (q *SQLiteQueue) Dequeue(tenantId int64, queue string, numToDequeue int) ([]*models.Message, error) {
	rc := []*models.Message{
		{
			ID:        1,
			Status:    models.MessageStatusDequeued,
			KeyValues: map[string]string{"a": "b", "c": "d"},
			Message:   []byte("hello world"),
		},
		{
			ID:        2,
			Status:    models.MessageStatusDequeued,
			KeyValues: map[string]string{"a": "e", "c": "f"},
			Message:   []byte("hello world 2"),
		},
	}

	return rc, nil
}

func (q *SQLiteQueue) Peek(tenantId int64, messageId int64) *models.Message {
	rc := &models.Message{
		ID:        messageId,
		Status:    models.MessageStatusQueued,
		KeyValues: map[string]string{"a": "b", "c": "d"},
		Message:   []byte("hello world"),
	}
	return rc
}

func (q *SQLiteQueue) Stats(tenantId int64, queue string) models.QueueStats {
	stats := models.QueueStats{}
	return stats
}

func (q *SQLiteQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	rc := []int64{1, 2, 3, 4}
	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queue string, messageId int64) error {
	return nil
}

func (q *SQLiteQueue) UpdateStatus(tenantId int64, messageId int64, newStatus models.MessageStatus) error {
	return errors.New("not implemented")
}

func (q *SQLiteQueue) UpdateDeliverAt(tenantId int64, messageId int64, newStatus models.MessageStatus) error {
	return errors.New("not implemented")
}
