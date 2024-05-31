package sqlite

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"q/models"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteQueue struct {
	Filename string
	db       *sql.DB
	mu       *sync.Mutex
}

func NewSQLiteQueue() *SQLiteQueue {
	filename := "queue.db"

	newDb := false
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		newDb = true
	}

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatal(err)
	}

	if newDb {
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		// TODO: check for errors
		tx.Exec("CREATE TABLE queues (id integer primary key,name string, tenant_id integer)")
		tx.Exec("CREATE TABLE messages (id integer primary key,queue_id integer, deliver_at integer, status integer, tenant_id integer,updated_at integer)")
		tx.Exec("CREATE TABLE kv (tenant_id integer, queue_id integer, message_id integer ,k string, v string)")
		tx.Exec("CREATE UNIQUE INDEX idx_queues on queues (tenant_id,name);")
		tx.Exec("CREATE INDEX idx_messages on messages (tenant_id, queue_id, status, deliver_at, updated_at);")
		tx.Exec("CREATE INDEX idx_kv on kv (tenant_id, queue_id,k, v);")

		tx.Commit()
	}

	return &SQLiteQueue{
		Filename: filename,
		db:       db,
		mu:       &sync.Mutex{},
	}
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, queue string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, err := q.db.Exec("INSERT INTO queues (tenant_id,name) VALUES (?,?)", tenantId, strings.ToLower(queue))

	return err
}

func (q *SQLiteQueue) DeleteQueue(tenantId int64, queue string) error {
	// Delete all messages with the queue, and then the queue itself

	q.mu.Lock()
	defer q.mu.Unlock()

	tx, err := q.db.Begin()
	if err != nil {
		return err
	}

	var queueId int64
	row := tx.QueryRow("select id from queues where name = ? and tenant_id = ?", strings.ToLower(queue), tenantId)
	err = row.Scan(&queueId)
	if err != nil {
		tx.Rollback()
		return err
	}

	// TODO: check for errors
	_, err = tx.Exec("DELETE FROM messages WHERE tenant_id = ? AND queue_id = ?", tenantId, queueId)
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM queues WHERE tenant_id = ? AND id = ?", tenantId, queueId)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (q *SQLiteQueue) ListQueues(tenantId int64) ([]string, error) {
	rows, err := q.db.Query("SELECT name FROM queues WHERE tenant_id=?", tenantId)
	if err != nil {
		return nil, err
	}

	rc := make([]string, 0)
	var name string

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		rc = append(rc, name)
	}

	return rc, nil
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

func (q *SQLiteQueue) Shutdown() error {
	return q.db.Close()
}
