package sqlite

import (
	// "database/sql"
	"database/sql"
	"errors"
	"log"
	"os"
	"q/models"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/bwmarrin/snowflake"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteQueue struct {
	Filename string
	db       *sqlx.DB
	mu       *sync.Mutex
	snow     *snowflake.Node
}

func NewSQLiteQueue() *SQLiteQueue {
	filename := "queue.db"

	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal(err)
	}

	newDb := false
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		newDb = true
	}

	db, err := sqlx.Open("sqlite3", filename)
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
		tx.Exec("CREATE TABLE messages (id integer primary key,queue_id integer, deliver_at integer, status integer, tenant_id integer,updated_at integer,requeue_in integer, message string)")
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
		snow:     snow,
	}
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, queue string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// TODO: validate, lowercase, trim queue names. ensure length and valid characters.

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

func (q *SQLiteQueue) queueId(tenantId int64, queue string) (int64, error) {

	row := q.db.QueryRow(
		"select id from queues where name = ? and tenant_id = ?",
		strings.TrimSpace(strings.ToLower(queue)), tenantId)

	var queueId int64

	err := row.Scan(&queueId)
	if err != nil {
		return 0, err
	}

	return queueId, nil
}

func (q *SQLiteQueue) Enqueue(tenantId int64, queue string, message string) (int64, error) {
	// TODO: make this configurable or a property of the queue
	const defaultRequeueSeconds = 60

	q.mu.Lock()
	defer q.mu.Unlock()

	messageId := q.snow.Generate().Int64()

	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return 0, err
	}

	_, err = q.db.Exec(
		"INSERT INTO messages (id ,queue_id , deliver_at , status , tenant_id ,updated_at,message,requeue_in ) VALUES (?,?,?,?,?,?,?,?)",
		messageId, queueId, 0, models.MessageStatusQueued, tenantId, time.Now().UTC().Unix(), message, defaultRequeueSeconds)

	if err != nil {
		return 0, err
	}

	return messageId, nil
}

func (q *SQLiteQueue) Dequeue(tenantId int64, queue string, numToDequeue int) ([]*models.Message, error) {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Unix()
	var messageId int64

	q.mu.Lock()
	defer q.mu.Unlock()

	query := `
	SELECT id FROM messages
	WHERE (
		(
			(status = ? AND deliver_at <= ?)
			OR
			(status = ? AND deliver_at <= ? AND (updated_at + requeue_in) <= ?)
		)
		AND tenant_id = ?
		AND queue_id = ?
	)
	LIMIT 1
	`
	messageRow := q.db.QueryRow(
		query,
		models.MessageStatusQueued, now,
		models.MessageStatusDequeued, now, now,
		tenantId, queueId,
	)

	err = messageRow.Scan(&messageId)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	query = `
	UPDATE messages
	SET status = ?, updated_at = ?
	WHERE tenant_id = ? AND queue_id = ? AND id = ?
	`
	result, err := q.db.Exec(
		query,
		models.MessageStatusDequeued, now,
		queueId,
		tenantId,
		messageId,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected != 1 {
		log.Printf("Dequeued unexpected number of messages %d rowsAffected %d", messageId, rowsAffected)
		return nil, nil
	}

	rc := []*models.Message{
		{
			ID:        messageId,
			Status:    models.MessageStatusDequeued,
			KeyValues: map[string]string{"a": "b", "c": "d"},
			Message:   []byte("hello world"),
		},
	}

	return rc, nil
}

func (q *SQLiteQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil
	}

	rc := &models.Message{}
	q.db.Get(rc, "SELECT * FROM messages WHERE tenant_id=? AND queue_id=? AND id=?", tenantId, queueId, messageId)

	return rc
}

func (q *SQLiteQueue) Stats(tenantId int64, queue string) models.QueueStats {
	stats := models.QueueStats{}
	return stats
}

func (q *SQLiteQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	var rc []int64

	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil
	}

	err = q.db.Select(&rc, "SELECT id FROM messages WHERE tenant_id=? AND queue_id=? LIMIT 10", tenantId, queueId)

	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queue string, messageId int64) error {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return err
	}

	query := `
	DELETE FROM messages
	WHERE tenant_id = ? AND queue_id = ? AND id = ?
	`
	result, err := q.db.Exec(
		query,
		queueId,
		tenantId,
		messageId,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		log.Printf("Deleted unexpected number of messages %d rowsAffected %d", messageId, rowsAffected)
		return nil
	}

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
