package sqlite

import (
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"

	"github.com/rs/zerolog/log"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/bwmarrin/snowflake"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteQueue struct {
	filename string
	db       *sqlx.DB
	mu       *sync.Mutex
	snow     *snowflake.Node
	ticker   *time.Ticker
}

var queueDiskSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "queue_disk_size",
		Help: "Size of queue data on disk",
	},
)

// var queueMessageCount = promauto.NewGaugeVec(
// 	prometheus.GaugeOpts{
// 		Name: "queue_message_count",
// 		Help: "Number of messages in queue",
// 	},
// 	[]string{"tenant_id", "queue", "status"},
// )

func NewSQLiteQueue(cfg config.SQLiteConfig) *SQLiteQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	newDb := false
	if _, err := os.Stat(cfg.Path); errors.Is(err, os.ErrNotExist) {
		newDb = true
	}

	db, err := sqlx.Open("sqlite3", cfg.Path+"?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full")
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	if newDb {
		tx, err := db.Begin()
		if err != nil {
			log.Fatal().Err(err).Send()
		}

		// TODO: check for errors
		tx.Exec("CREATE TABLE queues (id integer primary key ,name string, tenant_id integer)")
		tx.Exec(`
		CREATE TABLE messages 
		(
			id integer primary key ,queue_id integer, deliver_at integer, status integer, tenant_id integer,updated_at integer,requeue_in integer, message string,
			FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
		)
		`)
		tx.Exec(`
		CREATE TABLE kv 
		(tenant_id integer, queue_id integer, message_id integer ,k string, v string,
		FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE,
		FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
	)
		`)
		tx.Exec("CREATE UNIQUE INDEX idx_queues on queues (tenant_id,name);")
		tx.Exec("CREATE INDEX idx_messages on messages (tenant_id, queue_id, status, deliver_at, updated_at);")
		tx.Exec("CREATE INDEX idx_kv on kv (tenant_id, queue_id,message_id);")

		tx.Commit()
	}

	rc := &SQLiteQueue{
		filename: cfg.Path,
		db:       db,
		mu:       &sync.Mutex{},
		snow:     snow,
		ticker:   time.NewTicker(1 * time.Second),
	}

	go func() {
		for {
			select {
			case <-rc.ticker.C:
				stat, err := os.Stat(rc.filename)
				if err == nil {
					queueDiskSize.Set(float64(stat.Size()))
				}
			}
		}
	}()

	return rc
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, queue string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// TODO: validate, lowercase, trim queue names. ensure length and valid characters.

	qId := q.snow.Generate()

	_, err := q.db.Exec("INSERT INTO queues (id,tenant_id,name) VALUES (?,?,?)", qId.Int64(), tenantId, strings.ToLower(queue))

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

	defer tx.Rollback()

	var queueId int64
	row := tx.QueryRow("select id from queues where name = ? and tenant_id = ?", strings.ToLower(queue), tenantId)
	err = row.Scan(&queueId)
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM messages WHERE tenant_id = ? AND queue_id = ?", tenantId, queueId)
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM kv WHERE tenant_id = ? AND queue_id = ?", tenantId, queueId)
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
		log.Error().Err(err).Int64("tenant_id", tenantId).Msg("Unable to list queues")
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

func (q *SQLiteQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int) (int64, error) {
	// TODO: make some params configurable or a property of the queue

	messageSnow := q.snow.Generate()
	messageId := messageSnow.Int64()

	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC().Unix()
	deliverAt := now + int64(delay)

	q.mu.Lock()
	defer q.mu.Unlock()
	tx, err := q.db.Beginx()
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO messages (id ,queue_id , deliver_at , status , tenant_id ,updated_at,message, requeue_in) VALUES (?,?,?,?,?,?,?,?)",
		messageId, queueId, 0, models.MessageStatusQueued, tenantId, deliverAt, message, 0)
	if err != nil {
		return 0, err
	}

	for k, v := range kv {
		_, err = tx.Exec("INSERT INTO kv (tenant_id, queue_id,k, v,message_id) VALUES (?,?,?,?,?)",
			tenantId, queueId, k, v, messageId,
		)
		if err != nil {
			return 0, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	log.Debug().Int64("message_id", messageId).Msg("Enqueued message")

	return messageId, nil
}

func (q *SQLiteQueue) Dequeue(tenantId int64, queue string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Unix()

	q.mu.Lock()
	defer q.mu.Unlock()

	query := `
	SELECT * FROM messages
	WHERE (
		(
			(status = ? AND deliver_at <= ?)
			OR
			(status = ? AND deliver_at <= ? AND (updated_at + requeue_in) <= ?)
		)
		AND tenant_id = ?
		AND queue_id = ?
	)
	LIMIT ?
	`

	var rc []*models.Message

	err = q.db.Select(
		&rc,
		query,
		models.MessageStatusQueued, now,
		models.MessageStatusDequeued, now, now,
		tenantId, queueId,
		numToDequeue,
	)
	if err != nil {
		return nil, err
	}

	if len(rc) == 0 {
		return nil, nil
	}

	messageIDs := make([]int64, len(rc))
	msgIndex := make(map[int64]int)
	for i, message := range rc {
		messageIDs[i] = message.ID
		msgIndex[message.ID] = i
	}

	query = `
	UPDATE messages
	SET status = ?, updated_at = ?, requeue_in = ?
	WHERE tenant_id = ? AND queue_id = ? AND id IN (?)
	`

	query, args, err := sqlx.In(query, models.MessageStatusDequeued, now, requeueIn, tenantId, queueId, messageIDs)
	if err != nil {
		return nil, err
	}

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

	query = q.db.Rebind(query)
	result, err := q.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected != int64(len(messageIDs)) {
		log.Error().Int64("tenant_id", tenantId).Str("queue", queue).Int64("rowsAffected", rowsAffected).Ints64("message_ids", messageIDs).Msg("Dequeued unexpected number of messages")
		return nil, nil
	}

	query, args, err = sqlx.In("SELECT message_id,k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id IN (?)", tenantId, queueId, messageIDs)
	if err == nil {
		kvRows, err := q.db.Queryx(query, args...)
		if err == nil {
			for kvRows.Next() {
				var k, v string
				var messageId int64
				kvRows.Scan(&messageId, &k, &v)

				msgIdx, ok := msgIndex[messageId]
				if ok {
					if rc[msgIdx].KeyValues == nil {
						rc[msgIdx].KeyValues = make(map[string]string)
					}

					rc[msgIdx].KeyValues[k] = v
				}
			}
		}
	}

	return rc, nil
}

func (q *SQLiteQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil
	}

	rc := &models.Message{}

	err = q.db.Get(rc, "SELECT * FROM messages WHERE tenant_id=? AND queue_id=? AND id=?", tenantId, queueId, messageId)
	if err != nil {
		log.Error().Err(err).Interface("message", rc).Msg("Unable to peek")
	}

	rc.KeyValues = make(map[string]string)

	rows, err := q.db.Queryx("SELECT k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id=?", tenantId, queueId, messageId)
	if err != nil {
		log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queue).Int64("message_id", messageId).Msg("Unable to get k/v")
	} else {
		for rows.Next() {
			var k, v string
			rows.Scan(&k, &v)
			rc.KeyValues[k] = v
		}
	}

	return rc
}

func (q *SQLiteQueue) Stats(tenantId int64, queue string) models.QueueStats {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return models.QueueStats{}
	}

	rows, err := q.db.Queryx(`
		SELECT 
		CASE
			WHEN status = 2 AND updated_at + requeue_in <= ? THEN 1
			ELSE status
		END AS s,
		count(*) FROM messages WHERE queue_id=? AND tenant_id=? GROUP BY s
	`,
		time.Now().UTC().Unix(),
		queueId, tenantId,
	)
	if err != nil {
		return models.QueueStats{}
	}

	stats := models.QueueStats{
		Counts:        make(map[models.MessageStatus]int),
		TotalMessages: 0,
	}
	for rows.Next() {
		var statusType models.MessageStatus
		var count int

		rows.Scan(&statusType, &count)

		stats.TotalMessages += count
		stats.Counts[statusType] = count
	}

	rows.Close()

	return stats
}

func (q *SQLiteQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	var rc []int64

	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return nil
	}

	args := make([]any, 0)
	args = append(args, tenantId)
	args = append(args, queueId)

	sql := "SELECT id FROM messages WHERE tenant_id=? AND queue_id=? "

	if filterCriteria.MessageID > 0 {
		sql += " AND id = ? "
		args = append(args, filterCriteria.MessageID)
	}

	if len(filterCriteria.KV) > 0 {
		sql += " AND "
		sql += " id IN (SELECT message_id FROM kv WHERE ("

		for i := range len(filterCriteria.KV) {
			sql += "(k=? AND v=? and tenant_id=? and queue_id=?)"

			if i < len(filterCriteria.KV)-1 {
				sql += " OR "
			}
		}

		sql += " ) GROUP BY message_id HAVING count(*) = ? LIMIT 10"
		sql += " ) "

	}

	for k, v := range filterCriteria.KV {
		args = append(args, k, v, tenantId, queueId)
	}

	args = append(args, len(filterCriteria.KV))

	sql += "LIMIT 10"

	q.db.Select(&rc, sql, args...)

	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queue string, messageId int64) error {
	queueId, err := q.queueId(tenantId, queue)
	if err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	tx, err := q.db.Begin()
	if err != nil {
		return err
	}

	query := `
	DELETE FROM messages
	WHERE tenant_id = ? AND queue_id = ? AND id = ?
	`
	result, err := tx.Exec(
		query,
		tenantId,
		queueId,
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
		log.Error().Int64("tenant_id", tenantId).Str("queue", queue).Int64("rowsAffected", rowsAffected).Int64("message_id", messageId).Msg("Deleted unexpected number of messages")
		return nil
	}

	query = `
	DELETE FROM kv
	WHERE tenant_id = ? AND queue_id = ? AND message_id = ?
	`
	_, err = tx.Exec(
		query,
		tenantId,
		queueId,
		messageId,
	)
	if err != nil {
		log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queue).Int64("message_id", messageId).Msg("Unable to delete message")
		// return err
	}

	err = tx.Commit()

	if err == nil {
		log.Debug().Int64("message_id", messageId).Msg("Deleted message")
	}

	return err
}

func (q *SQLiteQueue) Shutdown() error {
	return q.db.Close()
}
