package sqlite

import (
	"database/sql"
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
	Filename string
	DB       *sqlx.DB
	Mu       *sync.Mutex
	snow     *snowflake.Node
	ticker   *time.Ticker
}

var queueDiskSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "queue_disk_size",
		Help: "Size of queue data on disk",
	},
)

type Queue struct {
	ID                int64 `db:"id"`
	TenantID          int64 `db:"tenant_id"`
	Name              int64 `db:"name"`
	RateLimit         int64 `db:"rate_limit"`
	Paused            bool  `db:"paused"`
	MaxRetries        int64 `db:"max_retries"`
	Backoff           int64 `db:"backoff"`
	VisibilityTimeout int64 `db:"visibility_timeout"`
}

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
		tx.Exec("CREATE TABLE queues (id integer primary key, name string, tenant_id integer, rate_limit integer, paused integer, max_retries integer, backoff integer, visibility_timeout integer)")
		_, err = tx.Exec(`
		CREATE TABLE messages 
		(
			id integer primary key not null,
			tenant_id integer not null,
			queue_id integer not null,
			
			deliver_at integer not null, 
			delivered_at integer not null,
			tries integer not null default 0,
			max_tries integer not null default 1,
			requeue_in integer not null,

			message string
		)
		`)
		tx.Exec(`
		CREATE TABLE kv 
		(tenant_id integer, queue_id integer, message_id integer ,k string, v string)
		`)
		tx.Exec("CREATE UNIQUE INDEX idx_queues on queues (tenant_id,name);")
		tx.Exec("CREATE INDEX idx_messages on messages (tenant_id, queue_id, deliver_at, delivered_at, tries, max_tries);")
		tx.Exec("CREATE INDEX idx_kv on kv (tenant_id, queue_id,message_id);")

		tx.Commit()
	}

	rc := &SQLiteQueue{
		Filename: cfg.Path,
		DB:       db,
		Mu:       &sync.Mutex{},
		snow:     snow,
		ticker:   time.NewTicker(1 * time.Second),
	}

	go func() {
		for {
			select {
			case <-rc.ticker.C:
				stat, err := os.Stat(rc.Filename)
				if err == nil {
					queueDiskSize.Set(float64(stat.Size()))
				}
			}
		}
	}()

	return rc
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, queue string, visibilityTimeout int) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// TODO: validate, lowercase, trim queue names. ensure length and valid characters.

	qId := q.snow.Generate()

	// id name , tenant_id , rate_limit , paused , max_retries , backoff , visibility )")
	_, err := q.DB.Exec(
		"INSERT INTO queues (id,tenant_id,name,rate_limit,paused,max_retries,backoff,visibility_timeout) VALUES (?,?,?,?,?,?,?,?)",
		qId.Int64(), tenantId, strings.ToLower(queue),
		0, 0, 0, 30, visibilityTimeout,
	)

	return err
}

func (q *SQLiteQueue) DeleteQueue(tenantId int64, queue string) error {
	// Delete all messages with the queue, and then the queue itself

	q.Mu.Lock()
	defer q.Mu.Unlock()

	tx, err := q.DB.Begin()
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
	rows, err := q.DB.Query("SELECT name FROM queues WHERE tenant_id=?", tenantId)
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

func (q *SQLiteQueue) getQueue(tenantId int64, queue string) (*Queue, error) {
	rc := &Queue{}

	row := q.DB.QueryRowx(
		"select id from queues where name = ? and tenant_id = ?",
		strings.TrimSpace(strings.ToLower(queue)), tenantId,
	)

	if row.Err() != nil {
		return nil, row.Err()
	}

	err := row.StructScan(rc)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("Queue does not exist")
		} else {
			return nil, err
		}
	}

	return rc, nil
	// var queueId int64

	// err := row.Scan(&queueId)
	// if err != nil {
	// 	return 0, err
	// }

	// return queueId, nil
}

func (q *SQLiteQueue) Enqueue(tenantId int64, queueName string, message string, kv map[string]string, delay int) (int64, error) {
	// TODO: make some params configurable or a property of the queue
	// requeueIn := 30

	messageSnow := q.snow.Generate()
	messageId := messageSnow.Int64()

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC().Unix()
	deliverAt := now + int64(delay)

	q.Mu.Lock()
	defer q.Mu.Unlock()
	tx, err := q.DB.Beginx()
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()

	_, err = tx.Exec(
		`
		INSERT INTO messages 
		(
			id,
			tenant_id,
			queue_id,
			deliver_at, 
			delivered_at,
			tries,
			max_tries,
			requeue_in,
			message 
		)
		VALUES 
		(?,?,?,?,?,?,?,?,?)
		`,
		messageId, tenantId, queue.ID, deliverAt, 0, 0, queue.MaxRetries+3, queue.Backoff+5, message)
	if err != nil {
		return 0, err
	}

	for k, v := range kv {
		_, err = tx.Exec("INSERT INTO kv (tenant_id, queue_id,k, v,message_id) VALUES (?,?,?,?,?)",
			tenantId, queue.ID, k, v, messageId,
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

func (q *SQLiteQueue) Dequeue(tenantId int64, queueName string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil, err
	}

	if queue.Paused {
		return nil, nil
	}

	now := time.Now().UTC().Unix()

	q.Mu.Lock()
	defer q.Mu.Unlock()

	query := `
	SELECT * FROM messages
	WHERE (
		deliver_at <= ?
		AND delivered_at <= ?
		AND tries < max_tries
		AND tenant_id = ?
		AND queue_id = ?
	)
	LIMIT ?
	`

	var rc []*models.Message

	err = q.DB.Select(
		&rc,
		query,
		now, now,
		tenantId, queue.ID,
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
	UPDATE messages SET
	tries = tries + 1, delivered_at = ?, deliver_at = (? + requeue_in)
	WHERE tenant_id = ? AND queue_id = ? AND id IN (?)
	`
	query, args, err := sqlx.In(query, now, now, tenantId, queue.ID, messageIDs)
	if err != nil {
		return nil, err
	}

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

	query = q.DB.Rebind(query)
	result, err := q.DB.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected != int64(len(messageIDs)) {
		log.Error().Int64("tenant_id", tenantId).Str("queue", queueName).Int64("rowsAffected", rowsAffected).Ints64("message_ids", messageIDs).Msg("Dequeued unexpected number of messages")
		return nil, nil
	}

	query, args, err = sqlx.In("SELECT message_id,k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id IN (?)", tenantId, queue.ID, messageIDs)
	if err == nil {
		kvRows, err := q.DB.Queryx(query, args...)
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

func (q *SQLiteQueue) Peek(tenantId int64, queueName string, messageId int64) *models.Message {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil
	}

	rc := &models.Message{}

	err = q.DB.Get(rc, "SELECT * FROM messages WHERE tenant_id=? AND queue_id=? AND id=?", tenantId, queue.ID, messageId)
	if err != nil {
		log.Error().Err(err).Interface("message", rc).Msg("Unable to peek")
	}

	rc.KeyValues = make(map[string]string)

	rows, err := q.DB.Queryx("SELECT k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id=?", tenantId, queue.ID, messageId)
	if err != nil {
		log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queueName).Int64("message_id", messageId).Msg("Unable to get k/v")
	} else {
		for rows.Next() {
			var k, v string
			rows.Scan(&k, &v)
			rc.KeyValues[k] = v
		}
	}

	return rc
}

func (q *SQLiteQueue) Stats(tenantId int64, queueName string) models.QueueStats {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return models.QueueStats{}
	}

	rows, err := q.DB.Queryx(`
		SELECT 
		CASE
			WHEN status = 2 AND updated_at + requeue_in <= ? THEN 1
			ELSE status
		END AS s,
		count(*) FROM messages WHERE queue_id=? AND tenant_id=? GROUP BY s
	`,
		time.Now().UTC().Unix(),
		queue.ID, tenantId,
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

func (q *SQLiteQueue) Filter(tenantId int64, queueName string, filterCriteria models.FilterCriteria) []int64 {
	var rc []int64

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil
	}

	args := make([]any, 0)
	args = append(args, tenantId)
	args = append(args, queue.ID)

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
		args = append(args, k, v, tenantId, queue.ID)
	}

	args = append(args, len(filterCriteria.KV))

	sql += "LIMIT 10"

	q.DB.Select(&rc, sql, args...)

	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queueName string, messageId int64) error {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	tx, err := q.DB.Begin()
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
		queue.ID,
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
		log.Error().Int64("tenant_id", tenantId).Str("queue", queueName).Int64("rowsAffected", rowsAffected).Int64("message_id", messageId).Msg("Deleted unexpected number of messages")
		return nil
	}

	query = `
	DELETE FROM kv
	WHERE tenant_id = ? AND queue_id = ? AND message_id = ?
	`
	_, err = tx.Exec(
		query,
		tenantId,
		queue.ID,
		messageId,
	)
	if err != nil {
		log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queueName).Int64("message_id", messageId).Msg("Unable to delete message")
		// return err
	}

	err = tx.Commit()

	if err == nil {
		log.Debug().Int64("message_id", messageId).Msg("Deleted message")
	}

	return err
}

func (q *SQLiteQueue) Shutdown() error {
	return q.DB.Close()
}
