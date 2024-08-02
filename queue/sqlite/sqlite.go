package sqlite

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"

	"github.com/rs/zerolog/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/bwmarrin/snowflake"
	_ "github.com/mattn/go-sqlite3"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SQLiteQueue struct {
	Filename string
	// DB       *sqlx.DB
	DBG    *gorm.DB
	Mu     *sync.Mutex
	snow   *snowflake.Node
	ticker *time.Ticker
}

var queueDiskSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "queue_disk_size",
		Help: "Size of queue data on disk",
	},
)

type Queue struct {
	ID                int64  `gorm:"primaryKey;autoIncrement:false"`
	TenantID          int64  `gorm:"not null;index:idx_queue_name,priority:1,unique"`
	Name              string `gorm:"not null;index:idx_queue_name,priority:2"`
	RateLimit         int64  `gorm:"not null"`
	Paused            bool   `gorm:"not null"`
	MaxRetries        int    `gorm:"not null"`
	VisibilityTimeout int    `gorm:"not null"`

	Messages []Message `gorm:"foreignKey:QueueID;references:ID"`
}

type Message struct {
	// tenant_id, queue_id, deliver_at, delivered_at, tries, max_tries
	ID       int64 `gorm:"primaryKey;autoIncrement:false"`
	TenantID int64 `gorm:"not null;index:idx_message,priority:1;not null"`
	QueueID  int64 `gorm:"not null;index:idx_message,priority:2;not null"`

	DeliverAt   int64 `gorm:"not null;index:idx_message,priority:3;not null"`
	DeliveredAt int64 `gorm:"not null;index:idx_message,priority:4;not null"`
	Tries       int   `gorm:"not null;index:idx_message,priority:5;not null"`
	MaxTries    int   `gorm:"not null;index:idx_message,priority:6;not null"`
	RequeueIn   int   `gorm:"not null"`

	Message string `gorm:"not null"`

	KV []KV `gorm:"foreignKey:TenantID,QueueID,MessageID;references:TenantID,QueueID,ID"`
}

func (message *Message) ToModel() *models.Message {
	rc := &models.Message{
		ID:       message.ID,
		TenantID: message.TenantID,
		QueueID:  message.QueueID,

		DeliverAt:   int(message.DeliverAt),
		DeliveredAt: int(message.DeliveredAt),
		Tries:       message.Tries,
		MaxTries:    message.MaxTries,
		RequeueIn:   message.RequeueIn,

		Message:   []byte(message.Message),
		KeyValues: make(map[string]string),
	}

	for _, kv := range message.KV {
		rc.KeyValues[kv.K] = kv.V
	}

	return rc
}

type KV struct {
	TenantID  int64  `gorm:"not null;index:idx_kv,priority:1"`
	QueueID   int64  `gorm:"not null;index:idx_kv,priority:2"`
	MessageID int64  `gorm:"not null;index:idx_kv,priority:3"`
	K         string `gorm:"not null"`
	V         string `gorm:"not null"`
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

	// newDb := false
	// if _, err := os.Stat(cfg.Path); errors.Is(err, os.ErrNotExist) {
	// newDb = true
	// }

	// olddb, err := sqlx.Open("sqlite3", cfg.Path+"?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full")

	db, err := gorm.Open(sqlite.Open(cfg.Path+"?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full"), &gorm.Config{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&Queue{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&Message{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&KV{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	// if newDb {
	// 	tx, err := db.Begin()
	// 	if err != nil {
	// 		log.Fatal().Err(err).Send()
	// 	}

	// 	// TODO: check for errors
	// 	tx.Exec("CREATE TABLE queues (id integer primary key, name string, tenant_id integer, rate_limit integer, paused integer, max_retries integer, backoff integer, visibility_timeout integer)")
	// 	_, err = tx.Exec(`
	// 	CREATE TABLE messages
	// 	(
	// 		id integer primary key not null,
	// 		tenant_id integer not null,
	// 		queue_id integer not null,

	// 		deliver_at integer not null,
	// 		delivered_at integer not null,
	// 		tries integer not null default 0,
	// 		max_tries integer not null default 1,
	// 		requeue_in integer not null,

	// 		message string
	// 	)
	// 	`)
	// 	tx.Exec(`
	// 	CREATE TABLE kv
	// 	(tenant_id integer, queue_id integer, message_id integer ,k string, v string)
	// 	`)
	// 	tx.Exec("CREATE UNIQUE INDEX idx_queues on queues (tenant_id,name);")
	// 	tx.Exec("CREATE INDEX idx_messages on messages (tenant_id, queue_id, deliver_at, delivered_at, tries, max_tries);")
	// 	tx.Exec("CREATE INDEX idx_kv on kv (tenant_id, queue_id,message_id);")

	// 	tx.Commit()
	// }

	rc := &SQLiteQueue{
		Filename: cfg.Path,
		// DB:       olddb,
		DBG:    db,
		Mu:     &sync.Mutex{},
		snow:   snow,
		ticker: time.NewTicker(1 * time.Second),
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

	res := q.DBG.Create(&Queue{
		ID:                qId.Int64(),
		TenantID:          tenantId,
		Name:              strings.ToLower(queue),
		RateLimit:         0,
		Paused:            false,
		MaxRetries:        -1,
		VisibilityTimeout: visibilityTimeout,
	})

	return res.Error
}

func (q *SQLiteQueue) DeleteQueue(tenantId int64, queueName string) error {
	// Delete all messages with the queue, and then the queue itself

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	rc := q.DBG.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant_id = ? AND queue_id = ?", tenantId, queue.ID).Delete(&KV{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND queue_id = ?", tenantId, queue.ID).Delete(&Message{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND queue_id = ?", tenantId, queue.ID).Delete(&Queue{}).Error; err != nil {
			return err
		}

		return nil
	})

	return rc

	// tx, err := q.DB.Begin()
	// if err != nil {
	// 	return err
	// }

	// defer tx.Rollback()

	// var queueId int64
	// row := tx.QueryRow("select id from queues where name = ? and tenant_id = ?", strings.ToLower(queue), tenantId)
	// err = row.Scan(&queueId)
	// if err != nil {
	// 	return err
	// }

	// _, err = tx.Exec("DELETE FROM messages WHERE tenant_id = ? AND queue_id = ?", tenantId, queueId)
	// if err != nil {
	// 	return err
	// }

	// _, err = tx.Exec("DELETE FROM kv WHERE tenant_id = ? AND queue_id = ?", tenantId, queueId)
	// if err != nil {
	// 	return err
	// }

	// _, err = tx.Exec("DELETE FROM queues WHERE tenant_id = ? AND id = ?", tenantId, queueId)
	// if err != nil {
	// 	return err
	// }

	// return tx.Commit()
}

func (q *SQLiteQueue) ListQueues(tenantId int64) ([]string, error) {
	var queues []Queue
	res := q.DBG.Where("tenant_id = ?", tenantId).Select("name").Find(&queues)
	if res.Error != nil {
		return nil, res.Error
	}

	rc := make([]string, len(queues))
	for i, queue := range queues {
		rc[i] = queue.Name
	}

	return rc, nil
}

func (q *SQLiteQueue) getQueue(tenantId int64, queueName string) (*Queue, error) {
	rc := &Queue{}
	res := q.DBG.Where("tenant_id = ? AND name = ?", tenantId, strings.ToLower(queueName)).Select("name").First(rc)
	return rc, res.Error
}

func (q *SQLiteQueue) Enqueue(tenantId int64, queueName string, message string, kv map[string]string, delay int) (int64, error) {
	messageSnow := q.snow.Generate()
	messageId := messageSnow.Int64()

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC().Unix()
	deliverAt := now + int64(delay)

	// 	messageId, tenantId, queue.ID, deliverAt, 0, 0, queue.MaxRetries, queue.VisibilityTimeout, message)

	// rc := q.DBG.Transaction(func(tx *gorm.DB) error {
	newMessage := &Message{
		ID:          messageId,
		TenantID:    tenantId,
		QueueID:     queue.ID,
		DeliverAt:   deliverAt,
		DeliveredAt: 0,
		MaxTries:    queue.MaxRetries,
		RequeueIn:   queue.VisibilityTimeout,
		Message:     message,

		KV: make([]KV, 0),
	}

	// if err := tx.Create(newMessage).Error; err != nil {
	// return err
	// }

	for k, v := range kv {
		newKv := KV{
			TenantID:  tenantId,
			MessageID: messageId,
			QueueID:   queue.ID,
			K:         k,
			V:         v,
		}
		newMessage.KV = append(newMessage.KV, newKv)
		// if err := tx.Create(newKv).Error; err != nil {
		// return err
		// }
	}

	// return nil
	// })

	q.Mu.Lock()
	defer q.Mu.Unlock()

	if err := q.DBG.Create(newMessage).Error; err != nil {
		return 0, err
	}

	// if rc != nil {
	// 	return 0, rc
	// }

	// tx, err := q.DB.Beginx()
	// if err != nil {
	// 	return 0, err
	// }

	// defer tx.Rollback()

	// // TODO: visibility timeout
	// _, err = tx.Exec(
	// 	`
	// 	INSERT INTO messages
	// 	(
	// 		id,
	// 		tenant_id,
	// 		queue_id,
	// 		deliver_at,
	// 		delivered_at,
	// 		tries,
	// 		max_tries,
	// 		requeue_in,
	// 		message
	// 	)
	// 	VALUES
	// 	(?,?,?,?,?,?,?,?,?)
	// 	`,
	// 	messageId, tenantId, queue.ID, deliverAt, 0, 0, queue.MaxRetries, queue.VisibilityTimeout, message)
	// if err != nil {
	// 	return 0, err
	// }

	// for k, v := range kv {
	// 	_, err = tx.Exec("INSERT INTO kv (tenant_id, queue_id,k, v,message_id) VALUES (?,?,?,?,?)",
	// 		tenantId, queue.ID, k, v, messageId,
	// 	)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// }

	// err = tx.Commit()
	// if err != nil {
	// 	return 0, err
	// }

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

	var messages []Message

	res := q.DBG.Preload("KV").Where(
		"deliver_at <= ? AND delivered_at <= ? AND (tries < max_tries OR max_tries = -1) AND tenant_id = ? AND queue_id = ?",
		now, now, tenantId, queue.ID).
		Limit(numToDequeue).
		Find(&messages)

	if res.Error != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, nil
	}
	// query := `
	// SELECT * FROM messages
	// WHERE (
	// 	deliver_at <= ?
	// 	AND delivered_at <= ?
	// 	AND (tries < max_tries OR max_tries = -1)
	// 	AND tenant_id = ?
	// 	AND queue_id = ?
	// )
	// LIMIT ?
	// `

	rc := make([]*models.Message, len(messages))

	for i, message := range messages {
		rc[i] = message.ToModel()
	}

	// err = q.DB.Select(
	// 	&rc,
	// 	query,
	// 	now, now,
	// 	tenantId, queue.ID,
	// 	numToDequeue,
	// )
	// if err != nil {
	// 	return nil, err
	// }

	messageIDs := make([]int64, len(rc))
	// msgIndex := make(map[int64]int)
	for i, message := range rc {
		messageIDs[i] = message.ID
		// msgIndex[message.ID] = i
	}

	res = q.DBG.Model(&Message{}).Where("tenant_id = ? AND queue_id = ? AND id in ?", tenantId, queue.ID, messageIDs).
		UpdateColumns(map[string]any{
			"tries":        gorm.Expr("tries+1"),
			"delivered_at": now,
			"deliver_at":   gorm.Expr("? + requeue_in", now),
		})

	if res.Error != nil {
		return nil, res.Error
	}

	// query = `
	// UPDATE messages SET
	// tries = tries + 1, delivered_at = ?, deliver_at = (? + requeue_in)
	// WHERE tenant_id = ? AND queue_id = ? AND id IN (?)
	// `
	// query, args, err := sqlx.In(query, now, now, tenantId, queue.ID, messageIDs)
	// if err != nil {
	// 	return nil, err
	// }

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

	// query = q.DB.Rebind(query)
	// result, err := q.DB.Exec(query, args...)
	// if err != nil {
	// 	return nil, err
	// }

	// rowsAffected, err := result.RowsAffected()
	// if err != nil {
	// 	return nil, err
	// }

	// if rowsAffected != int64(len(messageIDs)) {
	// 	log.Error().Int64("tenant_id", tenantId).Str("queue", queueName).Int64("rowsAffected", rowsAffected).Ints64("message_ids", messageIDs).Msg("Dequeued unexpected number of messages")
	// 	return nil, nil
	// }

	// query, args, err = sqlx.In("SELECT message_id,k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id IN (?)", tenantId, queue.ID, messageIDs)
	// if err == nil {
	// 	kvRows, err := q.DB.Queryx(query, args...)
	// 	if err == nil {
	// 		for kvRows.Next() {
	// 			var k, v string
	// 			var messageId int64
	// 			kvRows.Scan(&messageId, &k, &v)

	// 			msgIdx, ok := msgIndex[messageId]
	// 			if ok {
	// 				if rc[msgIdx].KeyValues == nil {
	// 					rc[msgIdx].KeyValues = make(map[string]string)
	// 				}

	// 				rc[msgIdx].KeyValues[k] = v
	// 			}
	// 		}
	// 	}
	// }

	return rc, nil
}

func (q *SQLiteQueue) Peek(tenantId int64, queueName string, messageId int64) *models.Message {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil
	}

	message := &Message{}

	res := q.DBG.Preload("KV").Where(
		"tenant_id = ? AND queue_id = ? AND id = ?",
		tenantId, queue.ID, messageId).
		First(message)

	if res.Error != nil {
		return nil
	}

	// rc := &models.Message{}

	// err = q.DB.Get(rc, "SELECT * FROM messages WHERE tenant_id=? AND queue_id=? AND id=?", tenantId, queue.ID, messageId)
	// if err != nil {
	// 	log.Error().Err(err).Interface("message", rc).Msg("Unable to peek")
	// }

	// rc.KeyValues = make(map[string]string)

	// rows, err := q.DB.Queryx("SELECT k,v FROM kv WHERE tenant_id=? AND queue_id=? AND message_id=?", tenantId, queue.ID, messageId)
	// if err != nil {
	// 	log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queueName).Int64("message_id", messageId).Msg("Unable to get k/v")
	// } else {
	// 	for rows.Next() {
	// 		var k, v string
	// 		rows.Scan(&k, &v)
	// 		rc.KeyValues[k] = v
	// 	}
	// }

	return message.ToModel()
}

func (q *SQLiteQueue) Stats(tenantId int64, queueName string) models.QueueStats {
	return models.QueueStats{}
	// queue, err := q.getQueue(tenantId, queueName)
	// if err != nil {
	// return models.QueueStats{}
	// }

	// rows, err := q.DB.Queryx(`
	// 	SELECT
	// 	CASE
	// 		WHEN status = 2 AND updated_at + requeue_in <= ? THEN 1
	// 		ELSE status
	// 	END AS s,
	// 	count(*) FROM messages WHERE queue_id=? AND tenant_id=? GROUP BY s
	// `,
	// 	time.Now().UTC().Unix(),
	// 	queue.ID, tenantId,
	// )
	// if err != nil {
	// 	return models.QueueStats{}
	// }

	// stats := models.QueueStats{
	// 	Counts:        make(map[models.MessageStatus]int),
	// 	TotalMessages: 0,
	// }
	// for rows.Next() {
	// 	var statusType models.MessageStatus
	// 	var count int

	// 	rows.Scan(&statusType, &count)

	// 	stats.TotalMessages += count
	// 	stats.Counts[statusType] = count
	// }

	// rows.Close()

	// return stats
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

	// q.DB.Select(&rc, sql, args...)

	res := q.DBG.Raw(sql, args...).Scan(&rc)
	if res.Error != nil {
		log.Error().Err(res.Error).Msg("Unable to filter")
	}

	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queueName string, messageId int64) error {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	err = q.DBG.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant_id = ? AND queue_id = ? AND message_id = ?", tenantId, queue.ID, messageId).Delete(&KV{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND queue_id = ? AND id = ?", tenantId, queue.ID, messageId).Delete(&Message{}).Error; err != nil {
			return err
		}

		return nil
	})

	// tx, err := q.DB.Begin()
	// if err != nil {
	// 	return err
	// }

	// query := `
	// DELETE FROM messages
	// WHERE tenant_id = ? AND queue_id = ? AND id = ?
	// `
	// result, err := tx.Exec(
	// 	query,
	// 	tenantId,
	// 	queue.ID,
	// 	messageId,
	// )
	// if err != nil {
	// 	return err
	// }

	// rowsAffected, err := result.RowsAffected()
	// if err != nil {
	// 	return err
	// }

	// if rowsAffected != 1 {
	// 	log.Error().Int64("tenant_id", tenantId).Str("queue", queueName).Int64("rowsAffected", rowsAffected).Int64("message_id", messageId).Msg("Deleted unexpected number of messages")
	// 	return nil
	// }

	// query = `
	// DELETE FROM kv
	// WHERE tenant_id = ? AND queue_id = ? AND message_id = ?
	// `
	// _, err = tx.Exec(
	// 	query,
	// 	tenantId,
	// 	queue.ID,
	// 	messageId,
	// )
	// if err != nil {
	// 	log.Error().Err(err).Int64("tenant_id", tenantId).Str("queue", queueName).Int64("message_id", messageId).Msg("Unable to delete message")
	// 	// return err
	// }

	// err = tx.Commit()

	if err == nil {
		log.Debug().Int64("message_id", messageId).Msg("Deleted message")
	}

	return err
}

func (q *SQLiteQueue) Shutdown() error {
	db, err := q.DBG.DB()
	if err != nil {
		return err
	}

	return db.Close()
}
