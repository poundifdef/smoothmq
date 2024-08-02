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
	ID                int64   `gorm:"primaryKey;autoIncrement:false"`
	TenantID          int64   `gorm:"not null;index:idx_queue_name,priority:1,unique"`
	Name              string  `gorm:"not null;index:idx_queue_name,priority:2"`
	RateLimit         float64 `gorm:"not null"`
	Paused            bool    `gorm:"not null"`
	MaxRetries        int     `gorm:"not null"`
	VisibilityTimeout int     `gorm:"not null"`

	Messages []Message `gorm:"foreignKey:QueueID;references:ID"`
}

type Message struct {
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

func NewSQLiteQueue(cfg config.SQLiteConfig) *SQLiteQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

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

	rc := &SQLiteQueue{
		Filename: cfg.Path,
		DBG:      db,
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

		if err := tx.Where("tenant_id = ? AND id = ?", tenantId, queue.ID).Delete(&Queue{}).Error; err != nil {
			return err
		}

		return nil
	})

	return rc
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
	res := q.DBG.Where("tenant_id = ? AND name = ?", tenantId, strings.ToLower(queueName)).First(rc)
	if res.RowsAffected != 1 {
		return nil, errors.New("Queue not found")
	}
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

	for k, v := range kv {
		newKv := KV{
			TenantID:  tenantId,
			MessageID: messageId,
			QueueID:   queue.ID,
			K:         k,
			V:         v,
		}
		newMessage.KV = append(newMessage.KV, newKv)
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	if err := q.DBG.Create(newMessage).Error; err != nil {
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

	rc := make([]*models.Message, len(messages))

	for i, message := range messages {
		rc[i] = message.ToModel()
	}

	messageIDs := make([]int64, len(rc))
	for i, message := range rc {
		messageIDs[i] = message.ID
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

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

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
