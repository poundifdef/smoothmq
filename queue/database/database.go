package database

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type DBQueue struct {
	Filename string
	DBG      *gorm.DB
	Mu       *sync.Mutex
	snow     *snowflake.Node
	ticker   *time.Ticker
}

type Queue struct {
	ID                int64   `gorm:"primaryKey;autoIncrement:false"`
	TenantID          int64   `gorm:"not null;index:idx_queue_name,priority:1,unique"`
	Name              string  `gorm:"not null;index:idx_queue_name,priority:2"`
	RateLimit         float64 `gorm:"not null"`
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

	Message string `gorm:"not null"`

	KV []KV `gorm:"foreignKey:MessageID;references:ID"`
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

type RateLimit struct {
	TenantID int64 `gorm:"not null;index:idx_ratelimit,priority:1,unique"`
	QueueID  int64 `gorm:"not null;index:idx_ratelimit,priority:2"`
	Ts       int64 `gorm:"not null;index:idx_ratelimit,priority:3"`
	N        int   `gorm:"not null;default:0"`
}

func NewQueue(cfg config.ServerCommand) *DBQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	gcfg := &gorm.Config{TranslateError: true}

	if cfg.DB.LogQueries {
		gcfg.Logger = logger.Default.LogMode(logger.Info)
	}

	dbq := &DBQueue{
		Mu:   &sync.Mutex{},
		snow: snow,
	}

	if cfg.DB.DSN == "" {
		NewSQLiteQueue(cfg, dbq, gcfg)
	} else {
		switch cfg.DB.Driver {
		case "postgres":
			NewDBQueue(cfg.DB, dbq, gcfg)
		case "sqlite":
			NewSQLiteQueue(cfg, dbq, gcfg)
		}
	}

	return dbq
}

func NewDBQueue(cfg config.DBConfig, dbq *DBQueue, gcfg *gorm.Config) {
	var err error

	switch cfg.Driver {
	case "postgres":
		dbq.DBG, err = gorm.Open(postgres.Open(cfg.DSN), gcfg)
	default:
		err = errors.New(fmt.Sprintf("Unknown database driver %s", cfg.Driver))
	}

	if err != nil {
		log.Fatal().Err(err).Send()
	}

	dbq.Migrate()
}

func (q *DBQueue) Migrate() {
	err := q.DBG.AutoMigrate(&Queue{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = q.DBG.AutoMigrate(&Message{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = q.DBG.AutoMigrate(&KV{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = q.DBG.AutoMigrate(&RateLimit{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}
}

func (q *DBQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// TODO: validate, trim queue names. ensure length and valid characters.

	qId := q.snow.Generate()

	res := q.DBG.Create(&Queue{
		ID:                qId.Int64(),
		TenantID:          tenantId,
		Name:              properties.Name,
		RateLimit:         properties.RateLimit,
		MaxRetries:        properties.MaxRetries,
		VisibilityTimeout: properties.VisibilityTimeout,
	})

	if errors.Is(res.Error, gorm.ErrDuplicatedKey) {
		return models.ErrQueueExists
	}

	return res.Error
}

func (q *DBQueue) UpdateQueue(tenantId int64, queueName string, properties models.QueueProperties) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// TODO: validate, trim queue names. ensure length and valid characters.

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	queue.RateLimit = properties.RateLimit
	queue.MaxRetries = properties.MaxRetries
	queue.VisibilityTimeout = properties.VisibilityTimeout

	// TODO: should we reset the rate limit table if the queue's rate limit changes?

	res := q.DBG.Save(queue)

	return res.Error
}

func (q *DBQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
	queue := models.QueueProperties{}

	properties, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return queue, err
	}

	queue.Name = properties.Name
	queue.RateLimit = properties.RateLimit
	queue.MaxRetries = properties.MaxRetries
	queue.VisibilityTimeout = properties.VisibilityTimeout

	return queue, nil
}

func (q *DBQueue) DeleteQueue(tenantId int64, queueName string) error {
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

func (q *DBQueue) ListQueues(tenantId int64) ([]string, error) {
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

func (q *DBQueue) getQueue(tenantId int64, queueName string) (*Queue, error) {
	rc := &Queue{}
	res := q.DBG.Where("tenant_id = ? AND name = ?", tenantId, queueName).First(rc)
	if res.RowsAffected != 1 {
		return nil, errors.New("Queue not found")
	}
	return rc, res.Error
}

func (q *DBQueue) Enqueue(tenantId int64, queueName string, message string, kv map[string]string, delay int) (int64, error) {
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

// Calculate how many messages to allow the user to dequeue based on queue's rate limit
func (q *DBQueue) calculateRateLimit(queue *Queue, now int64, numToDequeue int) (int, error) {
	maxToDequeue := numToDequeue
	bucketToCheck := now

	// If the rate limit is less than 1 per second, meaning 1 message every n seconds
	if queue.RateLimit > 0 && queue.RateLimit < 1 {
		// Go back n seconds, check how many messages have been sent in that time period

		limitReciprocal := 1.0 / queue.RateLimit
		earliestBucket := now - int64(limitReciprocal) + 1

		bucketToCheck = earliestBucket

		sql := `
			SELECT coalesce(cast(sum(n) as real) / (? - min(ts) + 1),0) FROM rate_limits
			WHERE tenant_id = ? AND queue_id = ? AND ts >= ?
			`

		res := q.DBG.Raw(sql, now, queue.TenantID, queue.ID, earliestBucket)
		if res.Error != nil {
			return 0, res.Error
		}

		var messageRate float64

		rateRes := res.Scan(&messageRate)
		if rateRes.Error != nil {
			return 0, rateRes.Error
		}

		if messageRate >= queue.RateLimit {
			return 0, nil
		}

		maxToDequeue = 1
	} else if queue.RateLimit >= 1 {
		// Check how many messages were sent in current bucket
		bucket := &RateLimit{}
		res := q.DBG.Where("tenant_id = ? AND queue_id = ? AND ts = ?", queue.TenantID, queue.ID, now).FirstOrCreate(bucket)

		if res.Error != nil && !errors.Is(res.Error, gorm.ErrDuplicatedKey) {
			return 0, res.Error
		}

		if bucket.N >= int(queue.RateLimit) {
			return 0, nil
		}

		allowedToDequeue := int(queue.RateLimit) - bucket.N
		if allowedToDequeue < 0 {
			allowedToDequeue = 0
		}

		maxToDequeue = min(numToDequeue, allowedToDequeue)
		if maxToDequeue <= 0 {
			return 0, nil
		}
	}

	if queue.RateLimit > 0 {
		res := q.DBG.Where("tenant_id = ? AND queue_id = ? AND ts < ? - 1", queue.TenantID, queue.ID, bucketToCheck).Delete(&RateLimit{})
		if res.Error != nil {
			log.Warn().Int64("tenant_id", queue.TenantID).Int64("queue_id", queue.ID).Int64("ts", bucketToCheck).Msg("Unable to clear previous rate limits")
		}
	}

	return maxToDequeue, nil
}

func (q *DBQueue) Dequeue(tenantId int64, queueName string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil, err
	}

	// Queue is "paused"
	if queue.RateLimit == 0 {
		return nil, nil
	}

	visibilityTimeout := queue.VisibilityTimeout
	if requeueIn > -1 {
		visibilityTimeout = requeueIn
	}

	now := time.Now().UTC().Unix()

	q.Mu.Lock()
	defer q.Mu.Unlock()

	maxToDequeue, err := q.calculateRateLimit(queue, now, numToDequeue)
	if err != nil {
		return nil, err
	}

	var messages []Message

	tx := q.DBG.Begin()
	defer tx.Rollback()

	res := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Preload("KV").Where(
		"deliver_at <= ? AND delivered_at <= ? AND (tries < max_tries OR max_tries = -1) AND tenant_id = ? AND queue_id = ?",
		now, now, tenantId, queue.ID).
		Limit(maxToDequeue).
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

	res = tx.Model(&Message{}).Where("tenant_id = ? AND queue_id = ? AND id in ?", tenantId, queue.ID, messageIDs).
		UpdateColumns(map[string]any{
			"tries":        gorm.Expr("tries+1"),
			"delivered_at": now,
			"deliver_at":   gorm.Expr("?", now+int64(visibilityTimeout)),
		})

	if res.Error != nil {
		return nil, res.Error
	}

	if queue.RateLimit > 0 {
		bucket := RateLimit{
			TenantID: tenantId,
			QueueID:  queue.ID,
			Ts:       now,
			N:        len(messageIDs),
		}
		res = tx.Clauses(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{"n": gorm.Expr("n + ?", len(messageIDs))}),
		}).Create(&bucket)

		if res.Error != nil && !errors.Is(res.Error, gorm.ErrDuplicatedKey) {
			return nil, res.Error
		}
	}

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

	tx.Commit()

	return rc, nil
}

func (q *DBQueue) Peek(tenantId int64, queueName string, messageId int64) *models.Message {
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

func (q *DBQueue) Stats(tenantId int64, queueName string) models.QueueStats {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return models.QueueStats{}
	}

	now := time.Now().UTC().Unix()

	res := q.DBG.Raw(`
		SELECT
		CASE
			WHEN tries = max_tries AND deliver_at < ? THEN 3
			WHEN delivered_at <= ? AND deliver_at > ? THEN 2
			ELSE 1
		END AS s,
		count(*) FROM messages WHERE queue_id=? AND tenant_id=? GROUP BY s
	`, now, now, now,
		queue.ID, tenantId,
	)

	if res.Error != nil {
		return models.QueueStats{}
	}

	rows, err := res.Rows()
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

func (q *DBQueue) Filter(tenantId int64, queueName string, filterCriteria models.FilterCriteria) []int64 {
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
		sql += " id IN (SELECT message_id FROM kvs WHERE ("

		for i := range len(filterCriteria.KV) {
			sql += "(k=? AND v=? and tenant_id=? and queue_id=?)"

			if i < len(filterCriteria.KV)-1 {
				sql += " OR "
			}
		}

		sql += " ) GROUP BY message_id HAVING count(*) = ? LIMIT 10"
		sql += " ) "

		for k, v := range filterCriteria.KV {
			args = append(args, k, v, tenantId, queue.ID)
		}

		args = append(args, len(filterCriteria.KV))
	}

	sql += "LIMIT 10"

	res := q.DBG.Raw(sql, args...).Scan(&rc)
	if res.Error != nil {
		log.Error().Err(res.Error).Msg("Unable to filter")
	}

	return rc
}

func (q *DBQueue) Delete(tenantId int64, queueName string, messageId int64) error {
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

func (q *DBQueue) Shutdown() error {
	db, err := q.DBG.DB()
	if err != nil {
		return err
	}

	return db.Close()
}
