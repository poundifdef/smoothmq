package sqlite

import (
	"errors"
	"os"
	"regexp"
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
	"gorm.io/gorm/clause"
)

type SQLiteQueue struct {
	Filename string
	DBG      *gorm.DB
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

func NewSQLiteQueue(cfg config.SQLiteConfig) *SQLiteQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	db, err := gorm.Open(sqlite.Open(cfg.Path+"?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full"), &gorm.Config{TranslateError: true})
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

	err = db.AutoMigrate(&RateLimit{})
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

func (q *SQLiteQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	if len(properties.Name) > 80 {
		return models.ErrInvalidQueueName
	}

	regex, err := regexp.Compile(`^[a-zA-Z0-9-_]+$`)
	if err != nil {
		return err
	}

	var nameTrimmed = strings.TrimSpace(properties.Name)
	if !regex.MatchString(nameTrimmed) {
		return models.ErrInvalidQueueName
	}

	qId := q.snow.Generate()

	res := q.DBG.Create(&Queue{
		ID:                qId.Int64(),
		TenantID:          tenantId,
		Name:              nameTrimmed,
		RateLimit:         properties.RateLimit,
		MaxRetries:        properties.MaxRetries,
		VisibilityTimeout: properties.VisibilityTimeout,
	})

	if errors.Is(res.Error, gorm.ErrDuplicatedKey) {
		return models.ErrQueueExists
	}

	return res.Error
}

func (q *SQLiteQueue) UpdateQueue(tenantId int64, queueName string, properties models.QueueProperties) error {
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

func (q *SQLiteQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
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
	res := q.DBG.Where("tenant_id = ? AND name = ?", tenantId, queueName).First(rc)
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

func (q *SQLiteQueue) UpdateMessage(tenantId int64, queue string, messageId int64, m *models.Message) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	result := q.DBG.Model(&Message{}).
		Where("tenant_id = ? AND queue_id = (SELECT id FROM queues WHERE tenant_id = ? AND name = ?) AND id = ?", tenantId, tenantId, queue, messageId).
		Update("deliver_at", m.DeliverAt)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return errors.New("message not found")
	}
	return nil
}

// Calculate how many messages to allow the user to dequeue based on queue's rate limit
func (q *SQLiteQueue) calculateRateLimit(queue *Queue, now int64, numToDequeue int) (int, error) {
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

func (q *SQLiteQueue) Dequeue(tenantId int64, queueName string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
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

	res := q.DBG.Preload("KV").Where(
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

	err = q.DBG.Transaction(func(tx *gorm.DB) error {
		// Use primary key only for faster lookup - IDs already filtered by tenant/queue in Find above
		res = tx.Model(&Message{}).Where("id IN ?", messageIDs).
			UpdateColumns(map[string]any{
				"tries":        gorm.Expr("tries+1"),
				"delivered_at": now,
				"deliver_at":   gorm.Expr("?", now+int64(visibilityTimeout)),
			})

		if res.Error != nil {
			return res.Error
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
				return res.Error
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
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
		sql += " id IN (SELECT message_id FROM kvs WHERE ("

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
		// KV uses composite index idx_kv(tenant_id, queue_id, message_id), so keep all filters
		if err := tx.Where("tenant_id = ? AND queue_id = ? AND message_id = ?", tenantId, queue.ID, messageId).Delete(&KV{}).Error; err != nil {
			return err
		}

		// Message uses primary key on id, so query by id only for faster lookup
		if err := tx.Where("id = ?", messageId).Delete(&Message{}).Error; err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		log.Debug().Int64("message_id", messageId).Msg("Deleted message")
	}

	return err
}

func (q *SQLiteQueue) DeleteBatch(tenantId int64, queueName string, messageIds []int64) error {
	if len(messageIds) == 0 {
		return nil
	}

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	err = q.DBG.Transaction(func(tx *gorm.DB) error {
		// KV uses composite index idx_kv(tenant_id, queue_id, message_id), so keep all filters
		if err := tx.Where("tenant_id = ? AND queue_id = ? AND message_id IN ?", tenantId, queue.ID, messageIds).Delete(&KV{}).Error; err != nil {
			return err
		}

		// Message uses primary key on id, so query by id only for faster lookup
		if err := tx.Where("id IN ?", messageIds).Delete(&Message{}).Error; err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		log.Debug().Interface("message_ids", messageIds).Msg("Deleted messages in batch")
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
