package pebble

import (
	"errors"
	"log"
	"math"
	"os"
	"q/models"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bwmarrin/snowflake"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	zl "github.com/rs/zerolog/log"
)

type PebbleQueue struct {
	filename string
	db       *pebble.DB
	// ch       chan Message
	mu     *sync.Mutex
	snow   *snowflake.Node
	ticker *time.Ticker
	// queued   *roaring64.Bitmap
	// queued   map[string]*roaring64.Bitmap
	queued   map[int64]map[string]*roaring64.Bitmap
	dequeued map[int64]map[string]*roaring64.Bitmap
	// dequeued *roaring64.Bitmap
}

// var queueDiskSize = promauto.NewGauge(
// 	prometheus.GaugeOpts{
// 		Name: "queue_disk_size",
// 		Help: "Size of queue data on disk",
// 	},
// )

// var queueMessageCount = promauto.NewGaugeVec(
// 	prometheus.GaugeOpts{
// 		Name: "queue_message_count",
// 		Help: "Number of messages in queue",
// 	},
// 	[]string{"tenant_id", "queue", "status"},
// )

type MessageStatus byte

const (
	MAX_QUEUE_NAME_LENGTH int = 80

	MessageQueued MessageStatus = 1
	MessageDequed MessageStatus = 2

	MaxReadyQueueMessages uint64 = 50_000
)

func NewPebbleQueue() *PebbleQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal(err)
	}

	// db, err := pebble.Open("/data/q.pebble", &pebble.Options{})
	db, err := pebble.Open("q", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}

	rc := &PebbleQueue{
		// filename: filename,
		db:     db,
		mu:     &sync.Mutex{},
		snow:   snow,
		ticker: time.NewTicker(1 * time.Second),

		queued:   make(map[int64]map[string]*roaring64.Bitmap),
		dequeued: make(map[int64]map[string]*roaring64.Bitmap),
	}

	go func() {

		for {
			select {
			case <-rc.ticker.C:
				rc.printQueueStats()
				rc.fillReadyQueue()
				// rc.markDequeued()

				// 	rc.mu.Lock()
				// 	// log.Println("queued size", rc.queued.GetSizeInBytes(), rc.queued.GetCardinality())
				// 	// log.Println("dequeued size", rc.dequeued.GetSizeInBytes(), rc.dequeued.GetCardinality())

				// 	// log.Println(rc.queued.Maximum())
				// 	// should it be 0? instead, if it is less than maxcapacity
				// 	if rc.queued.GetCardinality() < MaxReadyQueueMessages {
				// 		// 	// get max from bitmap
				// 		var maxQueued uint64 = 0
				// 		var maxDequeued uint64 = 0

				// 		if !rc.queued.IsEmpty() {
				// 			maxQueued = rc.queued.Maximum()
				// 		}
				// 		if !rc.dequeued.IsEmpty() {
				// 			maxDequeued = rc.dequeued.Maximum()
				// 		}

				// 		maxItem := max(maxQueued, maxDequeued) + 1

				// 		maxQueuedBytes := make([]byte, 8)
				// 		binary.BigEndian.PutUint64(maxQueuedBytes, maxItem)

				// 		// 	// iterate over items, starting at max+1, up to 100k, and fill bitmap
				// 		iter, err := db.NewIter(
				// 			&pebble.IterOptions{
				// 				UpperBound: []byte{1 << 7, 0, 0, 0, 0, 0, 0, 0},
				// 				LowerBound: maxQueuedBytes,
				// 			},
				// 		)
				// 		if err != nil {
				// 			log.Println(err)
				// 		}

				// 		for iter.First(); iter.Valid(); iter.Next() {
				// 			id := binary.BigEndian.Uint64(iter.Key())
				// 			rc.queued.Add(id)

				// 			if rc.queued.GetCardinality() >= MaxReadyQueueMessages {
				// 				break
				// 			}
				// 		}
				// 		iter.Close()
				// 	}

				// 	batch := db.NewBatch()
				// 	iter := rc.dequeued.Iterator()

				// 	toDelete := make([]byte, 8)
				// 	for iter.HasNext() {
				// 		id := iter.Next()
				// 		binary.BigEndian.PutUint64(toDelete, id)
				// 		val, closer, err := db.Get(toDelete)
				// 		if err != nil {
				// 			log.Println(err)
				// 		}

				// 		v := make([]byte, len(val))
				// 		copy(v, val)

				// 		k := make([]byte, len(toDelete))
				// 		copy(k, toDelete)
				// 		k[0] = k[0] | (1 << 7)
				// 		batch.Set(k, v, pebble.Sync)

				// 		batch.Delete(toDelete, pebble.Sync)
				// 		closer.Close()
				// 	}

				// 	batch.Commit(pebble.Sync)
				// 	batch.Close()

				// 	rc.dequeued.Clear()
				// 	rc.mu.Unlock()

			}
		}
	}()

	// go func() {
	// 	for {
	// 		select {
	// 		case <-rc.ticker.C:
	// 			// stat, err := os.Stat("q.txt")
	// 			stat, err := os.Stat(rc.filename)
	// 			if err == nil {
	// 				queueDiskSize.Set(float64(stat.Size()))
	// 			}
	// 		}
	// 	}
	// }()

	return rc
}

func now() int64 {
	return time.Now().UTC().Unix()
}

func (q *PebbleQueue) printQueueStats() {
	zl.Logger = zl.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
	for tenantId, queues := range q.queued {
		for queue, messages := range queues {
			zl.Debug().Int64("tenant", tenantId).Str("queue", queue).Uint64("messages", messages.GetCardinality()).Send()
		}
	}
}

func (q *PebbleQueue) fillReadyQueue() {

	queueKeyLower := NewQueueKey()
	queueKeyLowerBytes, _ := queueKeyLower.Bytes()

	queueKeyUpper := NewQueueKey()
	queueKeyUpper.Type += 1
	queueKeyUpperBytes, _ := queueKeyUpper.Bytes()

	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: queueKeyLowerBytes,
		UpperBound: queueKeyUpperBytes,
		// LowerBound: []byte{byte(KV_QUEUE)},
		// UpperBound: []byte{byte(KV_QUEUE + 1)},
	})
	if err != nil {
		log.Println(err)
		return
	}

	defer iter.Close()

	q.mu.Lock()
	defer q.mu.Unlock()

	for iter.First(); iter.Valid(); iter.Next() {
		// log.Println(iter.Key())
		// Iterate through each queue in our db
		queueKey := NewQueueKey()
		queueKey.Fill(iter.Key())

		// Ensure our data structure has a bitmap for all available queues
		enqueuedQueues, ok := q.queued[queueKey.TenantID]
		if !ok {
			enqueuedQueues = make(map[string]*roaring64.Bitmap)
			q.queued[queueKey.TenantID] = enqueuedQueues
		}

		queued, ok := enqueuedQueues[queueKey.Queue]
		if !ok {
			queued = roaring64.NewBitmap()
			enqueuedQueues[queueKey.Queue] = queued
		}

		dequeuedQueues, ok := q.dequeued[queueKey.TenantID]
		if !ok {
			dequeuedQueues = make(map[string]*roaring64.Bitmap)
			q.dequeued[queueKey.TenantID] = dequeuedQueues
		}

		dequeued, ok := dequeuedQueues[queueKey.Queue]
		if !ok {
			dequeued = roaring64.NewBitmap()
			dequeuedQueues[queueKey.Queue] = dequeued
		}

		if queued.GetCardinality() < MaxReadyQueueMessages {
			// 	// get max from bitmap
			var maxQueued uint64 = 0
			var maxDequeued uint64 = 0

			if !queued.IsEmpty() {
				maxQueued = queued.Maximum()
			}
			if !dequeued.IsEmpty() {
				maxDequeued = dequeued.Maximum()
			}

			maxItem := max(maxQueued, maxDequeued) + 1

			lowerBound := NewMetadataKey()
			lowerBound.TenantID = queueKey.TenantID
			lowerBound.Queue = queueKey.Queue
			lowerBound.MessageID = int64(maxItem)
			lowerBound.Timestamp = 0
			lowerBound.Status = byte(MessageQueued)

			lowerBoundBytes, _ := lowerBound.Bytes()
			// log.Println(lowerBoundBytes)

			// upperBound := QueuedMetadataToKey(queueKey.TenantID, queueKey.Queue, math.MaxInt64, now())

			upperBound := NewMetadataKey()
			upperBound.TenantID = queueKey.TenantID
			upperBound.Queue = queueKey.Queue
			upperBound.MessageID = math.MaxInt64
			upperBound.Timestamp = now()
			upperBound.Type += 1

			upperBoundBytes, _ := upperBound.Bytes()

			// maxQueuedBytes := make([]byte, 8)
			// binary.BigEndian.PutUint64(maxQueuedBytes, maxItem)

			// iterate over items, starting at max+1, up to 100k, and fill bitmap
			msgIter, err := q.db.NewIter(
				&pebble.IterOptions{
					LowerBound: lowerBoundBytes,
					UpperBound: upperBoundBytes,
				},
			)
			if err != nil {
				log.Println(err)
				return
			}

			for msgIter.First(); msgIter.Valid(); msgIter.Next() {
				// log.Println(msgIter.Key())
				msgMeta := NewMetadataKey()
				msgMeta.Fill(msgIter.Key())
				// // // _, _, id, _, _ := KeyToMetadata(iter.Key())
				queued.Add(uint64(msgMeta.MessageID))

				if queued.GetCardinality() >= MaxReadyQueueMessages {
					break
				}
			}
			msgIter.Close()
		}
	}

}

func (q *PebbleQueue) CreateQueue(tenantId int64, queue string) error {
	if len(queue) == 0 {
		return errors.New("invalid queue name")
	}

	// queueKey := &QueueKey{
	// 	TenantID: tenantId,
	// 	Queue:    queue,
	// }
	queueKey := NewQueueKey()
	queueKey.TenantID = tenantId
	queueKey.Queue = queue
	key, _ := queueKey.Bytes()
	// log.Println(key)
	err := q.db.Set(key, nil, pebble.Sync)
	return err
}

func (q *PebbleQueue) DeleteQueue(tenantId int64, queue string) error {
	// key: q (1) + tenantId (8) + queueName (80) = 1
	// key: msgs (1) + tenantId (8) + queueName (80) + status (1) + msgid (8) = message
	// xxx key: msgs (1) + tenantId (8) + msgid (8) = message
	return nil
}

func (q *PebbleQueue) ListQueues(tenantId int64) ([]string, error) {

	lower := NewQueueKey()
	lower.TenantID = tenantId
	// lower := &QueueKey{TenantID: tenantId, Queue: ""}
	lowerBytes, _ := lower.Bytes()

	upper := NewQueueKey()
	upper.TenantID = tenantId
	upper.Type += 1
	// upper := &QueueKey{TenantID: tenantId, Queue: ""}
	upperBytes, _ := upper.Bytes()
	// upperBytes[0] += 1

	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBytes,
		UpperBound: upperBytes,
	})
	if err != nil {
		return nil, err
	}

	rc := make([]string, 0)

	for iter.First(); iter.Valid(); iter.Next() {
		qk := &QueueKey{}
		qk.Fill(iter.Key())
		rc = append(rc, qk.Queue)
	}

	iter.Close()

	return rc, nil
}

func (q *PebbleQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int, requeueIn int) (int64, error) {
	queueKeyStruct := NewQueueKey()
	queueKeyStruct.TenantID = tenantId
	queueKeyStruct.Queue = queue
	// queueKeyStruct := &QueueKey{
	// TenantID: tenantId,
	// Queue:    queue,
	// }
	queueKey, _ := queueKeyStruct.Bytes()
	_, queueCloser, err := q.db.Get(queueKey)
	if err != nil {
		return 0, err
	}
	queueCloser.Close()

	messageSnow := q.snow.Generate()

	now := time.Now().UTC().Unix()
	deliverAt := now + int64(delay)

	// m := Message{id: &messageSnow, message: &message}
	// q.ch <- m

	batch := q.db.NewBatch()
	defer batch.Close()

	messageKey := NewMessageKey()
	messageKey.TenantID = tenantId
	messageKey.Queue = queue
	messageKey.MessageID = messageSnow.Int64()
	messageKeyBytes, _ := messageKey.Bytes()
	// log.Println(messageKeyBytes)
	err = batch.Set(messageKeyBytes, []byte(message), pebble.Sync)
	if err != nil {
		return 0, err
	}

	metadataKey := NewMetadataKey()
	metadataKey.TenantID = tenantId
	metadataKey.Queue = queue
	metadataKey.MessageID = messageSnow.Int64()
	metadataKey.Timestamp = deliverAt
	metadataKey.Status = byte(MessageQueued)
	metadataKeyBytes, _ := metadataKey.Bytes()
	// log.Println(metadataKey.Type, err)
	// log.Println(metadataKeyBytes)
	// metadataKey := QueuedMetadataToKey(tenantId, queue, messageSnow.Int64(), deliverAt)
	err = batch.Set(metadataKeyBytes, nil, pebble.Sync)
	if err != nil {
		return 0, err
	}

	for k, v := range kv {

		kvKey := NewKVKey()
		kvKey.TenantID = tenantId
		kvKey.Queue = queue
		kvKey.MessageID = messageSnow.Int64()
		kvKey.Key = k
		kvKeyBytes, _ := kvKey.Bytes()
		// kvKey := KVToKey(tenantId, queue, messageSnow.Int64(), k)
		// log.Println(kvKeyBytes)
		err = batch.Set(kvKeyBytes, []byte(v), pebble.Sync)
		if err != nil {
			log.Println(err)
		}
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return 0, err
	}

	return messageSnow.Int64(), nil
}

func (q *PebbleQueue) Dequeue(tenantId int64, queue string, numToDequeue int) ([]*models.Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return nil, nil
	// if q.queued.GetCardinality() == 0 {
	// 	return nil, nil
	// }

	// iter := q.queued.ManyIterator()
	// items := make([]uint64, numToDequeue)
	// numItems := iter.NextMany(items)

	// rc := make([]*models.Message, numItems)

	// k := make([]byte, 8)
	// for i := range numItems {
	// 	binary.BigEndian.PutUint64(k, items[i])
	// 	val, closer, err := q.db.Get(k)
	// 	if err != nil {
	// 		log.Println(err)
	// 		continue
	// 	}
	// 	v := make([]byte, len(val))
	// 	copy(v, val)
	// 	rc[i] = &models.Message{
	// 		ID:      int64(items[i]),
	// 		Message: v,
	// 	}
	// 	closer.Close()

	// 	q.dequeued.Add(items[i])
	// 	q.queued.Remove(items[i])

	// }

	// return rc, nil

}

func (q *PebbleQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
	// key: msgs (1) + tenantId (8) + queueName (80) + status (1) + deliverat (4) + msgid (8) = message
	return nil
}

func (q *PebbleQueue) Stats(tenantId int64, queue string) models.QueueStats {
	return models.QueueStats{}
}

func (q *PebbleQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	return nil
}

func (q *PebbleQueue) Delete(tenantId int64, queue string, messageId int64) error {
	// q.mu.Lock()
	// q.queued.Remove(uint64(messageId))
	// q.dequeued.Remove(uint64(messageId))
	// q.mu.Unlock()

	// var idBytes [8]byte
	// binary.BigEndian.PutUint64(idBytes[:], uint64(messageId))

	// var idBytesRemoved [8]byte
	// binary.BigEndian.PutUint64(idBytesRemoved[:], uint64(messageId))
	// idBytesRemoved[0] = idBytesRemoved[0] | (1 << 7)

	// batch := q.db.NewBatch()
	// defer batch.Close()

	// err := batch.Delete(idBytes[:], pebble.Sync)
	// if err != nil {
	// 	return err
	// }
	// err = batch.Delete(idBytesRemoved[:], pebble.Sync)
	// if err != nil {
	// 	return err
	// }

	// err = batch.Commit(pebble.Sync)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (q *PebbleQueue) Shutdown() error {
	return q.db.Close()
}
