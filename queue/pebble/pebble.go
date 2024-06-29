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

type MessageStatus byte

const (
	MAX_QUEUE_NAME_LENGTH int = 80

	MessageQueued MessageStatus = 1
	MessageDequed MessageStatus = 2

	MaxReadyQueueMessages uint64 = 1
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
			}
		}
	}()

	return rc
}

func now() int64 {
	return time.Now().UTC().Unix()
}

func (q *PebbleQueue) printQueueStats() {
	zl.Logger = zl.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	for tenantId, queues := range q.queued {
		for queue, messages := range queues {
			zl.Trace().Int64("tenant", tenantId).Str("queue", queue).Uint64("messages", messages.GetCardinality()).Send()
		}
	}

	for tenantId, queues := range q.dequeued {
		for queue, messages := range queues {
			zl.Trace().Int64("tenant", tenantId).Str("queue", queue).Uint64("dequeued", messages.GetCardinality()).Send()
		}
	}

}

func (q *PebbleQueue) markDequeued() {
	batch := q.db.NewIndexedBatch()
	defer batch.Close()

	q.mu.Lock()
	defer q.mu.Unlock()

	for tenantId, queues := range q.dequeued {
		for queue, messages := range queues {
			iter := messages.Iterator()
			for iter.HasNext() {
				id := int64(iter.Next())
				// log.Println(tenantId, queue, id)

				msgKey := NewMetadataKey()
				msgKey.TenantID = tenantId
				msgKey.Queue = queue
				msgKey.Status = 0
				msgKey.Timestamp = 0
				msgKey.MessageID = id

				keyBytes, _ := msgKey.Bytes()

				msgKeyUpper := NewMetadataKey()
				msgKeyUpper.TenantID = tenantId
				msgKeyUpper.Queue = queue
				// msgKeyUpper.Status = 0
				msgKeyUpper.Status = 0xFF
				msgKeyUpper.Timestamp = math.MaxInt64
				msgKeyUpper.MessageID = id + 1

				keyBytesUpper, _ := msgKeyUpper.Bytes()

				// zl.Debug().Hex("l", keyBytes).Send()
				// zl.Debug().Hex("u", keyBytesUpper).Send()
				log.Println(msgKey)
				log.Println(msgKeyUpper)

				msgIter, err := q.db.NewIter(
					&pebble.IterOptions{
						LowerBound: keyBytes,
						UpperBound: keyBytesUpper,
					},
				)
				if err != nil {
					log.Println(err)
					continue
				}

				for msgIter.First(); msgIter.Valid(); msgIter.Next() {
					mkey := NewMetadataKey()
					mkey.Fill(msgIter.Key())
					log.Println(mkey)

					log.Println(batch.Delete(msgIter.Key(), pebble.Sync))
					mkey.Status = byte(MessageDequed)
					newKey, _ := mkey.Bytes()

					// log.Println(msgIter.Key())
					// log.Println(newKey)
					log.Println(batch.Set(newKey, nil, pebble.Sync))
				}

				msgIter.Close()

			}

			messages.Clear()
		}
	}

	batch.Commit(pebble.Sync)

	// for _, queues := range q.dequeued {
	// 	for _, messages := range queues {
	// 		messages.Clear()
	// 	}
	// }

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

			log.Println(maxQueued, maxDequeued)

			maxItem := max(maxQueued, maxDequeued) + 1
			// log.Println(maxItem)

			lowerBound := NewMetadataKey()
			lowerBound.TenantID = queueKey.TenantID
			lowerBound.Queue = queueKey.Queue
			lowerBound.Status = byte(MessageQueued)
			lowerBound.Timestamp = 0
			lowerBound.MessageID = int64(maxItem)

			lowerBoundBytes, _ := lowerBound.Bytes()
			// log.Println(lowerBoundBytes)

			// upperBound := QueuedMetadataToKey(queueKey.TenantID, queueKey.Queue, math.MaxInt64, now())

			upperBound := NewMetadataKey()
			upperBound.TenantID = queueKey.TenantID
			upperBound.Queue = queueKey.Queue
			upperBound.Status = byte(MessageQueued)
			upperBound.Timestamp = now()
			upperBound.MessageID = math.MaxInt64

			log.Println(lowerBound)
			log.Println(upperBound)
			upperBoundBytes, _ := upperBound.Bytes()

			// zl.Debug().Interface("lower", lowerBound).Interface("upper", upperBound).Send()
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

				// log.Println(lowerBoundBytes)
				// log.Println(msgIter.Key())
				msgMeta := NewMetadataKey()
				msgMeta.Fill(msgIter.Key())
				log.Println(msgMeta)
				// log.Println(msgMeta.MessageID, msgMeta.Status)
				// // // _, _, id, _, _ := KeyToMetadata(iter.Key())
				// log.Println(lowerBound.MessageID)
				// log.Println(msgMeta.MessageID)
				// log.Println(lowerBound.MessageID - msgMeta.MessageID)

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

	queues, ok := q.queued[tenantId]
	if !ok {
		return nil, nil
	}

	queued, ok := queues[queue]
	if !ok {
		return nil, nil
	}

	dequeues, ok := q.dequeued[tenantId]
	if !ok {
		return nil, nil
	}

	dequeued, ok := dequeues[queue]
	if !ok {
		return nil, nil
	}

	if queued.GetCardinality() == 0 {
		return nil, nil
	}

	iter := queued.ManyIterator()

	msgIds := make([]uint64, numToDequeue)
	iter.NextMany(msgIds)

	rc := make([]*models.Message, 0)

	// k := make([]byte, 8)
	for _, msgId := range msgIds {
		// log.Println(msgId)
		msgKey := NewMessageKey()
		msgKey.TenantID = tenantId
		msgKey.Queue = queue
		msgKey.MessageID = int64(msgId)
		msgKeyBytes, _ := msgKey.Bytes()
		// 	binary.BigEndian.PutUint64(k, items[i])
		val, closer, err := q.db.Get(msgKeyBytes)
		if err != nil {
			log.Println(err)
			continue
		}
		v := make([]byte, len(val))
		copy(v, val)
		rc = append(rc, &models.Message{
			ID:      int64(msgId),
			Message: v,
		})
		closer.Close()

		// log.Println(msgId)
		// log.Println(queued.Contains(msgId))
		// log.Println(dequeued.Contains(msgId))

		dequeued.Add(msgId)
		queued.Remove(msgId)

		// log.Println(queued.Contains(msgId))
		// log.Println(dequeued.Contains(msgId))
	}

	return rc, nil

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
