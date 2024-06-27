package pebble

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"q/models"
	"slices"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bwmarrin/snowflake"
	"github.com/cockroachdb/pebble"
)

// type Message struct {
// 	id      *snowflake.ID
// 	message *string
// }

type PebbleQueue struct {
	filename string
	db       *pebble.DB
	// ch       chan Message
	mu       *sync.Mutex
	snow     *snowflake.Node
	ticker   *time.Ticker
	queued   *roaring64.Bitmap
	dequeued *roaring64.Bitmap
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

type KVType byte
type MessageStatus byte

const (
	KV_QUEUE    KVType = 1
	KV_MESSAGE  KVType = 2
	KV_METADATA KVType = 3
	KV_KEYVALUE KVType = 4

	MAX_QUEUE_NAME_LENGTH int = 80

	MessageQueued MessageStatus = 1
	MessageDequed MessageStatus = 2

	MaxReadyQueueMessages uint64 = 50_000
)

func QueueToKey(tenantID int64, queue string) []byte {
	// type + tenantid + queue
	rc := make([]byte, 1+8+80)

	rc[0] = byte(KV_QUEUE)
	binary.BigEndian.PutUint64(rc[1:], uint64(tenantID))
	copy(rc[8:], queue)

	return rc
}

func KeyToQueue(key []byte) (tenantId int64, queue string) {
	tenantUint := binary.BigEndian.Uint64(key[1:9])
	tenantId = int64(tenantUint)

	n := bytes.IndexByte(key[8:], 0)
	queue = string(key[8 : 8+n])

	return
}

func MessageToKey(tenantID int64, queue string, messageId int64) []byte {
	// type + tenantid + queue + messageid
	rc := QueueToKey(tenantID, queue)

	rc[0] = byte(KV_MESSAGE)
	binary.BigEndian.AppendUint64(rc, uint64(messageId))

	return rc
}

func QueuedMetadataToKey(tenantID int64, queue string, messageId int64, deliverAt int64) []byte {
	// type + tenantid + queue + status + deliverat + messageid
	rc := QueueToKey(tenantID, queue)

	rc[0] = byte(KV_METADATA)
	rc = append(rc, byte(MessageQueued))
	binary.BigEndian.AppendUint64(rc, uint64(deliverAt))
	binary.BigEndian.AppendUint64(rc, uint64(messageId))

	return rc
}

func DequeuedMetadataToKey(tenantID int64, queue string, messageId int64, redeliverAt int64) []byte {
	// type + tenantid + queue + status + redeliverat + messageid
	rc := QueueToKey(tenantID, queue)

	rc[0] = byte(KV_METADATA)
	rc = append(rc, byte(MessageDequed))
	binary.BigEndian.AppendUint64(rc, uint64(redeliverAt))
	binary.BigEndian.AppendUint64(rc, uint64(messageId))

	return rc
}

func KVToKey(tenantID int64, queue string, messageId int64, k string) []byte {
	// type + tenantid + queue + messageid + k
	rc := MessageToKey(tenantID, queue, messageId)

	rc[0] = byte(KV_KEYVALUE)
	slices.Concat(rc, []byte(k))

	return rc
}

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

		queued:   roaring64.NewBitmap(),
		dequeued: roaring64.NewBitmap(),
	}
	// rc.queued.Add(1)

	go func() {

		for {
			select {
			case <-rc.ticker.C:
				rc.mu.Lock()
				// log.Println("queued size", rc.queued.GetSizeInBytes(), rc.queued.GetCardinality())
				// log.Println("dequeued size", rc.dequeued.GetSizeInBytes(), rc.dequeued.GetCardinality())

				// log.Println(rc.queued.Maximum())
				// should it be 0? instead, if it is less than maxcapacity
				if rc.queued.GetCardinality() < MaxReadyQueueMessages {
					// 	// get max from bitmap
					var maxQueued uint64 = 0
					var maxDequeued uint64 = 0

					if !rc.queued.IsEmpty() {
						maxQueued = rc.queued.Maximum()
					}
					if !rc.dequeued.IsEmpty() {
						maxDequeued = rc.dequeued.Maximum()
					}

					maxItem := max(maxQueued, maxDequeued) + 1

					maxQueuedBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(maxQueuedBytes, maxItem)

					// 	// iterate over items, starting at max+1, up to 100k, and fill bitmap
					iter, err := db.NewIter(
						&pebble.IterOptions{
							UpperBound: []byte{1 << 7, 0, 0, 0, 0, 0, 0, 0},
							LowerBound: maxQueuedBytes,
						},
					)
					if err != nil {
						log.Println(err)
					}

					for iter.First(); iter.Valid(); iter.Next() {
						id := binary.BigEndian.Uint64(iter.Key())
						rc.queued.Add(id)

						if rc.queued.GetCardinality() >= MaxReadyQueueMessages {
							break
						}
					}
					iter.Close()
				}

				batch := db.NewBatch()
				iter := rc.dequeued.Iterator()

				toDelete := make([]byte, 8)
				for iter.HasNext() {
					id := iter.Next()
					binary.BigEndian.PutUint64(toDelete, id)
					val, closer, err := db.Get(toDelete)
					if err != nil {
						log.Println(err)
					}

					v := make([]byte, len(val))
					copy(v, val)

					k := make([]byte, len(toDelete))
					copy(k, toDelete)
					k[0] = k[0] | (1 << 7)
					batch.Set(k, v, pebble.Sync)

					batch.Delete(toDelete, pebble.Sync)
					closer.Close()
				}

				batch.Commit(pebble.Sync)
				batch.Close()

				rc.dequeued.Clear()
				rc.mu.Unlock()

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

func (q *PebbleQueue) CreateQueue(tenantId int64, queue string) error {
	if len(queue) == 0 {
		return errors.New("invalid queue name")
	}

	key := QueueToKey(tenantId, queue)
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
	lower := QueueToKey(tenantId, "")

	upper := QueueToKey(tenantId, "")
	upper[0] += 1

	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}

	rc := make([]string, 0)

	for iter.First(); iter.Valid(); iter.Next() {
		_, queue := KeyToQueue(iter.Key())
		rc = append(rc, queue)
	}

	iter.Close()

	return rc, nil
}

func (q *PebbleQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int, requeueIn int) (int64, error) {
	queueKey := QueueToKey(tenantId, queue)
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

	messageKey := MessageToKey(tenantId, queue, messageSnow.Int64())
	err = batch.Set(messageKey, []byte(message), pebble.Sync)
	if err != nil {
		return 0, err
	}

	metadataKey := QueuedMetadataToKey(tenantId, queue, messageSnow.Int64(), deliverAt)
	err = batch.Set(metadataKey, nil, pebble.Sync)
	if err != nil {
		return 0, err
	}

	for k, v := range kv {
		kvKey := KVToKey(tenantId, queue, messageSnow.Int64(), k)
		err = batch.Set(kvKey, []byte(v), pebble.Sync)
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

	if q.queued.GetCardinality() == 0 {
		return nil, nil
	}

	iter := q.queued.ManyIterator()
	items := make([]uint64, numToDequeue)
	numItems := iter.NextMany(items)

	rc := make([]*models.Message, numItems)

	k := make([]byte, 8)
	for i := range numItems {
		binary.BigEndian.PutUint64(k, items[i])
		val, closer, err := q.db.Get(k)
		if err != nil {
			log.Println(err)
			continue
		}
		v := make([]byte, len(val))
		copy(v, val)
		rc[i] = &models.Message{
			ID:      int64(items[i]),
			Message: v,
		}
		closer.Close()

		q.dequeued.Add(items[i])
		q.queued.Remove(items[i])

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
	q.mu.Lock()
	q.queued.Remove(uint64(messageId))
	q.dequeued.Remove(uint64(messageId))
	q.mu.Unlock()

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], uint64(messageId))

	var idBytesRemoved [8]byte
	binary.BigEndian.PutUint64(idBytesRemoved[:], uint64(messageId))
	idBytesRemoved[0] = idBytesRemoved[0] | (1 << 7)

	batch := q.db.NewBatch()
	defer batch.Close()

	err := batch.Delete(idBytes[:], pebble.Sync)
	if err != nil {
		return err
	}
	err = batch.Delete(idBytesRemoved[:], pebble.Sync)
	if err != nil {
		return err
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

func (q *PebbleQueue) Shutdown() error {
	return q.db.Close()
}
