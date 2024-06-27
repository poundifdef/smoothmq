package pebble

import (
	"encoding/binary"
	"log"
	"q/models"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bwmarrin/snowflake"
	"github.com/cockroachdb/pebble"
)

type Message struct {
	id      *snowflake.ID
	message *string
}

type PebbleQueue struct {
	filename string
	db       *pebble.DB
	ch       chan Message
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
		var maxMessages uint64 = 100_000

		for {
			select {
			case <-rc.ticker.C:

				i, err := db.NewIter(
					&pebble.IterOptions{
						// UpperBound: []byte{1 << 7, 0, 0, 0, 0, 0, 0, 0},
					},
				)
				if err != nil {
					log.Println(err)
				}

				log.Println(i.First(), i.Valid())

				for i.First(); i.Valid(); i.Next() {
					log.Println(i.Key())
					break
				}

				rc.mu.Lock()
				log.Println("queued size", rc.queued.GetSizeInBytes(), rc.queued.GetCardinality())
				log.Println("dequeued size", rc.dequeued.GetSizeInBytes(), rc.dequeued.GetCardinality())

				// log.Println(rc.queued.Maximum())
				// should it be 0? instead, if it is less than maxcapacity
				if rc.queued.GetCardinality() < maxMessages {
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

						if rc.queued.GetCardinality() >= maxMessages {
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
	return nil
}

func (q *PebbleQueue) DeleteQueue(tenantId int64, queue string) error {
	return nil
}

func (q *PebbleQueue) ListQueues(tenantId int64) ([]string, error) {
	return nil, nil
}

func (q *PebbleQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int, requeueIn int) (int64, error) {
	messageSnow := q.snow.Generate()

	// m := Message{id: &messageSnow, message: &message}
	// q.ch <- m

	batch := q.db.NewBatch()
	id := messageSnow.IntBytes()
	err := batch.Set(id[:], []byte(message), pebble.Sync)
	if err != nil {
		return 0, err
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return 0, err
	}
	err = batch.Close()
	if err != nil {
		// return 0, err
		log.Println(messageSnow.String(), err)
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

	// return nil, nil
	return rc, nil

	// // batch := q.db.NewBatch()
	// batch := q.db.NewIndexedBatch()
	// i, err := batch.NewIter(&pebble.IterOptions{
	// 	// i, err := q.db.NewIter(&pebble.IterOptions{
	// 	UpperBound: []byte{1 << 7, 0, 0, 0, 0, 0, 0, 0},
	// 	// LowerBound: []byte{1 << 7, 0, 0, 0, 0, 0, 0, 0},
	// 	// LowerBound: []byte{'q', '2', 0, 0, 0, 0, 0, 0, 0x1},
	// })
	// if err != nil {
	// 	return nil, err
	// }

	// // val, closer, _ := batch.Get([]byte{25, 15, 114, 24, 30, 64, 16, 16})
	// // id := binary.BigEndian.Uint64([]byte{25, 15, 114, 24, 30, 64, 16, 16})
	// // v := make([]byte, len(val))
	// // copy(v, val)
	// // msg := &models.Message{
	// // 	ID:      int64(id),
	// // 	Message: v,
	// // }
	// // rc = append(rc, msg)
	// // closer.Close()

	// msgs := 0
	// // log.Println(i)
	// // log.Println(i.First())
	// for kv := i.First(); kv; i.Next() {
	// 	// log.Println(batch.Get([]byte{'x'}))
	// 	// batch.Set([]byte{'x'}, []byte{1}, pebble.Sync)

	// 	// log.Println(i.Key())
	// 	id := binary.BigEndian.Uint64(i.Key())

	// 	// log.Println(int64(id))
	// 	// log.Println(id, msgs)

	// 	v := make([]byte, len(i.Value()))
	// 	copy(v, i.Value())

	// 	msg := &models.Message{
	// 		ID:      int64(id),
	// 		Message: v,
	// 	}
	// 	rc = append(rc, msg)

	// 	k := make([]byte, len(i.Key()))
	// 	copy(k, i.Key())
	// 	k[0] = k[0] | (1 << 7)
	// 	batch.Set(k, v, pebble.Sync)

	// 	batch.Delete(i.Key(), pebble.Sync)

	// 	msgs++

	// 	if msgs >= numToDequeue {
	// 		break
	// 	}
	// }

	// i.Close()

	// err = batch.Commit(pebble.Sync)
	// if err != nil {
	// 	return nil, err
	// }

	// batch.Close()

	// return rc, nil
}

func (q *PebbleQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
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
