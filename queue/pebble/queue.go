package pebble

// Queue represents the ID queue
type Queue struct {
	buffer       []byte
	enqueueIndex int
	dequeueIndex int
	capacity     int
	idSize       int
}

// NewQueue initializes a new Queue with capacity for n IDs of size idSize bytes each
func NewQueue(n, idSize int) *Queue {
	return &Queue{
		buffer:       make([]byte, n*idSize),
		enqueueIndex: 0,
		dequeueIndex: n,
		capacity:     n,
		idSize:       idSize,
	}
}

// Enqueue adds an ID to the enqueued segment
func (q *Queue) Enqueue(id []byte) bool {
	if len(id) != q.idSize {
		// ID must be of size idSize bytes
		return false
	}
	if q.enqueueIndex >= q.dequeueIndex {
		// Queue is full
		return false
	}
	copy(q.buffer[q.enqueueIndex*q.idSize:], id)
	q.enqueueIndex++
	return true
}

// Dequeue moves an ID from the enqueued segment to the dequeued segment
func (q *Queue) Dequeue() ([]byte, bool) {
	if q.enqueueIndex == 0 {
		// No elements to dequeue
		return nil, false
	}
	q.enqueueIndex--
	id := q.buffer[q.enqueueIndex*q.idSize : (q.enqueueIndex+1)*q.idSize]
	q.dequeueIndex--
	copy(q.buffer[q.dequeueIndex*q.idSize:], id)
	return id, true
}

// GetEnqueued returns the list of enqueued IDs
func (q *Queue) GetEnqueued() [][]byte {
	ids := make([][]byte, q.enqueueIndex)
	for i := 0; i < q.enqueueIndex; i++ {
		ids[i] = q.buffer[i*q.idSize : (i+1)*q.idSize]
	}
	return ids
}

// GetDequeued returns the list of dequeued IDs
func (q *Queue) GetDequeued() [][]byte {
	ids := make([][]byte, q.capacity-q.dequeueIndex)
	for i := q.dequeueIndex; i < q.capacity; i++ {
		ids[i-q.dequeueIndex] = q.buffer[i*q.idSize : (i+1)*q.idSize]
	}
	return ids
}

// func main() {
// 	// Example with 10 IDs of 8 bytes each
// 	q := NewQueue(10, 8)

// 	// Enqueue some IDs
// 	q.Enqueue([]byte{1, 2, 3, 4, 5, 6, 7, 8})
// 	q.Enqueue([]byte{9, 10, 11, 12, 13, 14, 15, 16})
// 	q.Enqueue([]byte{17, 18, 19, 20, 21, 22, 23, 24})

// 	fmt.Println("Enqueued IDs:", q.GetEnqueued())
// 	fmt.Println("Dequeued IDs:", q.GetDequeued())

// 	// Dequeue some IDs
// 	q.Dequeue()
// 	q.Dequeue()

// 	fmt.Println("Enqueued IDs after dequeue:", q.GetEnqueued())
// 	fmt.Println("Dequeued IDs after dequeue:", q.GetDequeued())
// }

// type Queue struct {
// 	buffer       []byte
// 	enqueueIndex int
// 	dequeueIndex int
// 	nodes        int
// 	nodeLen      int
// }

// func NewQueue(nodes int, nodeLen int) *Queue {
// 	return &Queue{
// 		buffer:       make([]byte, nodes*nodeLen),
// 		enqueueIndex: 0,
// 		dequeueIndex: nodes * nodeLen,
// 		nodes:        nodes,
// 		nodeLen:      nodeLen,
// 	}
// }

// func (q *Queue) AvailableNodes() int {
// 	if q.enqueueIndex == 0 {
// 		return q.nodes
// 	}

// 	if q.enqueueIndex == q.nodes*q.nodeLen {
// 		return 0
// 	}

// 	if q.enqueueIndex < 0 {
// 		return 0
// 	}

// 	return (q.dequeueIndex - q.enqueueIndex) / q.nodeLen
// }

// func (q *Queue) Enqueue(b []byte) {
// 	copy(q.buffer[q.enqueueIndex:], b)
// 	q.enqueueIndex += q.nodeLen
// }

// func (q *Queue) Dequeue() []byte {
// 	rc := q.buffer[q.enqueueIndex-q.nodeLen : q.enqueueIndex]
// 	copy(q.buffer[q.dequeueIndex-q.nodeLen:], rc)

// 	q.enqueueIndex -= q.nodeLen
// 	q.dequeueIndex -= q.nodeLen

// 	return rc
// }

// func (q *Queue) ListDequeued() [][]byte {
// 	rc := make([][]byte, q.dequeueIndex)
// }
