package pebble

import (
	"encoding/binary"

	"github.com/go-restruct/restruct"
)

type KVType byte

const (
	KV_QUEUE    KVType = 1
	KV_MESSAGE  KVType = 2
	KV_METADATA KVType = 3
	KV_KEYVALUE KVType = 4
)

func (v KVType) Byte() byte {
	return byte(v)
}

type QueueKey struct {
	Type     byte   `struct:byte`
	TenantID int64  `struct:"int64,big"`
	Queue    string `struct:"[10]byte"`
}

func NewQueueKey() *QueueKey {
	return &QueueKey{Type: KV_QUEUE.Byte()}
}

func (v *QueueKey) Bytes() ([]byte, error) {
	return restruct.Pack(binary.BigEndian, v)
}

func (v *QueueKey) Fill(key []byte) error {
	return restruct.Unpack(key, binary.BigEndian, v)
}

type MessageKey struct {
	Type      byte   `struct:byte`
	TenantID  int64  `struct:"int64,big"`
	Queue     string `struct:"[10]byte"`
	MessageID int64  `struct:"int64,big"`
}

func NewMessageKey() *MessageKey {
	return &MessageKey{Type: KV_MESSAGE.Byte()}
}

func (v *MessageKey) Bytes() ([]byte, error) {
	return restruct.Pack(binary.BigEndian, v)
}

func (v *MessageKey) Fill(key []byte) error {
	return restruct.Unpack(key, binary.BigEndian, v)
}

type MetadataKey struct {
	Type      byte   `struct:byte`
	TenantID  int64  `struct:"int64,big"`
	Queue     string `struct:"[10]byte"`
	Status    byte   `struct:byte`
	MessageID int64  `struct:"int64,big"`
	Timestamp int64  `struct:"int64,big"`
}

func NewMetadataKey() *MetadataKey {
	return &MetadataKey{Type: KV_METADATA.Byte()}
}

func (v *MetadataKey) Bytes() ([]byte, error) {
	return restruct.Pack(binary.BigEndian, v)
}

func (v *MetadataKey) Fill(key []byte) error {
	return restruct.Unpack(key, binary.BigEndian, v)
}

// func QueuedMetadataToKey(tenantID int64, queue string, messageId int64, deliverAt int64) []byte {
// 	// type + tenantid + queue + status + deliverat + messageid
// 	return nil
// 	// rc := QueueToKey(tenantID, queue)

// 	// rc[0] = byte(KV_METADATA)
// 	// rc = append(rc, byte(MessageQueued))
// 	// binary.BigEndian.AppendUint64(rc, uint64(deliverAt))
// 	// binary.BigEndian.AppendUint64(rc, uint64(messageId))

// 	// return rc
// }

// func DequeuedMetadataToKey(tenantID int64, queue string, messageId int64, redeliverAt int64) []byte {
// 	// type + tenantid + queue + status + redeliverat + messageid
// 	return nil
// 	// rc := QueueToKey(tenantID, queue)

// 	// rc[0] = byte(KV_METADATA)
// 	// rc = append(rc, byte(MessageDequed))
// 	// binary.BigEndian.AppendUint64(rc, uint64(redeliverAt))
// 	// binary.BigEndian.AppendUint64(rc, uint64(messageId))

// 	// return rc
// }

// func KeyToMetadata(key []byte) (tenantId int64, queue string, messageId int64, timestamp int64, status MessageStatus) {
// 	// tenantId, queue = KeyToQueue(key)

// 	// Extract status
// 	status = MessageStatus(key[9+len(queue)])

// 	timestamp = int64(binary.BigEndian.Uint64(key[10+len(queue) : 18+len(queue)]))
// 	messageId = int64(binary.BigEndian.Uint64(key[18+len(queue) : 26+len(queue)]))

// 	return
// }

type KVKey struct {
	Type      byte   `struct:byte`
	TenantID  int64  `struct:"int64,big"`
	Queue     string `struct:"[10]byte"`
	MessageID int64  `struct:"int64,big"`
	Key       string `struct:"string"`
}

func NewKVKey() *KVKey {
	return &KVKey{Type: KV_KEYVALUE.Byte()}
}

func (v *KVKey) Bytes() ([]byte, error) {
	return restruct.Pack(binary.BigEndian, v)
}

func (v *KVKey) Fill(key []byte) error {
	return restruct.Unpack(key, binary.BigEndian, v)
}

// func KVToKey(tenantID int64, queue string, messageId int64, k string) []byte {
// 	// type + tenantid + queue + messageid + k
// 	rc := MessageToKey(tenantID, queue, messageId)

// 	rc[0] = byte(KV_KEYVALUE)
// 	slices.Concat(rc, []byte(k))

// 	return rc
// }

// func KeyToKV(key []byte) (tenantId int64, queue string, messageId int64, k string) {
// 	// Extract tenantId and queue
// 	// tenantId, queue = KeyToQueue(key)

// 	// Extract messageId
// 	messageId = int64(binary.BigEndian.Uint64(key[9+len(queue) : 17+len(queue)]))

// 	// Extract k
// 	k = string(key[17+len(queue):])

// 	return
// }
