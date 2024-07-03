package models

import (
	"encoding/base64"
	"encoding/json"
)

type MessageStatus uint8

const (
	MessageStatusQueued   MessageStatus = 1
	MessageStatusDequeued MessageStatus = 2
	// MessageStatusPaused   MessageStatus = 3
	// MessageStatusDeleted  MessageStatus = 4
)

type Message struct {
	ID        int64         `db:"id"`
	TenantID  int64         `db:"tenant_id"`
	DeliverAt int           `db:"deliver_at"`
	UpdatedAt int           `db:"updated_at"`
	RequeueIn int           `db:"requeue_in"`
	Status    MessageStatus `db:"status"`
	Message   []byte        `db:"message"`

	// Only used for DB
	QueueID int64 `db:"queue_id"`

	KeyValues map[string]string
}

func (m Message) Base64Decode() []byte {
	data, err := base64.StdEncoding.DecodeString(string(m.Message))
	if err != nil {
		return m.Message
	}

	return data
}

func (m Message) IsJSON() bool {
	return json.Valid(m.Message)
}
