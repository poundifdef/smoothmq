package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type MessageStatus uint8

const (
	MessageStatusQueued   MessageStatus = 1
	MessageStatusDequeued MessageStatus = 2
	// MessageStatusPaused   MessageStatus = 3
	// MessageStatusDeleted  MessageStatus = 4
)

func (s MessageStatus) String() string {
	switch s {
	case MessageStatusQueued:
		return "Queued"
	case MessageStatusDequeued:
		return "Dequeued"
	}

	return fmt.Sprintf("%d", s)
}

type Message struct {
	ID       int64 `db:"id"`
	TenantID int64 `db:"tenant_id"`
	QueueID  int64 `db:"queue_id"`

	DeliverAt   int `db:"deliver_at"`
	DeliveredAt int `db:"delivered_at"`
	Tries       int `db:"tries"`
	MaxTries    int `db:"max_tries"`
	RequeueIn   int `db:"requeue_in"`

	Status MessageStatus `db:"status"`

	Message   []byte `db:"message"`
	KeyValues map[string]string
}

func (m *Message) IsB64() bool {
	_, err := base64.StdEncoding.DecodeString(string(m.Message))
	return err == nil
}

func (m *Message) Base64Decode() []byte {
	data, err := base64.StdEncoding.DecodeString(string(m.Message))
	if err != nil {
		return m.Message
	}

	return data
}

func (m *Message) IsJSON() bool {
	return json.Valid(m.Message)
}
