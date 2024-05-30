package models

type MessageStatus uint8

const (
	MessageStatusQueued   MessageStatus = 1
	MessageStatusDequeued MessageStatus = 2
	// MessageStatusPaused   MessageStatus = 3
	// MessageStatusDeleted  MessageStatus = 4
)

type Message struct {
	ID        int64
	TenantID  int64
	DeliverAt int
	UpdatedAt int
	RequeueIn int
	Status    MessageStatus
	Message   []byte
	KeyValues map[string]string
}
