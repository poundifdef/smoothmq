package models

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
