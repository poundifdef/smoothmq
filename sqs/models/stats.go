package models

type QueueStats struct {
	Counts        map[MessageStatus]int
	TotalMessages int

	// TODO: Sliding window of queue rates
	// ProduceRate float64
	// ConsumeRate float64
}
