package distributed

import (
	"io"

	"github.com/hashicorp/raft"
	zl "github.com/rs/zerolog/log"
)

type FSM struct {
	queue *DistributedQueue
}

// Apply is called once a log entry is committed by a majority of the cluster.
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f FSM) Apply(raftLog *raft.Log) any {
	msg := string(raftLog.Data)
	zl.Debug().Interface("log", raftLog).Str("msg", msg).Msg("applying raft message")

	// if strings.Contains(msg, "add") {
	// 	tokens := strings.Split(msg, " ")
	// 	f.queue.grpcs[tokens[1]] = tokens[3]
	// }

	return "applied raft message"
	// return errors.New("error applying")
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (f FSM) Snapshot() (raft.FSMSnapshot, error) {
	panic("not implemented") // TODO: Implement
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (f FSM) Restore(snapshot io.ReadCloser) error {
	panic("not implemented") // TODO: Implement
}
