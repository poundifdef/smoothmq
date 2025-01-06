package distributed

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/hashicorp/memberlist"
	zl "github.com/rs/zerolog/log"
)

type MyDelegate struct {
	Queue *DistributedQueue
}

type NodeData struct {
	Shard    string
	Replica  string
	RaftPort int
	GRPCPort int
}

func (d *MyDelegate) NodeMeta(limit int) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	data := NodeData{
		Shard:    d.Queue.config.Shard,
		Replica:  d.Queue.config.Replica,
		RaftPort: d.Queue.config.RaftPort,
		GRPCPort: d.Queue.config.GRPCPort,
	}
	if err := enc.Encode(data); err != nil {
		zl.Error().Err(err).Interface("data", data).Msg("Failed to encode node metadata")
		return []byte{}
	}
	return buf.Bytes()
}
func (d *MyDelegate) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}
func (d *MyDelegate) NotifyMsg(msg []byte) {
	// not use
}
func (d *MyDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}
func (d *MyDelegate) MergeRemoteState(buf []byte, join bool) {
	// not use
}

func (d *MyDelegate) NotifyJoin(node *memberlist.Node) {
	var data NodeData
	dec := gob.NewDecoder(bytes.NewBuffer(node.Meta))
	if err := dec.Decode(&data); err != nil {
		zl.Error().Err(err).Msg("Failed to decode node metadata")
		return
	}
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	zl.Debug().Str("join", hostPort).Interface("meta", data).Send()

	// for d.Queue.raft == nil {
	// 	zl.Print("raft is nil, waiting")
	// 	time.Sleep(250 * time.Millisecond)
	// }

	// zl.Print(d.Queue.raft.State())

	// if d.Queue.raft.State() == raft.Leader && data.Shard == d.Queue.config.Shard {
	// 	f := d.Queue.raft.AddVoter(
	// 		raft.ServerID(data.Replica),
	// 		raft.ServerAddress(fmt.Sprintf("%s:%d", node.Addr.To4().String(), data.RaftPort)), 0, 0)
	// 	zl.Debug().Err(f.Error()).Any("resp", f.Index()).Send()
	// }
}
func (d *MyDelegate) NotifyLeave(node *memberlist.Node) {
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	zl.Printf("leave %s", hostPort)
}
func (d *MyDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}
