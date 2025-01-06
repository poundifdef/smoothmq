package distributed

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	zl "github.com/rs/zerolog/log"
)

type DistributedQueue struct {
	queue  models.Queue
	config config.DistributedConfig

	grpcs map[string]string

	memberlist *memberlist.Memberlist
	raft       *raft.Raft
}

func (q *DistributedQueue) initMemberlist() error {
	d := &MyDelegate{Queue: q}
	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindPort = q.config.MemberlistPort
	memberlistConfig.AdvertisePort = memberlistConfig.BindPort
	memberlistConfig.Delegate = d
	memberlistConfig.Events = d
	memberlistConfig.Name = q.config.Replica

	list, err := memberlist.Create(memberlistConfig)
	q.memberlist = list

	q.memberlist.Join(q.config.Join)

	return err
}

func (q *DistributedQueue) initRaft() error {
	store, err := raftboltdb.NewBoltStore(q.config.Replica + "/bolt.boltdb")
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(q.config.Replica+"/snapshot", 2, os.Stderr)
	if err != nil {
		return err
	}

	hostname := fmt.Sprintf("localhost:%d", q.config.RaftPort)

	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(hostname, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(q.config.Replica)

	fsm := FSM{queue: q}
	r, err := raft.NewRaft(raftCfg, fsm, store, store, snapshots, transport)
	if err != nil {
		return err
	}

	obsChan := make(chan raft.Observation)

	go func() {
		for o := range obsChan {
			zl.Debug().Interface("obs", o).Msg("OBSERVATION")
		}
	}()

	observer := raft.NewObserver(obsChan, true, func(o *raft.Observation) bool {
		return true
	})
	r.RegisterObserver(observer)

	q.raft = r

	numServers := len(r.GetConfiguration().Configuration().Servers)
	if q.config.BootstrapShard && numServers == 0 {
		strapped := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(q.config.Replica),
					Address: transport.LocalAddr(),
				},
			},
		})

		if strapped.Error() != nil {
			return strapped.Error()
		}
	}

	return nil
}

func (q *DistributedQueue) processCmdline() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := scanner.Text()
		switch cmd {
		case "leader":
			addr, id := q.raft.LeaderWithID()
			zl.Info().Str("addr", string(addr)).Str("id", string(id)).Msg("Current leader")
		case "state":
			state := q.raft.State()
			zl.Info().Str("state", state.String()).Msg("Current state")
		case "stats":
			stats := q.raft.Stats()
			zl.Info().Interface("stats", stats).Msg("Raft stats")
		// case "grpc":
		// zl.Info().Interface("grpcs", rc.grpcs).Msg("GRPC mappings")
		case "m":
			for _, member := range q.memberlist.Members() {
				zl.Printf("Member: %s %s ", member.Name, member.Addr)
			}
		default:
			// add 1 localhost:4001 localhost:6001
			// add 2 localhost:4002 localhost:6002
			// add 3 localhost:4003 localhost:6003
			if strings.Contains(cmd, "add") {
				tokens := strings.Split(cmd, " ")

				msg := []byte(cmd)

				rc := q.raft.Apply(msg, 1*time.Second)
				zl.Debug().Err(rc.Error()).Any("resp", rc.Response()).Send()

				msgSelf := []byte(fmt.Sprintf("add %s localhost:%d localhost:%d", q.config.Replica, q.config.RaftPort, q.config.GRPCPort))
				rc = q.raft.Apply(msgSelf, 1*time.Second)
				zl.Debug().Err(rc.Error()).Any("resp", rc.Response()).Msg("reporting self")

				f := q.raft.AddVoter(raft.ServerID(tokens[1]), raft.ServerAddress(tokens[2]), 0, 0)
				zl.Debug().Err(f.Error()).Any("resp", f.Index()).Send()

			} else {
				zl.Warn().Str("command", cmd).Msg("Unknown command")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		zl.Error().Err(err).Msg("Error reading stdin")
	}
}

func NewDistributedQueue(config config.DistributedConfig, backingQueue models.Queue) *DistributedQueue {
	rc := &DistributedQueue{config: config, queue: backingQueue, grpcs: make(map[string]string)}

	go rc.processCmdline()

	err := rc.initRaft()
	if err != nil {
		zl.Panic().Err(err).Msg("Could not initialize raft")
	}

	// err = rc.initMemberlist()
	// if err != nil {
	// 	zl.Panic().Err(err).Msg("Could not initialize memberlist")
	// }

	// go func() {
	// 	scanner := bufio.NewScanner(os.Stdin)
	// 	for scanner.Scan() {
	// 		cmd := scanner.Text()
	// 		switch cmd {
	// 		case "m":
	// 			for _, member := range list.Members() {
	// 				zl.Printf("Member: %s %s %s", member.Name, member.Addr, string(member.Meta))
	// 			}
	// 		}
	// 	}
	// }()

	// return rc
	// store, err := raftboltdb.NewBoltStore(config.Replica + "/bolt.boltdb")
	// if err != nil {
	// 	log.Panic(err)
	// }

	// snapshots, err := raft.NewFileSnapshotStore(config.Replica+"/snapshot", 2, os.Stderr)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// hostname := fmt.Sprintf("localhost:%d", config.RaftPort)

	// tcpAddr, err := net.ResolveTCPAddr("tcp", hostname)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// transport, err := raft.NewTCPTransport(hostname, tcpAddr, 10, time.Second*10, os.Stderr)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// raftCfg := raft.DefaultConfig()
	// raftCfg.LocalID = raft.ServerID(config.Replica)

	// fsm := FSM{queue: rc}
	// r, err := raft.NewRaft(raftCfg, fsm, store, store, snapshots, transport)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// go func() {
	// 	zl.Print("Observing...")
	// 	for observation := range obsChan {
	// 		zl.Debug().Interface("data", observation.Data).Msg("raft observation")

	// 		if leaderObv, ok := observation.Data.(raft.LeaderObservation); ok {
	// 			if leaderObv.LeaderID == raft.ServerID(config.Replica) {
	// 				msg := []byte(fmt.Sprintf("add %s localhost:%d localhost:%d", config.Replica, config.RaftPort, config.GRPCPort))
	// 				rc := r.Apply(msg, 1*time.Second)
	// 				zl.Print(rc.Error())
	// 				zl.Print(rc.Response())

	// 			}
	// 		}
	// 	}
	// }()

	// strapped := r.BootstrapCluster(raft.Configuration{
	// 	Servers: []raft.Server{
	// 		{
	// 			ID:      raft.ServerID(config.Replica),
	// 			Address: transport.LocalAddr(),
	// 		},
	// 	},
	// })

	// rc.raft = r

	// // r.GetConfiguration().Configuration().Servers[0].Suffrage

	// zl.Print(strapped.Error())

	// d := &MyDelegate{Queue: rc}
	// memberlistConfig := memberlist.DefaultWANConfig()
	// memberlistConfig.BindPort = config.MemberlistPort
	// memberlistConfig.AdvertisePort = memberlistConfig.BindPort
	// memberlistConfig.Delegate = d
	// memberlistConfig.Events = d
	// memberlistConfig.Name = config.Replica

	// list, err := memberlist.Create(memberlistConfig)
	// if err != nil {
	// 	panic("Failed to create memberlist: " + err.Error())
	// }

	// n, err := list.Join(config.Join)
	// if err != nil {
	// 	panic("Failed to join cluster: " + err.Error())
	// }
	// zl.Print(n)

	// zl.Print(list.NumMembers())

	// zl.Debug().Interface("local", list.LocalNode()).Send()

	// r.AddNonvoter()
	// r.AddVoter()
	// time.Sleep(5 * time.Second)
	// if config.Replica != "1" {
	// 	f := r.AddVoter(raft.ServerID("3"), raft.ServerAddress("localhost:4001"), 0, 0)
	// 	zl.Error().Err(f.Error()).Send()
	// }

	// time.Sleep(5 * time.Second)
	// lAddr, lId := r.LeaderWithID()
	// zl.Print(lAddr)
	// zl.Print(lId)

	return rc
}

func (q *DistributedQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	zl.Print(q.raft.State())
	msg := []byte("create")

	zl.Debug().Interface("stats", q.raft.Stats()).Send()

	if q.raft.State() == raft.Leader {
		rc := q.raft.Apply(msg, 1*time.Second)
		// error if there was a raft error
		zl.Debug().Err(rc.Error()).Send()
		// this is the function response - could be an error or not, application error, have to check the type
		zl.Debug().Any("resp", rc.Response()).Send()
	} else {

	}

	return nil
}

func (q *DistributedQueue) UpdateQueue(tenantId int64, queue string, properties models.QueueProperties) error {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) DeleteQueue(tenantId int64, queue string) error {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) ListQueues(tenantId int64) ([]string, error) {
	return []string{"a"}, nil
}

func (q *DistributedQueue) Enqueue(tenantId int64, queue string, message string, kv map[string]string, delay int) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) Dequeue(tenantId int64, queue string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) Peek(tenantId int64, queue string, messageId int64) *models.Message {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) Stats(tenantId int64, queue string) models.QueueStats {
	return models.QueueStats{}
}

func (q *DistributedQueue) Filter(tenantId int64, queue string, filterCriteria models.FilterCriteria) []int64 {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) Delete(tenantId int64, queue string, messageId int64) error {
	panic("not implemented") // TODO: Implement
}

func (q *DistributedQueue) Shutdown() error {
	return nil
}
