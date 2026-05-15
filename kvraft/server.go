package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"infra/labgob"
	"infra/labrpc"
	"infra/raft"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	SeqId    int
	ClientId int64
}

type OpReply struct {
	Err   Err
	Value string
}

type LastOpRecord struct {
	LastSeqId int
	LastReply OpReply
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	notifyChans  map[int]chan OpReply
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	commitIndex  int
	leaderId     int
	kvStore      map[string]string
	recordMap    map[int64]LastOpRecord
	persister    *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make a command

	if !kv.killed() {
		command := Op{
			Type:     "Get",
			Key:      args.Key,
			SeqId:    args.SeqId,
			ClientId: args.ClientId,
		}
		kv.mu.Lock()
		if record, ok := kv.recordMap[args.ClientId]; ok && args.SeqId <= record.LastSeqId {
			reply.Value = record.LastReply.Value
			reply.Err = record.LastReply.Err
			kv.mu.Unlock()
			return
		}

		index, startTerm, isLeader := kv.rf.Start(command)
		kv.notifyChans[index] = make(chan OpReply, 1)

		if !isLeader {
			reply.LeaderId = kv.leaderId
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		// build a channel using index, then wait OpReply from channel
		kv.mu.Unlock()

		select {
		case opreply := <-kv.notifyChans[index]:
			currTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if !isLeader || currTerm != startTerm {
				reply.Err = ErrWrongLeader
			} else {
				reply.Value = opreply.Value
				reply.Err = opreply.Err
				kv.leaderId = kv.me
			}
			kv.mu.Unlock()
		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.killed() {

		command := Op{
			Type:     args.Op,
			Key:      args.Key,
			Value:    args.Value,
			SeqId:    args.SeqId,
			ClientId: args.ClientId,
		}

		kv.mu.Lock()

		if record, ok := kv.recordMap[args.ClientId]; ok && args.SeqId <= record.LastSeqId {
			reply.Err = record.LastReply.Err
			kv.mu.Unlock()
			return
		}

		idx, startTerm, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.LeaderId = kv.leaderId
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		kv.notifyChans[idx] = make(chan OpReply, 1)
		kv.mu.Unlock()

		select {
		case opreply := <-kv.notifyChans[idx]:
			currTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if !isLeader || currTerm != startTerm {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = opreply.Err
				kv.leaderId = kv.me
			}
			kv.mu.Unlock()
		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		kv.mu.Lock()
		delete(kv.notifyChans, idx)
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvStore = make(map[string]string)
	kv.notifyChans = make(map[int]chan OpReply)
	kv.commitIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.recordMap = make(map[int64]LastOpRecord)
	kv.persister = persister
	labgob.Register(Op{})
	labgob.Register(OpReply{})
	labgob.Register(LastOpRecord{})

	// restore snapshot when rebooting, if len(snapshot) > 0
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.restoreSnapshot(snapshot)
	}

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	// Listen msg from kv.applyCh
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// Apply cmd from chan one by one, each time take a cmd
			// send it to coresponding handler. set index to check
			// whether get can be applied
			cmd := msg.Command.(Op)
			idx := msg.CommandIndex

			var reply OpReply

			kv.mu.Lock()
			kv.commitIndex = msg.CommandIndex
			if record, ok := kv.recordMap[cmd.ClientId]; ok && record.LastSeqId >= cmd.SeqId {
				reply = record.LastReply
			} else {
				reply = kv.applyToStateMachine(cmd)
			}

			kv.recordMap[cmd.ClientId] = LastOpRecord{
				LastSeqId: cmd.SeqId,
				LastReply: reply,
			}

			// check whether need to snapshot or not
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvStore)
				e.Encode(kv.recordMap)
				data := w.Bytes()
				kv.mu.Unlock()
				kv.rf.Snapshot(kv.commitIndex, data)
				kv.mu.Lock()
			}

			if notifyChan, ok := kv.notifyChans[idx]; ok {
				notifyChan <- reply
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// msg is a snapshot
			// check snapshot index to dicide whether to update
			kv.mu.Lock()
			if msg.SnapshotIndex > kv.commitIndex {
				kv.commitIndex = msg.SnapshotIndex
				kv.restoreSnapshot(msg.Snapshot)

				var toDelete []int
				for idx, ch := range kv.notifyChans {
					select {
					case ch <- OpReply{Err: ErrWrongLeader}:
						toDelete = append(toDelete, idx)
					default:
					}
				}
				for _, idx := range toDelete {
					delete(kv.notifyChans, idx)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyToStateMachine(cmd Op) OpReply {
	var reply OpReply
	switch cmd.Type {
	case "Get":
		reply = kv.getHandler(cmd)
	case "Put":
		reply = kv.putHandler(cmd)
	case "Append":
		reply = kv.appendHandler(cmd)
	}
	return reply
}

func (kv *KVServer) getHandler(cmd Op) OpReply {
	// handle cmd to state machine if it's leader
	// return the result
	// otherwise(dead, leader change) return err
	var reply OpReply
	if val, ok := kv.kvStore[cmd.Key]; ok {
		reply = OpReply{
			Value: val,
			Err:   OK,
		}
	} else {
		reply = OpReply{
			Err: ErrNoKey,
		}
	}
	return reply
}

func (kv *KVServer) putHandler(cmd Op) OpReply {
	var reply OpReply
	kv.kvStore[cmd.Key] = cmd.Value
	reply = OpReply{
		Err: OK,
	}
	return reply
}
func (kv *KVServer) appendHandler(cmd Op) OpReply {
	var reply OpReply
	kv.kvStore[cmd.Key] += cmd.Value
	reply = OpReply{
		Err: OK,
	}
	return reply
}

func (kv *KVServer) restoreSnapshot(data []byte) {
	// Decode data from msg
	// and restore recordMap and kvstore
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var recordMap map[int64]LastOpRecord
	var kvStore map[string]string
	if d.Decode(&kvStore) != nil || d.Decode(&recordMap) != nil {
		fmt.Printf("restore snapshot error\n")
	} else {
		kv.kvStore = kvStore
		kv.recordMap = recordMap
	}

}
