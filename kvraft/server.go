package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"infra/labgob"
	"infra/labrpc"
	"infra/raft"
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
	Type  string
	Key   string
	Value string
	SeqId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	commitIndex int
	leaderId    int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make a command

	for !kv.killed() {
		command := Op{
			Type:  "Get",
			Key:   args.Key,
			SeqId: args.SeqId,
		}
		_, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.LeaderId = kv.leaderId
			reply.Err = ErrWrongLeader
			return
		}

		// start a goroutine to wait for commitment

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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

	// You may need initialization code here.
	kv.commitIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applier() {
	// Listen msg from kv.applyCh
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// Apply cmd from chan one by one, each time take a cmd
			// send it to coresponding handler. set index to check
			// whether get can be applied
			kv.commitIndex = msg.CommandIndex
			cmd := msg.Command.(Op)
			switch cmd.Type {
			case "Get":
				kv.getHandler()
			case "Put":
				kv.putHandler()
			case "Append":
				kv.appendHandler()
			}
		}
	}
}

func (kv *KVServer) getHandler() {

}

func (kv *KVServer) putHandler() {

}
func (kv *KVServer) appendHandler() {

}
