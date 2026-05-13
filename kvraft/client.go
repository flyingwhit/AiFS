package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"infra/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	currLeader int
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// Start a goroutine to connect to server, each operation with its unique nrand
	// Call kvserver handler to handler coresponding op
	// Prepare args and reply
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// Make a new Clerk
	var err Err
	unum := nrand()
	reply := GetReply{}
	for err != "OK" {
		args := GetArgs{
			SeqId: unum,
			Key:   key,
		}
		ok := ck.servers[ck.currLeader].Call("KVServer.Get", &args, reply)
		if ok {
			ck.mu.Lock()
			err = reply.Err
			switch reply.Err {
			case ErrWrongLeader:
				ck.updateLeader(reply.LeaderId)
			case ErrNoKey:
				return ""
			case OK:
				break
			}
			ck.mu.Unlock()
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var err Err
	unum := nrand()
	reply := PutAppendReply{}
	for err != "OK" {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			UniqueNum: unum,
		}
		ok := ck.servers[ck.currLeader].Call("KVServer.PutAppend", &args, reply)
		if ok {
			ck.mu.Lock()
			err = reply.Err
			switch reply.Err {
			case ErrWrongLeader:
				ck.updateLeader(reply.LeaderId)
			case ErrNoKey:
			case OK:
				break
			}
			ck.mu.Unlock()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) updateLeader(leaderId int) {
	ck.currLeader = leaderId
}
