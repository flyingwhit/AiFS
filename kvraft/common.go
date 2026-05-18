package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrBadRequest  = "ErrBadRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	SeqId    int
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key      string
	SeqId    int
	ClientId int64
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

type DeleteArgs struct {
	Key      string
	SeqId    int
	ClientId int64
}

type DeleteReply struct {
	Err      Err
	LeaderId int
}
