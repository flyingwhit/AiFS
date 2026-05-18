package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"infra/labgob"

	"infra/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistant state on all servers
	currLeader int
	currTerm   int
	votedFor   int
	state      int // 0:leader 1:candidate 2: follower
	logs       []LogEntry
	applyCh    chan ApplyMsg
	// presistant state on follower and candidate
	timer             time.Time
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currTerm, rf.state == 0
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("readPersist error\n")
	} else {
		rf.currTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.logs[rf.getPhysicalIndex(index)].Term
	newlogs := []LogEntry{
		{
			Term: rf.logs[rf.getPhysicalIndex(index)].Term,
		},
	}
	if rf.getPhysicalIndex(index)+1 < len(rf.logs) {
		newlogs = append(newlogs, rf.logs[rf.getPhysicalIndex(index+1):]...)
	}
	rf.logs = newlogs
	rf.lastIncludedIndex = index
	rf.lastApplied = max(index, rf.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type AppendEntriesReply struct {
	Success       bool
	Term          int
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currTerm

	if args.Term > rf.currTerm {
		rf.state = 2
		rf.votedFor = -1
		rf.currTerm = args.Term
		rf.persist()
	}

	reply.Term = rf.currTerm
	if args.Term < reply.Term {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastlog := rf.logs[rf.getPhysicalIndex(rf.lastLogicalIndex())]
		if args.LastLogTerm > lastlog.Term ||
			(args.LastLogTerm == lastlog.Term && args.LastLogIndex >= rf.lastLogicalIndex()) {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			rf.timer = time.Now()
			rf.state = 2
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currTerm {
		reply.Success = false
		reply.Term = rf.currTerm
		return
	}

	rf.state = 2
	rf.timer = time.Now()
	rf.currLeader = args.LeaderId

	if args.Term > rf.currTerm {
		rf.votedFor = -1
		rf.currTerm = args.Term
		rf.persist()
	}

	// Syncing logs
	if args.PrevLogIndex > rf.lastLogicalIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogicalIndex() + 1
		return
	}
	// handling old RPC, make sure rf.getPhysicalIndex(args.PrevLogIndex) to be valid
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currTerm
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// ???
	if args.PrevLogTerm != rf.logs[rf.getPhysicalIndex(args.PrevLogIndex)].Term {
		reply.Success = false
		reply.ConflictTerm = rf.logs[rf.getPhysicalIndex(args.PrevLogIndex)].Term
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.logs[rf.getPhysicalIndex(i)].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
			} else {
				break
			}
		}
		return
	}

	reply.Success = true

	// syncing logs, append Entries to local logs. Modifying original logs when
	// being different from leader entries.
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		currIndex := insertIndex + i
		if currIndex <= rf.lastLogicalIndex() {
			if rf.logs[rf.getPhysicalIndex(currIndex)].Term != entry.Term {
				rf.logs = rf.logs[:rf.getPhysicalIndex(currIndex)]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
		} else {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogicalIndex())
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// update snapshot, lastIncludedIndex, lastIncludedTerm, commitIndex(if need?)
	// and logs.

	rf.mu.Lock()
	// check args's term
	if rf.currTerm > args.Term {
		reply.Term = rf.currTerm
		rf.mu.Unlock()
		return
	}

	// set variables
	rf.state = 2
	rf.timer = time.Now()
	rf.currLeader = args.LeaderId

	// update term if finding higher term
	if args.Term > rf.currTerm {
		rf.currTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currTerm

	// Blocking old RPC
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	//
	hasFound := false // whether lastIncludedIndex exists in local logs
	splitIdx := -1    // where to cut
	for i := range rf.logs {
		if rf.getLogicalIndex(i) == args.LastIncludedIndex && rf.logs[i].Term == args.LastIncludedTerm {
			hasFound = true
			splitIdx = i
		}
	}
	if hasFound {
		rf.logs = append([]LogEntry{
			{
				Term: args.LastIncludedTerm,
			},
		}, rf.logs[splitIdx+1:]...)
	} else {
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	rf.currTerm = args.Term
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	snapshotIndex := rf.lastIncludedIndex
	snapshotTerm := rf.lastIncludedTerm

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	rf.mu.Unlock()

	// Inform server to update
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
	})
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != 0 {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	term = rf.currTerm
	index = rf.lastLogicalIndex() + 1
	newEntry := LogEntry{
		Term:    rf.currTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newEntry)
	rf.persist()
	rf.mu.Unlock()
	go rf.replicationLog()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		timeout := rf.randomElectionTimeout()
		time.Sleep(timeout)

		rf.mu.Lock()
		need := rf.state != 0 && time.Since(rf.timer) > timeout
		rf.mu.Unlock()

		if need {
			go rf.startElection()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currTerm:          0,
		commitIndex:       0,
		lastApplied:       0,
		votedFor:          -1,
		state:             2,
		timer:             time.Now(),
		logs:              []LogEntry{{Term: 0}},
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		applyCh:           applyCh,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	// Your initialization code here (2A, 2B, 2C).

	// goroutines to check if tester wait for receive, if do, return quickly
	// goroutines to checktime, sending requestVote

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	snapshot := persister.ReadSnapshot()
	rf.mu.Unlock()

	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
	if len(snapshot) > 0 {
		snapshotIndex := rf.lastIncludedIndex
		snapshotTerm := rf.lastIncludedTerm
		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotIndex: snapshotIndex,
				SnapshotTerm:  snapshotTerm,
			}
		}()
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg()

	return rf
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	timeout := time.Duration(500+rand.Intn(500)) * time.Millisecond
	return timeout
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.currTerm++
	rf.votedFor = rf.me
	rf.timer = time.Now()
	rf.state = 1
	startTerm := rf.currTerm
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogicalIndex(),
		LastLogTerm:  rf.logs[rf.getPhysicalIndex(rf.lastLogicalIndex())].Term,
	}
	rf.mu.Unlock()

	votes := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int, startTerm int) {
			reply := RequestVoteReply{}
			newargs := args
			rf.sendRequestVote(peer, &newargs, &reply)
			rf.mu.Lock()

			if rf.state != 1 || rf.currTerm != startTerm {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.mu.Unlock()
					rf.becomeLeader(startTerm)
					rf.mu.Lock()
				}
			}

			if reply.Term > rf.currTerm {
				// modify state currTerm votedFor
				rf.currTerm = reply.Term
				rf.state = 2
				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}(peer, startTerm)
	}

	// use global variable to sleep and wait for modification

}

func (rf *Raft) becomeLeader(electionTerm int) {
	rf.mu.Lock()
	if rf.state != 1 || electionTerm != rf.currTerm {
		rf.mu.Unlock()
		return
	}
	rf.state = 0
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.nextIndex[peer] = rf.lastLogicalIndex() + 1
	}
	rf.mu.Unlock()

	go rf.broadcastHeartbeat()
}

func (rf *Raft) broadcastHeartbeat() {
	timeout := time.Duration(80 * time.Millisecond)

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != 0 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.makeAppendEntry()
		time.Sleep(timeout)
	}
}

func (rf *Raft) makeAppendEntry() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			// check lagging peers
			if rf.nextIndex[peer] <= rf.lastIncludedIndex {
				rf.mu.Unlock()
				rf.sendInstallSnapshotToPeers(peer)
				return
			}
			entries := make([]LogEntry, len(rf.logs[rf.getPhysicalIndex(rf.nextIndex[peer]):]))
			copy(entries, rf.logs[rf.getPhysicalIndex(rf.nextIndex[peer]):])
			args := AppendEntriesArgs{
				Term:         rf.currTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				PrevLogTerm:  rf.logs[rf.getPhysicalIndex(rf.nextIndex[peer])-1].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			// respond logic
			if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
				rf.mu.Lock()
				if rf.currTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currTerm {
					rf.currTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.state = 2
					rf.timer = time.Now()
					rf.mu.Unlock()
					return
				}
				if rf.state != 0 {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.commitIndexUpdate()
				} else {
					if reply.ConflictTerm == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex
					} else {
						if reply.ConflictIndex < rf.lastIncludedIndex {
							rf.mu.Unlock()
							go rf.sendInstallSnapshotToPeers(peer)
							return
						}
						found := false
						for i := rf.getPhysicalIndex(rf.lastLogicalIndex()); i >= 0; i-- {
							if rf.logs[i].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = rf.getLogicalIndex(i) + 1
								found = true
								break
							}
						}
						if found == false {
							rf.nextIndex[peer] = reply.ConflictIndex
						}
					}
				}
				if rf.nextIndex[peer] < 1 {
					rf.nextIndex[peer] = 1
				}
				// send each follower entries started from nextIndex[peer]
				// index before commitIndex[peer] means the index has been replicated
				// find a N,(as high as possible) exists in more than half in list
				rf.mu.Unlock()
			}
		}(peer)
	}
}

func (rf *Raft) commitIndexUpdate() {
	for N := rf.lastLogicalIndex(); N > rf.commitIndex; N-- {
		if rf.logs[rf.getPhysicalIndex(N)].Term != rf.currTerm {
			continue
		}

		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count = count + 1
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			return
		}
	}
}

func (rf *Raft) replicationLog() {
	rf.mu.Lock()
	if rf.state != 0 {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.makeAppendEntry()
}

func (rf *Raft) sendApplyMsg() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++

			if rf.lastApplied > rf.lastLogicalIndex() {
				rf.lastApplied--
				break
			}

			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getPhysicalIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()

			rf.applyCh <- msg

			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) sendInstallSnapshotToPeers(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	if ok := rf.sendInstallSnapshot(peer, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currTerm {
			rf.currTerm = reply.Term
			rf.state = 2
			rf.votedFor = -1
			rf.persist()
			rf.timer = time.Now()
			return
		}
		if rf.state != 0 || rf.currTerm != args.Term {
			return
		}
		rf.matchIndex[peer] = max(args.LastIncludedIndex, rf.matchIndex[peer])
		rf.nextIndex[peer] = max(rf.matchIndex[peer]+1, rf.nextIndex[peer])
	}
}

func (rf *Raft) Persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) getPhysicalIndex(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex
}

func (rf *Raft) getLogicalIndex(physicalIndex int) int {
	return physicalIndex + rf.lastIncludedIndex
}

func (rf *Raft) lastLogicalIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
