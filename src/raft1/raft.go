package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

type Log struct {
	term    int
	index   int
	command string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	id          int
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat time.Time

	role RaftRole

	name string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	details := fmt.Sprintf("Requested vote %v from candidate %v", rf.me, args.CandidateId)
	tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))

	// Your code here (3A, 3B).
	term := args.Term

	reply.Term = rf.currentTerm

	if rf.me == args.CandidateId {
		reply.VoteGranted = true
		return
	}

	if term < rf.currentTerm {
		details := fmt.Sprintf("id %v term %v is less than currentTerm %v", rf.me, term, rf.currentTerm)
		tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = term

	if rf.votedFor == 0 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}

	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogIndex >= lastLog.index && args.LastLogTerm >= lastLog.term {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}

	tester.Annotate("RequestVote", "Vote not granted", fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
	reply.VoteGranted = false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) parseVotes(votes []bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Follower {
		return
	}

	n := len(rf.peers)
	count := 0
	for _, i := range votes {
		if i {
			count += 1
		}
	}
	if count > n/2 {
		// make leader
		details := fmt.Sprintf("Leader %v elected at term %v with votes %v", rf.me, rf.currentTerm, votes)
		tester.Annotate("Candidate -> Leader", details, "elected new leader")
		rf.role = Leader
		rf.name = "Jessica"
	} else {
		// split votes
		details := fmt.Sprintf("Split votes with count %v for leader %v elected at term %v, votes %v", count, rf.me, rf.currentTerm, votes)
		tester.Annotate("Split Votes", details, "Split votes")
	}
}

func (rf *Raft) startElection(ms int64) {
	// random duration
	tester.Annotate("Start Election -> Candidate", strconv.Itoa(rf.id), "Candidate selection")

	n := len(rf.peers)
	votes := make([]bool, n)

	rf.mu.Lock()
	rf.currentTerm += 1

	lastLog := rf.log[len(rf.log)-1]
	rf.role = Candidate
	rf.mu.Unlock()

	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func() {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.id,
				LastLogIndex: lastLog.index,
				LastLogTerm:  lastLog.term,
			}
			reply := RequestVoteReply{}
			defer wg.Done()
			for {
				ok := rf.sendRequestVote(i, &args, &reply)

				if ok {
					votes[i] = reply.VoteGranted
					break
				}

				time.Sleep(100 * time.Millisecond)
			}

		}()
	}

	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		rf.parseVotes(votes)
	case <-time.After(time.Duration(ms) * time.Millisecond):
		rf.parseVotes(votes)
	}
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	tester.Annotate("AppendEntries", strconv.Itoa(rf.id), fmt.Sprintf("append entries vote from leader %v", args.LeaderId))

	term := args.Term

	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm
	if term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.currentTerm = term

	if rf.role != Follower && args.LeaderId != rf.me {
		tester.Annotate("Candidate -> Follower", strconv.Itoa(rf.id), "converting to follower")
		rf.role = Follower
		reply.Success = false
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	n := len(rf.peers)

	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func() {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.id,
			}
			reply := AppendEntriesReply{}
			defer wg.Done()
			for {
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					break
				}

				time.Sleep(100 * time.Millisecond)
			}
		}()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) electionsTicker() {
	for true {
		_, isLeader := rf.GetState()

		if isLeader {
			continue
		}

		ms := 300 + (rand.Int63() % 150)
		now := time.Now()

		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		if now.Sub(lastHeartbeat).Milliseconds() > ms {
			rf.startElection(ms)
			continue
		}

		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeatsTicker() {
	for true {
		_, isLeader := rf.GetState()

		if !isLeader {
			// send out append entries calls
			continue
		}

		rf.sendHeartbeats()

		ms := 120
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		id:            me,
		role:          Follower,
		log:           []Log{{index: 0, term: 0, command: "init"}},
		lastHeartbeat: time.Now(),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionsTicker()
	go rf.heartbeatsTicker()

	return rf
}
