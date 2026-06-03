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
	"log"
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
	Term    int
	Index   int
	Command any
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

	ch chan raftapi.ApplyMsg
}

func (rf *Raft) serverLog(v ...any) {
	log.Printf("Server %d: %v\n", rf.me, v)
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
	details := fmt.Sprintf("Requested vote for server %v from candidate %v", rf.me, args.CandidateId)
	rf.serverLog(details)
	tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))

	// Your code here (3A, 3B).
	term := args.Term

	reply.Term = rf.currentTerm

	if rf.me == args.CandidateId {
		rf.serverLog("Vote granted because requested itself")
		reply.VoteGranted = true
		return
	}

	if term < rf.currentTerm {
		details := fmt.Sprintf("id %v term %v is less than currentTerm %v", rf.me, term, rf.currentTerm)
		tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
		reply.VoteGranted = false
		rf.serverLog("Vote denied from server", rf.me, "candidate term", term, "<", "current term", rf.currentTerm)
		return
	}

	rf.currentTerm = term

	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.serverLog("Vote granted because voted for is -1")
		return
	}

	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogIndex >= lastLog.Index && args.LastLogTerm >= lastLog.Term {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.serverLog("Vote granted because server log is up to date")
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
		rf.serverLog(details)
		rf.role = Leader
		rf.name = "Jessica"

		nextIndex := make([]int, n)
		for i := range n {
			nextIndex[i] = len(rf.log)
		}

		rf.nextIndex = nextIndex
	} else {
		// split votes
		details := fmt.Sprintf("Split votes with count %v for leader %v elected at term %v, votes %v", count, rf.me, rf.currentTerm, votes)
		tester.Annotate("Split Votes", details, "Split votes")
	}
}

func (rf *Raft) startElection(ms int64) {
	// random duration
	tester.Annotate("Start Election -> Candidate", strconv.Itoa(rf.me), "Candidate selection")

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
				CandidateId:  rf.me,
				LastLogIndex: lastLog.Index,
				LastLogTerm:  lastLog.Term,
			}
			var reply RequestVoteReply
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
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.LeaderId == rf.me {
		reply.Success = true
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := args.Term

	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm

	if term < rf.currentTerm {
		details := fmt.Sprintf("leader term %v < server currentTerm %v for leader %v, server %v", term, rf.currentTerm, args.LeaderId, rf.me)
		rf.serverLog(details)
		reply.Success = false
		return
	}

	rf.currentTerm = term

	if rf.role != Follower {
		tester.Annotate("Candidate -> Follower", strconv.Itoa(rf.me), "converting to follower")
		rf.role = Follower
		reply.Success = true
		return
	}

	prevLogIndex := args.PrevLogIndex
	if prevLogIndex >= len(rf.log) {
		details := fmt.Sprintf("Index %v not in log for follower %v", prevLogIndex, rf.me)
		rf.serverLog(details)
		tester.Annotate("Index in log", details, "")
		reply.Success = false
		return
	}

	prevLogTerm := args.PrevLogTerm
	if rf.log[prevLogIndex].Term != prevLogTerm {
		details := fmt.Sprintf("Term %v not in log for follower %v at index %v", prevLogTerm, rf.me, prevLogIndex)
		rf.serverLog(details)
		tester.Annotate("Term in log", details, "")
		reply.Success = false
		return
	}

	if len(args.Entries) > 0 {
		rf.serverLog("Adding entries", args.Entries, "to server", rf.me, "from leader", args.LeaderId)

		details := fmt.Sprintf("Appending entries %v to server %v from leader %v", args.Entries, rf.me, args.LeaderId)
		tester.Annotate("AppendEntries", details, fmt.Sprintf("append entries vote from leader %v", args.LeaderId))
	}

	for i, _log := range args.Entries {
		index := i + prevLogIndex + 1
		if index >= len(rf.log) {
			rf.log = append(rf.log, Log{Term: args.Term, Index: index, Command: _log.Command})
			rf.serverLog("Appending command", _log.Command, "at index", index, "for server", rf.me)
			// rf.ch <- raftapi.ApplyMsg{CommandValid: false, Command: _log.Command, CommandIndex: index}
			continue
		}

		if rf.log[index].Term != args.Term {
			rf.log[index] = Log{Term: _log.Term, Index: index, Command: _log.Command}
			rf.serverLog("Replacing command", _log.Command, "at index", index, "for server", rf.me)
			// rf.ch <- raftapi.ApplyMsg{CommandValid: false, Command: _log.Command, CommandIndex: index}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]

			rf.serverLog("Committing command", entry.Command, "for server", rf.me)
			rf.ch <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
		}
	}

	if len(args.Entries) > 0 {
		rf.serverLog("Log for server", rf.me, rf.log)
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	n := len(rf.peers)
	prevIndex := len(rf.log) - 1

	for i := range n {
		go func() {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log[prevIndex].Term,
				Entries:      []Log{},
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
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

func (rf *Raft) sendCommand(command interface{}, commandIndex int) {
	n := len(rf.peers)

	done := make(chan struct{}, n)
	k := n/2 + 1

	finished := make([]int, n)
	for i := range n {
		if i == rf.me {
			finished[i] = 1
			continue
		}
		finished[i] = -1
	}

	var finishedLock sync.Mutex

	newTerm := -1

	for i := range n {
		go func() {
			if i == rf.me {
				done <- struct{}{}
				return
			}

			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			prevIndex := nextIndex - 1

			entries := rf.log[nextIndex:]
			// rf.serverLog"Leader", rf.me, "log", rf.log, "nextIndex", nextIndex, "for server", i)

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log[prevIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()

			for {
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					if reply.Success {
						finishedLock.Lock()
						finished[i] = len(args.Entries)
						finishedLock.Unlock()
						break
					}

					rf.serverLog("Leader", rf.me, "term", rf.currentTerm, "follower", i, "term", reply.Term)

					if reply.Term > rf.currentTerm {
						// convert to follower
						finishedLock.Lock()
						newTerm = reply.Term
						finishedLock.Unlock()
						break
					}

					rf.mu.Lock()
					rf.serverLog("decrementing nextIndex", nextIndex, "for server", i, "log", rf.log)

					nextIndex -= 1
					prevIndex := nextIndex - 1
					args.PrevLogIndex = prevIndex
					args.PrevLogTerm = rf.log[prevIndex].Term
					args.Term = rf.currentTerm
					args.Entries = rf.log[nextIndex:]

					rf.serverLog("Entries after decrement", entries, "with command", command)
					rf.mu.Unlock()
				}

				time.Sleep(10 * time.Millisecond)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < k; i++ {
		<-done
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if newTerm != -1 {
		rf.serverLog("Converting leader", rf.me, "to follower")
		rf.role = Follower
		rf.currentTerm = newTerm
		return
	}

	finishedLock.Lock()
	count := 0
	for i := range n {
		if finished[i] != -1 {
			rf.nextIndex[i] += finished[i]
			rf.matchIndex[i] += finished[i]
			count += 1
		}
	}
	finishedLock.Unlock()

	if count >= k {
		rf.commitIndex = max(rf.commitIndex, commandIndex)

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]

			rf.serverLog("Committing command", entry.Command, "to leader", rf.me)

			rf.ch <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
		}
		return
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
	rf.mu.Lock()
	isLeader = rf.role == Leader
	rf.mu.Unlock()

	if !isLeader {
		return index, term, false
	}

	details := fmt.Sprintf("Sending command from leader %v", rf.me)
	tester.Annotate("Start", details, fmt.Sprintf("Sending command %v", command))

	rf.mu.Lock()
	term = rf.currentTerm
	index = len(rf.log)
	rf.log = append(rf.log, Log{Index: index, Term: term, Command: command})
	rf.mu.Unlock()

	rf.serverLog("Starting with leader", rf.me, "command", command, "leader log", rf.log, "nextIndex", rf.nextIndex)

	go rf.sendCommand(command, index)

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
	nextIndex := make([]int, len(peers))
	for i := range len(peers) {
		nextIndex[i] = 1
	}

	matchIndex := make([]int, len(peers))
	rf := &Raft{
		id:            me,
		role:          Follower,
		log:           []Log{{Index: 0, Term: 0, Command: "first"}},
		lastHeartbeat: time.Now(),
		nextIndex:     nextIndex,
		matchIndex:    matchIndex,
		ch:            applyCh,
		votedFor:      -1,
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
