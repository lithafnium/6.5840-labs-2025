package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type PersistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Log
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	persistedState PersistedState

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
	// log.Printf("Server %d: %v\n", rf.me, v)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.persistedState.CurrentTerm
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.persistedState)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ps PersistedState

	if d.Decode(&ps) != nil {
		log.Panicf("Failed to load state for server %v", rf.me)
		return
	} else {
		rf.persistedState = ps
	}
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
	// tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
	reply.VoteGranted = false

	// Your code here (3A, 3B).

	reply.Term = rf.persistedState.CurrentTerm

	if rf.me == args.CandidateId {
		rf.serverLog("Vote granted because requested itself")
		reply.VoteGranted = true
		return
	}

	if args.Term < rf.persistedState.CurrentTerm {
		// details := fmt.Sprintf("id %v term %v is less than currentTerm %v", rf.me, term, rf.persistedState.CurrentTerm)
		// tester.Annotate("RequestVote", details, fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
		rf.serverLog("Vote denied from server", rf.me, "candidate term", args.Term, "<", "current term", rf.persistedState.CurrentTerm)
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(args.Term)
		rf.persist()
	}

	reply.Term = rf.persistedState.CurrentTerm

	lastLog := rf.persistedState.Log[len(rf.persistedState.Log)-1]
	upToDate := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if upToDate && (rf.persistedState.VotedFor == -1 || rf.persistedState.VotedFor == args.CandidateId) {
		rf.persistedState.VotedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() // reset election timer when granting vote
	}

	// tester.Annotate("RequestVote", "Vote not granted", fmt.Sprintf("requested vote from candidate %v", args.CandidateId))
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.role = Candidate
	rf.persistedState.CurrentTerm++
	termStarted := rf.persistedState.CurrentTerm
	rf.persistedState.VotedFor = rf.me

	rf.persist()

	rf.lastHeartbeat = time.Now()

	lastLog := rf.persistedState.Log[len(rf.persistedState.Log)-1]

	rf.mu.Unlock()

	votes := 1
	majority := len(rf.peers)/2 + 1

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLog.Index,
				LastLogTerm:  lastLog.Term,
			}

			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Candidate || rf.persistedState.CurrentTerm != termStarted {
				return
			}

			if reply.Term > rf.persistedState.CurrentTerm {
				rf.demoteLocked(reply.Term)
				rf.persist()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority && rf.role == Candidate {
					rf.role = Leader

					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.persistedState.Log)
						rf.matchIndex[i] = 0
					}

					rf.persist()

					// Optional but useful: send heartbeats immediately.
					go rf.sendHeartbeats()
				}
			}
		}(server)
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

	XTerm  int // term of the conflicting entry, or -1
	XIndex int // first index leader should try
}

// Assumes lock is held
func (rf *Raft) demoteLocked(term int) error {
	rf.persistedState.CurrentTerm = term
	rf.persistedState.VotedFor = -1
	rf.role = Follower

	return nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.persistedState.CurrentTerm

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(args.Term)
	}

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.persistedState.CurrentTerm

	if rf.role != Follower {
		rf.role = Follower
	}

	prevLogIndex := args.PrevLogIndex
	if prevLogIndex >= len(rf.persistedState.Log) {
		details := fmt.Sprintf("Index %v not in log for follower %v", prevLogIndex, rf.me)
		rf.serverLog(details)
		tester.Annotate("Index in log", details, "")
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = len(rf.persistedState.Log)
		return
	}

	prevLogTerm := args.PrevLogTerm
	if rf.persistedState.Log[prevLogIndex].Term != prevLogTerm {
		details := fmt.Sprintf("Term %v not in log for follower %v at index %v", prevLogTerm, rf.me, prevLogIndex)
		rf.serverLog(details)
		tester.Annotate("Term in log", details, "")
		reply.Success = false

		conflictTerm := rf.persistedState.Log[prevLogIndex].Term
		// walk back to the FIRST index storing that term
		i := prevLogIndex
		for i > 0 && rf.persistedState.Log[i-1].Term == conflictTerm {
			i--
		}
		reply.XTerm = conflictTerm
		reply.XIndex = i

		return
	}

	if len(args.Entries) > 0 {
		rf.serverLog("Adding entries", args.Entries, "to server", rf.me, "from leader", args.LeaderId)

		details := fmt.Sprintf("Appending entries %v to server %v from leader %v", args.Entries, rf.me, args.LeaderId)
		tester.Annotate("AppendEntries", fmt.Sprintf("append entries vote from leader %v", args.LeaderId), details)
	}

	for i, _log := range args.Entries {
		index := i + prevLogIndex + 1
		if index >= len(rf.persistedState.Log) {
			rf.persistedState.Log = append(rf.persistedState.Log, Log{Term: _log.Term, Index: index, Command: _log.Command})
			rf.serverLog("Appending command", _log.Command, "at index", index, "for server", rf.me)
			// rf.ch <- raftapi.ApplyMsg{CommandValid: false, Command: _log.Command, CommandIndex: index}
			continue
		}

		if rf.persistedState.Log[index].Term != _log.Term {
			// truncate everything after
			rf.persistedState.Log = rf.persistedState.Log[:index]

			rf.persistedState.Log = append(rf.persistedState.Log, Log{Term: _log.Term, Index: index, Command: _log.Command})

			// rf.persistedState.Log[index] = Log{Term: _log.Term, Index: index, Command: _log.Command}
			rf.serverLog("Replacing command", _log.Command, "at index", index, "for server", rf.me)
			// rf.ch <- raftapi.ApplyMsg{CommandValid: false, Command: _log.Command, CommandIndex: index}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.persistedState.Log[rf.lastApplied]

			rf.serverLog("Committing command", entry.Command, "to server", rf.me)
			rf.ch <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
		}
	}

	if len(args.Entries) > 0 {
		rf.serverLog("Log for server", rf.me, rf.persistedState.Log)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	n := len(rf.peers)

	for i := range n {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		currentTerm := rf.persistedState.CurrentTerm
		next := rf.nextIndex[i]
		prevIndex := next - 1

		commitIndex := rf.commitIndex
		prevlogTerm := rf.persistedState.Log[prevIndex].Term
		rf.mu.Unlock()
		go func() {
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevlogTerm,
				Entries:      []Log{},
				LeaderCommit: commitIndex,
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

func (rf *Raft) sendCommand(commandIndex int) {
	n := len(rf.peers)
	majority := n/2 + 1

	// Used only so this Start() can return/apply once this specific command
	// reaches a majority. Follower goroutines may continue after this.
	committed := make(chan struct{}, 1)

	for server := range n {
		if server == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()

				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[server]
				if nextIndex < 1 {
					nextIndex = 1
				}

				if nextIndex > len(rf.persistedState.Log) {
					nextIndex = len(rf.persistedState.Log)
				}

				prevIndex := nextIndex - 1
				prevTerm := rf.persistedState.Log[prevIndex].Term

				entries := make([]Log, len(rf.persistedState.Log[nextIndex:]))
				copy(entries, rf.persistedState.Log[nextIndex:])

				args := AppendEntriesArgs{
					Term:         rf.persistedState.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				rf.mu.Lock()

				if rf.role != Leader || args.Term != rf.persistedState.CurrentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.persistedState.CurrentTerm {
					rf.demoteLocked(reply.Term)
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					count := 1 // leader itself
					for i := range n {
						if i != rf.me && rf.matchIndex[i] >= commandIndex {
							count++
						}
					}

					if count >= majority && rf.commitIndex < commandIndex {
						rf.commitIndex = commandIndex

						for rf.lastApplied < rf.commitIndex {
							rf.lastApplied++
							entry := rf.persistedState.Log[rf.lastApplied]

							rf.serverLog("Committing command", entry.Command, "to leader", rf.me)

							rf.ch <- raftapi.ApplyMsg{
								CommandValid: true,
								Command:      entry.Command,
								CommandIndex: rf.lastApplied,
							}
						}

						select {
						case committed <- struct{}{}:
						default:
						}
					}

					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.XTerm == -1 {
					rf.nextIndex[server] = reply.XIndex
				} else {
					found := -1
					for j := len(rf.persistedState.Log) - 1; j >= 0; j-- {
						if rf.persistedState.Log[j].Term == reply.XTerm {
							found = j
							break
						}

						if rf.persistedState.Log[j].Term < reply.XTerm {
							break
						}
					}

					if found > 0 {
						rf.nextIndex[server] = found + 1
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
				}

				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}(server)
	}

	<-committed
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

	// details := fmt.Sprintf("Sending command from leader %v", rf.me)
	// tester.Annotate("Start", details, fmt.Sprintf("Sending command %v", command))

	rf.mu.Lock()
	term = rf.persistedState.CurrentTerm
	index = len(rf.persistedState.Log)
	rf.persistedState.Log = append(rf.persistedState.Log, Log{Index: index, Term: term, Command: command})

	rf.persist()

	rf.serverLog("Starting with leader", rf.me, "command", command, "leader log", rf.persistedState.Log, "nextIndex", rf.nextIndex)
	rf.mu.Unlock()

	go rf.sendCommand(index)

	return index, term, isLeader
}

func (rf *Raft) electionsTicker() {
	for {
		ms := 300 + rand.Int63()%150
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		timedOut := rf.role != Leader &&
			time.Since(rf.lastHeartbeat).Milliseconds() >= ms
		rf.mu.Unlock()

		if timedOut {
			rf.startElection()
		}
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
		lastHeartbeat: time.Now(),
		nextIndex:     nextIndex,
		matchIndex:    matchIndex,
		ch:            applyCh,
		role:          Follower,
		persistedState: PersistedState{
			CurrentTerm: 0,
			Log:         []Log{{Index: 0, Term: 0, Command: "first"}},
			VotedFor:    -1,
		},
		commitIndex: 0,
		lastApplied: 0,
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
