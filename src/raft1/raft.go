package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

type PersistedState struct {
	CurrentTerm int
	VotedFor    int
	// Log[0] is a marker entry. Its Index/Term are the snapshot's
	// lastIncludedIndex/lastIncludedTerm. It is never applied.
	Log []Log
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32

	persistedState PersistedState

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat time.Time

	role RaftRole

	ch        chan raftapi.ApplyMsg
	applyCond *sync.Cond

	// staged snapshot waiting to be handed to the service by the applier
	hasPendingSnapshot   bool
	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int
}

func (rf *Raft) serverLog(v ...any) {
	// log.Printf("Server %d: %v\n", rf.me, v)
}

// ---------------------------------------------------------------------------
// Index helpers. All assume rf.mu is held.
//
// The log is trimmed by snapshotting, so a log entry's absolute Index is NOT
// its position in the slice. Convert with off().
// ---------------------------------------------------------------------------

// absolute index of the marker entry == snapshot's lastIncludedIndex
func (rf *Raft) baseIndex() int {
	return rf.persistedState.Log[0].Index
}

// term of the marker entry == snapshot's lastIncludedTerm
func (rf *Raft) baseTerm() int {
	return rf.persistedState.Log[0].Term
}

// absolute index of the last entry in the log
func (rf *Raft) lastIndex() int {
	return rf.persistedState.Log[len(rf.persistedState.Log)-1].Index
}

// absolute index -> slice offset
func (rf *Raft) off(index int) int {
	return index - rf.baseIndex()
}

// entry at an absolute index; caller must ensure baseIndex <= index <= lastIndex
func (rf *Raft) at(index int) Log {
	return rf.persistedState.Log[rf.off(index)]
}

func (rf *Raft) lastLogTerm() int {
	return rf.persistedState.Log[len(rf.persistedState.Log)-1].Term
}

// ---------------------------------------------------------------------------

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persistedState.CurrentTerm, rf.role == Leader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistedState)
	return w.Bytes()
}

// save Raft's persistent state to stable storage. Assumes lock is held.
// Always re-saves the current snapshot alongside it, otherwise a restart
// would lose the snapshot.
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ps PersistedState
	if d.Decode(&ps) != nil {
		log.Panicf("Failed to load state for server %v", rf.me)
		return
	}

	rf.persistedState = ps

	// Everything up to lastIncludedIndex is already in the service's snapshot,
	// which the service restores itself. Never re-apply it.
	rf.commitIndex = ps.Log[0].Index
	rf.lastApplied = ps.Log[0].Index
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has all info up to and
// including index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.baseIndex() || index > rf.lastIndex() {
		// stale snapshot, or one we don't have the entries for
		return
	}

	cut := rf.off(index)
	// Copy into a fresh slice so the trimmed entries can actually be GC'd.
	newLog := make([]Log, len(rf.persistedState.Log)-cut)
	copy(newLog, rf.persistedState.Log[cut:])
	newLog[0].Command = nil // marker entry, never applied
	rf.persistedState.Log = newLog

	rf.persister.Save(rf.encodeState(), snapshot)
	rf.serverLog("snapshotted through", index)
}

// ---------------------------------------------------------------------------
// InstallSnapshot
// ---------------------------------------------------------------------------

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistedState.CurrentTerm

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(args.Term)
	}
	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.persistedState.CurrentTerm

	if args.LastIncludedIndex <= rf.baseIndex() {
		rf.persist()
		return // we already have this snapshot or a newer one
	}

	if args.LastIncludedIndex <= rf.lastIndex() &&
		rf.at(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// We have a matching entry: keep the tail after it.
		tail := rf.persistedState.Log[rf.off(args.LastIncludedIndex):]
		newLog := make([]Log, len(tail))
		copy(newLog, tail)
		newLog[0].Command = nil
		rf.persistedState.Log = newLog
	} else {
		// No match: discard the whole log.
		rf.persistedState.Log = []Log{{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
		}}
	}

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.persister.Save(rf.encodeState(), args.Data)

	// Hand it to the applier rather than sending on rf.ch here, so that all
	// applies (commands + snapshots) come from a single ordered writer.
	if args.LastIncludedIndex > rf.lastApplied {
		rf.pendingSnapshot = args.Data
		rf.pendingSnapshotIndex = args.LastIncludedIndex
		rf.pendingSnapshotTerm = args.LastIncludedTerm
		rf.hasPendingSnapshot = true
		rf.lastApplied = args.LastIncludedIndex
		rf.applyCond.Signal()
	}
}

// ---------------------------------------------------------------------------
// The applier: the ONLY goroutine that ever writes to rf.ch.
// ---------------------------------------------------------------------------

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && !rf.hasPendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Snapshots take priority, and this is re-checked every iteration, so a
		// command apply can never be delivered across a snapshot boundary.
		if rf.hasPendingSnapshot {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshot,
				SnapshotIndex: rf.pendingSnapshotIndex,
				SnapshotTerm:  rf.pendingSnapshotTerm,
			}
			rf.hasPendingSnapshot = false
			rf.pendingSnapshot = nil
			rf.mu.Unlock()

			rf.ch <- msg
			continue
		}

		next := rf.lastApplied + 1
		if next <= rf.baseIndex() {
			// A snapshot moved us past this entry; skip it.
			rf.lastApplied = rf.baseIndex()
			rf.mu.Unlock()
			continue
		}

		entry := rf.at(next)
		rf.lastApplied = next
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: next,
		}
		rf.mu.Unlock()

		rf.ch <- msg
	}
}

// ---------------------------------------------------------------------------
// RequestVote
// ---------------------------------------------------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.persistedState.CurrentTerm

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(args.Term)
		rf.persist()
	}

	reply.Term = rf.persistedState.CurrentTerm

	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastIndex())

	if upToDate && (rf.persistedState.VotedFor == -1 || rf.persistedState.VotedFor == args.CandidateId) {
		rf.persistedState.VotedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() // reset election timer when granting vote
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.role = Candidate
	rf.persistedState.CurrentTerm++
	termStarted := rf.persistedState.CurrentTerm
	rf.persistedState.VotedFor = rf.me
	rf.persist()

	rf.lastHeartbeat = time.Now()

	args := RequestVoteArgs{
		Term:         termStarted,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	rf.mu.Unlock()

	votes := 1
	majority := len(rf.peers)/2 + 1

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, &args, &reply) {
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

			if !reply.VoteGranted {
				return
			}

			votes++
			if votes >= majority && rf.role == Candidate {
				rf.role = Leader
				for i := range rf.peers {
					rf.nextIndex[i] = rf.lastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.lastIndex()

				rf.serverLog("became leader for term", termStarted)
				go rf.replicateAll()
			}
		}(server)
	}
}

// ---------------------------------------------------------------------------
// AppendEntries
// ---------------------------------------------------------------------------

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

	XTerm  int // term of the conflicting entry, or -1 if the follower's log is too short
	XIndex int // first index leader should try
}

// Assumes lock is held
func (rf *Raft) demoteLocked(term int) {
	rf.persistedState.CurrentTerm = term
	rf.persistedState.VotedFor = -1
	rf.role = Follower
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.persistedState.CurrentTerm
	reply.XTerm = -1
	reply.XIndex = -1

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(args.Term)
	}

	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.persistedState.CurrentTerm

	prevLogIndex := args.PrevLogIndex

	// The leader is behind our snapshot: everything through baseIndex is already
	// committed and durable here. Ask it to resume from just past the snapshot.
	if prevLogIndex < rf.baseIndex() {
		reply.XTerm = -1
		reply.XIndex = rf.baseIndex() + 1
		return
	}

	// Our log is too short.
	if prevLogIndex > rf.lastIndex() {
		reply.XTerm = -1
		reply.XIndex = rf.lastIndex() + 1
		return
	}

	// Term mismatch at prevLogIndex.
	if rf.at(prevLogIndex).Term != args.PrevLogTerm {
		conflictTerm := rf.at(prevLogIndex).Term
		i := prevLogIndex
		for i > rf.baseIndex()+1 && rf.at(i-1).Term == conflictTerm {
			i--
		}
		reply.XTerm = conflictTerm
		reply.XIndex = i
		return
	}

	// Append/overwrite entries.
	for i, entry := range args.Entries {
		index := prevLogIndex + 1 + i

		if index <= rf.baseIndex() {
			continue // already covered by our snapshot
		}

		if index > rf.lastIndex() {
			rf.persistedState.Log = append(rf.persistedState.Log,
				Log{Term: entry.Term, Index: index, Command: entry.Command})
			continue
		}

		if rf.at(index).Term != entry.Term {
			// Conflict: truncate everything from here on, then append the rest.
			rf.persistedState.Log = rf.persistedState.Log[:rf.off(index)]
			rf.persistedState.Log = append(rf.persistedState.Log, args.Entries[i:]...)
			break
		}
		// else: identical entry already present, leave it alone
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if rf.commitIndex > rf.lastIndex() {
			rf.commitIndex = rf.lastIndex()
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// ---------------------------------------------------------------------------
// Leader replication: one path for heartbeats, entries, and snapshots.
// ---------------------------------------------------------------------------

func (rf *Raft) replicateAll() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.replicateTo(server)
	}
}

// One round of replication to a single follower. Picks InstallSnapshot when we
// no longer have the entries the follower needs, otherwise AppendEntries.
func (rf *Raft) replicateTo(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] <= rf.baseIndex() {
		rf.sendSnapshotLocked(server) // releases the lock
		return
	}

	next := rf.nextIndex[server]
	if next < 1 {
		next = 1
	}
	if next > rf.lastIndex()+1 {
		next = rf.lastIndex() + 1
	}

	prevIndex := next - 1
	prevTerm := rf.at(prevIndex).Term

	entries := make([]Log, rf.lastIndex()-next+1)
	copy(entries, rf.persistedState.Log[rf.off(next):])

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
	if !rf.sendAppendEntries(server, &args, &reply) {
		return // dropped; the next heartbeat tick will retry
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || args.Term != rf.persistedState.CurrentTerm {
		return
	}

	if reply.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(reply.Term)
		rf.persist()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = max(rf.matchIndex[server], match)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.advanceCommitLocked()
		return
	}

	// Failure: back nextIndex up using the conflict hint.
	if reply.XTerm == -1 {
		if reply.XIndex >= 1 {
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		// Look for the last entry we have with XTerm.
		found := -1
		for j := len(rf.persistedState.Log) - 1; j >= 1; j-- {
			if rf.persistedState.Log[j].Term == reply.XTerm {
				found = rf.persistedState.Log[j].Index
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

	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}

	// Retry right away rather than waiting a full heartbeat interval.
	go rf.replicateTo(server)
}

// Assumes the lock is held; releases it before returning.
func (rf *Raft) sendSnapshotLocked(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.persistedState.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.baseIndex(),
		LastIncludedTerm:  rf.baseTerm(),
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if !rf.sendInstallSnapshot(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || args.Term != rf.persistedState.CurrentTerm {
		return
	}

	if reply.Term > rf.persistedState.CurrentTerm {
		rf.demoteLocked(reply.Term)
		rf.persist()
		return
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.advanceCommitLocked()
}

// Assumes lock is held. Advance commitIndex to the highest N such that a
// majority of matchIndex >= N and log[N].Term == currentTerm.
func (rf *Raft) advanceCommitLocked() {
	if rf.role != Leader {
		return
	}

	majority := len(rf.peers)/2 + 1

	for n := rf.lastIndex(); n > rf.commitIndex && n > rf.baseIndex(); n-- {
		// Never commit an entry from a previous term by counting replicas
		// (Figure 8 in the paper).
		if rf.at(n).Term != rf.persistedState.CurrentTerm {
			continue
		}

		count := 1 // the leader itself
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	if rf.role != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	term := rf.persistedState.CurrentTerm
	index := rf.lastIndex() + 1

	rf.persistedState.Log = append(rf.persistedState.Log,
		Log{Index: index, Term: term, Command: command})
	rf.matchIndex[rf.me] = index
	rf.persist()

	rf.serverLog("leader", rf.me, "starting command at index", index)
	rf.mu.Unlock()

	go rf.replicateAll()

	return index, term, true
}

// ---------------------------------------------------------------------------
// Tickers
// ---------------------------------------------------------------------------

func (rf *Raft) electionsTicker() {
	for !rf.killed() {
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
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			rf.replicateAll()
		}
		time.Sleep(120 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Signal() // wake the applier so it can exit
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		lastHeartbeat: time.Now(),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		ch:            applyCh,
		role:          Follower,
		persistedState: PersistedState{
			CurrentTerm: 0,
			VotedFor:    -1,
			Log:         []Log{{Index: 0, Term: 0}},
		},
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	for i := range peers {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := range peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}

	go rf.electionsTicker()
	go rf.heartbeatsTicker()
	go rf.applier()

	return rf
}
