package raft

import (
	"6.824/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

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

type state int

const (
	Follower state = iota
	Candidate
	Leader
)

type LogCommand struct {
	Term    int
	Command interface{}
}

const (
	// raft paper says a good timeout is between 150-300 but since tests limits us to 10 heartbeats/second we
	// should have it more than the raft paper range but not too large
	ELECTION_INTERVAL = 400
	// test needs no more 10 heartbeats every secon, a good number can be 150-200
	HEART_BEAT_INTERVAL = 150
)

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
	// persisten
	currentTerm int
	votedFor    int
	log         []LogCommand
	// volatile all followers
	commitIndex int
	lastApplied int
	// volatile for leaders only
	// gets re-initialised after each election
	nextIndex  []int
	matchIndex []int

	state           state
	electionTimeout time.Duration
	lastHeartBeat   time.Time
	applyChannel    chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
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

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // for candidate to get updated if his term is lower
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderTerm        int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogCommand
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPrint("[%v] receives request to vote from [%v] at term[%v]", rf.me, args.CandidateId, args.Term)
	DebugPrint("current term for voter [%v]", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.votedFor = args.CandidateId
	} else {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
		}
	}
	currentLastLogTerm := rf.log[len(rf.log)-1].Term
	currentLastLogIndex := len(rf.log) - 1
	currentLogIsBetter := (currentLastLogTerm > args.LastLogTerm) ||
		(currentLastLogTerm == args.LastLogTerm && currentLastLogIndex > args.LastLogIndex)

	grantVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !currentLogIsBetter
	if grantVote {
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPrint("[%v] received heartbeat from [%v]", rf.me, args.LeaderId)
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.LeaderTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.LeaderTerm > rf.currentTerm {
		rf.ConvertToFollower(args.LeaderTerm)
	}
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if len(rf.log) == args.PrevLogIndex+1 || (len(args.Entries) > 0 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term) {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.log)-1)
		rf.ApplyLog()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	DebugPrint("is thi sgetting called")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term, isLeader := len(rf.log), rf.currentTerm, rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogCommand{Term: term, Command: command})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
	}
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
func (rf *Raft) LeaderElection() {
	for rf.killed() == false {
		electionTimeout := ELECTION_INTERVAL + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastHeartBeat.Before(startTime) {
			if rf.state != Leader {
				DebugPrint("[%v] peer : starting election", rf.me)
				go rf.StartElection()
			}
		}
		rf.ApplyLog()
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.ConvertToCandidate()
	DebugPrint("[%v] peer: attempting election at term %v", rf.me, rf.currentTerm)
	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	votes := 1
	finished := false
	rf.mu.Unlock()
	// now we need to send concurrent RPCs
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			request := RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			voteGranted := rf.sendRequestVote(server, &request, &reply)
			DebugPrint("[%v] voted: %v for candidate[%v]", server, reply.VoteGranted, request.CandidateId)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !voteGranted { // connection issue we don't get a reply
				return
			}
			if reply.Term > rf.currentTerm {
				rf.ConvertToFollower(reply.Term)
				return
			} else if reply.VoteGranted {
				votes++
			}
			if votes <= len(rf.peers)/2 || finished {
				return
			}
			finished = true
			if rf.currentTerm != term || rf.state != Candidate {
				return
			}
			DebugPrint("[%v] is elected leader at term[%v] with votes[%v]", candidateId, term, votes)
			rf.ConvertToLeader()
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me {
					go rf.LeaderOperation(j)
				}
			}
		}(i)
	}
}

func (rf *Raft) LeaderOperation(pid int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			DebugPrint("[%v] not leader anymore", rf.me)
			rf.mu.Unlock()
			return
		}
		DebugPrint("[%v] leader attempting to send heartbeat every %v milliseconds", rf.me, HEART_BEAT_INTERVAL)
		rf.mu.Unlock()
		go rf.sendAppendEntries(pid)
		time.Sleep(time.Duration(HEART_BEAT_INTERVAL) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	previousLogIndex := rf.nextIndex[server] - 1
	DebugPrint("current log: %v", rf.log)
	DebugPrint("prevLogIndex: %v, rf.nextIndex[%v]: %v", previousLogIndex, server, rf.nextIndex)
	args := AppendEntriesArgs{
		LeaderTerm:        rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      previousLogIndex,
		PrevLogTerm:       rf.log[previousLogIndex].Term,
		LeaderCommitIndex: rf.commitIndex,
		Entries:           rf.log[rf.nextIndex[server]:],
	}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	DebugPrint("[%v] sending heartbeat to [%v]", rf.me, server)
	// remember don't lock while doing RPC call
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if !ok {
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		// update commitIndex
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= i {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					rf.ApplyLog()
					break
				}
			}
		}()
	} else {
		if reply.Term > rf.currentTerm {
			rf.ConvertToFollower(reply.Term)
		} else {
			rf.nextIndex[server]--
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) ConvertToCandidate() {
	DebugPrint("[%v] converting to candidate", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now() // resetting election timer
	rf.state = Candidate
}

func (rf *Raft) ConvertToFollower(correctTerm int) {
	DebugPrint("[%v] converting to follower", rf.me)
	rf.state = Follower
	rf.votedFor = -1
	rf.lastHeartBeat = time.Now() // reset timeout
	rf.currentTerm = correctTerm
}

func (rf *Raft) ConvertToLeader() {
	DebugPrint("[%v] converting to leader", rf.me)
	rf.state = Leader
	rf.lastHeartBeat = time.Now()
	rf.InitialiseLeaderState()
}

func (rf *Raft) InitialiseLeaderState() {
	if rf.state != Leader {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) ApplyLog() {
	if rf.commitIndex > rf.lastApplied {
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				var applyMsg ApplyMsg
				applyMsg.CommandValid = true
				applyMsg.Command = rf.log[i].Command
				applyMsg.CommandIndex = i
				rf.applyChannel <- applyMsg
				rf.lastApplied = applyMsg.CommandIndex
			}
		}()
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
	DebugPrint("[%v] starting", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.lastHeartBeat = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogCommand, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.applyChannel = applyCh
	DebugPrint("[%v] initialised", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.LeaderElection()

	return rf
}
