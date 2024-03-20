package raft

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

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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
type stateship int

const (
	Leader stateship = iota
	follower
	candidate
)

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

type Entry struct {
	Command interface{}
	Term    int
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
	state           stateship
	currentTerm     int
	votedFor        int
	log             []Entry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	timeouts        time.Duration
	electExpiryTime time.Time
	hrtBtExpiryTime time.Time
	applyChan       *chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	// Your code here (2A).
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
	// Your code here (2C).
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
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
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
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.ResetElectionTimer()
	}
	meLastLog := rf.log[len(rf.log)-1]
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (meLastLog.Term < args.LastLogTerm || (meLastLog.Term == args.LastLogTerm && (len(rf.log)-1) <= args.LastLogIndex)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%d %d %d %d", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.state = follower
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.ResetElectionTimer()
	reply.Term = args.Term
	reply.Success = true
	log.Printf("AppendEntries %d %d", rf.me, args.PrevLogIndex)
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		rf.log = rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
		log.Printf("AppendEntriesUp %d %d %d", rf.me, rf.log, args.Entries)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		return
	} else if args.PrevLogIndex == (len(rf.log) - 1) {
		rf.log = rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
		log.Printf("AppendEntriesDown %d %d %d", rf.me, rf.log, args.Entries)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		return
	} else {
		reply.Success = false
	}
	rf.commitIndex = rf.matchIndex[rf.me]
	for i := rf.lastApplied + 1; i <= rf.commitIndex && i <= args.PrevLogIndex; i++ {
		log.Printf("------ %d %d %d %d", rf.me, args.PrevLogIndex, rf.commitIndex, rf.log)
		(*rf.applyChan) <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied++
	}
	// log.Printf("%d %d %d", rf.commitIndex, args.LeaderCommit, rf.log)
}
func (rf *Raft) ResetElectionTimer() {
	timeout := time.Duration(250+rand.Intn(300)) * time.Millisecond
	rf.electExpiryTime = time.Now().Add(timeout)
}
func (rf *Raft) ResetHrtBtTimer() {
	timeout := time.Duration(10+rand.Intn(10)) * time.Millisecond
	rf.hrtBtExpiryTime = time.Now().Add(timeout)
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}
	entry := Entry{command, rf.currentTerm}
	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, term, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// log.Printf("%d %d", rf.me, rf.log)
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		log.Printf("rf.me %d  rf.currentTerm%d rf.log%d rf.state %d", rf.me, rf.currentTerm, rf.log, rf.state)
		if rf.state != Leader && time.Now().After(rf.electExpiryTime) {
			log.Printf("election %d %d", rf.me, rf.currentTerm)
			go func() {
				rf.mu.Lock()
				rf.state = candidate
				rf.votedFor = rf.me
				rf.currentTerm++
				rf.ResetElectionTimer()
				numGrantVote := 1

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(i int) {
						reply := RequestVoteReply{}
						if ok := rf.sendRequestVote(i, &args, &reply); ok {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.state = follower
								rf.currentTerm = reply.Term
								rf.votedFor = -1
							} else if reply.VoteGranted {
								numGrantVote++
								if numGrantVote == len(rf.peers)/2+1 {
									rf.state = Leader
									//log.Printf("+- %d %d", rf.me, rf.currentTerm)
									rf.votedFor = -1
									for idx := range rf.peers {
										rf.nextIndex[idx] = rf.matchIndex[idx] + 1
									}
									rf.mu.Unlock()
									return
								}
							}
							rf.mu.Unlock()
						}
					}(i)
				}
			}()
		}
		if rf.state == Leader {
			// log.Printf("leader %d", rf.me)
			if time.Now().After(rf.hrtBtExpiryTime) {
				rf.ResetHrtBtTimer()
				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}
					// log.Printf("hrtbt %d", idx)
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					rf.mu.Lock()
					args.Term = rf.currentTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[idx] - 1
					log.Printf("inLeader %d %d %d %d", idx, args.PrevLogIndex, rf.nextIndex[idx], len(rf.log))
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.LeaderId = rf.me
					// //log.Printf("111  %d %d %d", rf.nextIndex[idx], len(rf.log), len(args.Entries))
					if args.PrevLogIndex <= len(rf.log) {
						args.Entries = make([]Entry, len(rf.log[args.PrevLogIndex:]))
						// log.Printf("_=_+_+_+_+_+ %d %d %d", rf.nextIndex[idx], rf.log[rf.nextIndex[idx]:len(rf.log)], args.Entries)
						copy(args.Entries, rf.log[args.PrevLogIndex:])

					}
					rf.mu.Unlock()
					go func(idx int) {
						ok := rf.sendAppendEntries(idx, &args, &reply)
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > args.Term {
							rf.state = follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.ResetElectionTimer()
							return
						}
						if args.Entries != nil {
							if reply.Success {
								commitCount := 0
								rf.nextIndex[idx] = len(rf.log)
								rf.matchIndex[idx] = rf.nextIndex[idx] - 1
								if rf.lastApplied < rf.matchIndex[rf.me] {
									for idx := range rf.peers {
										if rf.matchIndex[rf.me] == rf.matchIndex[idx] {
											commitCount++
										}
									}
									if commitCount == (len(rf.peers)/2 + 1) {
										rf.commitIndex = rf.matchIndex[rf.me]
										for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
											(*rf.applyChan) <- ApplyMsg{
												CommandValid: true,
												Command:      rf.log[i].Command,
												CommandIndex: i,
											}
											rf.lastApplied++
										}
									}
								}
							} else {
								if rf.nextIndex[idx] < len(rf.log) {
									rf.nextIndex[idx]++
								}
							}
						}
					}(idx)
				}
			}
		}
		ms := 50 + (rand.Int63() % 300)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = follower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChan = &applyCh
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.log = append(rf.log, Entry{})
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ResetElectionTimer()
	rf.ResetHrtBtTimer()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
