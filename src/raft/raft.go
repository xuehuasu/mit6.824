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
// 翻译：
// rf = Make(...)
//   创建一个新的Raft服务器
// rf.Start(command interface{}) (index, term, isleader)
//   开始在日志中达成一致
// rf.GetState() (term, isLeader)
//   询问Raft当前的任期，以及它是否认为自己是领导者
//   每当一个新的日志条目被提交到日志中，每个Raft节点都应该向服务（或测试程序）发送一个ApplyMsg
//   在同一个服务器中。
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // 所有的RPC端点
	persister *Persister          // Object to hold this peer's persisted state 持久化状态
	me        int                 // 当前节点在peers中的索引
	dead      int32               // 用于标记当前节点是否已经被关闭

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	state        int       // 当前节点的状态
	electiontime time.Time // 选举超时时间

	currentTerm int // 当前任期
	votedFor    int // 当前任期投票给谁了
	log         Log // 日志

	commitIndex int // 已经提交的日志索引
	lastApplied int // 已经应用的日志索引

	nextIndex  []int // 每个节点的日志索引
	matchIndex []int // 每个节点已经复制的日志索引

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	DPrintf(true, "persist: %d currentTerm: %d votedFor: %d log: %v commitIndex: %d lastApplied: %d nextIndex: %v matchIndex: %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log.cutEntryToEnd(rf.log.endIndex()-2), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	var commitIndex int
	var lastApplied int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil || d.Decode(&lastApplied) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("error readPersist: %d\n", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.state = Candidate

		rf.snapshot = rf.persister.ReadSnapshot()
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		DPrintf(true, "readPersist: %d currentTerm: %d votedFor: %d log: %v commitIndex: %d lastApplied: %d nextIndex: %v matchIndex: %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log.cutEntryToEnd(rf.log.endIndex()-2), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	DPrintf(true, "CondInstallSnapshot lock: %d lastIncludedTerm %d me %d lastIncludedIndex %d me %d\n", rf.me, lastIncludedTerm, rf.lastIncludedTerm, lastIncludedIndex, rf.lastIncludedIndex)
	if lastIncludedIndex >= rf.lastIncludedIndex {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.log.Entries = rf.log.cutEntryToEnd(lastIncludedIndex)
		rf.log.Index0 = lastIncludedIndex

		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex

		rf.persist()
		rf.mu.Unlock()
		return true
	} else {
		rf.mu.Unlock()
	}
	return false
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(true, "Snapshot lock: %d index %d\n", rf.me, index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log.getTerm(index)
	rf.snapshot = snapshot
	rf.log.Entries = rf.log.cutEntryToEnd(index)
	rf.log.Index0 = index

	rf.lastApplied = index
	rf.commitIndex = index
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 使用Raft的服务（例如，k/v服务器）希望开始就要附加到Raft的日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，启动协议并立即返回。
// 不能保证这个命令会被提交给Raft日志，因为领导者可能会失败或输掉选举。即使Raft实例已被终止，此函数也应正常返回。
// 第一个返回值是命令在提交时将出现的索引。第二个返回值是当前项。如果此服务器认为它是领导者，则第三个返回值为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) { // 返回的是当前日志索引，任期，是否是leader
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(false, "Start lock: %d\n", rf.me)

	index = rf.log.endIndex()
	term = rf.currentTerm
	isLeader = (rf.state == Leader)

	if isLeader {
		rf.log.append(Entry{rf.currentTerm, command})
		rf.persist()
		DPrintf(true, "appendLog lock: %d index: %d log: %v\n", rf.me, index, rf.log)
	}
	return index, term, isLeader
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	i := rf.lastApplied + 1
	for ; i <= rf.commitIndex; i++ {
		if i <= rf.log.offset() {
			continue
		}
		apply := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.getEntry(i).Command,
			CommandIndex: i,
		}
		DPrintf(true, "commitLog lock: %d, apply: %v\n", rf.me, apply)
		rf.mu.Unlock()
		rf.applyCh <- apply
		rf.mu.Lock()
	}
	rf.lastApplied = rf.commitIndex
	rf.persist()
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
		rf.mu.Lock()
		DPrintf(true, "ticker lock: %d log: %v Term: %d\n", rf.me, rf.log, rf.currentTerm)

		if rf.state == Leader {
			rf.sendMsgToAllL() // 发送心跳
			rf.setElectiontime()
		}
		if time.Now().After(rf.electiontime) {
			// 开始选举
			DPrintf(true, "startElection: %d\n", rf.me)
			rf.currentTerm += 1
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.persist()

			rf.sendVoteToAllL() // 发送投票请求给所有节点
			rf.setElectiontime()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf(true, "exit: %d\n", rf.me)
}

func (rf *Raft) setElectiontime() {
	t := time.Now()
	t = t.Add(time.Duration(rand.Int63()%300+150) * time.Millisecond)
	rf.electiontime = t
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

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.cond = sync.NewCond(&rf.mu)

	rf.state = Follower
	rf.electiontime = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = Log{make([]Entry, 0), 0}
	rf.log.append(Entry{0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.snapshot = make([]byte, 0)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
