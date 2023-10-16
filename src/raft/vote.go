package raft

import (
	"time"
)

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.

// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().

// look at the comments in ../labrpc/labrpc.go for more details.

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendTickMsg(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendSnapshotR(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendVoteToAllL() { // 发送投票请求给所有节点
	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log.endIndex() - 1,
			LastLogTerm:  rf.log.lastTerm(),
		}
		go rf.sendVote(i, &args, &votes)
	}
	time.Sleep(20 * time.Millisecond)
}

func (rf *Raft) sendVote(server int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	DPrintf(true, "sendVote: args: %v to %d\n", args, server)
	term := args.Term
	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if term != rf.currentTerm || rf.state != Candidate {
			return
		}
		DPrintf(false, "sendVote lock: %d\n", rf.me)

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = server
			rf.persist()
			return
		}

		if reply.VoteGranted {
			*votes += 1
			DPrintf(true, "VoteReply: id is %d, voteFrom: %d Term: %d\n", rf.me, server, rf.currentTerm)
			if *votes > len(rf.peers)/2 {
				if rf.state != Leader {
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.log.endIndex()
					}
					rf.matchIndex = make([]int, len(rf.peers))
					rf.persist()
					go rf.sendMsgToAll()
					DPrintf(true, "VoteResult: leader is %d, log: %v Term: %d\n", rf.me, rf.log, rf.currentTerm)
				}
			}
		}
	}
}

func (rf *Raft) sendMsgToAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	rf.sendMsgToAllL()
}

func (rf *Raft) sendMsgToAllL() {
	DPrintf(true, "sendMsgToAll lock: %d , log: %v\n", rf.me, rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.log.offset() {
			arg := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Offset:            rf.lastIncludedIndex,
				Data:              rf.snapshot,
				Done:              true,
			}
			DPrintf(true, "sendSnapshot  %d to %d args: %v\n", rf.me, i, arg)
			go rf.sendSnapshot(i, &arg)
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log.getTerm(rf.nextIndex[i] - 1),
			Entries:      make([]Entry, 0),
			LeaderCommit: rf.lastApplied,
		}
		for j := rf.nextIndex[i]; j < rf.log.endIndex(); j++ {
			args.Entries = append(args.Entries, rf.log.getEntry(j))
		}
		go rf.sendMsg(i, &args)
	}
}

func (rf *Raft) sendMsg(server int, args *AppendEntriesArgs) {
	DPrintf(true, "sendMsg: args: %v to %d\n", args, server)
	reply := AppendEntriesReply{}
	term := args.Term
	ok := rf.sendTickMsg(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if term != rf.currentTerm || rf.state != Leader {
			return
		}
		DPrintf(true, "sendMsg lock: %d %d args: %v, reply: %v\n", rf.me, server, args, reply)
		if reply.Term > rf.currentTerm { // 此时它不再是leader，新一轮选举将会在下次心跳开始
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		} else if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1

			DPrintf(len(args.Entries) != 0, "sendMsg: %d to %d success matchIndex: %d commitIndex: %d \n", rf.me, server, rf.matchIndex[server], rf.commitIndex)
			// 更新commitIndex
			if rf.matchIndex[server] > rf.commitIndex && rf.log.getTerm(rf.matchIndex[server]) == rf.currentTerm { // 只能提交当前任期
				num := 1
				minmatch := rf.matchIndex[server]
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] > rf.commitIndex {
						num++
						if rf.matchIndex[i] < minmatch {
							minmatch = rf.matchIndex[i]
						}
					}
				}
				if num > len(rf.peers)/2 {
					rf.commitIndex = minmatch
					go rf.commitLog() // 内部会持久化
				}
			}
		} else {
			if rf.nextIndex[server] <= rf.log.Index0+1 { // 现有日志无法同步，发送快照
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Offset:            rf.lastIncludedIndex,
					Data:              rf.snapshot,
					Done:              true,
				}
				DPrintf(true, "sendSnapshot  %d to %d args: %v\n", rf.me, server, args)
				go rf.sendSnapshot(server, &args)
			} else {
				for rf.nextIndex[server] > rf.log.Index0 && rf.log.getTerm(rf.nextIndex[server]-1) == args.PrevLogTerm {
					rf.nextIndex[server]--
				}
			}

		}
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	term := args.Term
	ok := rf.sendSnapshotR(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if term != rf.currentTerm || rf.state != Leader {
			return
		}
		DPrintf(true, "sendSnapshot lock: %d to %d args: %v, reply: %v\n", rf.me, server, args, reply)
		if reply.Term > rf.currentTerm { // 此时它不再是leader，新一轮选举将会在下次心跳开始
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = server
			rf.persist()
		} else {
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.matchIndex[server] = rf.lastIncludedIndex
		}
	}
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(true, "AppendEntry lock: %d args: %v\n", rf.me, args)
	rf.setElectiontime()

	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// 同步日志
		myTerm := rf.log.getTerm(args.PrevLogIndex)
		if myTerm != args.PrevLogTerm {
			reply.Success = false
		} else {

			myIndex := args.PrevLogIndex

			for i, j := 0, myIndex+1; i < len(args.Entries); i++ {
				rf.log.setEntry(j, args.Entries[i])
				j++
			}
			if rf.log.endIndex() > myIndex+len(args.Entries)+1 {
				rf.log.Entries = rf.log.cutEntryToIndex(myIndex + len(args.Entries) + 1) //[:idx]
			}
			// 更新commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLog()
			}
			rf.persist()
			reply.Success = true
			DPrintf(len(args.Entries) != 0, "AppendEntry lock log: %d rf.log: %v args: %v\n", rf.me, rf.log, args)
		}
	}

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setElectiontime()

	DPrintf(false, "RPC RequestVote lock: %d from %d argsTerm %d\n", rf.me, args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	myLastIndex := rf.log.endIndex() - 1
	myLastTerm := rf.log.lastTerm()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // 没有投票或者已经投票给了当前节点
		if args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.setElectiontime()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(true, " args.Term < rf.currentTerm RPC InstallSnapshot lock: %d from %d args %v log: %v\n", rf.me, args.LeaderId, args, rf.log)
		rf.mu.Unlock()
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log.Index0 = args.Offset
	rf.snapshot = args.Data
	rf.log.Entries = make([]Entry, 0)
	rf.log.append(Entry{Term: args.LastIncludedTerm})

	DPrintf(true, "RPC InstallSnapshot lock: %d from %d args %v log: %v\n", rf.me, args.LeaderId, args, rf.log)

	rf.persist()
	apply := ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.Data,
	}
	rf.mu.Unlock()
	rf.applyCh <- apply
}
