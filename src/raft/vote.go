package raft

import (
	"math/rand"
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

func (rf *Raft) sendVoteToAllL() { // 发送投票请求给所有节点
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.endIndex() - 1,
		LastLogTerm:  rf.log.lastTerm(),
	}
	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendVote(i, &args, &votes)
	}
}
func (rf *Raft) sendVote(server int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	DPrintf(true, "sendVote: args: %v to %d\n", args, server)
	ok := rf.sendRequestVote(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf(true, "sendVote lock: %d\n", rf.me)

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
		if reply.VoteGranted {
			*votes += 1
			if *votes > len(rf.peers)/2 {
				if rf.state != Leader {
					go rf.sendMsgToAll()
				}
				rf.state = Leader
				DPrintf(true, "VoteResult: leader is %d, log: %v\n", rf.me, rf.log)
			}
		}
	} else {
		DPrintf(true, "sendVote: %d to %d failed\n", rf.me, server)
	}
}
func (rf *Raft) sendMsgToAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(true, "sendMsgToAll lock: %d\n", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
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
func (rf *Raft) sendMsgToAllL() {
	DPrintf(true, "sendMsgToAll lock: %d , log: %v\n", rf.me, rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
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
	ok := rf.peers[server].Call("Raft.AppendEntry", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf(true, "sendMsg lock: %d %d reply: %v\n", rf.me, server, reply)
		if reply.Term > rf.currentTerm { // 此时它不再是leader，将会新一轮选举将会在下次心跳开始
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		} else if reply.Success {
			rf.nextIndex[server] = rf.log.endIndex()
			rf.matchIndex[server] = rf.log.endIndex() - 1
			DPrintf(len(args.Entries) != 0, "sendMsg: %d to %d success matchIndex: %d commitIndex: %d \n", rf.me, server, rf.matchIndex[server], rf.commitIndex)
			// 更新commitIndex
			if rf.matchIndex[server] > rf.commitIndex && rf.log.getTerm(rf.matchIndex[server]) == rf.currentTerm { // 只能提交当前任期
				num := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] > rf.commitIndex {
						num++
					}
				}
				if num > len(rf.peers)/2 {
					rf.commitIndex = rf.matchIndex[server]
					go rf.commitLog()
				}
			}
		} else {
			for rf.nextIndex[server] > 0 && rf.log.getTerm(rf.nextIndex[server]-1) == args.PrevLogTerm {
				rf.nextIndex[server]--
			}
		}
	}
}

func (rf *Raft) setElectiontime() {
	t := time.Now()
	t = t.Add(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	rf.electiontime = t
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(true, "AppendEntry lock: %d args: %v\n", rf.me, args)
	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// 同步日志
		myTerm := rf.log.getTerm(args.PrevLogIndex)
		if myTerm != args.PrevLogTerm {
			reply.Success = false
		} else {
			myIndex := args.PrevLogIndex
			for i, j := 0, myIndex+1; i < len(args.Entries); i++ { // 发送来的是逆序日志
				rf.log.setEntry(j, args.Entries[i])
				j++
			}
			rf.log.cutEntryToEnd(myIndex + len(args.Entries) + 1)
			reply.Success = true
			DPrintf(len(args.Entries) != 0, "AppendEntry log: %d rf.log: %v args: %v\n", rf.me, rf.log, args)
			// 更新commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLog()
			}
		}
	}

	rf.setElectiontime()

	DPrintf(true, "RPC AppendEntry: %d, args: %v, reply: %v\n", rf.me, args, reply)
}
