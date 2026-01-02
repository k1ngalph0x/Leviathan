package raft

import (
	"fmt"
	"sync"
)

type State int

const (
	Follower State = iota
	Candidate 
	Leader
)


type Raft struct{
	state State
	currentTerm int32
	votedFor int32
	commitIndex int32
	lastApplied int32
	log []LogEntry
	me int32
	mu sync.Mutex
}

type LogEntry struct{
	Term int32
	Command interface{} 
}

type RequestVoteArgs struct{
	Term int32
	CandidateId int32
	LastLogIndex int32
	LastLogTerm int32
}

type RequestVoteReply struct{
	Term int32
	VoteGranted bool
}


type AppendEntriesArgs struct{
	Term int32
	LeaderId int32
	PrevLogIndex int32
	PrevLogTerm int32
	Entries []LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct{
	Term int32
	Success bool
}


func(rf *Raft) StartElection(){
	fmt.Println("Starting Election")

	rf.mu.Lock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	reply := &RequestVoteReply{}
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}

	server := 1

	rf.sendRequestVote(server, &args, reply)

	rf.mu.Unlock()
}

func(rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	fmt.Println("Requesting Vote")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return 
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
}

func(rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply){

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	fmt.Println("Appending Entries")
}



//go test ./... -v
