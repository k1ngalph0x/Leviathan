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

type Peer interface{
	Call(method string, args any, reply any) bool
}

type Raft struct{
	state State
	currentTerm int32
	peers []Peer
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

type VoteResult struct{
	Server int
	Reply *RequestVoteReply
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

func(rf *Raft) GetState()(int32, State){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state
}

func(rf *Raft) StartElection(){
	fmt.Println("Starting Election")

	rf.mu.Lock()

	votes := 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	totalPeers := len(rf.peers)
	voteChan := make(chan VoteResult, totalPeers)

	//reply := &RequestVoteReply{}
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}

	rf.mu.Unlock()
	//server := 1

	
	for i:= 0; i< totalPeers; i++{
		if i == int(rf.me){
			continue
		}

		go func(server int){
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, reply)

			if ok{
				voteChan <- VoteResult{server, reply}
			}else{
				voteChan <- VoteResult{server, nil}
			}
		}(i)
	}


	//Votescounting
	for i:= 0; i<totalPeers - 1; i++{
		result := <- voteChan
		reply := result.Reply

		if reply == nil{
			continue
		}

		rf.mu.Lock()

		if reply.VoteGranted{
			votes++

			//I become the leader
			if votes > totalPeers/2 && rf.state == Candidate{
				rf.state = Leader

				rf.mu.Unlock()
				return 
			}
		}

		rf.mu.Unlock()

	}
	//rf.sendRequestVote(server, &args, reply)
}

func(rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	fmt.Println("Requesting Vote")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		//reply.Term = rf.currentTerm
		return 
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}else{
		reply.VoteGranted = false
	}

	
}

func(rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println("Sending Votes")
	return rf.peers[server].Call("RequestVote", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	fmt.Println("Appending Entries")
}

func Make(me int32, peers []Peer) *Raft {
	rf := &Raft{}

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.me = me
	rf.peers = peers

	return rf
}

func(rf *Raft) Serve(){
	
}

//go test ./... -v
