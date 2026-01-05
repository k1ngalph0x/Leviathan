package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.etcd.io/bbolt"
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
	me int32
	state State
	peers []Peer
	mu sync.Mutex
	db *bbolt.DB
	log []LogEntry
	votedFor int32
	commitIndex int32
	lastApplied int32
	currentTerm int32
	lastheartBeat time.Time
	electionTimeout time.Duration
	heartbeatInterval time.Duration	
}

var (
	metaBucket = []byte("meta")
	logBucket  = []byte("log")
)


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
	PrevLogTerm int32
	PrevLogIndex int32
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

func itob(v int32) []byte{
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(v))
	return buffer
}

func(rf *Raft) PersistMeta(){
	rf.db.Update(func(tx *bbolt.Tx)error {
		
		//bucket := tx.Bucket([]byte("meta"))
		bucket := tx.Bucket(metaBucket)
		bucket.Put([]byte("currentTerm"), itob(rf.currentTerm))
		bucket.Put([]byte("votedFor"), itob(rf.currentTerm))
		return nil
	})
}

func(rf *Raft) PersistLog(index int32, entry LogEntry){
	rf.db.Update(func(tx *bbolt.Tx)error{
		buffer := tx.Bucket(logBucket)
		data, _ := json.Marshal(entry)
		buffer.Put(itob(index), data)
		return nil
	})
}

func (rf *Raft) readPersist(){
	rf.db.View(func(tx *bbolt.Tx)error{
		
		meta := tx.Bucket(metaBucket)
		
		if v:= meta.Get([]byte("currentTerm")); v!=nil{
			rf.currentTerm = int32(binary.BigEndian.Uint32(v))
		}

		if v:= meta.Get([]byte("votedFor")); v!=nil{
			rf.currentTerm = int32(binary.BigEndian.Uint32(v))
		}

		logB := tx.Bucket(logBucket)
		logB.ForEach(func(k, v []byte)error{
			var e LogEntry
			json.Unmarshal(v, &e)
			rf.log = append(rf.log, e)	
			return nil
		})

		return nil
	})
}

func(rf *Raft) StartHeartBeat(){
	go func(){
		for {
			rf.mu.Lock()

			if rf.state != Leader{
				rf.mu.Unlock()
				return 
			}

			term := rf.currentTerm 
			rf.mu.Unlock()

			args := AppendEntriesArgs{
				Term: term,
				LeaderId: rf.me,
				Entries: nil,
			}

			for i:= range rf.peers{
				if i == int(rf.me){
					continue
				}

				go func(server int){
					reply := &AppendEntriesReply{}
					rf.peers[server].Call("AppendEntries", &args, reply)
				}(i)
			}

			time.Sleep(rf.heartbeatInterval)
		}
	}()
}

func(rf *Raft) StartElection(){
	fmt.Println("Starting Election")

	rf.mu.Lock()

	votes := 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	totalPeers := len(rf.peers)
	electionTerm := rf.currentTerm
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

		//if election term is not equal to mine
		if reply.Term != electionTerm || rf.state != Candidate || rf.currentTerm != electionTerm{
			rf.mu.Unlock()
			return 
		}

		//if election term is greater than mine
		if reply.Term > rf.currentTerm{
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}

		if reply.Term < electionTerm{
			rf.mu.Unlock()
			continue
		}

		if reply.VoteGranted{
			votes++

			//I become the leader
			if votes > totalPeers/2 && rf.state == Candidate{
				rf.state = Leader
				rf.StartHeartBeat()
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
		rf.state = Follower
		reply.Term = rf.currentTerm
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

	rf.mu.Lock()
	defer rf.mu.Unlock()


	if args.Term < rf.currentTerm {
		reply.Success = false
		return 
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.lastheartBeat = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true
}

func Make(me int32, peers []Peer) *Raft {
	rf := &Raft{}

	db, err := bbolt.Open(
		fmt.Sprintf("raft-%d.db", me),
		0600,
		nil,
	)


	if err != nil{
		fmt.Println(err)
		panic(err)
	}

	rf.db = db

	db.Update(func(tx *bbolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(metaBucket)
		_, _ = tx.CreateBucketIfNotExists(logBucket)

		return nil
	})

	rf.me = me
	rf.peers = peers
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.log = make([]LogEntry, 1)
	rf.lastheartBeat  = time.Now()
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.readPersist()

	return rf
}

func(rf *Raft) Serve(){
	go func(){

		for {
			time.Sleep(10 * time.Millisecond)

			rf.mu.Lock()


			if rf.state != Leader && time.Since(rf.lastheartBeat) > rf.electionTimeout {
				rf.mu.Unlock()
				rf.StartElection()
				continue
			}

			rf.mu.Unlock()
		}
	}()
}
//go test ./... -v
