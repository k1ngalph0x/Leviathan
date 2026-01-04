package raft

import (
	"fmt"
	"testing"
	"time"
)


func MakeCluster(n int) []*Raft{
	nodes := make([]*Raft, n)

	for i:= 0 ;i< n; i++{
		nodes[i] = Make(int32(i), nil)
	}

	for i:= 0; i< n; i++{
		peers := make([]Peer, n)

		for j:= 0; j< n; j++{
			if i!=j{
				peers[j] = &LocalPeer{
					id: int32(j),
					raft: nodes[j],
				}
			}
		}

		nodes[i].peers = peers

	}
	
	return nodes
}

func TestElectionRaft(t *testing.T){

	nodes := MakeCluster(3)

	nodes[0].StartElection()

	time.Sleep(50 * time.Millisecond)

	leaders := 0

	for _, n := range nodes{
		term, state := n.GetState()

		fmt.Printf("Node %d | Term %d | State %v\n", n.me, term, state)

		if state == Leader{
			leaders ++
			fmt.Printf("Leader Elected: Node %d and Term %d\n", n.me, term)
		}
	}

	if leaders != 1{
		t.Fatalf("Leaders: %d", leaders)
	}

}