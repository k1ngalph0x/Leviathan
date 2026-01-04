package raft

import (
	"fmt"
	"net/rpc"
)

type NetworkPeer struct {
	id      int32
	address string
}

func (np *NetworkPeer) Call(method string, args any, reply any) bool {
	client, err := rpc.Dial("tcp", np.address)
	if err != nil{
		fmt.Println(err)
		return false
	}
	defer client.Close()

	conn := client.Call("Raft." + method, args, reply)

	if conn != nil{
		fmt.Println(conn)
		return false
	}

	return true
}