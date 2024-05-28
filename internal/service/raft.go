package raft

import (
	"encoding/json"
	"fmt"
	"if3230-tubes-williamjeffersonclinton/constants"
	dto "if3230-tubes-williamjeffersonclinton/internal/dto"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type RaftNode struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
	NodeType           int
	Address            dto.Address
	Log                []dto.LogEntry
	Application        interface{}
	ElectionTerm       int
	ClusterAddrList    []dto.Address
	ClusterLeaderAddr  dto.Address
	HeartbeatThread    *time.Ticker
	HeartbeatTimeout   *time.Ticker
	LastHeartbeat      time.Time
	Mu				   sync.Mutex
	CurrentTerm		   int64
}

type RaftInterface interface {
	TryToApplyMembership(contactAddr dto.Address)
}

// func (node *RaftNode) AppendEntries(entries ...dto.LogEntry) {
// 	node.Log = append(node.Log, entries...)
// }

func (n *RaftNode) AppendEntries(req *dto.AppendEntriesRequest) *dto.AppendEntriesResponse {
    n.Mu.Lock()
    defer n.Mu.Unlock()

    // Check if the request term is greater than the current term
    if req.Term < n.CurrentTerm {
        return &dto.AppendEntriesResponse{
            Term:    n.CurrentTerm,
            Success: false,
        }
    }

    // Update the current term
    n.CurrentTerm = req.Term

    // Convert []*auto_generated.LogEntry to []auto_generated.LogEntry
    entries := make([]dto.LogEntry, len(req.Entries))
    for i, entry := range req.Entries {
        entries[i] = *entry
    }

	// Leader Heartbeat (Empty Entries)
	if (len(entries) == 0) {
		return &dto.AppendEntriesResponse{
            Term:    n.CurrentTerm,
            Success: true,
        }
	}

    // If the log is empty or the previous log entry matches, append new entries
    if (len(n.Log) == 0 || (int(req.PrevLogIndex) < len(n.Log) && n.Log[req.PrevLogIndex].Term == req.PrevLogTerm)) {
        n.Log = append(n.Log[:req.PrevLogIndex+1], entries...)
        return &dto.AppendEntriesResponse{
            Term:    n.CurrentTerm,
            Success: true,
        }
    }

    // Search for a consistent log entry
	for i := req.PrevLogIndex - 1; i >= 0; i-- {
		if len(n.Log) <= int(i) {
			break
		}
		if n.Log[i].Term == req.PrevLogTerm && n.Log[i].Index == req.PrevLogIndex {
			// Found a consistent entry, append new entries after this position
			n.Log = n.Log[:i+1] // Keep entries up to i (inclusive)

			// Append new entries
			n.Log = append(n.Log, entries...)
			return &dto.AppendEntriesResponse{
				Term:    n.CurrentTerm,
				Success: true,
			}
		}
	}

	// Leader should repeat with lower index
	return &dto.AppendEntriesResponse{
		Term:    n.CurrentTerm,
		Success: false,
	}
}

func NewRaftNode(application interface{}, addr dto.Address, contactAddr *dto.Address) *RaftNode {
	node := &RaftNode{
		HeartbeatInterval:  1 * time.Second,
		ElectionTimeoutMin: 5 * time.Second,
		ElectionTimeoutMax: 3 * time.Second,
		RPCTimeout:         500 * time.Millisecond,
		NodeType:           constants.NODE_TYPE.FOLLOWER,
		Address:            addr,
		Log:                make([]dto.LogEntry, 0),
		Application:        application,
		ElectionTerm:       0,
		ClusterAddrList:    make([]dto.Address, 0),
		ClusterLeaderAddr:  dto.Address{},
	}

	if contactAddr == nil {
		node.ClusterAddrList = append(node.ClusterAddrList, node.Address)
		node.InitializeAsLeader()
	} else {
		node.TryToApplyMembership(*contactAddr)
		node.InitializeAsFollower()
	}

	// Start RPC server
	go node.startRPCServer()

	return node
}

func (node *RaftNode) startRPCServer() {
	rpc.Register(node)

	listener, err := net.Listen("tcp", net.JoinHostPort(node.Address.IP, node.Address.Port))
	if err != nil {
		node.printLog(fmt.Sprintf("Failed to start RPC server: %v", err))
		return
	}
	defer listener.Close()

	node.printLog(fmt.Sprintf("RPC server started on %s", listener.Addr()))

	err = http.Serve(listener, nil)

	if err != nil {
		node.printLog(fmt.Sprintf("RPC server error: %v", err))
	}
}

func (node *RaftNode) printLog(text string) {
	fmt.Printf("[%s] [%s] %s\n", node.Address, time.Now().Format("15:04:05"), text)
}

func (node *RaftNode) InitializeAsLeader() {
	node.printLog("Initialize as leader node...")
	node.ClusterLeaderAddr = node.Address
	node.NodeType = constants.NODE_TYPE.LEADER
	node.HeartbeatThread = time.NewTicker(node.HeartbeatInterval)
	go node.leaderHeartbeat()
}

func (node *RaftNode) leaderHeartbeat() {
	for range node.HeartbeatThread.C {
		node.printLog("[Leader] Sending heartbeat...")
		for _, followerAddr := range node.ClusterAddrList {
			if followerAddr != node.Address { // Do not send heartbeat to self
				args := dto.HeartbeatArgs{LeaderAddr: node.Address, Term: node.ElectionTerm}
				var reply dto.HeartbeatReply
				node.sendHeartbeat(args, followerAddr, &reply)
				node.printLog(fmt.Sprintf("Heartbeat sent to %v, reply: %v", followerAddr.Port, reply.Success))
			}
		}
	}
}

func (node *RaftNode) InitializeAsFollower() {
	node.NodeType = constants.NODE_TYPE.FOLLOWER
	node.HeartbeatTimeout = time.NewTicker(node.ElectionTimeoutMin)
	go node.checkHeartbeatTimeout()
}

func (node *RaftNode) checkHeartbeatTimeout() {
	for range node.HeartbeatTimeout.C {
		if time.Since(node.LastHeartbeat) > node.ElectionTimeoutMin {
			node.printLog("Heartbeat timeout, starting election...")
			// Trigger election process
			// TODO: update this
			node.InitializeAsFollower()
		}
	}
}

func (node *RaftNode) TryToApplyMembership(contactAddr dto.Address) {
	redirectedAddr := contactAddr
	for {
		args := dto.ApplyMembershipArgs{ContactAddr: node.Address}
		node.printLog(fmt.Sprintf("Applying membership to %v ...", args.ContactAddr))
		response := node.sendMembershipRequest(args, "RaftNode.ApplyMembership", redirectedAddr)
		if response.Success {
			node.Log = response.Log
			node.ClusterAddrList = response.ClusterAddrList
			node.ClusterLeaderAddr = response.Address
			break
		}
	}
}

func (node *RaftNode) ApplyMembership(args dto.ApplyMembershipArgs, reply *dto.ApplyMembershipReply) error {
	node.printLog(fmt.Sprintf("Membership request from IP: %v, Port: %v", args.ContactAddr.IP, args.ContactAddr.Port))

	node.ClusterAddrList = append(node.ClusterAddrList, args.ContactAddr)
	reply.Success = true
	reply.Log = []dto.LogEntry{}
	reply.ClusterAddrList = []dto.Address{}
	reply.Address = node.Address

	return nil
}

func (node *RaftNode) sendMembershipRequest(request interface{}, rpcName string, addr dto.Address) dto.ApplyMembershipReply {
	node.printLog(fmt.Sprintf("Sending request to addr: %v", addr.Port))
	client, err := rpc.DialHTTP("tcp", net.JoinHostPort(addr.IP, addr.Port))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var response dto.ApplyMembershipReply
	err = client.Call(rpcName, request, &response)
	if err != nil {
		panic(err)
	}

	// node.printLog(fmt.Sprintf("%v", response))
	return dto.ApplyMembershipReply{
		Success:         response.Success,
		Address:         response.Address,
		Log:             response.Log,
		ClusterAddrList: response.ClusterAddrList,
	}
}

func (node *RaftNode) sendRequest(request interface{}, rpcName string, addr dto.Address) map[string]interface{} {
	node.printLog(fmt.Sprintf("Sending request to addr: %v", addr.Port))
	client, err := rpc.DialHTTP("tcp", net.JoinHostPort(addr.IP, addr.Port))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var response dto.ApplyMembershipReply
	err = client.Call(rpcName, request, &response)
	if err != nil {
		panic(err)
	}

	// node.printLog(fmt.Sprintf("%v", response))
	return map[string]interface{}{
		"success":           response.Success,
		"address":           response.Address,
		"log":               response.Log,
		"cluster_addr_list": response.ClusterAddrList,
	}
}

func (node *RaftNode) sendHeartbeat(args dto.HeartbeatArgs, addr dto.Address, reply *dto.HeartbeatReply) {
	client, err := rpc.DialHTTP("tcp", net.JoinHostPort(addr.IP, addr.Port))
	if err != nil {
		node.printLog(fmt.Sprintf("Failed to dial follower %v: %v", addr.Port, err))
		return
	}
	defer client.Close()

	node.printLog(fmt.Sprintf("Sending heartbeat to %v", addr))
	err = client.Call("RaftNode.Heartbeat", args, reply)
	if err != nil {
		node.printLog(fmt.Sprintf("Failed to send heartbeat to %v: %v", addr, err))
		return
	}

	node.printLog(fmt.Sprintf("Heartbeat response from %v: %v", addr, reply))
}

func (node *RaftNode) Heartbeat(args dto.HeartbeatArgs, reply *dto.HeartbeatReply) error {
	node.printLog(fmt.Sprintf("Received heartbeat from %v", args.LeaderAddr))

	if args.Term >= node.ElectionTerm {
		node.ElectionTerm = args.Term
		node.ClusterLeaderAddr = args.LeaderAddr
		node.LastHeartbeat = time.Now()
		reply.Success = true
	} else {
		reply.Success = false
	}
	reply.Term = node.ElectionTerm
	return nil
}

func (node *RaftNode) Execute(jsonRequest string) string {
	var request map[string]interface{}
	json.Unmarshal([]byte(jsonRequest), &request)
	res, _ := json.Marshal(request)
	return string(res)
}
