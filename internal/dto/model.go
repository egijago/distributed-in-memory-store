package dto

import (
	keyvaluestore "if3230-tubes-williamjeffersonclinton/pkg/proto/auto-generated"
)

type LogEntry = keyvaluestore.LogEntry


type Address struct {
	IP   string
	Port string
}

type ApplyMembershipArgs struct {
	ContactAddr Address
}

type ApplyMembershipReply struct {
	Success         bool
	Address         Address
	Log             []LogEntry
	ClusterAddrList []Address
}

type HeartbeatArgs struct {
	LeaderAddr Address
	Term       int
}

type HeartbeatReply struct {
	Success bool
	Term    int
}

type AppendEntriesRequest = keyvaluestore.AppendEntriesRequest


type AppendEntriesResponse = keyvaluestore.AppendEntriesResponse
