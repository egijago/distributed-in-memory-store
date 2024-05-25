package dto

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
	Log             []string
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
